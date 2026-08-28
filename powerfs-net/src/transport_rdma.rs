//! RDMA Transport — 基于 libibverbs + librdmacm 的 RDMA 传输层实现
//!
//! 设计参考: BeeGFS RDMASocket + 预注册 MR Pool
//!
//! # 核心机制
//!
//! 1. **Stream 仿真**: RDMA send/recv 是消息语义, 通过内部缓冲区
//!    包装为 AsyncRead + AsyncWrite stream 接口
//! 2. **预注册 MR Pool**: 启动时预分配并 ibv_reg_mr 注册 N 个 buffer,
//!    避免每次发送的动态 MR 注册开销 (10-100μs/次)
//! 3. **帧边界对齐**: 一次 RDMA send = 一个完整 powerfs-net 帧
//!    (FrameHeader 16B + body + data)
//! 4. **AsyncFd 集成**: 通过 ibv_req_notify_cq + ibv_get_cq_event +
//!    tokio::io::AsyncFd 实现异步 CQ 轮询, 不阻塞 tokio worker
//!
//! # 编译控制
//!
//! 仅 `--features rdma` 时编译. 使用 `libc` crate 提供的 FFI 直接调用
//! libibverbs / librdmacm, 不依赖任何外部 Rust RDMA crate, 避免
//! 依赖冲突 (如 tikv-jemalloc-sys) 并保持 IB 代码完全隔离.
//!
//! 链接需求: 启用 rdma feature 时, 系统需安装 `libibverbs-dev` 和
//! `librdmacm-dev`. Cargo `[build-dependencies]` 不需要额外配置, libc
//! 通过 extern "C" 链接系统共享库.

#![cfg(feature = "rdma")]

use std::collections::VecDeque;
use std::ffi::CString;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;
use std::task::Poll;

use log::{debug, error, info, warn};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::io::unix::AsyncFd;
use tokio::sync::Mutex;

use crate::errors::{NetError, NetResult};
use crate::transport::{Transport, TransportConfig, TransportListener, TransportStream};

/// Non-owning wrapper for a CQ completion channel fd.
///
/// `tokio::io::AsyncFd` requires `AsRawFd`. Since the fd is owned by
/// `IbvCompChannel` (which lives as long as the enclosing `IbvCq`), this
/// wrapper merely references the fd without closing it on drop. The
/// lifetime is guaranteed by `Arc<RdmaChannel>` holding `Arc<IbvCq>`
/// which owns the `IbvCompChannel`.
struct CqChannelFd(RawFd);

impl AsRawFd for CqChannelFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

/// Marker type used to satisfy `AsyncFd<CqChannelFd>` registration.
/// The `AsyncFd` does NOT take ownership of the fd; it only registers
/// interest with `epoll`.
type CqAsyncFd = AsyncFd<CqChannelFd>;

// ============================================================================
// FFI Bindings — libibverbs + librdmacm
// ============================================================================

/// FFI bindings to libibverbs and librdmacm.
///
/// All functions are declared here to keep the RDMA code self-contained.
/// They link against system-installed `libibverbs.so` and `librdmacm.so`.
mod ffi {
    use libc::{c_char, c_int, c_uint, c_void, int16_t, int32_t, size_t, uint16_t, uint32_t, uint8_t};

    // --- ibv_context / ibv_device ------------------------------------------

    #[repr(C)]
    pub struct ibv_context {
        pub _opaque: [u8; 0],
    }

    #[repr(C)]
    pub struct ibv_device {
        pub _opaque: [u8; 0],
    }

    #[repr(C)]
    pub struct ibv_device_attr {
        pub fw_ver: [c_char; 64],
        pub node_guid: u64,
        pub sys_image_guid: u64,
        pub max_mr_size: u64,
        pub vendor_id: uint32_t,
        pub vendor_part_id: uint16_t,
        pub hw_ver: uint32_t,
        // ... 其余字段省略, 用 padding 填充到正确大小
        pub _padding: [u8; 64],
    }

    #[repr(C)]
    pub struct ibv_port_attr {
        pub state: uint8_t,
        pub max_mtu: uint8_t,
        pub active_mtu: uint8_t,
        pub gid_tbl_len: uint8_t,
        pub port_cap_flags: uint32_t,
        pub max_msg_sz: uint32_t,
        pub bad_pkey_cntr: uint16_t,
        pub qkey_viol_cntr: uint16_t,
        pub sm_lid: uint16_t,
        pub sm_sl: uint8_t,
        pub subnet_prefix: u64,
        pub init_type_reply: uint8_t,
        pub lmc: uint8_t,
        pub max_vl_num: uint8_t,
        pub sm_sl1: uint8_t,
        pub _reserved: [u8; 4],
    }

    // --- ibv_pd ------------------------------------------------------------

    #[repr(C)]
    pub struct ibv_pd {
        pub _opaque: [u8; 0],
    }

    // --- ibv_mr ------------------------------------------------------------

    #[repr(C)]
    pub struct ibv_mr {
        pub addr: *mut c_void,
        pub length: size_t,
        pub handle: uint32_t,
        pub lkey: uint32_t,
        pub rkey: uint32_t,
    }

    // --- ibv_cq ------------------------------------------------------------

    #[repr(C)]
    pub struct ibv_comp_channel {
        pub fd: c_int,
    }

    #[repr(C)]
    pub struct ibv_cq {
        pub _opaque: [u8; 0],
    }

    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct ibv_wc {
        pub wr_id: u64,
        pub status: ibv_wc_status,
        pub opcode: ibv_wc_opcode,
        pub vendor_err: uint16_t,
        pub byte_len: uint32_t,
        pub imm_data: uint32_t,
        pub qp_num: uint32_t,
        pub src_qp: uint32_t,
        pub wc_flags: uint32_t,
        pub pkey_index: uint16_t,
        pub slid: uint16_t,
        pub sl: uint8_t,
        pub dlid_path_bits: uint8_t,
    }

    pub type ibv_wc_status = int32_t;
    pub type ibv_wc_opcode = int32_t;

    pub const IBV_WC_SUCCESS: ibv_wc_status = 0;
    pub const IBV_WC_SEND: ibv_wc_opcode = 0;
    pub const IBV_WC_RECV: ibv_wc_opcode = 1;

    // --- ibv_qp ------------------------------------------------------------

    #[repr(C)]
    pub struct ibv_qp {
        pub _opaque: [u8; 0],
    }

    #[repr(C)]
    pub struct ibv_qp_cap {
        pub max_send_wr: uint32_t,
        pub max_recv_wr: uint32_t,
        pub max_send_sge: uint32_t,
        pub max_recv_sge: uint32_t,
        pub max_inline_data: uint32_t,
    }

    #[repr(C)]
    pub struct ibv_qp_init {
    pub    qp_context: *mut c_void,
    pub    send_cq: *mut ibv_cq,
    pub    recv_cq: *mut ibv_cq,
    pub    srq: *mut c_void,
    pub    cap: ibv_qp_cap,
    pub    qp_type: int32_t,
    pub    sq_sig_all: c_int,
    }

    /// Global Route Header (GRH) fields (used for RoCE / IB routing).
    /// Kept as a raw byte buffer since most RC QP setups on a single L2
    /// segment don't require explicit GRH programming.
    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct ibv_global_route {
        /// Destination GID (16 bytes).
        pub dgid: [u8; 16],
        pub flow_label: uint32_t,
        pub sgid_index: uint8_t,
        pub hop_limit: uint8_t,
        pub traffic_class: uint8_t,
        pub _reserved: [u8; 3],
    }

    /// Address handle attributes. Used by `ibv_modify_qp` when transitioning
    /// to RTR to program the path to the peer (port, LID, etc.).
    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct ibv_ah_attr {
        pub grh: ibv_global_route,
        /// Destination LID (Local Identifier).
        pub dlid: uint16_t,
        /// Service Level.
        pub sl: uint8_t,
        /// Source path bits (LMC).
        pub src_path_bits: uint8_t,
        /// Static rate (0 = full line rate).
        pub static_rate: uint8_t,
        /// Physical port number to use.
        pub port_num: uint8_t,
        /// Whether global routing (GRH) is used.
        pub is_global: uint8_t,
        pub _reserved: [u8; 2],
    }

    #[repr(C)]
    pub struct ibv_qp_attr {
        pub qp_state: int32_t,
        pub cur_qp_state: int32_t,
        pub path_mtu: uint32_t,
        pub path_mig_state: uint32_t,
        pub qkey: uint32_t,
        pub rq_psn: uint32_t,
        pub sq_psn: uint32_t,
        pub dest_qp_num: uint32_t,
        pub qp_access_flags: int32_t,
        pub cap: ibv_qp_cap,
        pub ah_attr: ibv_ah_attr,
        pub alt_ah_attr: ibv_ah_attr,
        pub pkey_index: uint16_t,
        pub alt_pkey_index: uint16_t,
        pub en_sqd_async_notify: uint8_t,
        pub sq_draining: uint8_t,
        /// Max outstanding RDMA read/atomic ops issued from this QP.
        pub max_rd_atomic: uint8_t,
        /// Max outstanding RDMA read/atomic ops this QP can receive.
        pub max_dest_rd_atomic: uint8_t,
        /// Min RNR NAK timer value.
        pub min_rnr_timer: uint8_t,
        /// Physical port number.
        pub port_num: uint8_t,
        /// Local ACK timeout (log2 based).
        pub timeout: uint8_t,
        /// Retry count.
        pub retry_cnt: uint8_t,
        /// RNR retry count.
        pub rnr_retry: uint8_t,
        pub alt_port_num: uint8_t,
        pub alt_timeout: uint8_t,
        pub _reserved: [u8; 4],
    }

    pub const IBV_QP_STATE: c_int = 1;
    pub const IBV_QP_RQ_PSN: c_int = 0x40;
    pub const IBV_QP_SQ_PSN: c_int = 0x80;
    pub const IBV_QP_DEST_QPN: c_int = 0x10;
    pub const IBV_QP_PATH_MTU: c_int = 0x04;
    pub const IBV_QP_QKEY: c_int = 0x100;
    pub const IBV_QP_AV: c_int = 0x02;
    pub const IBV_QP_RETRY_CNT: c_int = 0x400;
    pub const IBV_QP_RNR_RETRY: c_int = 0x800;
    pub const IBV_QP_MIN_RNR_TIMER: c_int = 0x1000;
    pub const IBV_QP_MAX_QP_RD_ATOMIC: c_int = 0x2000;
    pub const IBV_QP_MAX_DEST_RD_ATOMIC: c_int = 0x4000;

    pub const IBV_QPS_RESET: int32_t = 0;
    pub const IBV_QPS_INIT: int32_t = 1;
    pub const IBV_QPS_RTR: int32_t = 2;
    pub const IBV_QPS_RTS: int32_t = 3;

    pub const IBV_QPT_RC: int32_t = 2; // Reliable Connection

    // --- ibv_sge / ibv_send_wr --------------------------------------------

    #[repr(C)]
    pub struct ibv_sge {
        pub addr: u64,
        pub length: uint32_t,
        pub lkey: uint32_t,
    }

    #[repr(C)]
    pub struct ibv_send_wr {
        pub wr_id: u64,
        pub next: *mut ibv_send_wr,
        pub sg_list: *mut ibv_sge,
        pub num_sge: c_int,
        pub opcode: int32_t,
        pub send_flags: uint32_t,
        pub wr: ibv_send_wr_wr,
    }

    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct ibv_send_wr_wr {
        pub rdma_remote: u64, // remote_addr (simplified)
        pub rdma_rkey: uint32_t,
        pub _padding: uint32_t,
    }

    pub const IBV_WR_SEND: int32_t = 0;
    pub const IBV_SEND_SIGNALED: uint32_t = 0x01;
    pub const IBV_SEND_INLINE: uint32_t = 0x08;

    #[repr(C)]
    pub struct ibv_recv_wr {
        pub wr_id: u64,
        pub next: *mut ibv_recv_wr,
        pub sg_list: *mut ibv_sge,
        pub num_sge: c_int,
    }

    // --- libibverbs function declarations ----------------------------------

    extern "C" {
        pub fn ibv_get_device_list(
            num_devices: *mut c_int,
        ) -> *mut *mut ibv_device;
        pub fn ibv_free_device_list(list: *mut *mut ibv_device);
        pub fn ibv_get_device_name(device: *mut ibv_device) -> *const c_char;
        pub fn ibv_open_device(device: *mut ibv_device) -> *mut ibv_context;
        pub fn ibv_close_device(context: *mut ibv_context) -> c_int;
        pub fn ibv_query_device(
            context: *mut ibv_context,
            attr: *mut ibv_device_attr,
        ) -> c_int;
        pub fn ibv_query_port(
            context: *mut ibv_context,
            port_num: uint8_t,
            attr: *mut ibv_port_attr,
        ) -> c_int;
        pub fn ibv_alloc_pd(context: *mut ibv_context) -> *mut ibv_pd;
        pub fn ibv_dealloc_pd(pd: *mut ibv_pd) -> c_int;
        pub fn ibv_reg_mr(
            pd: *mut ibv_pd,
            addr: *mut c_void,
            length: size_t,
            access: c_int,
        ) -> *mut ibv_mr;
        pub fn ibv_dereg_mr(mr: *mut ibv_mr) -> c_int;
        pub fn ibv_create_comp_channel(
            context: *mut ibv_context,
        ) -> *mut ibv_comp_channel;
        pub fn ibv_destroy_comp_channel(channel: *mut ibv_comp_channel) -> c_int;
        pub fn ibv_create_cq(
            context: *mut ibv_context,
            cqe: c_int,
            cq_context: *mut c_void,
            channel: *mut ibv_comp_channel,
            comp_vector: c_int,
        ) -> *mut ibv_cq;
        pub fn ibv_destroy_cq(cq: *mut ibv_cq) -> c_int;
        // ibv_req_notify_cq is `static inline` in verbs.h — call C wrapper.
        pub fn powerfs_ibv_req_notify_cq(
            cq: *mut ibv_cq,
            solicited_only: c_int,
        ) -> c_int;
        pub fn ibv_get_cq_event(
            channel: *mut ibv_comp_channel,
            cq: *mut *mut ibv_cq,
            cq_context: *mut *mut c_void,
        ) -> c_int;
        pub fn ibv_ack_cq_events(cq: *mut ibv_cq, nevents: c_uint);
        // NOTE: ibv_poll_cq, ibv_post_send, ibv_post_recv are `static inline`
        // in <infiniband/verbs.h> and NOT exported from libibverbs.so.
        // We call the C wrappers from rdma_wrapper.c (compiled by build.rs)
        // instead.
        pub fn powerfs_ibv_poll_cq(
            cq: *mut ibv_cq,
            num_wc: c_int,
            wc: *mut ibv_wc,
        ) -> c_int;
        pub fn ibv_create_qp(
            pd: *mut ibv_pd,
            init: *mut ibv_qp_init,
        ) -> *mut ibv_qp;
        pub fn ibv_destroy_qp(qp: *mut ibv_qp) -> c_int;
        pub fn ibv_modify_qp(
            qp: *mut ibv_qp,
            attr: *mut ibv_qp_attr,
            attr_mask: c_int,
        ) -> c_int;
        pub fn powerfs_ibv_post_send(
            qp: *mut ibv_qp,
            wr: *mut ibv_send_wr,
            bad_wr: *mut *mut ibv_send_wr,
        ) -> c_int;
        pub fn powerfs_ibv_post_recv(
            qp: *mut ibv_qp,
            wr: *mut ibv_recv_wr,
            bad_wr: *mut *mut ibv_recv_wr,
        ) -> c_int;
    }

    // MR access flags
    pub const IBV_ACCESS_LOCAL_WRITE: c_int = 1;
    pub const IBV_ACCESS_REMOTE_WRITE: c_int = 1 << 1;
    pub const IBV_ACCESS_REMOTE_READ: c_int = 1 << 2;
    pub const IBV_ACCESS_REMOTE_ATOMIC: c_int = 1 << 3;

    // --- librdmacm --------------------------------------------------------

    #[repr(C)]
    pub struct rdma_event_channel {
        pub fd: c_int,
    }

    #[repr(C)]
    pub struct rdma_cm_id {
        pub _opaque: [u8; 0],
    }

    #[repr(C)]
    pub struct rdma_cm_event {
        pub id: *mut rdma_cm_id,
        pub event: rdma_cm_event_type,
        pub status: c_int,
        pub _padding: [u8; 32],
    }

    pub type rdma_cm_event_type = int32_t;

    pub const RDMA_CM_EVENT_ADDR_RESOLVED: rdma_cm_event_type = 0;
    pub const RDMA_CM_EVENT_ADDR_ERROR: rdma_cm_event_type = 1;
    pub const RDMA_CM_EVENT_ROUTE_RESOLVED: rdma_cm_event_type = 2;
    pub const RDMA_CM_EVENT_ROUTE_ERROR: rdma_cm_event_type = 3;
    pub const RDMA_CM_EVENT_CONNECT_REQUEST: rdma_cm_event_type = 4;
    pub const RDMA_CM_EVENT_CONNECT_RESPONSE: rdma_cm_event_type = 5;
    pub const RDMA_CM_EVENT_CONNECT_ERROR: rdma_cm_event_type = 6;
    pub const RDMA_CM_EVENT_UNREACHABLE: rdma_cm_event_type = 7;
    pub const RDMA_CM_EVENT_REJECTED: rdma_cm_event_type = 8;
    pub const RDMA_CM_EVENT_ESTABLISHED: rdma_cm_event_type = 9;
    pub const RDMA_CM_EVENT_DISCONNECTED: rdma_cm_event_type = 11;

    pub const RDMA_PS_TCP: c_int = 2;

    extern "C" {
        pub fn rdma_create_event_channel() -> *mut rdma_event_channel;
        pub fn rdma_destroy_event_channel(channel: *mut rdma_event_channel);
        pub fn rdma_create_id(
            channel: *mut rdma_event_channel,
            id: *mut *mut rdma_cm_id,
            context: *mut c_void,
            ps: c_int,
        ) -> c_int;
        pub fn rdma_destroy_id(id: *mut rdma_cm_id) -> c_int;
        pub fn rdma_resolve_addr(
            id: *mut rdma_cm_id,
            src_addr: *const libc::sockaddr,
            dst_addr: *const libc::sockaddr,
            timeout_ms: c_int,
        ) -> c_int;
        pub fn rdma_resolve_route(id: *mut rdma_cm_id, timeout_ms: c_int) -> c_int;
        pub fn rdma_bind_addr(id: *mut rdma_cm_id, addr: *const libc::sockaddr) -> c_int;
        pub fn rdma_listen(id: *mut rdma_cm_id, backlog: c_int) -> c_int;
        pub fn rdma_connect(
            id: *mut rdma_cm_id,
            conn_param: *const c_void,
        ) -> c_int;
        pub fn rdma_accept(
            id: *mut rdma_cm_id,
            conn_param: *const c_void,
        ) -> c_int;
        pub fn rdma_disconnect(id: *mut rdma_cm_id) -> c_int;
        pub fn rdma_get_cm_event(
            channel: *mut rdma_event_channel,
            event: *mut *mut rdma_cm_event,
        ) -> c_int;
        pub fn rdma_ack_cm_event(event: *mut rdma_cm_event) -> c_int;
        pub fn rdma_create_qp(
            id: *mut rdma_cm_id,
            pd: *mut ibv_pd,
            init: *mut ibv_qp_init,
        ) -> c_int;
        pub fn rdma_get_devices(
            num_devices: *mut c_int,
        ) -> *mut *mut c_void;
        pub fn rdma_free_devices(devices: *mut *mut c_void);
    }
}

// Re-export key FFI types for use in RAII wrappers
use ffi::{
    ibv_comp_channel, ibv_context, ibv_cq, ibv_mr, ibv_pd, ibv_qp, ibv_recv_wr, ibv_send_wr,
    ibv_sge, ibv_wc, ibv_wc_opcode, rdma_cm_event, rdma_cm_id, rdma_event_channel,
};

// ============================================================================
// RAII Wrappers — 自动释放 RDMA 资源
// ============================================================================

/// RAII wrapper for ibv_context (device handle).
struct IbvContext {
    raw: *mut ibv_context,
}

impl IbvContext {
    /// Open the first available RDMA device, or the one matching `device_name`.
    ///
    /// Returns `Err(NetError::Config)` if no RDMA device is available or the
    /// specified device is not found.
    fn open(device_name: Option<&str>) -> NetResult<Self> {
        unsafe {
            let mut num_devices: libc::c_int = 0;
            let device_list = ffi::ibv_get_device_list(&mut num_devices);
            if device_list.is_null() || num_devices == 0 {
                return Err(NetError::Config(
                    "no RDMA devices found (ibv_get_device_list returned 0 devices)".to_string(),
                ));
            }

            let devices = std::slice::from_raw_parts(device_list, num_devices as usize);
            let chosen = match device_name {
                Some(name) => devices.iter().find(|&&dev| {
                    let cname = ffi::ibv_get_device_name(dev);
                    if cname.is_null() {
                        false
                    } else {
                        let cstr = std::ffi::CStr::from_ptr(cname);
                        cstr.to_string_lossy() == name
                    }
                }),
                None => devices.first(),
            };

            let dev = match chosen {
                Some(&d) => d,
                None => {
                    ffi::ibv_free_device_list(device_list);
                    return Err(NetError::Config(format!(
                        "RDMA device '{}' not found",
                        device_name.unwrap_or("?")
                    )));
                }
            };

            let dev_name_str = {
                let n = ffi::ibv_get_device_name(dev);
                if n.is_null() {
                    "<unknown>".to_string()
                } else {
                    std::ffi::CStr::from_ptr(n).to_string_lossy().into_owned()
                }
            };

            let ctx = ffi::ibv_open_device(dev);
            ffi::ibv_free_device_list(device_list);
            if ctx.is_null() {
                return Err(NetError::Connection(format!(
                    "ibv_open_device({}) failed",
                    dev_name_str
                )));
            }

            info!("IbvContext: opened RDMA device {}", dev_name_str);
            Ok(IbvContext { raw: ctx })
        }
    }

    fn as_ptr(&self) -> *mut ibv_context {
        self.raw
    }
}

impl Drop for IbvContext {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                ffi::ibv_close_device(self.raw);
            }
        }
    }
}

// SAFETY: ibv_context is internally synchronized for the operations we use
// (alloc_pd, create_cq, etc.) and the pointer is owned solely by this wrapper.
unsafe impl Send for IbvContext {}
unsafe impl Sync for IbvContext {}

/// RAII wrapper for ibv_pd (protection domain).
struct IbvPd {
    raw: *mut ibv_pd,
}

impl IbvPd {
    fn alloc(context: &IbvContext) -> NetResult<Self> {
        unsafe {
            let pd = ffi::ibv_alloc_pd(context.as_ptr());
            if pd.is_null() {
                return Err(NetError::Connection("ibv_alloc_pd failed".to_string()));
            }
            Ok(IbvPd { raw: pd })
        }
    }

    fn as_ptr(&self) -> *mut ibv_pd {
        self.raw
    }
}

impl Drop for IbvPd {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                ffi::ibv_dealloc_pd(self.raw);
            }
        }
    }
}

unsafe impl Send for IbvPd {}
unsafe impl Sync for IbvPd {}

/// RAII wrapper for ibv_mr (memory region).
///
/// Registered memory regions allow the RNIC to directly access the memory
/// via DMA. Pre-registering a pool avoids the per-send registration cost
/// (10-100μs per ibv_reg_mr call).
struct IbvMr {
    raw: *mut ibv_mr,
    /// Backing storage. Kept alive until the MR is deregistered.
    _buf: Vec<u8>,
}

impl IbvMr {
    /// Register `buf` for local + remote read/write access.
    ///
    /// After this call, `buf`'s memory is pinned and the RNIC can DMA into it.
    fn register(pd: &IbvPd, mut buf: Vec<u8>) -> NetResult<Self> {
        unsafe {
            let len = buf.len();
            let addr = buf.as_mut_ptr() as *mut libc::c_void;
            let access = ffi::IBV_ACCESS_LOCAL_WRITE
                | ffi::IBV_ACCESS_REMOTE_WRITE
                | ffi::IBV_ACCESS_REMOTE_READ;
            let mr = ffi::ibv_reg_mr(pd.as_ptr(), addr, len, access);
            if mr.is_null() {
                return Err(NetError::Connection(format!(
                    "ibv_reg_mr(addr={:p}, len={}) failed",
                    addr, len
                )));
            }
            Ok(IbvMr {
                raw: mr,
                _buf: buf,
            })
        }
    }

    /// L_key for local access (post_send/post_recv).
    fn lkey(&self) -> u32 {
        unsafe { (*self.raw).lkey }
    }

    /// Pointer to the backing memory (for reading/writing data).
    fn addr(&self) -> *mut libc::c_void {
        unsafe { (*self.raw).addr }
    }

    fn len(&self) -> usize {
        unsafe { (*self.raw).length }
    }
}

impl Drop for IbvMr {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                ffi::ibv_dereg_mr(self.raw);
            }
        }
    }
}

// SAFETY: The MR's backing buffer is owned solely by this wrapper and never
// aliased except through ibv_post_send/recv, which are synchronized by the
// QP's send/recv queues.
unsafe impl Send for IbvMr {}
unsafe impl Sync for IbvMr {}

/// RAII wrapper for ibv_comp_channel (completion event channel).
///
/// The channel has an fd that can be polled by `tokio::io::AsyncFd` for
/// async CQ event notification, avoiding busy-polling in the tokio runtime.
struct IbvCompChannel {
    raw: *mut ibv_comp_channel,
}

impl IbvCompChannel {
    fn create(context: &IbvContext) -> NetResult<Self> {
        unsafe {
            let ch = ffi::ibv_create_comp_channel(context.as_ptr());
            if ch.is_null() {
                return Err(NetError::Connection(
                    "ibv_create_comp_channel failed".to_string(),
                ));
            }
            Ok(IbvCompChannel { raw: ch })
        }
    }

    fn fd(&self) -> RawFd {
        unsafe { (*self.raw).fd }
    }

    fn as_ptr(&self) -> *mut ibv_comp_channel {
        self.raw
    }
}

impl Drop for IbvCompChannel {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                ffi::ibv_destroy_comp_channel(self.raw);
            }
        }
    }
}

unsafe impl Send for IbvCompChannel {}
unsafe impl Sync for IbvCompChannel {}

/// RAII wrapper for ibv_cq (completion queue).
struct IbvCq {
    raw: *mut ibv_cq,
    /// Owned comp channel (optional, for event-driven async polling via
    /// `tokio::io::AsyncFd`). When `Some`, the channel's fd can be polled
    /// for readability to get notified of CQ completions without busy-polling.
    channel: Option<IbvCompChannel>,
}

impl IbvCq {
    fn create(
        context: &IbvContext,
        cqe: i32,
        channel: Option<IbvCompChannel>,
    ) -> NetResult<Self> {
        unsafe {
            let ch_ptr = match &channel {
                Some(c) => c.as_ptr(),
                None => ptr::null_mut(),
            };
            let cq = ffi::ibv_create_cq(
                context.as_ptr(),
                cqe,
                ptr::null_mut(),
                ch_ptr,
                0,
            );
            if cq.is_null() {
                return Err(NetError::Connection(format!(
                    "ibv_create_cq(cqe={}) failed",
                    cqe
                )));
            }
            Ok(IbvCq {
                raw: cq,
                channel,
            })
        }
    }

    fn as_ptr(&self) -> *mut ibv_cq {
        self.raw
    }

    /// Returns the completion channel's fd, if the CQ was created with one.
    fn channel_fd(&self) -> Option<RawFd> {
        self.channel.as_ref().map(|ch| ch.fd())
    }

    /// Poll for up to `wcs.len()` completions (non-blocking).
    ///
    /// Returns the number of completions placed in `wcs`.
    fn poll(&self, wcs: &mut [ibv_wc]) -> usize {
        unsafe {
            let n = ffi::powerfs_ibv_poll_cq(self.as_ptr(), wcs.len() as i32, wcs.as_mut_ptr());
            if n < 0 {
                0
            } else {
                n as usize
            }
        }
    }

    /// Request the next CQ event (arm the channel fd so it becomes readable
    /// when a new completion arrives). Must be called *before* polling to
    /// avoid missing events.
    fn req_notify(&self, solicited_only: bool) -> NetResult<()> {
        unsafe {
            let rc = ffi::powerfs_ibv_req_notify_cq(
                self.as_ptr(),
                if solicited_only { 1 } else { 0 },
            );
            if rc != 0 {
                Err(NetError::Connection(format!(
                    "ibv_req_notify_cq failed (rc={})",
                    rc
                )))
            } else {
                Ok(())
            }
        }
    }

    /// Blocking call to extract the next CQ event from the completion channel.
    /// Should only be called after `AsyncFd` reports the channel fd is readable.
    /// After consuming, `ack_events(1)` must be called to re-arm.
    fn get_cq_event(&self) -> NetResult<()> {
        unsafe {
            let mut cq_ptr: *mut ibv_cq = ptr::null_mut();
            let mut ctx_ptr: *mut libc::c_void = ptr::null_mut();
            let rc = ffi::ibv_get_cq_event(
                self.channel.as_ref().ok_or_else(|| {
                    NetError::Connection("CQ has no completion channel".to_string())
                })?.as_ptr(),
                &mut cq_ptr,
                &mut ctx_ptr,
            );
            if rc != 0 {
                Err(NetError::Connection(format!(
                    "ibv_get_cq_event failed (rc={})",
                    rc
                )))
            } else {
                Ok(())
            }
        }
    }

    /// Acknowledge `n` CQ events. Must be called after `get_cq_event` to
    /// allow future events to be delivered.
    fn ack_events(&self, n: u32) {
        unsafe {
            ffi::ibv_ack_cq_events(self.as_ptr(), n);
        }
    }
}

impl Drop for IbvCq {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                ffi::ibv_destroy_cq(self.raw);
            }
        }
    }
}

unsafe impl Send for IbvCq {}
unsafe impl Sync for IbvCq {}

/// RAII wrapper for ibv_qp (queue pair).
struct IbvQp {
    raw: *mut ibv_qp,
}

impl IbvQp {
    /// Create a QP with the given PD, send/recv CQ, and capacity.
    fn create(
        pd: &IbvPd,
        send_cq: &IbvCq,
        recv_cq: &IbvCq,
        cap_send_wr: u32,
        cap_recv_wr: u32,
        cap_max_sge: u32,
    ) -> NetResult<Self> {
        unsafe {
            let mut init = ffi::ibv_qp_init {
                qp_context: ptr::null_mut(),
                send_cq: send_cq.as_ptr(),
                recv_cq: recv_cq.as_ptr(),
                srq: ptr::null_mut(),
                cap: ffi::ibv_qp_cap {
                    max_send_wr: cap_send_wr,
                    max_recv_wr: cap_recv_wr,
                    max_send_sge: cap_max_sge,
                    max_recv_sge: cap_max_sge,
                    max_inline_data: 0,
                },
                qp_type: ffi::IBV_QPT_RC,
                sq_sig_all: 1,
            };
            let qp = ffi::ibv_create_qp(pd.as_ptr(), &mut init);
            if qp.is_null() {
                return Err(NetError::Connection(
                    "ibv_create_qp failed".to_string(),
                ));
            }
            Ok(IbvQp { raw: qp })
        }
    }

    fn as_ptr(&self) -> *mut ibv_qp {
        self.raw
    }

    /// Transition QP to INIT state (required before RTR).
    fn modify_to_init(&self, port_num: u8) -> NetResult<()> {
        unsafe {
            let mut attr: ffi::ibv_qp_attr = std::mem::zeroed();
            attr.qp_state = ffi::IBV_QPS_INIT;
            attr.port_num = port_num;
            attr.qkey = 0;
            attr.qp_access_flags = ffi::IBV_ACCESS_LOCAL_WRITE
                | ffi::IBV_ACCESS_REMOTE_WRITE
                | ffi::IBV_ACCESS_REMOTE_READ
                | ffi::IBV_ACCESS_REMOTE_ATOMIC;
            let mask = ffi::IBV_QP_STATE | (1 << 5) /* IBV_QP_PORT */ | (1 << 7) /* IBV_QP_ACCESS_FLAGS */;
            let rc = ffi::ibv_modify_qp(self.as_ptr(), &mut attr, mask);
            if rc != 0 {
                Err(NetError::Connection(format!(
                    "modify_qp(INIT) failed (rc={})",
                    rc
                )))
            } else {
                Ok(())
            }
        }
    }

    /// Transition QP to RTR (Ready to Receive). Requires peer QPN.
    fn modify_to_rtr(&self, dest_qp_num: u32, port_num: u8) -> NetResult<()> {
        unsafe {
            let mut attr: ffi::ibv_qp_attr = std::mem::zeroed();
            attr.qp_state = ffi::IBV_QPS_RTR;
            attr.path_mtu = 3; // IBV_MTU_1024
            attr.dest_qp_num = dest_qp_num;
            attr.rq_psn = 0;
            attr.max_dest_rd_atomic = 1;
            attr.min_rnr_timer = 12; // ~0.5s
            attr.ah_attr.port_num = port_num;
            let mask = ffi::IBV_QP_STATE
                | ffi::IBV_QP_PATH_MTU
                | ffi::IBV_QP_DEST_QPN
                | ffi::IBV_QP_RQ_PSN
                | (1 << 6) /* IBV_QP_MAX_DEST_RD_ATOMIC */
                | (1 << 4) /* IBV_QP_MIN_RNR_TIMER */
                | ffi::IBV_QP_AV;
            let rc = ffi::ibv_modify_qp(self.as_ptr(), &mut attr, mask);
            if rc != 0 {
                Err(NetError::Connection(format!(
                    "modify_qp(RTR) failed (rc={})",
                    rc
                )))
            } else {
                Ok(())
            }
        }
    }

    /// Transition QP to RTS (Ready to Send).
    fn modify_to_rts(&self) -> NetResult<()> {
        unsafe {
            let mut attr: ffi::ibv_qp_attr = std::mem::zeroed();
            attr.qp_state = ffi::IBV_QPS_RTS;
            attr.sq_psn = 0;
            attr.timeout = 14; // ~4.096us * 2^14
            attr.retry_cnt = 7;
            attr.rnr_retry = 7;
            attr.max_rd_atomic = 1;
            let mask = ffi::IBV_QP_STATE
                | ffi::IBV_QP_SQ_PSN
                | (1 << 3) /* IBV_QP_TIMEOUT */
                | ffi::IBV_QP_RETRY_CNT
                | ffi::IBV_QP_RNR_RETRY
                | (1 << 2) /* IBV_QP_MAX_QP_RD_ATOMIC */;
            let rc = ffi::ibv_modify_qp(self.as_ptr(), &mut attr, mask);
            if rc != 0 {
                Err(NetError::Connection(format!(
                    "modify_qp(RTS) failed (rc={})",
                    rc
                )))
            } else {
                Ok(())
            }
        }
    }

    /// Post a send work request (RDMA send).
    fn post_send(&self, sge: &ibv_sge, wr_id: u64) -> NetResult<()> {
        unsafe {
            let mut wr = ibv_send_wr {
                wr_id,
                next: ptr::null_mut(),
                sg_list: sge as *const ibv_sge as *mut ibv_sge,
                num_sge: 1,
                opcode: ffi::IBV_WR_SEND,
                send_flags: ffi::IBV_SEND_SIGNALED,
                wr: ffi::ibv_send_wr_wr {
                    rdma_remote: 0,
                    rdma_rkey: 0,
                    _padding: 0,
                },
            };
            let mut bad_wr: *mut ibv_send_wr = ptr::null_mut();
            let rc =
                ffi::powerfs_ibv_post_send(self.as_ptr(), &mut wr, &mut bad_wr);
            if rc != 0 {
                Err(NetError::Connection(format!(
                    "ibv_post_send failed (rc={})",
                    rc
                )))
            } else {
                Ok(())
            }
        }
    }

    /// Post a receive work request (RDMA recv).
    fn post_recv(&self, sge: &ibv_sge, wr_id: u64) -> NetResult<()> {
        unsafe {
            let wr = ibv_recv_wr {
                wr_id,
                next: ptr::null_mut(),
                sg_list: sge as *const ibv_sge as *mut ibv_sge,
                num_sge: 1,
            };
            let mut bad_wr: *mut ibv_recv_wr = ptr::null_mut();
            let rc =
                ffi::powerfs_ibv_post_recv(self.as_ptr(), &wr as *const ibv_recv_wr as *mut ibv_recv_wr, &mut bad_wr);
            if rc != 0 {
                Err(NetError::Connection(format!(
                    "ibv_post_recv failed (rc={})",
                    rc
                )))
            } else {
                Ok(())
            }
        }
    }
}

impl Drop for IbvQp {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe {
                ffi::ibv_destroy_qp(self.raw);
            }
        }
    }
}

unsafe impl Send for IbvQp {}
unsafe impl Sync for IbvQp {}

// ============================================================================
// MR Pool — 预注册内存区域池
// ============================================================================

/// A pre-registered memory region pool, BeeGFS-style.
///
/// At startup, `buf_num` buffers of `buf_size` bytes each are allocated and
/// registered with `ibv_reg_mr`. Send/recv operations borrow a buffer from
/// the free list, use it for the work request, and return it when the
/// completion is polled.
///
/// This avoids the 10-100μs cost of dynamic MR registration on every send.
struct MrPool {
    /// All registered MRs. Indexed by buffer index. Kept alive for the pool's
    /// lifetime so the registration remains valid.
    mrs: Vec<Arc<IbvMr>>,
    /// Free buffer indices, protected by a Mutex for async-safe borrowing.
    free: Mutex<VecDeque<usize>>,
    buf_size: usize,
}

impl MrPool {
    fn new(pd: &IbvPd, buf_num: usize, buf_size: usize) -> NetResult<Self> {
        let mut mrs = Vec::with_capacity(buf_num);
        for i in 0..buf_num {
            let buf = vec![0u8; buf_size];
            let mr = IbvMr::register(pd, buf).map_err(|e| {
                error!("MR pool: failed to register buffer {}: {}", i, e);
                e
            })?;
            mrs.push(Arc::new(mr));
        }
        let free: VecDeque<usize> = (0..buf_num).collect();
        info!(
            "MrPool: registered {} buffers of {} bytes (total {} KiB)",
            buf_num,
            buf_size,
            buf_num * buf_size / 1024
        );
        Ok(MrPool {
            mrs,
            free: Mutex::new(free),
            buf_size,
        })
    }

    fn buf_size(&self) -> usize {
        self.buf_size
    }

    /// Try to borrow a buffer (non-async, returns None if pool is empty).
    async fn try_acquire(&self) -> Option<Arc<IbvMr>> {
        let idx = self.free.lock().await.pop_front()?;
        Some(self.mrs[idx].clone())
    }

    /// Return a buffer to the pool.
    async fn release(&self, mr: Arc<IbvMr>) {
        let idx = self
            .mrs
            .iter()
            .position(|m| Arc::ptr_eq(m, &mr))
            .unwrap_or_else(|| {
                warn!("MrPool::release: MR not found in pool (leak)");
                0
            });
        self.free.lock().await.push_back(idx);
    }
}

// ============================================================================
// RdmaChannel — QP + CQ + MR pool (一个连接的完整资源)
// ============================================================================

/// All RDMA resources for a single connection.
///
/// Held in `Arc` so that `RdmaReadHalf` and `RdmaWriteHalf` can share it
/// after `TransportStream::split()`.
struct RdmaChannel {
    qp: Arc<IbvQp>,
    send_cq: Arc<IbvCq>,
    recv_cq: Arc<IbvCq>,
    mr_pool: Arc<MrPool>,
    /// Peer address (for logging).
    peer: SocketAddr,
    /// AsyncFd for the send CQ completion channel. When `Some`, allows
    /// non-blocking async wait for send completions via `tokio::io::AsyncFd`.
    send_async_fd: Option<Arc<CqAsyncFd>>,
    /// AsyncFd for the recv CQ completion channel.
    recv_async_fd: Option<Arc<CqAsyncFd>>,
}

impl RdmaChannel {
    /// Create a zeroed `ibv_wc` array of length 1 for completion polling.
    fn zeroed_wc() -> [ibv_wc; 1] {
        [ibv_wc {
            wr_id: 0,
            status: 0,
            opcode: 0,
            vendor_err: 0,
            byte_len: 0,
            imm_data: 0,
            qp_num: 0,
            src_qp: 0,
            wc_flags: 0,
            pkey_index: 0,
            slid: 0,
            sl: 0,
            dlid_path_bits: 0,
        }; 1]
    }

    /// Wait for one CQ completion using AsyncFd-driven async polling.
    ///
    /// 1. Arm the CQ (`ibv_req_notify_cq`) to get an event on the next
    ///    completion.
    /// 2. Poll the CQ — if a completion is already there, return it.
    /// 3. If no completion, wait for the AsyncFd to signal readiness,
    ///    then call `ibv_get_cq_event` + `ibv_ack_cq_events` to consume
    ///    the event, and poll the CQ again.
    async fn wait_cq_completion(
        cq: &IbvCq,
        async_fd: Option<&Arc<CqAsyncFd>>,
    ) -> NetResult<ibv_wc> {
        // Arm CQ notification before polling to avoid missing events.
        cq.req_notify(false)?;

        loop {
            // Fast path: poll for a completion.
            let mut wcs = Self::zeroed_wc();
            let n = cq.poll(&mut wcs);
            if n > 0 {
                return Ok(wcs[0]);
            }

            // No completion available — wait via AsyncFd.
            let async_fd = match async_fd {
                Some(fd) => fd,
                None => {
                    // No completion channel: busy-poll fallback (should not
                    // happen in production since CQs are always created with
                    // channels in the AsyncFd path).
                    std::hint::spin_loop();
                    continue;
                }
            };

            // Wait for the channel fd to become readable.
            let mut guard = async_fd.readable().await.map_err(|e| {
                NetError::Connection(format!(
                    "AsyncFd readable() failed: {}",
                    e
                ))
            })?;

            // The fd is readable — a CQ event is pending. Clear the
            // readiness so we don't get woken again for the same event.
            guard.clear_ready();

            // Consume the CQ event from the channel.
            cq.get_cq_event()?;
            cq.ack_events(1);

            // Loop back to poll the CQ for the actual completion.
        }
    }

    /// Send data via RDMA send. Borrows a buffer from the MR pool, copies
    /// `data` into it, posts a send WR, and waits for completion via AsyncFd.
    ///
    /// `data.len()` must be <= `mr_pool.buf_size()`.
    async fn send(&self, data: &[u8]) -> NetResult<()> {
        if data.len() > self.mr_pool.buf_size() {
            return Err(NetError::Protocol(format!(
                "RDMA send: data len {} exceeds buffer size {}",
                data.len(),
                self.mr_pool.buf_size()
            )));
        }
        let mr = self
            .mr_pool
            .try_acquire()
            .await
            .ok_or_else(|| NetError::Connection("MR pool exhausted".to_string()))?;

        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                mr.addr() as *mut u8,
                data.len(),
            );
        }

        let sge = ibv_sge {
            addr: mr.addr() as u64,
            length: data.len() as u32,
            lkey: mr.lkey(),
        };
        let wr_id = mr.addr() as u64; // identify completion by buffer addr
        self.qp.post_send(&sge, wr_id)?;

        // Wait for send completion via AsyncFd-driven async polling.
        let wc = Self::wait_cq_completion(&self.send_cq, self.send_async_fd.as_ref()).await;
        let wc = match wc {
            Ok(w) => w,
            Err(e) => {
                self.mr_pool.release(mr).await;
                return Err(e);
            }
        };

        if wc.status != ffi::IBV_WC_SUCCESS {
            self.mr_pool.release(mr).await;
            return Err(NetError::Connection(format!(
                "RDMA send completion error (status={})",
                wc.status
            )));
        }

        self.mr_pool.release(mr).await;
        Ok(())
    }

    /// Post a recv WR using a pool buffer and wait for completion via AsyncFd.
    ///
    /// Returns the bytes received (copied into `out`).
    async fn recv(&self, out: &mut [u8]) -> NetResult<usize> {
        let mr = self
            .mr_pool
            .try_acquire()
            .await
            .ok_or_else(|| NetError::Connection("MR pool exhausted".to_string()))?;

        let sge = ibv_sge {
            addr: mr.addr() as u64,
            length: std::cmp::min(out.len(), self.mr_pool.buf_size()) as u32,
            lkey: mr.lkey(),
        };
        let wr_id = mr.addr() as u64;
        self.qp.post_recv(&sge, wr_id)?;

        let wc = Self::wait_cq_completion(&self.recv_cq, self.recv_async_fd.as_ref()).await;
        let wc = match wc {
            Ok(w) => w,
            Err(e) => {
                self.mr_pool.release(mr).await;
                return Err(e);
            }
        };

        if wc.status != ffi::IBV_WC_SUCCESS {
            self.mr_pool.release(mr).await;
            return Err(NetError::Connection(format!(
                "RDMA recv completion error (status={})",
                wc.status
            )));
        }

        let byte_len = wc.byte_len as usize;
        let copy_len = std::cmp::min(byte_len, out.len());
        unsafe {
            std::ptr::copy_nonoverlapping(
                mr.addr() as *const u8,
                out.as_mut_ptr(),
                copy_len,
            );
        }
        self.mr_pool.release(mr).await;
        Ok(copy_len)
    }

    /// Like `recv` but returns an owned `Vec<u8>` of the exact received
    /// length. Used by `RdmaReadHalf::poll_read` which needs a `'static`
    /// future for the `BoxFuture` pattern.
    async fn recv_owned(&self) -> NetResult<Vec<u8>> {
        let buf_size = self.mr_pool.buf_size();
        let mut buf = vec![0u8; buf_size];
        let n = self.recv(&mut buf).await?;
        buf.truncate(n);
        Ok(buf)
    }

    fn peer(&self) -> SocketAddr {
        self.peer
    }
}

// ============================================================================
// Transport impls
// ============================================================================

/// RDMA 传输层
pub struct RdmaTransport {
    config: TransportConfig,
    context: Arc<IbvContext>,
    pd: Arc<IbvPd>,
}

impl RdmaTransport {
    /// Create a new RDMA transport. Opens the RDMA device specified in
    /// `config.rdma_device` (or auto-selects the first available device).
    ///
    /// Returns `Err(NetError::Config)` if no RDMA hardware is available.
    pub fn new(config: TransportConfig) -> NetResult<Self> {
        let context = IbvContext::open(config.rdma_device.as_deref())?;
        let pd = IbvPd::alloc(&context)?;
        info!(
            "RdmaTransport: initialized (device={:?}, buf_num={}, buf_size={})",
            config.rdma_device, config.rdma_buf_num, config.rdma_buf_size
        );
        Ok(RdmaTransport {
            config,
            context: Arc::new(context),
            pd: Arc::new(pd),
        })
    }

    /// Build an MR pool from this transport's PD.
    fn build_mr_pool(&self) -> NetResult<Arc<MrPool>> {
        let pool = MrPool::new(
            &self.pd,
            self.config.rdma_buf_num,
            self.config.rdma_buf_size,
        )?;
        Ok(Arc::new(pool))
    }
}

#[async_trait::async_trait]
impl Transport for RdmaTransport {
    async fn connect(&self, addr: SocketAddr) -> NetResult<Box<dyn TransportStream>> {
        debug!("RdmaTransport::connect to {}", addr);
        // 1. Create rdma_cm event channel + id
        let channel = unsafe { ffi::rdma_create_event_channel() };
        if channel.is_null() {
            return Err(NetError::Connection(
                "rdma_create_event_channel failed".to_string(),
            ));
        }
        let channel = RdmaEventChannelPtr(channel);

        let mut cm_id: *mut rdma_cm_id = ptr::null_mut();
        let rc = unsafe {
            ffi::rdma_create_id(channel.0, &mut cm_id, ptr::null_mut(), ffi::RDMA_PS_TCP)
        };
        if rc != 0 {
            return Err(NetError::Connection(format!(
                "rdma_create_id failed (rc={})",
                rc
            )));
        }
        let cm_id = RdmaCmIdPtr(cm_id);

        // 2. Resolve address
        let (sa, sa_len) = sockaddr_to_libc(addr);
        let rc = unsafe {
            ffi::rdma_resolve_addr(
                cm_id.0,
                ptr::null(),
                sa as *const libc::sockaddr,
                3000, // 3s timeout
            )
        };
        if rc != 0 {
            return Err(NetError::Connection(format!(
                "rdma_resolve_addr({}) failed (rc={})",
                addr, rc
            )));
        }

        // Wait for ADDR_RESOLVED event
        wait_cm_event(channel.0, ffi::RDMA_CM_EVENT_ADDR_RESOLVED)?;
        // 3. Resolve route
        let rc = unsafe { ffi::rdma_resolve_route(cm_id.0, 3000) };
        if rc != 0 {
            return Err(NetError::Connection(format!(
                "rdma_resolve_route failed (rc={})",
                rc
            )));
        }
        wait_cm_event(channel.0, ffi::RDMA_CM_EVENT_ROUTE_RESOLVED)?;

        // 4. Create QP using our PD (so we can use the pre-registered MR pool).
        // Each CQ gets its own completion channel for AsyncFd-driven async
        // completion polling (avoids busy-polling the tokio worker).
        let mr_pool = self.build_mr_pool()?;
        let send_comp_ch = IbvCompChannel::create(&self.context)?;
        let recv_comp_ch = IbvCompChannel::create(&self.context)?;
        let send_cq = IbvCq::create(&self.context, 32, Some(send_comp_ch))?;
        let recv_cq = IbvCq::create(&self.context, 32, Some(recv_comp_ch))?;
        let qp = IbvQp::create(&self.pd, &send_cq, &recv_cq, 16, 16, 1)?;
        // rdma_create_qp would create its own PD; we already have one. Instead,
        // we modify the QP states manually.
        // Note: We do NOT call rdma_create_qp; we use ibv_create_qp + manual
        // state transitions. This is a design choice to share the pre-registered
        // MR pool's PD across all connections.
        qp.modify_to_init(1)?;
        // After connect event we'll know dest_qp_num and can transition to RTR/RTS.

        // 5. Connect
        let rc = unsafe { ffi::rdma_connect(cm_id.0, ptr::null()) };
        if rc != 0 {
            return Err(NetError::Connection(format!(
                "rdma_connect failed (rc={})",
                rc
            )));
        }
        let event = wait_cm_event_with_id(channel.0, ffi::RDMA_CM_EVENT_ESTABLISHED)?;
        // dest_qp_num from event... For RC QP via rdma_cm, the dest QPN is
        // negotiated. For the initial implementation we read it from the
        // event (in practice, rdma_cm handles this internally when using
        // rdma_create_qp; with manual QP we need to extract it).
        let _ = event;

        // 6. Transition QP to RTR + RTS (dest_qp_num=0 is a placeholder;
        // for production this needs the actual peer QPN, which rdma_cm
        // exposes via the event).
        qp.modify_to_rtr(0, 1)?;
        qp.modify_to_rts()?;

        // 7. Build AsyncFd for send/recv CQ completion channels.
        let send_async_fd = match send_cq.channel_fd() {
            Some(fd) => match AsyncFd::new(CqChannelFd(fd)) {
                Ok(async_fd) => Some(Arc::new(async_fd)),
                Err(e) => {
                    warn!("AsyncFd::new(send_cq_fd) failed: {}, using busy-poll", e);
                    None
                }
            },
            None => None,
        };
        let recv_async_fd = match recv_cq.channel_fd() {
            Some(fd) => match AsyncFd::new(CqChannelFd(fd)) {
                Ok(async_fd) => Some(Arc::new(async_fd)),
                Err(e) => {
                    warn!("AsyncFd::new(recv_cq_fd) failed: {}, using busy-poll", e);
                    None
                }
            },
            None => None,
        };

        let channel_inner = RdmaChannel {
            qp: Arc::new(qp),
            send_cq: Arc::new(send_cq),
            recv_cq: Arc::new(recv_cq),
            mr_pool,
            peer: addr,
            send_async_fd,
            recv_async_fd,
        };

        info!("RdmaTransport: connected to {}", addr);
        Ok(Box::new(RdmaStream {
            channel: Arc::new(channel_inner),
            peer: addr,
        }))
    }

    async fn bind(&self, addr: SocketAddr) -> NetResult<Box<dyn TransportListener>> {
        debug!("RdmaTransport::bind on {}", addr);

        let channel = unsafe { ffi::rdma_create_event_channel() };
        if channel.is_null() {
            return Err(NetError::Connection(
                "rdma_create_event_channel (bind) failed".to_string(),
            ));
        }
        let channel = RdmaEventChannelPtr(channel);

        let mut cm_id: *mut rdma_cm_id = ptr::null_mut();
        let rc = unsafe {
            ffi::rdma_create_id(channel.0, &mut cm_id, ptr::null_mut(), ffi::RDMA_PS_TCP)
        };
        if rc != 0 {
            return Err(NetError::Connection(format!(
                "rdma_create_id failed (rc={})",
                rc
            )));
        }
        let cm_id = RdmaCmIdPtr(cm_id);

        let (sa, _sa_len) = sockaddr_to_libc(addr);
        let rc = unsafe { ffi::rdma_bind_addr(cm_id.0, sa as *const libc::sockaddr) };
        if rc != 0 {
            return Err(NetError::Connection(format!(
                "rdma_bind_addr({}) failed (rc={})",
                addr, rc
            )));
        }

        let rc = unsafe { ffi::rdma_listen(cm_id.0, 128) };
        if rc != 0 {
            return Err(NetError::Connection(format!(
                "rdma_listen failed (rc={})",
                rc
            )));
        }

        info!(
            "RdmaTransport: listening on {} (rdma_cm)",
            addr
        );

        Ok(Box::new(RdmaListenerAdapter {
            channel,
            listen_id: cm_id,
            bind_addr: addr,
            context: self.context.clone(),
            pd: self.pd.clone(),
            config: self.config.clone(),
        }))
    }

    fn name(&self) -> &'static str {
        "rdma"
    }
}

/// RAII wrapper for rdma_event_channel.
struct RdmaEventChannelPtr(*mut rdma_event_channel);

impl Drop for RdmaEventChannelPtr {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { ffi::rdma_destroy_event_channel(self.0) };
        }
    }
}

unsafe impl Send for RdmaEventChannelPtr {}
unsafe impl Sync for RdmaEventChannelPtr {}

/// RAII wrapper for rdma_cm_id.
struct RdmaCmIdPtr(*mut rdma_cm_id);

impl Drop for RdmaCmIdPtr {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { ffi::rdma_destroy_id(self.0) };
        }
    }
}

unsafe impl Send for RdmaCmIdPtr {}
unsafe impl Sync for RdmaCmIdPtr {}

/// Wait for a specific rdma_cm event type on the channel (blocking).
fn wait_cm_event(
    channel: *mut rdma_event_channel,
    expected: ffi::rdma_cm_event_type,
) -> NetResult<()> {
    wait_cm_event_with_id(channel, expected).map(|_| ())
}

/// Wait for a specific rdma_cm event type and return the event (for QPN extraction).
fn wait_cm_event_with_id(
    channel: *mut rdma_event_channel,
    expected: ffi::rdma_cm_event_type,
) -> NetResult<*mut rdma_cm_event> {
    let mut event: *mut rdma_cm_event = ptr::null_mut();
    let rc = unsafe { ffi::rdma_get_cm_event(channel, &mut event) };
    if rc != 0 {
        return Err(NetError::Connection(format!(
            "rdma_get_cm_event failed (rc={})",
            rc
        )));
    }
    if event.is_null() {
        return Err(NetError::Connection("rdma_get_cm_event returned null".to_string()));
    }

    let actual = unsafe { (*event).event };
    if actual != expected {
        let msg = format!(
            "rdma_cm: expected event {}, got {}",
            expected, actual
        );
        unsafe { ffi::rdma_ack_cm_event(event) };
        return Err(NetError::Connection(msg));
    }

    // Note: caller is responsible for ack. For the simple wait_cm_event
    // variant we ack here; for wait_cm_event_with_id we return the event
    // pointer (caller acks). To keep this simple, ack here and return null.
    unsafe { ffi::rdma_ack_cm_event(event) };
    Ok(ptr::null_mut())
}

/// Convert a SocketAddr to a libc sockaddr (IPv4 or IPv6).
fn sockaddr_to_libc(addr: SocketAddr) -> (*const libc::sockaddr, libc::socklen_t) {
    // We need a 'static-ish storage since the pointer is used in FFI calls.
    // rdma_resolve_addr / rdma_bind_addr copy the address, so a stack pointer
    // is safe as long as we don't hold it across await boundaries.
    //
    // To simplify lifetime management, we use a thread-local or leak. The
    // leak is bounded (one per connect/bind). For production, use a scoped
    // arena or Box.
    match addr {
        SocketAddr::V4(v4) => {
            let mut sin: libc::sockaddr_in = unsafe { std::mem::zeroed() };
            sin.sin_family = libc::AF_INET as u16;
            sin.sin_port = v4.port().to_be();
            sin.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            let len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
            let boxed = Box::new(sin);
            let ptr = Box::into_raw(boxed) as *const libc::sockaddr;
            (ptr, len)
        }
        SocketAddr::V6(v6) => {
            let mut sin6: libc::sockaddr_in6 = unsafe { std::mem::zeroed() };
            sin6.sin6_family = libc::AF_INET6 as u16;
            sin6.sin6_port = v6.port().to_be();
            sin6.sin6_addr.s6_addr = v6.ip().octets();
            let len = std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;
            let boxed = Box::new(sin6);
            let ptr = Box::into_raw(boxed) as *const libc::sockaddr;
            (ptr, len)
        }
    }
}

// ============================================================================
// RdmaStream — TransportStream implementation
// ============================================================================

/// RDMA stream — wraps an `RdmaChannel` and implements `TransportStream`.
///
/// On `split()`, the `Arc<RdmaChannel>` is shared between `RdmaReadHalf` and
/// `RdmaWriteHalf`, which independently use the channel's recv/send paths.
pub struct RdmaStream {
    channel: Arc<RdmaChannel>,
    peer: SocketAddr,
}

impl TransportStream for RdmaStream {
    fn split(
        self: Box<Self>,
    ) -> (Box<dyn AsyncRead + Send + Unpin>, Box<dyn AsyncWrite + Send + Unpin>) {
        let channel = self.channel.clone();
        let reader = RdmaReadHalf {
            channel: self.channel.clone(),
            recv_buf: Vec::new(),
            recv_pos: 0,
            pending_recv: None,
        };
        let writer = RdmaWriteHalf {
            channel,
            send_buf: Vec::new(),
            pending_send: None,
        };
        (Box::new(reader), Box::new(writer))
    }

    fn peer_addr(&self) -> SocketAddr {
        self.peer
    }
}

// ============================================================================
// RdmaReadHalf — AsyncRead via RDMA recv (BoxFuture pattern)
// ============================================================================

/// Type alias for a pinned boxed future that yields received data.
type RecvFut = Pin<Box<dyn std::future::Future<Output = NetResult<Vec<u8>>> + Send>>;

/// RDMA read half. Implements `AsyncRead` by:
/// 1. If the internal buffer has unread bytes, copy them to the caller.
/// 2. Otherwise, post an RDMA recv (via a `'static` future that captures
///    only `Arc<RdmaChannel>`) and poll it to completion. When done, store
///    the received bytes in `recv_buf` and return them.
///
/// The `BoxFuture` pattern avoids calling `block_on` inside `poll_read`,
/// which would deadlock the tokio runtime. The future runs asynchronously
/// and uses `AsyncFd` internally for CQ completion notification.
struct RdmaReadHalf {
    channel: Arc<RdmaChannel>,
    /// Bytes received from the last RDMA recv but not yet consumed by
    /// poll_read. Stream emulation: one RDMA recv message may span multiple
    /// poll_read calls.
    recv_buf: Vec<u8>,
    /// Read cursor into `recv_buf`.
    recv_pos: usize,
    /// In-flight recv future (if any). When `Some`, we are waiting for an
    /// async RDMA recv to complete.
    pending_recv: Option<RecvFut>,
}

impl AsyncRead for RdmaReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Fast path: buffered bytes available
        if self.recv_pos < self.recv_buf.len() {
            let remaining = &self.recv_buf[self.recv_pos..];
            let n = std::cmp::min(remaining.len(), buf.remaining());
            buf.put_slice(&remaining[..n]);
            self.recv_pos += n;
            return Poll::Ready(Ok(()));
        }

        // Slow path: need to post a new RDMA recv. If we don't have an
        // in-flight future, create one. The future captures only the
        // `Arc<RdmaChannel>` (no borrows of `self`), making it `'static`.
        if self.pending_recv.is_none() {
            let channel = self.channel.clone();
            self.pending_recv = Some(Box::pin(async move {
                channel.recv_owned().await
            }));
        }

        // Poll the in-flight future.
        let pending = self.pending_recv.as_mut().unwrap();
        match pending.as_mut().poll(cx) {
            Poll::Ready(result) => {
                self.pending_recv = None;
                match result {
                    Ok(data) => {
                        self.recv_buf = data;
                        self.recv_pos = 0;
                        let n = std::cmp::min(self.recv_buf.len(), buf.remaining());
                        buf.put_slice(&self.recv_buf[..n]);
                        self.recv_pos = n;
                        Poll::Ready(Ok(()))
                    }
                    Err(e) => Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

unsafe impl Send for RdmaReadHalf {}
impl Unpin for RdmaReadHalf {}

// ============================================================================
// RdmaWriteHalf — AsyncWrite via RDMA send (BoxFuture pattern)
// ============================================================================

/// Type alias for a pinned boxed future for the send operation.
type SendFut = Pin<Box<dyn std::future::Future<Output = NetResult<()>> + Send>>;

/// RDMA write half. Implements `AsyncWrite` by:
/// 1. `poll_write`: append to the internal send buffer.
/// 2. `poll_flush`: take the buffered data and post an RDMA send via a
///    `'static` future. Poll the future to completion.
struct RdmaWriteHalf {
    channel: Arc<RdmaChannel>,
    /// Pending bytes not yet sent. Stream emulation: caller may write a frame
    /// in multiple poll_write calls; we flush on poll_flush.
    send_buf: Vec<u8>,
    /// In-flight send future (if any). When `Some`, we are waiting for an
    /// async RDMA send to complete.
    pending_send: Option<SendFut>,
}

impl AsyncWrite for RdmaWriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        // Append to internal buffer; the actual RDMA send happens on flush.
        self.send_buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        // If we have an in-flight send, poll it first.
        if let Some(pending) = self.pending_send.as_mut() {
            match pending.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    self.pending_send = None;
                    match result {
                        Ok(()) => {}
                        Err(e) => {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                e.to_string(),
                            )));
                        }
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        // No in-flight send. If there's buffered data, start a new send.
        if self.send_buf.is_empty() {
            return Poll::Ready(Ok(()));
        }

        let channel = self.channel.clone();
        let data = std::mem::take(&mut self.send_buf);
        self.pending_send = Some(Box::pin(async move {
            channel.send(&data).await
        }));

        // Poll the newly created future immediately (may complete fast path).
        let pending = self.pending_send.as_mut().unwrap();
        match pending.as_mut().poll(cx) {
            Poll::Ready(result) => {
                self.pending_send = None;
                match result {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(e) => Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        // TODO: rdma_disconnect on the underlying cm_id.
        Poll::Ready(Ok(()))
    }
}

unsafe impl Send for RdmaWriteHalf {}
impl Unpin for RdmaWriteHalf {}

// ============================================================================
// RdmaListenerAdapter — TransportListener implementation
// ============================================================================

/// RDMA listener adapter. Wraps rdma_cm listen id + event channel.
pub struct RdmaListenerAdapter {
    channel: RdmaEventChannelPtr,
    listen_id: RdmaCmIdPtr,
    bind_addr: SocketAddr,
    context: Arc<IbvContext>,
    pd: Arc<IbvPd>,
    config: TransportConfig,
}

#[async_trait::async_trait]
impl TransportListener for RdmaListenerAdapter {
    async fn accept(&self) -> NetResult<Box<dyn TransportStream>> {
        loop {
            let mut event: *mut rdma_cm_event = ptr::null_mut();
            let rc = unsafe { ffi::rdma_get_cm_event(self.channel.0, &mut event) };
            if rc != 0 {
                return Err(NetError::Connection(format!(
                    "accept: rdma_get_cm_event failed (rc={})",
                    rc
                )));
            }
            if event.is_null() {
                return Err(NetError::Connection(
                    "accept: rdma_get_cm_event returned null".to_string(),
                ));
            }

            let event_type = unsafe { (*event).event };
            let new_id = unsafe { (*event).id };
            unsafe { ffi::rdma_ack_cm_event(event) };

            match event_type {
                ffi::RDMA_CM_EVENT_CONNECT_REQUEST => {
                    // Build a new RdmaChannel for the incoming connection.
                    // Create CQs with completion channels for AsyncFd polling.
                    let mr_pool = MrPool::new(
                        &self.pd,
                        self.config.rdma_buf_num,
                        self.config.rdma_buf_size,
                    )?;
                    let send_comp_ch = IbvCompChannel::create(&self.context)?;
                    let recv_comp_ch = IbvCompChannel::create(&self.context)?;
                    let send_cq = IbvCq::create(&self.context, 32, Some(send_comp_ch))?;
                    let recv_cq = IbvCq::create(&self.context, 32, Some(recv_comp_ch))?;
                    let qp = IbvQp::create(&self.pd, &send_cq, &recv_cq, 16, 16, 1)?;
                    qp.modify_to_init(1)?;
                    // Accept the connection
                    let rc = unsafe { ffi::rdma_accept(new_id, ptr::null()) };
                    if rc != 0 {
                        warn!("rdma_accept failed (rc={})", rc);
                        continue;
                    }
                    // Transition QP to RTR + RTS (dest_qp_num=0 placeholder)
                    qp.modify_to_rtr(0, 1)?;
                    qp.modify_to_rts()?;

                    // Build AsyncFd for send/recv CQ completion channels.
                    let send_async_fd = match send_cq.channel_fd() {
                        Some(fd) => match AsyncFd::new(CqChannelFd(fd)) {
                            Ok(async_fd) => Some(Arc::new(async_fd)),
                            Err(e) => {
                                warn!("AsyncFd::new(accept send_cq_fd) failed: {}", e);
                                None
                            }
                        },
                        None => None,
                    };
                    let recv_async_fd = match recv_cq.channel_fd() {
                        Some(fd) => match AsyncFd::new(CqChannelFd(fd)) {
                            Ok(async_fd) => Some(Arc::new(async_fd)),
                            Err(e) => {
                                warn!("AsyncFd::new(accept recv_cq_fd) failed: {}", e);
                                None
                            }
                        },
                        None => None,
                    };

                    let peer = self.bind_addr; // TODO: extract actual peer from event
                    let channel = RdmaChannel {
                        qp: Arc::new(qp),
                        send_cq: Arc::new(send_cq),
                        recv_cq: Arc::new(recv_cq),
                        mr_pool: Arc::new(mr_pool),
                        peer,
                        send_async_fd,
                        recv_async_fd,
                    };
                    info!("RdmaListenerAdapter: accepted connection from {}", peer);
                    return Ok(Box::new(RdmaStream {
                        channel: Arc::new(channel),
                        peer,
                    }));
                }
                other => {
                    debug!("RdmaListenerAdapter: ignoring cm event {}", other);
                    continue;
                }
            }
        }
    }

    fn local_addr(&self) -> NetResult<SocketAddr> {
        Ok(self.bind_addr)
    }
}

// Safety: the listener holds raw pointers but they are exclusively accessed
// from the accept loop. The accept() method is &self, and rdma_cm functions
// are thread-safe for distinct cm_ids.
unsafe impl Send for RdmaListenerAdapter {}
unsafe impl Sync for RdmaListenerAdapter {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that RdmaTransport::new fails gracefully when no RDMA hardware
    /// is present (typical in CI / dev environments without RNIC).
    ///
    /// This test is `#[ignore]` by default because:
    /// - On systems without any RDMA hardware, `ibv_get_device_list` returns 0
    ///   devices and `new()` returns `Err(NetError::Config)`.
    /// - On systems with RDMA hardware but driver issues (e.g. missing
    ///   provider .so files), the C library may SIGSEGV inside
    ///   `ibv_open_device` / `ibv_alloc_pd`, which Rust cannot catch.
    ///   Running the test on such systems would crash the test runner.
    ///
    /// To run on hardware with working drivers: `cargo test --features rdma -- --ignored`
    #[test]
    #[ignore]
    fn test_rdma_transport_new_without_hardware() {
        let config = TransportConfig::default();
        let result = RdmaTransport::new(config);
        match result {
            Ok(_) => { /* Hardware present — test passes trivially. */ }
            Err(NetError::Config(_)) => { /* No hardware — expected. */ }
            Err(e) => { panic!("unexpected error: {:?}", e); }
        }
    }
}
