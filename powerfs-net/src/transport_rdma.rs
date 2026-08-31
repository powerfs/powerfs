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
use std::ffi::c_void;
use std::ffi::CString;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::pin::Pin;
use std::ptr;
use std::sync::Arc;
use std::task::Poll;

use log::{debug, error, info, warn};
use tokio::io::unix::AsyncFd;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
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
    use libc::{
        c_char, c_int, c_uint, c_void, int16_t, int32_t, size_t, uint16_t, uint32_t, uint8_t,
    };

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

    /// `struct ibv_port_attr` from /usr/include/infiniband/verbs.h.
    /// Field order and types MUST match the C definition.
    /// Note: enum fields (state, max_mtu, active_mtu) are `int` (4 bytes)
    /// in C, NOT uint8_t.
    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct ibv_port_attr {
        /// enum ibv_port_state (int, 4 bytes)
        pub state: int32_t,
        /// enum ibv_mtu (int, 4 bytes)
        pub max_mtu: int32_t,
        /// enum ibv_mtu (int, 4 bytes)
        pub active_mtu: int32_t,
        pub gid_tbl_len: int32_t,
        pub port_cap_flags: uint32_t,
        pub max_msg_sz: uint32_t,
        pub bad_pkey_cntr: uint32_t,
        pub qkey_viol_cntr: uint32_t,
        pub pkey_tbl_len: uint16_t,
        /// Local Identifier assigned by the subnet manager.
        pub lid: uint16_t,
        pub sm_lid: uint16_t,
        pub lmc: uint8_t,
        pub max_vl_num: uint8_t,
        pub sm_sl: uint8_t,
        pub subnet_timeout: uint8_t,
        pub init_type_reply: uint8_t,
        pub active_width: uint8_t,
        pub active_speed: uint8_t,
        pub phys_state: uint8_t,
        pub link_layer: uint8_t,
        pub flags: uint8_t,
        pub port_cap_flags2: uint16_t,
        pub active_speed_ex: uint32_t,
    }

    // --- ibv_pd ------------------------------------------------------------

    /// `struct ibv_pd` from /usr/include/infiniband/verbs.h.
    #[repr(C)]
    pub struct ibv_pd {
        pub context: *mut ibv_context,
        pub handle: uint32_t,
    }

    // --- ibv_mr ------------------------------------------------------------

    /// `struct ibv_mr` from /usr/include/infiniband/verbs.h.
    /// Field order MUST match the C struct.
    #[repr(C)]
    pub struct ibv_mr {
        pub context: *mut ibv_context,
        pub pd: *mut ibv_pd,
        pub addr: *mut c_void,
        pub length: size_t,
        pub handle: uint32_t,
        pub lkey: uint32_t,
        pub rkey: uint32_t,
    }

    // --- ibv_cq ------------------------------------------------------------

    #[repr(C)]
    pub struct ibv_comp_channel {
        /// Pad to reach `fd` at offset 8 (verified on rdma-core 28.0-1ubuntu1:
        /// sizeof(struct ibv_comp_channel) = 16, offsetof(fd) = 8).
        pub _pad_before_fd: [u8; 8],
        pub fd: c_int,
        /// Trailing padding (fd is 4 bytes, struct size is 16 → 4 more bytes).
        pub _pad_after_fd: [u8; 4],
    }

    /// Opaque type for `struct ibv_srq` (Shared Receive Queue).
    /// Not used directly; only needed as a pointer in `rdma_cm_id`.
    #[repr(C)]
    pub struct ibv_srq {
        _opaque: [u8; 0],
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
        /// Trailing padding (verified sizeof = 48 on rdma-core 28.0).
        pub _pad_tail: [u8; 4],
    }

    pub type ibv_wc_status = int32_t;
    pub type ibv_wc_opcode = int32_t;

    pub const IBV_WC_SUCCESS: ibv_wc_status = 0;
    pub const IBV_WC_SEND: ibv_wc_opcode = 0;
    pub const IBV_WC_RECV: ibv_wc_opcode = 1;

    // --- ibv_qp ------------------------------------------------------------

    #[repr(C)]
    pub struct ibv_qp {
        /// Padding to reach qp_num at offset 52.
        /// Fields: context(8), qp_context(8), pd(8), send_cq(8), recv_cq(8),
        /// srq(8), qp_type(4) = 52 bytes.
        _opaque: [u8; 52],
        /// Local QP number (assigned by hardware at QP creation).
        pub qp_num: u32,
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
        pub qp_context: *mut c_void,
        pub send_cq: *mut ibv_cq,
        pub recv_cq: *mut ibv_cq,
        pub srq: *mut ibv_srq,
        pub cap: ibv_qp_cap,
        pub qp_type: int32_t,
        pub sq_sig_all: c_int,
    }

    /// Global Route Header (GRH) fields (used for RoCE / IB routing).
    /// Must match `struct ibv_global_route` in /usr/include/infiniband/verbs.h.
    /// Do NOT add a `_reserved` field: `#[repr(C)]` inserts the correct
    /// trailing padding (1 byte → 24 bytes total) automatically.
    #[repr(C)]
    #[derive(Copy, Clone)]
    pub struct ibv_global_route {
        /// Destination GID (16 bytes).
        pub dgid: [u8; 16],
        pub flow_label: uint32_t,
        pub sgid_index: uint8_t,
        pub hop_limit: uint8_t,
        pub traffic_class: uint8_t,
    }

    /// Address handle attributes. Used by `ibv_modify_qp` when transitioning
    /// to RTR to program the path to the peer (port, LID, etc.).
    /// Field order MUST match `struct ibv_ah_attr` in
    /// /usr/include/infiniband/verbs.h: is_global comes BEFORE port_num.
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
        /// Whether global routing (GRH) is used.
        pub is_global: uint8_t,
        /// Physical port number to use.
        pub port_num: uint8_t,
        /// Trailing padding (verified sizeof = 32 on rdma-core 28.0).
        pub _pad_tail: [u8; 1],
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
        /// Rate limit in kbps (last field of `struct ibv_qp_attr` in verbs.h).
        pub rate_limit: uint32_t,
    }

    // ibv_qp_attr_mask — values MUST match enum ibv_qp_attr_mask in
    // /usr/include/infiniband/verbs.h. Using `1 << N` makes the bit index
    // obvious and keeps the source in sync with the system header.
    pub const IBV_QP_STATE: c_int = 1 << 0; // 1
    pub const IBV_QP_ACCESS_FLAGS: c_int = 1 << 3; // 8
    pub const IBV_QP_PKEY_INDEX: c_int = 1 << 4; // 16
    pub const IBV_QP_PORT: c_int = 1 << 5; // 32
    pub const IBV_QP_QKEY: c_int = 1 << 6; // 64
    pub const IBV_QP_AV: c_int = 1 << 7; // 128
    pub const IBV_QP_PATH_MTU: c_int = 1 << 8; // 256
    pub const IBV_QP_TIMEOUT: c_int = 1 << 9; // 512
    pub const IBV_QP_RETRY_CNT: c_int = 1 << 10; // 1024
    pub const IBV_QP_RNR_RETRY: c_int = 1 << 11; // 2048
    pub const IBV_QP_RQ_PSN: c_int = 1 << 12; // 4096
    pub const IBV_QP_MAX_QP_RD_ATOMIC: c_int = 1 << 13; // 8192
    pub const IBV_QP_MIN_RNR_TIMER: c_int = 1 << 15; // 32768
    pub const IBV_QP_SQ_PSN: c_int = 1 << 16; // 65536
    pub const IBV_QP_MAX_DEST_RD_ATOMIC: c_int = 1 << 17; // 131072
    pub const IBV_QP_DEST_QPN: c_int = 1 << 20; // 1048576

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

    /// `struct ibv_send_wr` from <infiniband/verbs.h>.
    ///
    /// CRITICAL: The C struct is 128 bytes. It has fields:
    ///   imm_data (u32, offset 36), wr (union, offset 40, 32 bytes),
    ///   qp_type (union, offset 72), bind_mw/tso (union, offset 80, 48 bytes).
    /// We use opaque padding for unused trailing fields to match the size.
    #[repr(C)]
    pub struct ibv_send_wr {
        pub wr_id: u64,                        // offset 0
        pub next: *mut ibv_send_wr,            // offset 8
        pub sg_list: *mut ibv_sge,             // offset 16
        pub num_sge: c_int,                    // offset 24
        pub opcode: int32_t,                   // offset 28
        pub send_flags: uint32_t,              // offset 32
        pub imm_data: uint32_t,                // offset 36 (union: imm_data/invalidate_rkey)
        pub wr: ibv_send_wr_wr,                // offset 40 (32 bytes, union)
        pub qp_type_xrc_remote_srqn: uint32_t, // offset 72
        pub _qp_type_pad: [u8; 4],             // offset 76 (padding for bind_mw alignment)
        pub _bind_mw_tso: [u8; 48],            // offset 80 (bind_mw/tso union, 48 bytes)
                                               // Total: 128 bytes
    }

    #[repr(C)]
    #[derive(Copy, Clone)]
    /// `wr` union in `struct ibv_send_wr`. The C union is 32 bytes (max of
    /// atomic variant). We use a flat layout with enough padding to match.
    pub struct ibv_send_wr_wr {
        // rdma variant: remote_addr (8) + rkey (4) + reserved (4) = 16
        pub rdma_remote: u64,
        pub rdma_rkey: uint32_t,
        // atomic variant extends to 32 bytes:
        //   remote_addr(8) + compare_add(4) + swap_add(4) +
        //   rkey(4) + reserved(4) + compare_mask(8) = 32
        pub _atomic_pad: [u32; 5], // 20 bytes padding to 32 total
    }

    pub const IBV_WR_RDMA_WRITE: int32_t = 0;
    pub const IBV_WR_RDMA_WRITE_WITH_IMM: int32_t = 1;
    pub const IBV_WR_SEND: int32_t = 2;
    pub const IBV_WR_SEND_WITH_IMM: int32_t = 3;
    pub const IBV_WR_RDMA_READ: int32_t = 4;
    pub const IBV_SEND_FENCE: uint32_t = 1 << 0;
    pub const IBV_SEND_SIGNALED: uint32_t = 1 << 1;
    pub const IBV_SEND_SOLICITED: uint32_t = 1 << 2;
    pub const IBV_SEND_INLINE: uint32_t = 1 << 3;

    #[repr(C)]
    pub struct ibv_recv_wr {
        pub wr_id: u64,
        pub next: *mut ibv_recv_wr,
        pub sg_list: *mut ibv_sge,
        pub num_sge: c_int,
    }

    // --- libibverbs function declarations ----------------------------------

    extern "C" {
        pub fn ibv_get_device_list(num_devices: *mut c_int) -> *mut *mut ibv_device;
        pub fn ibv_free_device_list(list: *mut *mut ibv_device);
        pub fn ibv_get_device_name(device: *mut ibv_device) -> *const c_char;
        pub fn ibv_open_device(device: *mut ibv_device) -> *mut ibv_context;
        pub fn ibv_close_device(context: *mut ibv_context) -> c_int;
        pub fn ibv_query_device(context: *mut ibv_context, attr: *mut ibv_device_attr) -> c_int;
        pub fn ibv_query_port(
            context: *mut ibv_context,
            port_num: uint8_t,
            attr: *mut ibv_port_attr,
        ) -> c_int;

        // C wrapper for ibv_query_port (which is a macro, not a direct
        // symbol — the exported symbol is the OLD compat version with a
        // smaller struct layout). This wrapper uses the macro to call
        // ___ibv_query_port which handles struct size detection correctly.
        pub fn powerfs_ibv_query_port(
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
        pub fn ibv_create_comp_channel(context: *mut ibv_context) -> *mut ibv_comp_channel;
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
        pub fn powerfs_ibv_req_notify_cq(cq: *mut ibv_cq, solicited_only: c_int) -> c_int;
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
        pub fn powerfs_ibv_poll_cq(cq: *mut ibv_cq, num_wc: c_int, wc: *mut ibv_wc) -> c_int;
        pub fn ibv_create_qp(pd: *mut ibv_pd, init: *mut ibv_qp_init) -> *mut ibv_qp;
        pub fn ibv_destroy_qp(qp: *mut ibv_qp) -> c_int;
        pub fn ibv_modify_qp(qp: *mut ibv_qp, attr: *mut ibv_qp_attr, attr_mask: c_int) -> c_int;
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

        // C wrappers for rdma_cm functions (avoid Rust struct layout issues)
        pub fn powerfs_rdma_create_qp(
            id: *mut rdma_cm_id,
            pd: *mut ibv_pd,
            qp_context: *mut c_void,
            send_cq: *mut ibv_cq,
            recv_cq: *mut ibv_cq,
            srq: *mut ibv_srq,
            max_send_wr: uint32_t,
            max_recv_wr: uint32_t,
            max_send_sge: uint32_t,
            max_recv_sge: uint32_t,
            max_inline_data: uint32_t,
            qp_type: c_int,
            sq_sig_all: c_int,
        ) -> c_int;
        pub fn powerfs_rdma_connect(
            id: *mut rdma_cm_id,
            responder_resources: u8,
            initiator_depth: u8,
            flow_control: u8,
            retry_count: u8,
            rnr_retry_count: u8,
            srq: u8,
            qp_num: uint32_t,
        ) -> c_int;
        pub fn powerfs_rdma_accept(
            id: *mut rdma_cm_id,
            responder_resources: u8,
            initiator_depth: u8,
            flow_control: u8,
            retry_count: u8,
            rnr_retry_count: u8,
            srq: u8,
            qp_num: uint32_t,
        ) -> c_int;
    }

    // MR access flags
    pub const IBV_ACCESS_LOCAL_WRITE: c_int = 1;
    pub const IBV_ACCESS_REMOTE_WRITE: c_int = 1 << 1;
    pub const IBV_ACCESS_REMOTE_READ: c_int = 1 << 2;
    pub const IBV_ACCESS_REMOTE_ATOMIC: c_int = 1 << 3;

    /// Port states (from ibv_port_state enum).
    // Port states — match `enum ibv_port_state` (int, 4 bytes)
    pub const IBV_PORT_NOP: int32_t = 0;
    pub const IBV_PORT_DOWN: int32_t = 1;
    pub const IBV_PORT_INIT: int32_t = 2;
    pub const IBV_PORT_ARMED: int32_t = 3;
    pub const IBV_PORT_ACTIVE: int32_t = 4;

    // --- librdmacm --------------------------------------------------------

    #[repr(C)]
    pub struct rdma_event_channel {
        pub fd: c_int,
    }

    /// `struct rdma_cm_id` from /usr/include/rdma/rdma_cma.h.
    /// MUST be 416 bytes (verified via sizeof in C). Field order and
    /// alignment match the C definition exactly.
    #[repr(C)]
    pub struct rdma_cm_id {
        /// Device context (ibv_context*). Set by rdma_cm after addr resolution.
        pub verbs: *mut ibv_context,
        /// Event channel this id belongs to.
        pub channel: *mut rdma_event_channel,
        /// User-supplied context pointer (opaque).
        pub context: *mut c_void,
        /// QP bound by `rdma_create_qp` or manually set after `ibv_create_qp`.
        pub qp: *mut ibv_qp,
        /// Resolved route (rdma_route, 312 bytes). We skip the full definition
        /// and use padding to advance to `ps`.
        _route_pad: [u8; 312], // sizeof(rdma_route) = 312 on 64-bit
        /// Port space (RDMA_PS_TCP etc.).
        pub ps: c_int,
        /// Physical port number resolved by rdma_cm. Used for QP path programming.
        pub port_num: u8,
        // 3 bytes padding to align `event` (pointer) to 8 bytes.
        _pad1: [u8; 3],
        /// Pending event (set by rdma_cm internally).
        pub event: *mut rdma_cm_event,
        /// Completion channel for send CQ (set by rdma_create_qp).
        pub send_cq_channel: *mut ibv_comp_channel,
        /// Send CQ (set by rdma_create_qp).
        pub send_cq: *mut ibv_cq,
        /// Completion channel for recv CQ (set by rdma_create_qp).
        pub recv_cq_channel: *mut ibv_comp_channel,
        /// Recv CQ (set by rdma_create_qp).
        pub recv_cq: *mut ibv_cq,
        /// Shared Receive Queue (NULL if not using SRQ).
        pub srq: *mut ibv_srq,
        /// Protection Domain (set by rdma_create_qp).
        pub pd: *mut ibv_pd,
        /// QP type (IBV_QPT_RC etc.).
        pub qp_type: int32_t,
        // 4 bytes padding to reach 416 bytes total.
        _pad2: [u8; 4],
    }

    /// `struct rdma_conn_param` from <rdma/rdma_cma.h>.
    ///
    /// Used in `rdma_cm_event.param.conn` to extract the peer's QPN from
    /// CONNECT_REQUEST (server) and CONNECT_RESPONSE (client) events.
    /// `qp_num` at offset 16 within the struct.
    #[repr(C)]
    pub struct rdma_conn_param {
        pub private_data: *const c_void,
        pub private_data_len: u8,
        pub responder_resources: u8,
        pub initiator_depth: u8,
        pub flow_control: u8,
        pub retry_count: u8,
        pub rnr_retry_count: u8,
        pub srq: u8,
        /// Padding to align qp_num at offset 16 (verified: offsetof(qp_num)=16,
        /// sizeof(rdma_conn_param)=24 on rdma-core 28.0-1ubuntu1).
        pub _pad_before_qp_num: [u8; 1],
        /// Peer's QPN, extracted from REQ/REP message by rdma_cm.
        pub qp_num: u32,
        /// Trailing padding to reach 24 bytes total.
        pub _pad_tail: [u8; 4],
    }

    /// rdma_cm event (must match `struct rdma_cm_event` in <rdma/rdma_cma.h>).
    ///
    /// CRITICAL: the `listen_id` field at offset 8 is easy to miss. If it
    /// is omitted, `event` would be read at offset 8 (the listen_id bytes)
    /// instead of offset 16, causing every non-ADDR_RESOLVED event to be
    /// misread. For ADDR_RESOLVED listen_id is NULL (== 0 == ADDR_RESOLVED),
    /// so the bug masquerades as "duplicate ADDR_RESOLVED".
    ///
    /// The `param` union is 56 bytes (sizeof(rdma_ud_param) > sizeof(rdma_conn_param));
    /// we define `rdma_conn_param` (24 bytes) + 32 bytes padding = 56.
    #[repr(C)]
    pub struct rdma_cm_event {
        pub id: *mut rdma_cm_id,
        pub listen_id: *mut rdma_cm_id,
        pub event: rdma_cm_event_type,
        pub status: c_int,
        /// Connection parameters from peer (for CONNECT_REQUEST/CONNECT_RESPONSE).
        /// In C this is a union; we use the conn variant + padding to match size.
        pub param: rdma_conn_param,
        _param_pad: [u8; 32], // pad to 80-byte total (56-byte union - 24 conn = 32)
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

    pub const RDMA_PS_TCP: c_int = 0x0106;

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
        pub fn rdma_init_qp_attr(
            id: *mut rdma_cm_id,
            qp_attr: *mut ibv_qp_attr,
            qp_attr_mask: *mut c_int,
        ) -> c_int;
        pub fn rdma_bind_addr(id: *mut rdma_cm_id, addr: *const libc::sockaddr) -> c_int;
        pub fn rdma_listen(id: *mut rdma_cm_id, backlog: c_int) -> c_int;
        pub fn rdma_connect(id: *mut rdma_cm_id, conn_param: *const rdma_conn_param) -> c_int;
        pub fn rdma_accept(id: *mut rdma_cm_id, conn_param: *const rdma_conn_param) -> c_int;
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
        pub fn rdma_destroy_qp(id: *mut rdma_cm_id);
        pub fn rdma_get_devices(num_devices: *mut c_int) -> *mut *mut c_void;
        pub fn rdma_free_devices(devices: *mut *mut c_void);
    }
}

// Re-export key FFI types for use in RAII wrappers
use ffi::{
    ibv_comp_channel, ibv_context, ibv_cq, ibv_device, ibv_mr, ibv_pd, ibv_port_attr, ibv_qp,
    ibv_qp_attr, ibv_qp_cap, ibv_recv_wr, ibv_send_wr, ibv_sge, ibv_wc, ibv_wc_opcode,
    rdma_cm_event, rdma_cm_id, rdma_event_channel,
};

// ============================================================================
// RAII Wrappers — 自动释放 RDMA 资源
// ============================================================================

/// RAII wrapper for ibv_context (device handle).
///
/// When `owned` is true (created via `open()`), the context is closed in
/// Drop. When `owned` is false (created via `from_raw_borrowed()`), the
/// context belongs to rdma_cm and must NOT be closed — it is destroyed
/// when the owning `rdma_cm_id` is destroyed.
struct IbvContext {
    raw: *mut ibv_context,
    owned: bool,
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

            // If a specific device name is requested, find it directly.
            if let Some(name) = device_name {
                let dev = devices.iter().find(|&&dev| {
                    let cname = ffi::ibv_get_device_name(dev);
                    if cname.is_null() {
                        false
                    } else {
                        let cstr = std::ffi::CStr::from_ptr(cname);
                        cstr.to_string_lossy() == name
                    }
                });
                let dev = match dev {
                    Some(&d) => d,
                    None => {
                        ffi::ibv_free_device_list(device_list);
                        return Err(NetError::Config(format!(
                            "RDMA device '{}' not found",
                            name
                        )));
                    }
                };
                let ctx = ffi::ibv_open_device(dev);
                ffi::ibv_free_device_list(device_list);
                if ctx.is_null() {
                    return Err(NetError::Connection(format!(
                        "ibv_open_device({}) failed",
                        name
                    )));
                }
                info!("IbvContext: opened RDMA device {}", name);
                return Ok(IbvContext {
                    raw: ctx,
                    owned: true,
                });
            }

            // No specific device requested — scan all devices and prefer
            // one with an Active port (avoids mlx5_0 with port Down).
            let mut best_dev: Option<*mut ibv_device> = None;
            let mut best_name = String::new();
            let mut fallback_dev: Option<*mut ibv_device> = None;
            let mut fallback_name = String::new();

            for &dev in devices {
                let cname = ffi::ibv_get_device_name(dev);
                let name_str = if cname.is_null() {
                    "<unknown>".to_string()
                } else {
                    std::ffi::CStr::from_ptr(cname)
                        .to_string_lossy()
                        .into_owned()
                };

                let ctx = ffi::ibv_open_device(dev);
                if ctx.is_null() {
                    debug!("IbvContext: ibv_open_device({}) failed, skipping", name_str);
                    continue;
                }

                // Query port 1 state.
                let mut port_attr: ibv_port_attr = std::mem::zeroed();
                let rc = ffi::powerfs_ibv_query_port(ctx, 1, &mut port_attr);
                if rc == 0 && port_attr.state == ffi::IBV_PORT_ACTIVE {
                    info!("IbvContext: found device {} with Active port", name_str);
                    ffi::ibv_close_device(ctx);
                    best_dev = Some(dev);
                    best_name = name_str;
                    break;
                }

                // Remember first opened device as fallback.
                if fallback_dev.is_none() {
                    fallback_dev = Some(dev);
                    fallback_name = name_str.clone();
                }
                ffi::ibv_close_device(ctx);
            }

            ffi::ibv_free_device_list(device_list);

            let (dev, name_str) = match (best_dev, fallback_dev) {
                (Some(d), _) => (d, best_name),
                (None, Some(d)) => (d, fallback_name),
                (None, None) => {
                    // All ibv_open_device calls failed — try first device raw.
                    let dev = devices[0];
                    let n = ffi::ibv_get_device_name(dev);
                    let name = if n.is_null() {
                        "<unknown>".to_string()
                    } else {
                        std::ffi::CStr::from_ptr(n).to_string_lossy().into_owned()
                    };
                    (dev, name)
                }
            };

            let ctx = ffi::ibv_open_device(dev);
            if ctx.is_null() {
                return Err(NetError::Connection(format!(
                    "ibv_open_device({}) failed",
                    name_str
                )));
            }

            info!("IbvContext: opened RDMA device {}", name_str);
            Ok(IbvContext {
                raw: ctx,
                owned: true,
            })
        }
    }

    /// Create a non-owning wrapper around a context pointer borrowed from
    /// rdma_cm (e.g. `cm_id->verbs`). The context will NOT be closed in
    /// Drop — it is owned by the rdma_cm_id.
    fn from_raw_borrowed(raw: *mut ibv_context) -> Self {
        IbvContext { raw, owned: false }
    }

    fn as_ptr(&self) -> *mut ibv_context {
        self.raw
    }

    /// Query the port's LID (Local Identifier) assigned by the subnet manager.
    ///
    /// Used to set `ah_attr.dlid` in `ibv_modify_qp` when transitioning a
    /// manually-created QP to RTR. For loopback connections (same machine),
    /// the peer's LID equals the local port's LID.
    fn query_port_lid(&self, port_num: u8) -> NetResult<u16> {
        let mut port_attr: ibv_port_attr = unsafe { std::mem::zeroed() };
        // Use the C wrapper (powerfs_ibv_query_port) which correctly invokes
        // the macro → ___ibv_query_port, handling struct size detection.
        // The direct ibv_query_port symbol is the OLD compat version with
        // a smaller struct layout that would read lid at wrong offsets.
        let rc = unsafe { ffi::powerfs_ibv_query_port(self.raw, port_num, &mut port_attr) };
        if rc != 0 {
            return Err(NetError::Connection(format!(
                "ibv_query_port(port={}) failed (rc={})",
                port_num, rc
            )));
        }
        let lid = port_attr.lid;
        if lid == 0 || lid == 0xFFFF {
            return Err(NetError::Connection(format!(
                "ibv_query_port(port={}) returned invalid lid=0x{:04x}",
                port_num, lid
            )));
        }
        debug!(
            "[ctx] port {} lid=0x{:04x} state={} active_speed={}",
            port_num, lid, port_attr.state, port_attr.active_speed
        );
        Ok(lid)
    }
}

impl Drop for IbvContext {
    fn drop(&mut self) {
        if self.owned && !self.raw.is_null() {
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
            Ok(IbvMr { raw: mr, _buf: buf })
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
    fn create(context: &IbvContext, cqe: i32, channel: Option<IbvCompChannel>) -> NetResult<Self> {
        unsafe {
            let ch_ptr = match &channel {
                Some(c) => c.as_ptr(),
                None => ptr::null_mut(),
            };
            let cq = ffi::ibv_create_cq(context.as_ptr(), cqe, ptr::null_mut(), ch_ptr, 0);
            if cq.is_null() {
                return Err(NetError::Connection(format!(
                    "ibv_create_cq(cqe={}) failed",
                    cqe
                )));
            }
            Ok(IbvCq { raw: cq, channel })
        }
    }

    fn as_ptr(&self) -> *mut ibv_cq {
        self.raw
    }

    /// Returns the completion channel's fd, if the CQ was created with one.
    fn channel_fd(&self) -> Option<RawFd> {
        self.channel.as_ref().map(|ch| ch.fd())
    }

    /// Returns the completion channel's raw pointer as `usize`, if the
    /// CQ was created with one. Used by `spawn_blocking` closures that
    /// need to call `ibv_get_cq_event` (which takes the channel pointer).
    fn channel_ptr(&self) -> Option<usize> {
        self.channel.as_ref().map(|ch| ch.as_ptr() as usize)
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
            let rc =
                ffi::powerfs_ibv_req_notify_cq(self.as_ptr(), if solicited_only { 1 } else { 0 });
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
                self.channel
                    .as_ref()
                    .ok_or_else(|| {
                        NetError::Connection("CQ has no completion channel".to_string())
                    })?
                    .as_ptr(),
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
///
/// When `owned` is true (created via `create()`), `ibv_destroy_qp` is
/// called in Drop. When `owned` is false (created via
/// `from_raw_non_owning()`), the QP is managed by rdma_cm and destroyed
/// when the owning `rdma_cm_id` is destroyed.
struct IbvQp {
    raw: *mut ibv_qp,
    owned: bool,
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
                return Err(NetError::Connection("ibv_create_qp failed".to_string()));
            }
            Ok(IbvQp {
                raw: qp,
                owned: true,
            })
        }
    }

    /// Wrap a QP created by `rdma_create_qp` (non-owning — the QP is
    /// destroyed by `rdma_destroy_id`, not by this wrapper's Drop).
    fn from_raw_non_owning(raw: *mut ibv_qp) -> Self {
        IbvQp { raw, owned: false }
    }

    /// Wrap a QP created by `ibv_create_qp` (owning — this wrapper calls
    /// `ibv_destroy_qp` in Drop). Used when `cm_id->qp` is NOT set.
    fn from_raw_owned(raw: *mut ibv_qp) -> Self {
        IbvQp { raw, owned: true }
    }

    /// Read the local QP number assigned by hardware.
    fn qp_num(&self) -> u32 {
        unsafe { (*self.raw).qp_num }
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
            attr.pkey_index = 0;
            let mask = ffi::IBV_QP_STATE
                | ffi::IBV_QP_PKEY_INDEX
                | ffi::IBV_QP_PORT
                | ffi::IBV_QP_ACCESS_FLAGS;
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

    /// Transition QP to RTR (Ready to Receive). Requires peer QPN and
    /// the peer's LID (Local Identifier) for the address handle.
    ///
    /// `peer_lid` is the destination LID to set in `ah_attr.dlid`. For
    /// loopback (same machine), this equals the local port's LID. For
    /// multi-host, it should be the peer's LID obtained from route
    /// resolution.
    fn modify_to_rtr(&self, dest_qp_num: u32, port_num: u8, peer_lid: u16) -> NetResult<()> {
        unsafe {
            let mut attr: ffi::ibv_qp_attr = std::mem::zeroed();
            attr.qp_state = ffi::IBV_QPS_RTR;
            attr.path_mtu = 3; // IBV_MTU_1024
            attr.dest_qp_num = dest_qp_num;
            attr.rq_psn = 0;
            attr.max_dest_rd_atomic = 1;
            attr.min_rnr_timer = 12; // ~0.5s
            attr.ah_attr.port_num = port_num;
            attr.ah_attr.dlid = peer_lid;
            // is_global=0 by default (zeroed). For IB (non-RoCE), LID-only
            // routing is sufficient; GRH is not needed.
            let mask = ffi::IBV_QP_STATE
                | ffi::IBV_QP_PATH_MTU
                | ffi::IBV_QP_DEST_QPN
                | ffi::IBV_QP_RQ_PSN
                | ffi::IBV_QP_MAX_DEST_RD_ATOMIC
                | ffi::IBV_QP_MIN_RNR_TIMER
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
                | ffi::IBV_QP_TIMEOUT
                | ffi::IBV_QP_RETRY_CNT
                | ffi::IBV_QP_RNR_RETRY
                | ffi::IBV_QP_MAX_QP_RD_ATOMIC;
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

    /// Transition QP from INIT → RTR → RTS using explicit peer QPN, port,
    /// and peer LID (Local Identifier).
    ///
    /// Used after `rdma_connect`/`rdma_accept` when the QP was created via
    /// `ibv_create_qp` (not `rdma_create_qp`). The peer QPN is extracted from
    /// the rdma_cm event's `param.conn.qp_num` field (populated by rdma_cm
    /// from the REQ/REP message). The port_num comes from `cm_id->port_num`.
    /// The peer_lid is the peer's LID for the address handle (dlid). For
    /// loopback connections (same machine), it equals the local port LID.
    fn transition_to_rtr_rts(&self, peer_qpn: u32, port_num: u8, peer_lid: u16) -> NetResult<()> {
        debug!(
            "[qp] transition_to_rtr_rts: peer_qpn={}, port_num={}, peer_lid={}",
            peer_qpn, port_num, peer_lid
        );
        self.modify_to_rtr(peer_qpn, port_num, peer_lid)?;
        debug!("[qp] RTR transition succeeded (dlid={})", peer_lid);
        self.modify_to_rts()?;
        debug!("[qp] RTS transition succeeded");
        Ok(())
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
                imm_data: 0,
                wr: ffi::ibv_send_wr_wr {
                    rdma_remote: 0,
                    rdma_rkey: 0,
                    _atomic_pad: [0; 5],
                },
                qp_type_xrc_remote_srqn: 0,
                _qp_type_pad: [0; 4],
                _bind_mw_tso: [0; 48],
            };
            let mut bad_wr: *mut ibv_send_wr = ptr::null_mut();
            let rc = ffi::powerfs_ibv_post_send(self.as_ptr(), &mut wr, &mut bad_wr);
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
            let rc = ffi::powerfs_ibv_post_recv(
                self.as_ptr(),
                &wr as *const ibv_recv_wr as *mut ibv_recv_wr,
                &mut bad_wr,
            );
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
        if self.owned && !self.raw.is_null() {
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

// (removed blocking helpers — hardware RDMA uses AsyncFd, not spawn_blocking)

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
    /// Pre-posted recv buffers (always in-flight on the RQ).  This is the
    /// standard RDMA pattern: the receiver must post recv WRs *before*
    /// the sender sends, otherwise the sender exhausts RNR retries and
    /// completes with `IBV_WC_RETRY_EXC_ERR`.  We pre-post N buffers at
    /// connection time and replenish after each completion so there is
    /// always a recv buffer available.  FIFO order is guaranteed for RC
    /// QPs, so `pop_front` returns the buffer that just completed.
    recv_pre_posted: tokio::sync::Mutex<std::collections::VecDeque<Arc<IbvMr>>>,
    /// Owned rdma_cm_id — keeps the QP (and borrowed context) alive.
    /// When None, the connection was created without rdma_cm (raw ibv only).
    cm_id: Option<RdmaCmIdPtr>,
    /// Per-connection PD (when created via rdma_cm). Must outlive the MR pool.
    pd: Option<Arc<IbvPd>>,
    /// Event channel for the connect side. Must outlive cm_id. For accepted
    /// connections this is None (the listener owns the shared channel).
    event_channel: Option<RdmaEventChannelPtr>,
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
            _pad_tail: [0; 4],
        }; 1]
    }

    /// Pre-post N recv buffers onto the RQ.
    ///
    /// This is called once after the QP reaches RTS (READY_TO_SEND) state.
    /// It acquires N buffers from the MR pool, posts N recv WRs, and stores
    /// them in `recv_pre_posted`.  This ensures that when the remote side
    /// sends data, a recv buffer is already available — preventing
    /// `IBV_WC_RETRY_EXC_ERR` (RNR retry exhaustion).
    async fn pre_post_recv(&self, n: usize) -> NetResult<()> {
        let mut queue = self.recv_pre_posted.lock().await;
        for i in 0..n {
            let mr = self.mr_pool.try_acquire().await.ok_or_else(|| {
                NetError::Connection(format!(
                    "MR pool exhausted during pre-post recv ({}/{})",
                    i, n
                ))
            })?;
            let sge = ibv_sge {
                addr: mr.addr() as u64,
                length: self.mr_pool.buf_size() as u32,
                lkey: mr.lkey(),
            };
            let wr_id = mr.addr() as u64;
            self.qp.post_recv(&sge, wr_id)?;
            queue.push_back(mr);
        }
        Ok(())
    }

    /// Wait for a single CQ completion using AsyncFd (hardware RDMA).
    ///
    /// For hardware RDMA, all verb calls are fast user-space operations:
    /// - `ibv_poll_cq` is a memory read from the CQ ring buffer
    /// - `ibv_req_notify_cq` arms the hardware interrupt
    /// - `ibv_get_cq_event` reads from the channel fd (non-blocking after
    ///   AsyncFd reports readability, because the hardware interrupt
    ///   guarantees the event is ready)
    ///
    /// AsyncFd provides efficient epoll-based notification without
    /// `spawn_blocking` overhead (5-10μs per call), preserving RDMA's
    /// sub-microsecond latency advantage.
    async fn wait_cq_completion(
        cq: &IbvCq,
        _async_fd: Option<&Arc<CqAsyncFd>>,
    ) -> NetResult<ibv_wc> {
        // ====================================================================
        // ROOT27 FIX 2: ABANDON AsyncFd CQ-event notification ENTIRELY for
        // completion polling.  The previous req_notify-in-loop fix still
        // left rare-but-real hang cases where hardware CQE was in the ring
        // buffer but AsyncFd never woke (lost edge, or get_cq_event blocked
        // spuriously).  Symptom confirmed: qp409 first-accept write_all()
        // for HandshakeResponse never returned despite kernel having
        // received the bytes AND a subsequent msg_type=24 req POST_SEND_OK
        // firing on the same QP.  This is classic CQ-notify deadlock.
        //
        // Replacement: simple yield_now driven busy-poll loop.  `ibv_poll_cq`
        // is a pure user-space CQ-ring pointer read (no syscall), so this
        // adds ~20-50us latency per completion while guaranteeing NO lost
        // wakeups ever.  Tokio yield avoids starving other tasks.  This
        // matches the lesson learned from soft-RoCE: "CQ event fd based
        // notification through AsyncFd is unreliable; use poll loops with
        // yield_now or spawn_blocking."
        //
        // The `_async_fd` parameter and `req_notify`/`get_cq_event` are now
        // unused in this path; the Arc<CqAsyncFd> is kept in RdmaChannel
        // only for potential future low-power mode (not latency path).
        // ====================================================================
        let mut spins: u64 = 0;
        loop {
            let mut wcs = Self::zeroed_wc();
            let n = cq.poll(&mut wcs);
            if n > 0 {
                return Ok(wcs[0]);
            }
            spins += 1;
            // After 10k pure spins (~microseconds), back off to yield to
            // the tokio runtime so accept loop / other conns don't starve.
            if spins & 0x3F == 0 {
                // Yield every 64 iterations to be friendly to other tasks
                // on this worker (single digit microsecond overhead).
                tokio::task::yield_now().await;
            } else {
                // Short in-user-space pause hint (nop-pause, ~dozen cycles).
                // (std::hint::spin_loop on aarch64 yields; on x86 it's PAUSE.)
                std::hint::spin_loop();
            }
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
            std::ptr::copy_nonoverlapping(data.as_ptr(), mr.addr() as *mut u8, data.len());
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

    /// Receive data via RDMA recv using pre-posted buffers.
    ///
    /// Waits for a pre-posted recv WR to complete, copies the received
    /// bytes into `out`, then immediately replenishes the RQ by posting
    /// the same buffer again.  This ensures there is always a recv buffer
    /// available for incoming sends.
    ///
    /// Returns the bytes received (copied into `out`).
    async fn recv(&self, out: &mut [u8]) -> NetResult<usize> {
        // Wait for a recv CQ completion (one of the pre-posted buffers).
        let wc = Self::wait_cq_completion(&self.recv_cq, self.recv_async_fd.as_ref()).await?;

        if wc.status != ffi::IBV_WC_SUCCESS {
            // On error, remove the failed buffer from the queue and return
            // it to the pool.  The connection is likely dead anyway.
            let mut queue = self.recv_pre_posted.lock().await;
            if let Some(mr) = queue.pop_front() {
                drop(queue);
                self.mr_pool.release(mr).await;
            }
            return Err(NetError::Connection(format!(
                "RDMA recv completion error (status={})",
                wc.status
            )));
        }

        // Pop the completed buffer (FIFO: RC QP guarantees in-order
        // completions, so the front of the queue is the one that just
        // completed).
        let mr = {
            let mut queue = self.recv_pre_posted.lock().await;
            queue
                .pop_front()
                .ok_or_else(|| NetError::Connection("recv: no pre-posted buffer".to_string()))?
        };

        // Copy received data from the MR buffer to the caller's buffer.
        let byte_len = wc.byte_len as usize;
        let copy_len = std::cmp::min(byte_len, out.len());
        unsafe {
            std::ptr::copy_nonoverlapping(mr.addr() as *const u8, out.as_mut_ptr(), copy_len);
        }

        // Replenish: re-post the same buffer for the next recv.
        let sge = ibv_sge {
            addr: mr.addr() as u64,
            length: self.mr_pool.buf_size() as u32,
            lkey: mr.lkey(),
        };
        let wr_id = mr.addr() as u64;
        self.qp.post_recv(&sge, wr_id)?;

        // Push the buffer back to the pre-posted queue.
        self.recv_pre_posted.lock().await.push_back(mr);

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
        debug!("[client] connect to {}", addr);
        // 1. Create rdma_cm event channel + id
        let channel = unsafe { ffi::rdma_create_event_channel() };
        if channel.is_null() {
            return Err(NetError::Connection(
                "rdma_create_event_channel failed".to_string(),
            ));
        }
        let channel = RdmaEventChannelPtr(channel);

        let cm_id = {
            let mut cm_id_raw: *mut rdma_cm_id = ptr::null_mut();
            let rc = unsafe {
                ffi::rdma_create_id(channel.0, &mut cm_id_raw, ptr::null_mut(), ffi::RDMA_PS_TCP)
            };
            if rc != 0 {
                return Err(NetError::Connection(format!(
                    "rdma_create_id failed (rc={})",
                    rc
                )));
            }
            RdmaCmIdPtr(cm_id_raw)
        };
        debug!("[client] cm_id created (ptr={:p})", cm_id.0);

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
        debug!("[client] rdma_resolve_addr called, waiting for ADDR_RESOLVED...");

        // Wait for ADDR_RESOLVED event
        wait_cm_event(channel.0 as usize, ffi::RDMA_CM_EVENT_ADDR_RESOLVED).await?;
        debug!("[client] ADDR_RESOLVED received");

        // 3. Resolve route
        let rc = unsafe { ffi::rdma_resolve_route(cm_id.0, 3000) };
        debug!("[client] rdma_resolve_route rc={}", rc);
        if rc != 0 {
            let errno = std::io::Error::last_os_error();
            return Err(NetError::Connection(format!(
                "rdma_resolve_route failed (rc={}, errno={})",
                rc, errno
            )));
        }
        debug!("[client] rdma_resolve_route called, waiting for ROUTE_RESOLVED...");
        wait_cm_event(channel.0 as usize, ffi::RDMA_CM_EVENT_ROUTE_RESOLVED).await?;
        debug!("[client] ROUTE_RESOLVED received (cm_id ptr={:p})", cm_id.0);

        // 4. After route resolution, cm_id->verbs points to the device
        //    context that rdma_cm resolved to. We MUST use this context
        //    (not the transport's pre-allocated one) for all per-connection
        //    resources — using a different context causes SIGSEGV in the
        //    RDMA driver.
        let (ctx, pd) = {
            let verbs_ptr = unsafe { (*cm_id.0).verbs };
            if verbs_ptr.is_null() {
                return Err(NetError::Connection(
                    "cm_id->verbs is null after ROUTE_RESOLVED".to_string(),
                ));
            }
            let ctx = IbvContext::from_raw_borrowed(verbs_ptr);
            debug!("[client] got device context from cm_id");

            // 5. Allocate per-connection PD and create CQs from the cm_id's
            //    device context.
            let pd = IbvPd::alloc(&ctx)?;
            // Debug: verify pd->context matches verbs_ptr
            let pd_ctx = unsafe { (*pd.as_ptr()).context };
            debug!(
                "[client] pd->context={:p}, verbs_ptr={:p}, match={}",
                pd_ctx,
                verbs_ptr,
                pd_ctx == verbs_ptr
            );
            (ctx, pd)
        };
        let send_comp_ch = IbvCompChannel::create(&ctx)?;
        let recv_comp_ch = IbvCompChannel::create(&ctx)?;
        let send_cq = IbvCq::create(&ctx, 32, Some(send_comp_ch))?;
        let recv_cq = IbvCq::create(&ctx, 32, Some(recv_comp_ch))?;

        // 6. Create QP via rdma_create_qp (C wrapper). This sets both
        //    cm_id->qp AND the internal id_priv->qp, so rdma_cm correctly
        //    manages QP state transitions (RESET→INIT→RTR→RTS) during
        //    rdma_connect. The C wrapper avoids Rust struct layout issues.
        let rc = unsafe {
            ffi::powerfs_rdma_create_qp(
                cm_id.0,
                pd.as_ptr(),
                ptr::null_mut(),
                send_cq.as_ptr(),
                recv_cq.as_ptr(),
                ptr::null_mut(),
                16,
                16,
                1,
                1,
                0,
                ffi::IBV_QPT_RC,
                1,
            )
        };
        if rc != 0 {
            let errno = std::io::Error::last_os_error();
            return Err(NetError::Connection(format!(
                "rdma_create_qp failed (rc={}, errno={})",
                rc, errno
            )));
        }
        let qp_ptr = unsafe { (*cm_id.0).qp };
        let local_qpn = unsafe { (*qp_ptr).qp_num };
        debug!(
            "[client] rdma_create_qp succeeded (local_qpn={}), rdma_cm will manage QP transitions",
            local_qpn
        );
        // Use non-owning wrapper: rdma_destroy_id will destroy the QP.
        let qp = IbvQp::from_raw_non_owning(qp_ptr);

        // 7. Build MR pool from the per-connection PD.
        let mr_pool = MrPool::new(&pd, self.config.rdma_buf_num, self.config.rdma_buf_size)?;

        // 8. Connect. Since rdma_create_qp was called, qp_num is ignored
        //    — rdma_cm uses cm_id->qp->qp_num and handles QP state
        //    transitions (RESET→INIT→RTR→RTS) automatically. C wrapper
        //    avoids Rust struct layout issues.
        let rc = unsafe { ffi::powerfs_rdma_connect(cm_id.0, 7, 7, 1, 3, 7, 0, 0) };
        if rc != 0 {
            return Err(NetError::Connection(format!(
                "rdma_connect failed (rc={})",
                rc
            )));
        }
        debug!("[client] rdma_connect called (local_qpn={})", qp.qp_num());

        // 8a. Wait for ESTABLISHED. rdma_cm auto-transitions the QP
        //     (RESET→INIT→RTR→RTS) based on connection events.
        loop {
            let (event_type, _, _) = wait_for_any_cm_event(channel.0 as usize).await?;
            if event_type == ffi::RDMA_CM_EVENT_ESTABLISHED {
                debug!("[client] ESTABLISHED received, QP should be in RTS");
                break;
            }
            // Other events are handled by wait_for_any_cm_event (errors)
            // or looped over (repeated ADDR_RESOLVED etc.)
        }

        // 9. Build AsyncFd for send/recv CQ completion channels.
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
            mr_pool: Arc::new(mr_pool),
            peer: addr,
            send_async_fd,
            recv_async_fd,
            recv_pre_posted: tokio::sync::Mutex::new(std::collections::VecDeque::new()),
            cm_id: Some(cm_id),
            pd: Some(Arc::new(pd)),
            event_channel: Some(channel),
        };

        // Pre-post recv buffers so the server can send responses
        // without hitting RNR (Receiver Not Ready) retry exhaustion.
        channel_inner.pre_post_recv(4).await?;

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

        info!("RdmaTransport: listening on {} (rdma_cm)", addr);

        Ok(Box::new(RdmaListenerAdapter {
            channel,
            listen_id: cm_id,
            bind_addr: addr,
            context: self.context.clone(),
            pd: self.pd.clone(),
            config: self.config.clone(),
            pending: parking_lot::Mutex::new(std::collections::HashMap::new()),
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

/// Async wrapper for `poll(event_channel_fd) + rdma_get_cm_event`.
///
/// Uses `spawn_blocking` to avoid blocking the tokio worker thread.
/// The event channel fd is polled with a 30s timeout. When an event is
/// available, `rdma_get_cm_event` is called (which returns immediately
/// because poll confirmed readability). The returned event is NOT acked;
/// the caller is responsible for `rdma_ack_cm_event`.
///
/// Takes the channel pointer as `usize` (not `*mut`) because raw pointers
/// are `!Send`, which would make the returned future `!Send` and break
/// the `async_trait` `Send` bound on `Transport::connect`.
///
/// Returns the event pointer as `usize` for the same reason: `*mut` is
/// `!Send` so it cannot be the return type of `spawn_blocking`.
async fn async_get_cm_event(channel_raw: usize) -> NetResult<usize> {
    if channel_raw == 0 {
        return Err(NetError::Connection("null event channel".to_string()));
    }

    tokio::task::spawn_blocking(move || -> NetResult<usize> {
        let channel = channel_raw as *mut rdma_event_channel;
        let fd = unsafe { (*channel).fd };

        let mut pfd = libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        };
        let prc = unsafe { libc::poll(&mut pfd, 1, 30000) };
        if prc == 0 {
            return Err(NetError::Connection(
                "rdma_cm: timeout (30s) waiting for event".to_string(),
            ));
        }
        if prc < 0 {
            let e = std::io::Error::last_os_error();
            return Err(NetError::Connection(format!(
                "rdma_cm: poll on channel fd {} failed: {}",
                fd, e
            )));
        }

        let mut event: *mut rdma_cm_event = ptr::null_mut();
        let rc = unsafe { ffi::rdma_get_cm_event(channel, &mut event) };
        if rc != 0 {
            return Err(NetError::Connection(format!(
                "rdma_get_cm_event failed (rc={})",
                rc
            )));
        }
        if event.is_null() {
            return Err(NetError::Connection(
                "rdma_get_cm_event returned null".to_string(),
            ));
        }
        Ok(event as usize)
    })
    .await
    .map_err(|e| NetError::Connection(format!("spawn_blocking join error: {}", e)))?
}

/// Wait for a specific rdma_cm event type on the channel (async, non-blocking).
async fn wait_cm_event(channel_raw: usize, expected: ffi::rdma_cm_event_type) -> NetResult<()> {
    wait_cm_event_with_id(channel_raw, expected)
        .await
        .map(|_| ())
}

/// Wait for a specific rdma_cm event type and return the event (async).
///
/// Uses `async_get_cm_event` (spawn_blocking) to avoid blocking the tokio
/// worker thread. Acks unexpected events and retries. Error events cause
/// immediate return.
async fn wait_cm_event_with_id(
    channel_raw: usize,
    expected: ffi::rdma_cm_event_type,
) -> NetResult<()> {
    loop {
        let event_raw = async_get_cm_event(channel_raw).await?;
        let event = event_raw as *mut rdma_cm_event;

        let actual = unsafe { (*event).event };
        let status = unsafe { (*event).status };
        debug!(
            "[rdma_cm] got event {} (status={}), expected {}",
            actual, status, expected
        );

        if actual == expected {
            unsafe { ffi::rdma_ack_cm_event(event) };
            return Ok(());
        }

        debug!(
            "[rdma_cm] unexpected event {} (expected {}), acking and retrying",
            actual, expected
        );
        unsafe { ffi::rdma_ack_cm_event(event) };

        // If it's an error event, return error instead of looping forever.
        if actual == ffi::RDMA_CM_EVENT_ADDR_ERROR
            || actual == ffi::RDMA_CM_EVENT_ROUTE_ERROR
            || actual == ffi::RDMA_CM_EVENT_CONNECT_ERROR
            || actual == ffi::RDMA_CM_EVENT_UNREACHABLE
            || actual == ffi::RDMA_CM_EVENT_REJECTED
        {
            return Err(NetError::Connection(format!(
                "rdma_cm: received error event {} (status={}, expected {})",
                actual, status, expected
            )));
        }
    }
}

/// Wait for the next rdma_cm event and return (event_type, peer_qpn, cm_id).
///
/// Acks the event before returning. The peer QPN is extracted from the
/// event's `param.private_data` (where we put it in the conn_param) or
/// from `param.qp_num` (populated by some rdma_cm implementations).
///
/// Returns cm_id as `usize` to avoid `!Send` raw pointer issues.
async fn wait_for_any_cm_event(
    channel_raw: usize,
) -> NetResult<(ffi::rdma_cm_event_type, u32, usize)> {
    let event_raw = async_get_cm_event(channel_raw).await?;
    let event = event_raw as *mut rdma_cm_event;

    let event_type = unsafe { (*event).event };
    let status = unsafe { (*event).status };
    let cm_id = unsafe { (*event).id };

    // Extract peer QPN: first try param.qp_num, then private_data.
    let param_qp_num = unsafe { (*event).param.qp_num };
    let pd_ptr = unsafe { (*event).param.private_data };
    let pd_len = unsafe { (*event).param.private_data_len };
    let peer_qpn = if param_qp_num != 0 {
        param_qp_num
    } else if !pd_ptr.is_null() && pd_len >= 4 {
        // Extract QPN from private_data (first 4 bytes, native endian).
        let bytes = unsafe { std::slice::from_raw_parts(pd_ptr as *const u8, 4) };
        u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
    } else {
        0
    };

    debug!(
        "[rdma_cm] got event {} (status={}, param_qp={}, pd_len={}, peer_qpn={})",
        event_type, status, param_qp_num, pd_len, peer_qpn
    );

    unsafe { ffi::rdma_ack_cm_event(event) };

    // Check for error events
    if event_type == ffi::RDMA_CM_EVENT_ADDR_ERROR
        || event_type == ffi::RDMA_CM_EVENT_ROUTE_ERROR
        || event_type == ffi::RDMA_CM_EVENT_CONNECT_ERROR
        || event_type == ffi::RDMA_CM_EVENT_UNREACHABLE
        || event_type == ffi::RDMA_CM_EVENT_REJECTED
    {
        return Err(NetError::Connection(format!(
            "rdma_cm: received error event {} (status={})",
            event_type, status
        )));
    }

    Ok((event_type, peer_qpn, cm_id as usize))
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
    ) -> (
        Box<dyn AsyncRead + Send + Unpin>,
        Box<dyn AsyncWrite + Send + Unpin>,
    ) {
        let channel = self.channel.clone();
        let reader = RdmaReadHalf {
            channel: self.channel.clone(),
            recv_buf: Vec::new(),
            recv_pos: 0,
            pending_recv: None,
        };
        let writer = RdmaWriteHalf {
            channel,
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
            self.pending_recv = Some(Box::pin(async move { channel.recv_owned().await }));
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

/// RDMA write half. Implements `AsyncWrite` by posting an RDMA send
/// directly in `poll_write` (no buffering). This matches TCP semantics
/// where `poll_write` sends data immediately, so `write_all` works
/// without requiring an explicit `flush()`.
struct RdmaWriteHalf {
    channel: Arc<RdmaChannel>,
    /// In-flight send future (if any). When `Some`, we are waiting for an
    /// async RDMA send to complete.
    pending_send: Option<SendFut>,
}

impl AsyncWrite for RdmaWriteHalf {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
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

        // No in-flight send. If buf is empty, nothing to do.
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        // Truncate to MR buffer size (partial write — write_all will retry).
        let max_len = self.channel.mr_pool.buf_size();
        let write_len = std::cmp::min(buf.len(), max_len);

        let channel = self.channel.clone();
        let data = buf[..write_len].to_vec();
        self.pending_send = Some(Box::pin(async move { channel.send(&data).await }));

        // Poll the newly created future (may complete fast path).
        let pending = self.pending_send.as_mut().unwrap();
        match pending.as_mut().poll(cx) {
            Poll::Ready(result) => {
                self.pending_send = None;
                match result {
                    Ok(()) => Poll::Ready(Ok(write_len)),
                    Err(e) => Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Wait for any in-flight send to complete.
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
        Poll::Ready(Ok(()))
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

/// Accept-side state that was prepared during CONNECT_REQUEST processing,
/// waiting for the matching ESTABLISHED event on the listener event channel.
struct PendingAccepted {
    pd: Arc<IbvPd>,
    send_cq: Arc<IbvCq>,
    recv_cq: Arc<IbvCq>,
    send_async_fd: Option<Arc<AsyncFd<CqChannelFd>>>,
    recv_async_fd: Option<Arc<AsyncFd<CqChannelFd>>>,
    mr_pool: Arc<MrPool>,
    qp: IbvQp,
    cm_id: RdmaCmIdPtr,
}

/// RDMA listener adapter. Wraps rdma_cm listen id + event channel.
pub struct RdmaListenerAdapter {
    channel: RdmaEventChannelPtr,
    listen_id: RdmaCmIdPtr,
    bind_addr: SocketAddr,
    context: Arc<IbvContext>,
    pd: Arc<IbvPd>,
    config: TransportConfig,
    /// Per-connection items built during CONNECT_REQUEST handling,
    /// keyed by the child rdma_cm_id pointer cast to usize. These are
    /// awaiting the matching ESTABLISHED event on the shared listener
    /// event channel.
    pending: parking_lot::Mutex<std::collections::HashMap<usize, PendingAccepted>>,
}

#[async_trait::async_trait]
impl TransportListener for RdmaListenerAdapter {
    async fn accept(&self) -> NetResult<Box<dyn TransportStream>> {
        loop {
            let event_raw = async_get_cm_event(self.channel.0 as usize).await?;
            let event = event_raw as *mut rdma_cm_event;

            let event_type = unsafe { (*event).event };
            let new_id = unsafe { (*event).id };

            // Extract peer QPN: first try param.qp_num, then private_data.
            let param_qp_num = unsafe { (*event).param.qp_num };
            let pd_ptr = unsafe { (*event).param.private_data };
            let pd_len = unsafe { (*event).param.private_data_len };
            let peer_qpn = if param_qp_num != 0 {
                param_qp_num
            } else if !pd_ptr.is_null() && pd_len >= 4 {
                let bytes = unsafe { std::slice::from_raw_parts(pd_ptr as *const u8, 4) };
                u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
            } else {
                0
            };
            debug!(
                "[server] event {} (param_qp={}, pd_len={}, peer_qpn={})",
                event_type, param_qp_num, pd_len, peer_qpn
            );
            // IMPORTANT: Ack the event AFTER reading all fields we need from it.
            // rdma_ack_cm_event may free event memory.
            unsafe { ffi::rdma_ack_cm_event(event) };

            match event_type {
                ffi::RDMA_CM_EVENT_CONNECT_REQUEST => {
                    if new_id.is_null() {
                        warn!("accept: CONNECT_REQUEST with null id");
                        continue;
                    }

                    // Use the new cm_id's device context (verbs) for all
                    // per-connection resources; the listener's pre-allocated
                    // context is only for the listen-id endpoint.
                    let verbs_ptr = unsafe { (*new_id).verbs };
                    if verbs_ptr.is_null() {
                        warn!("accept: new_id->verbs is null");
                        unsafe { ffi::rdma_destroy_id(new_id) };
                        continue;
                    }
                    let ctx = IbvContext::from_raw_borrowed(verbs_ptr);

                    // Per-connection PD + CQs from the cm_id's context.
                    let pd = match IbvPd::alloc(&ctx) {
                        Ok(p) => Arc::new(p),
                        Err(e) => {
                            warn!("accept: IbvPd::alloc failed: {}", e);
                            unsafe { ffi::rdma_destroy_id(new_id) };
                            continue;
                        }
                    };
                    let send_comp_ch = match IbvCompChannel::create(&ctx) {
                        Ok(c) => c,
                        Err(e) => {
                            warn!("accept: IbvCompChannel::create(send) failed: {}", e);
                            unsafe { ffi::rdma_destroy_id(new_id) };
                            continue;
                        }
                    };
                    let recv_comp_ch = match IbvCompChannel::create(&ctx) {
                        Ok(c) => c,
                        Err(e) => {
                            warn!("accept: IbvCompChannel::create(recv) failed: {}", e);
                            unsafe { ffi::rdma_destroy_id(new_id) };
                            continue;
                        }
                    };
                    let send_cq = match IbvCq::create(&ctx, 32, Some(send_comp_ch)) {
                        Ok(c) => Arc::new(c),
                        Err(e) => {
                            warn!("accept: IbvCq::create(send) failed: {}", e);
                            unsafe { ffi::rdma_destroy_id(new_id) };
                            continue;
                        }
                    };
                    let recv_cq = match IbvCq::create(&ctx, 32, Some(recv_comp_ch)) {
                        Ok(c) => Arc::new(c),
                        Err(e) => {
                            warn!("accept: IbvCq::create(recv) failed: {}", e);
                            unsafe { ffi::rdma_destroy_id(new_id) };
                            continue;
                        }
                    };

                    // Create QP via rdma_create_qp (C wrapper). This sets both
                    // cm_id->qp AND internal id_priv->qp, so rdma_cm correctly
                    // manages QP state transitions during rdma_accept.
                    let rc = unsafe {
                        ffi::powerfs_rdma_create_qp(
                            new_id,
                            pd.as_ptr(),
                            ptr::null_mut(),
                            send_cq.as_ptr(),
                            recv_cq.as_ptr(),
                            ptr::null_mut(),
                            16,
                            16,
                            1,
                            1,
                            0,
                            ffi::IBV_QPT_RC,
                            1,
                        )
                    };
                    if rc != 0 {
                        let errno = std::io::Error::last_os_error();
                        warn!("rdma_create_qp failed (rc={}, errno={})", rc, errno);
                        unsafe { ffi::rdma_destroy_id(new_id) };
                        continue;
                    }
                    let qp_ptr = unsafe { (*new_id).qp };
                    let local_qpn = unsafe { (*qp_ptr).qp_num };
                    debug!(
                        "[server] rdma_create_qp succeeded (local_qpn={}), rdma_cm will manage QP transitions",
                        local_qpn
                    );
                    // Use non-owning wrapper: rdma_destroy_id will destroy the QP.
                    let qp = IbvQp::from_raw_non_owning(qp_ptr);

                    // Build MR pool from the per-connection PD.
                    let mr_pool =
                        match MrPool::new(&pd, self.config.rdma_buf_num, self.config.rdma_buf_size)
                        {
                            Ok(p) => Arc::new(p),
                            Err(e) => {
                                warn!("accept: MrPool::new failed: {}", e);
                                unsafe { ffi::rdma_destroy_id(new_id) };
                                continue;
                            }
                        };

                    // Call rdma_accept; QP state is managed by rdma_cm.
                    // The matching ESTABLISHED event will arrive asynchronously
                    // on the shared listener event channel.
                    let rc = unsafe { ffi::powerfs_rdma_accept(new_id, 7, 7, 1, 3, 7, 0, 0) };
                    if rc != 0 {
                        warn!("rdma_accept failed (rc={})", rc);
                        unsafe { ffi::rdma_destroy_id(new_id) };
                        continue;
                    }
                    debug!(
                        "[server] rdma_accept called (local_qpn={}), deferring ESTABLISHED to outer event loop",
                        qp.qp_num()
                    );

                    // Build AsyncFd wrappers now — they don't require QP in RTS yet.
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

                    // Wrap new_id for lifetime RAII; store everything into
                    // pending map keyed by id pointer.
                    let cm_id = RdmaCmIdPtr(new_id);
                    let pending = PendingAccepted {
                        pd,
                        send_cq,
                        recv_cq,
                        send_async_fd,
                        recv_async_fd,
                        mr_pool,
                        qp,
                        cm_id,
                    };
                    self.pending.lock().insert(new_id as usize, pending);

                    // Loop: outer accept() will either return ESTABLISHED for
                    // this id on a future iteration, or pick up more
                    // CONNECT_REQUESTs. Do NOT block here waiting for
                    // ESTABLISHED inline — that starves concurrent connects.
                    continue;
                }
                ffi::RDMA_CM_EVENT_ESTABLISHED => {
                    // Match against our pending set.
                    let key = new_id as usize;
                    let pending = self.pending.lock().remove(&key);
                    let pending = match pending {
                        Some(p) => p,
                        None => {
                            debug!(
                                "accept: ESTABLISHED event for unknown id {:p}, ignoring",
                                new_id
                            );
                            continue;
                        }
                    };
                    debug!(
                        "[server] ESTABLISHED received and acked (local_qpn={})",
                        pending.qp.qp_num()
                    );

                    let peer = self.bind_addr;
                    let channel = RdmaChannel {
                        qp: Arc::new(pending.qp),
                        send_cq: pending.send_cq,
                        recv_cq: pending.recv_cq,
                        mr_pool: pending.mr_pool,
                        peer,
                        send_async_fd: pending.send_async_fd,
                        recv_async_fd: pending.recv_async_fd,
                        recv_pre_posted: tokio::sync::Mutex::new(std::collections::VecDeque::new()),
                        cm_id: Some(pending.cm_id),
                        pd: Some(pending.pd),
                        event_channel: None,
                    };
                    // Pre-post recv buffers so the client can send data
                    // without hitting RNR (Receiver Not Ready) retries.
                    channel.pre_post_recv(4).await?;
                    info!("RdmaListenerAdapter: accepted connection from {}", peer);
                    return Ok(Box::new(RdmaStream {
                        channel: Arc::new(channel),
                        peer,
                    }));
                }
                ffi::RDMA_CM_EVENT_DISCONNECTED
                | ffi::RDMA_CM_EVENT_CONNECT_ERROR
                | ffi::RDMA_CM_EVENT_REJECTED
                | ffi::RDMA_CM_EVENT_UNREACHABLE => {
                    // Clean up matching pending (if any).
                    let key = new_id as usize;
                    if self.pending.lock().remove(&key).is_some() {
                        warn!(
                            "accept: id {:p} error event {}, destroying pending connection",
                            new_id, event_type
                        );
                    }
                    continue;
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

    /// Verify that RdmaTransport::new either succeeds (hardware present)
    /// or returns a Config error (no hardware). With the Active-port
    /// selection logic, it should prefer mlx5_1 (port Active) over
    /// mlx5_0 (port Down).
    #[test]
    fn test_rdma_transport_init() {
        let config = TransportConfig::default();
        match RdmaTransport::new(config) {
            Ok(transport) => {
                assert_eq!(transport.name(), "rdma");
            }
            Err(NetError::Config(_)) => {}
            Err(e) => panic!("unexpected error: {:?}", e),
        }
    }

    /// RDMA local loopback test: bind + connect + send + recv on the same
    /// machine via the IPoIB interface (192.168.100.3 = mlx5_1).
    ///
    /// rdma_get_cm_event is blocking, so server and client must run on
    /// separate OS threads to avoid deadlock.
    ///
    /// Requires: Mellanox mlx5_1 with Active port and IPoIB configured.
    #[test]
    #[ignore = "requires RDMA hardware with IPoIB"]
    fn test_rdma_loopback_send_recv() {
        let ip =
            std::env::var("POWERFS_RDMA_TEST_IP").unwrap_or_else(|_| "192.168.100.3".to_string());
        let port: u16 = 18900;
        let addr: SocketAddr = format!("{}:{}", ip, port).parse().unwrap();

        let config = TransportConfig::default();
        let transport = match RdmaTransport::new(config) {
            Ok(t) => t,
            Err(NetError::Config(_)) => {
                eprintln!("skipping: no RDMA hardware");
                return;
            }
            Err(e) => panic!("RdmaTransport::new failed: {:?}", e),
        };

        let transport = std::sync::Arc::new(transport);

        // --- Server thread: bind + accept + read ---
        let server_transport = transport.clone();
        let server_handle = std::thread::Builder::new()
            .name("rdma-test-server".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(async move {
                    // Bind and listen
                    let listener = server_transport.bind(addr).await.expect("bind failed");
                    eprintln!("[server] listening on {}", addr);

                    // Accept one connection
                    let stream = listener.accept().await.expect("accept failed");
                    eprintln!("[server] accepted connection");

                    // Read data from client
                    use tokio::io::AsyncReadExt;
                    let (mut read_half, _write_half) = stream.split();
                    let mut buf = vec![0u8; 64];
                    let n = read_half.read(&mut buf).await.expect("read failed");
                    eprintln!("[server] read {} bytes: {:?}", n, &buf[..n]);
                    assert_eq!(&buf[..n], b"hello rdma!");
                    eprintln!("[server] data verified OK");
                });
            })
            .expect("failed to spawn server thread");

        // Give server time to bind + listen
        std::thread::sleep(std::time::Duration::from_millis(500));

        // --- Client: connect + write ---
        let client_transport = transport.clone();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let stream = client_transport
                .connect(addr)
                .await
                .expect("connect failed");
            eprintln!("[client] connected to {}", addr);

            // Write data to server
            use tokio::io::AsyncWriteExt;
            let (_read_half, mut write_half) = stream.split();
            write_half
                .write_all(b"hello rdma!")
                .await
                .expect("write failed");
            write_half.flush().await.expect("flush failed");
            eprintln!("[client] sent 'hello rdma!'");
        });

        // Wait for server to finish
        server_handle.join().expect("server thread panicked");
        eprintln!("test_rdma_loopback_send_recv: PASSED");
    }
}
