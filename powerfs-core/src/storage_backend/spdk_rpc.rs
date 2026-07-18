//! SPDK JSON-RPC client
//!
//! 通过 Unix socket 连接进程内的 SPDK JSON-RPC server,调用 `bdev_nvme_attach_controller`
//! 等 RPC 方法来动态管理设备。
//!
//! 只在 `spdk` feature 下编译 (真实 SPDK 环境才有 RPC server)。
//! `spdk-stub` 模式下不编译,attach 走 stub 路径直接调 `add_device`。

#![cfg(feature = "spdk")]

use crate::storage_backend::{StorageBackendError, StorageResult};
use base64::Engine;
use log::{debug, info, warn};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// 默认 SPDK JSON-RPC Unix socket 路径
pub const DEFAULT_SPDK_RPC_SOCKET: &str = "/var/tmp/spdk.sock";

/// SPDK JSON-RPC 2.0 client
///
/// 通过 Unix socket 连接 SPDK target 进程的 JSON-RPC server。
/// 在进程内模式下,SPDK reactor 和 RPC server 都在当前进程内运行,
/// 连接的是 `/var/tmp/spdk.sock` (可通过配置覆盖)。
pub struct SpdkRpcClient {
    socket_path: PathBuf,
}

impl SpdkRpcClient {
    pub fn new<P: AsRef<Path>>(socket_path: P) -> Self {
        Self {
            socket_path: socket_path.as_ref().to_path_buf(),
        }
    }

    /// 使用默认 socket 路径 `/var/tmp/spdk.sock`
    pub fn with_default_socket() -> Self {
        Self::new(DEFAULT_SPDK_RPC_SOCKET)
    }

    /// 轮询等待 SPDK RPC server 就绪
    ///
    /// SPDK subsystem 初始化是异步的,`powerfs_spdk_init` 返回后 RPC server
    /// 可能还没开始监听。这个函数会反复尝试连接 socket,直到成功或超时。
    pub async fn wait_ready(&self, timeout: Duration) -> StorageResult<()> {
        let deadline = Instant::now() + timeout;
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            match UnixStream::connect(&self.socket_path).await {
                Ok(_) => {
                    debug!(
                        "SPDK RPC server ready at {} (attempt {})",
                        self.socket_path.display(),
                        attempt
                    );
                    return Ok(());
                }
                Err(e) => {
                    if Instant::now() >= deadline {
                        return Err(StorageBackendError::InvalidOperation(format!(
                            "SPDK RPC server not ready at {} after {:?} (last error: {})",
                            self.socket_path.display(),
                            timeout,
                            e
                        )));
                    }
                    debug!(
                        "Waiting for SPDK RPC server at {} (attempt {}: {})",
                        self.socket_path.display(),
                        attempt,
                        e
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }

    /// 底层:发送 JSON-RPC 2.0 请求并读取响应
    ///
    /// SPDK JSON-RPC server 使用换行符分隔的 JSON 请求/响应。
    /// 请求格式: `{"jsonrpc":"2.0","id":<n>,"method":"<method>","params":{...}}\n`
    async fn call_rpc(&self, method: &str, params: Value) -> StorageResult<Value> {
        let id = next_rpc_id();
        let request = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        });

        let stream = UnixStream::connect(&self.socket_path).await.map_err(|e| {
            StorageBackendError::InvalidOperation(format!(
                "failed to connect to SPDK RPC socket {}: {}",
                self.socket_path.display(),
                e
            ))
        })?;

        let mut request_bytes = serde_json::to_vec(&request).map_err(|e| {
            StorageBackendError::InvalidOperation(format!("failed to serialize RPC request: {}", e))
        })?;
        request_bytes.push(b'\n');

        // split 成读写两半,用 BufReader 包装读端以支持 read_until
        let (read_half, mut write_half) = tokio::io::split(stream);
        let mut reader = BufReader::new(read_half);

        write_half.write_all(&request_bytes).await.map_err(|e| {
            StorageBackendError::InvalidOperation(format!("failed to write RPC request: {}", e))
        })?;

        // 读取响应 (以换行符结尾的 JSON)
        let mut buf = Vec::with_capacity(4096);
        reader.read_until(b'\n', &mut buf).await.map_err(|e| {
            StorageBackendError::InvalidOperation(format!("failed to read RPC response: {}", e))
        })?;

        let response: Value = serde_json::from_slice(&buf).map_err(|e| {
            StorageBackendError::InvalidOperation(format!(
                "failed to parse RPC response: {} (raw: {:?})",
                e,
                String::from_utf8_lossy(&buf)
            ))
        })?;

        // 检查错误
        if let Some(error) = response.get("error") {
            let code = error.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
            let message = error
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("unknown error");
            return Err(StorageBackendError::InvalidOperation(format!(
                "SPDK RPC '{}' failed (code {}): {}",
                method, code, message
            )));
        }

        // 返回 result 字段
        response.get("result").cloned().ok_or_else(|| {
            StorageBackendError::InvalidOperation(format!(
                "SPDK RPC '{}' returned no result field",
                method
            ))
        })
    }

    /// 调用 `bdev_nvme_attach_controller` attach 一个 NVMe 控制器
    ///
    /// SPDK RPC 方法: `bdev_nvme_attach_controller`
    /// 参数:
    /// - name: 控制器名称 (如 "Nvme0")
    /// - trtype: transport 类型 ("PCIe" / "TCP" / "RDMA")
    /// - traddr: transport 地址 (PCIe 模式下是 PCI BDF,如 "0000:03:00.0")
    /// - trsvcid: transport service ID (PCIe 下为空,TCP/RDMA 下是端口)
    /// - subnqn: subsystem NQN (PCIe 下为空)
    ///
    /// 返回: 创建的 bdev 名称列表 (如 ["Nvme0n1"])
    pub async fn attach_nvme_controller(
        &self,
        name: &str,
        trtype: &str,
        traddr: &str,
        trsvcid: Option<&str>,
        subnqn: Option<&str>,
    ) -> StorageResult<Vec<String>> {
        let mut params = json!({
            "name": name,
            "trtype": trtype,
            "traddr": traddr,
        });

        if let Some(svcid) = trsvcid {
            params["trsvcid"] = json!(svcid);
        }
        if let Some(nqn) = subnqn {
            params["subnqn"] = json!(nqn);
        }

        info!(
            "Attaching NVMe controller via SPDK RPC: name={} trtype={} traddr={}",
            name, trtype, traddr
        );

        let result = self.call_rpc("bdev_nvme_attach_controller", params).await?;

        // SPDK 返回格式可能有两种:
        // 1. 直接返回数组: ["Nvme0n1"]
        // 2. 嵌套格式: {"bdevs": [{"name": "Nvme0n1", ...}, ...]}
        let names: Vec<String> = if let Some(arr) = result.as_array() {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        } else if let Some(bdevs_obj) = result.get("bdevs").and_then(|b| b.as_array()) {
            bdevs_obj
                .iter()
                .filter_map(|b| {
                    b.get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string())
                })
                .collect()
        } else {
            return Err(StorageBackendError::InvalidOperation(format!(
                "SPDK RPC attach returned unexpected format: {:?}",
                result
            )));
        };

        if names.is_empty() {
            return Err(StorageBackendError::InvalidOperation(format!(
                "SPDK RPC attach succeeded but no bdevs returned for controller {}",
                name
            )));
        }

        debug!("Attached NVMe controller {}, bdevs: {:?}", name, names);
        Ok(names)
    }

    /// 调用 `bdev_nvme_detach_controller` detach 一个 NVMe 控制器
    pub async fn detach_nvme_controller(&self, name: &str) -> StorageResult<()> {
        let params = json!({ "name": name });
        info!("Detaching NVMe controller via SPDK RPC: name={}", name);
        self.call_rpc("bdev_nvme_detach_controller", params).await?;
        Ok(())
    }

    /// 调用 `bdev_get_bdevs` 列出所有 bdev
    pub async fn list_bdevs(&self) -> StorageResult<Vec<String>> {
        let params = json!({});
        let result = self.call_rpc("bdev_get_bdevs", params).await?;

        let bdevs = if let Some(arr) = result.as_array() {
            arr
        } else if let Some(obj) = result.get("bdevs").and_then(|b| b.as_array()) {
            obj
        } else {
            return Err(StorageBackendError::InvalidOperation(format!(
                "SPDK RPC bdev_get_bdevs returned unexpected format: {:?}",
                result
            )));
        };

        Ok(bdevs
            .iter()
            .filter_map(|b| {
                b.get("name")
                    .and_then(|n| n.as_str())
                    .map(|s| s.to_string())
            })
            .collect())
    }

    /// 调用 `bdev_get_bdevs` 检查 RPC server 是否真正可用
    ///
    /// 比 `wait_ready` 更严格:不仅 socket 可连,还能响应 RPC 请求。
    /// 用于确认 SPDK subsystem 初始化完成。
    pub async fn ping(&self) -> StorageResult<()> {
        self.list_bdevs().await?;
        Ok(())
    }

    /// 完整的就绪检查:socket 连通 + 能响应 RPC
    pub async fn wait_fully_ready(&self, timeout: Duration) -> StorageResult<()> {
        let deadline = Instant::now() + timeout;
        loop {
            // 先等 socket 可连
            self.wait_ready(Duration::from_secs(1)).await?;
            // 再 ping 确认能响应
            match self.ping().await {
                Ok(_) => {
                    info!("SPDK RPC server fully ready");
                    return Ok(());
                }
                Err(e) => {
                    if Instant::now() >= deadline {
                        return Err(e);
                    }
                    warn!(
                        "SPDK RPC server connected but not responding, retrying: {}",
                        e
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }

    /// 创建 NVMe-oF subsystem
    ///
    /// SPDK RPC 方法: `nvmf_create_subsystem`
    /// 参数:
    /// - nqn: subsystem NQN
    /// - serial_number: 序列号
    /// - allow_any_host: 是否允许任意主机连接
    pub async fn create_nvmf_subsystem(
        &self,
        nqn: &str,
        serial_number: &str,
        allow_any_host: bool,
    ) -> StorageResult<()> {
        let params = json!({
            "nqn": nqn,
            "serial_number": serial_number,
            "allow_any_host": allow_any_host,
        });
        info!("Creating NVMe-oF subsystem via SPDK RPC: nqn={}", nqn);
        self.call_rpc("nvmf_create_subsystem", params).await?;
        Ok(())
    }

    /// 为 NVMe-oF subsystem 添加 TCP listener
    ///
    /// SPDK RPC 方法: `nvmf_subsystem_add_listener`
    /// 参数:
    /// - nqn: subsystem NQN
    /// - trtype: transport type ("TCP")
    /// - traddr: 监听地址
    /// - trsvcid: 监听端口
    pub async fn add_nvmf_listener(
        &self,
        nqn: &str,
        trtype: &str,
        traddr: &str,
        trsvcid: &str,
    ) -> StorageResult<()> {
        let params = json!({
            "nqn": nqn,
            "trtype": trtype,
            "traddr": traddr,
            "trsvcid": trsvcid,
        });
        info!(
            "Adding NVMe-oF listener via SPDK RPC: nqn={} trtype={} traddr={}:{}",
            nqn, trtype, traddr, trsvcid
        );
        self.call_rpc("nvmf_subsystem_add_listener", params).await?;
        Ok(())
    }

    /// 为 NVMe-oF subsystem 添加 bdev
    ///
    /// SPDK RPC 方法: `nvmf_subsystem_add_ns`
    /// 参数:
    /// - nqn: subsystem NQN
    /// - bdev_name: bdev 名称
    /// - nsid: namespace ID (默认 1)
    pub async fn add_nvmf_namespace(
        &self,
        nqn: &str,
        bdev_name: &str,
        nsid: Option<u32>,
    ) -> StorageResult<()> {
        let params = json!({
            "nqn": nqn,
            "bdev_name": bdev_name,
            "nsid": nsid.unwrap_or(1),
        });
        info!(
            "Adding NVMe-oF namespace via SPDK RPC: nqn={} bdev={} nsid={}",
            nqn,
            bdev_name,
            nsid.unwrap_or(1)
        );
        self.call_rpc("nvmf_subsystem_add_ns", params).await?;
        Ok(())
    }

    /// 从 bdev 读取数据 (通过 RPC) - **DEPRECATED**
    ///
    /// SPDK RPC 方法: `bdev_read`
    /// 参数:
    /// - name: bdev 名称
    /// - offset: 偏移量 (字节)
    /// - size: 读取大小 (字节)
    ///
    /// ⚠️ **DEPRECATED - 仅测试用，生产环境必须使用 NVMe-oF 数据面**
    /// 此方法通过 JSON-RPC 传输 IO 数据，性能损失高达 85% 以上，仅用于功能验证。
    /// 生产环境业务 IO 必须通过 NVMe-oF TCP/RDMA 数据通道执行。
    #[deprecated(
        note = "Use NVMe-oF data plane for production IO. This method is for testing only."
    )]
    pub async fn read_bdev(&self, name: &str, offset: u64, size: u64) -> StorageResult<Vec<u8>> {
        let params = json!({
            "name": name,
            "offset": offset,
            "size": size,
        });
        debug!(
            "Reading bdev via SPDK RPC: name={} offset={} size={}",
            name, offset, size
        );
        let result = self.call_rpc("bdev_read", params).await?;

        if let Some(data_str) = result.get("data").and_then(|d| d.as_str()) {
            let data = base64::engine::general_purpose::STANDARD
                .decode(data_str)
                .map_err(|e| {
                    StorageBackendError::InvalidOperation(format!(
                        "failed to decode bdev_read result: {}",
                        e
                    ))
                })?;
            Ok(data)
        } else {
            Err(StorageBackendError::InvalidOperation(
                "bdev_read returned no data field".to_string(),
            ))
        }
    }

    /// 向 bdev 写入数据 (通过 RPC) - **DEPRECATED**
    ///
    /// SPDK RPC 方法: `bdev_write`
    /// 参数:
    /// - name: bdev 名称
    /// - offset: 偏移量 (字节)
    /// - data: 数据 (base64 编码)
    ///
    /// ⚠️ **DEPRECATED - 仅测试用，生产环境必须使用 NVMe-oF 数据面**
    /// 此方法通过 JSON-RPC 传输 IO 数据，性能损失高达 85% 以上，仅用于功能验证。
    /// 生产环境业务 IO 必须通过 NVMe-oF TCP/RDMA 数据通道执行。
    #[deprecated(
        note = "Use NVMe-oF data plane for production IO. This method is for testing only."
    )]
    pub async fn write_bdev(&self, name: &str, offset: u64, data: &[u8]) -> StorageResult<()> {
        let encoded = base64::engine::general_purpose::STANDARD.encode(data);
        let params = json!({
            "name": name,
            "offset": offset,
            "data": encoded,
        });
        debug!(
            "Writing bdev via SPDK RPC: name={} offset={} size={}",
            name,
            offset,
            data.len()
        );
        self.call_rpc("bdev_write", params).await?;
        Ok(())
    }
}

/// 生成简单的递增 RPC ID (线程安全)
fn next_rpc_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// 从配置的 transport_string 解析出 transport 参数
///
/// transport_string 格式:
/// - PCIe 本地设备: "0000:03:00.0" (纯 PCI BDF)
/// - NVMe-oF TCP: "trtype:tcp traddr:10.0.0.1 trsvcid:4420 subnqn:nqn.2016-06.io.spdk:cnode1"
/// - NVMe-oF RDMA: "trtype:rdma traddr:10.0.0.1 trsvcid:4420 subnqn:nqn.2016-06.io.spdk:cnode1"
pub fn parse_transport_string(transport_string: &str) -> StorageResult<TransportParams> {
    let ts = transport_string.trim();

    // 如果是纯 PCI BDF (如 "0000:03:00.0"),按 PCIe 处理
    if is_pci_bdf(ts) {
        return Ok(TransportParams {
            trtype: "PCIe".to_string(),
            traddr: ts.to_string(),
            trsvcid: None,
            subnqn: None,
        });
    }

    // 否则按 "key:value key:value" 格式解析
    let mut trtype = None;
    let mut traddr = None;
    let mut trsvcid = None;
    let mut subnqn = None;

    for part in ts.split_whitespace() {
        if let Some((k, v)) = part.split_once(':') {
            match k.trim() {
                "trtype" => trtype = Some(v.trim().to_string()),
                "traddr" => traddr = Some(v.trim().to_string()),
                "trsvcid" => trsvcid = Some(v.trim().to_string()),
                "subnqn" => subnqn = Some(v.trim().to_string()),
                _ => {}
            }
        }
    }

    let trtype = trtype.ok_or_else(|| {
        StorageBackendError::InvalidOperation(format!(
            "missing trtype in transport_string: {}",
            transport_string
        ))
    })?;
    let traddr = traddr.ok_or_else(|| {
        StorageBackendError::InvalidOperation(format!(
            "missing traddr in transport_string: {}",
            transport_string
        ))
    })?;

    Ok(TransportParams {
        trtype,
        traddr,
        trsvcid,
        subnqn,
    })
}

/// 简单判断字符串是否是 PCI BDF 格式 (如 "0000:03:00.0")
fn is_pci_bdf(s: &str) -> bool {
    // 格式: DDDD:BB:DD.F (domain:bus:device.function)
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 3 {
        return false;
    }
    // 最后一部分应该是 "DD.F" (device.function)
    parts[2].contains('.')
}

/// 解析后的 transport 参数
#[derive(Debug, Clone)]
pub struct TransportParams {
    pub trtype: String,
    pub traddr: String,
    pub trsvcid: Option<String>,
    pub subnqn: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pci_bdf() {
        let p = parse_transport_string("0000:03:00.0").unwrap();
        assert_eq!(p.trtype, "PCIe");
        assert_eq!(p.traddr, "0000:03:00.0");
        assert!(p.trsvcid.is_none());
        assert!(p.subnqn.is_none());
    }

    #[test]
    fn test_parse_tcp() {
        let p = parse_transport_string(
            "trtype:tcp traddr:127.0.0.1 trsvcid:4420 subnqn:nqn.2016-06.io.spdk:cnode1",
        )
        .unwrap();
        assert_eq!(p.trtype, "tcp");
        assert_eq!(p.traddr, "127.0.0.1");
        assert_eq!(p.trsvcid.as_deref(), Some("4420"));
        assert_eq!(p.subnqn.as_deref(), Some("nqn.2016-06.io.spdk:cnode1"));
    }

    #[test]
    fn test_parse_rdma() {
        let p = parse_transport_string(
            "trtype:rdma traddr:10.0.0.1 trsvcid:4420 subnqn:nqn.2016-06.io.spdk:cnode3",
        )
        .unwrap();
        assert_eq!(p.trtype, "rdma");
        assert_eq!(p.traddr, "10.0.0.1");
    }

    #[test]
    fn test_is_pci_bdf() {
        assert!(is_pci_bdf("0000:03:00.0"));
        assert!(is_pci_bdf("03:00.0"));
        assert!(!is_pci_bdf("trtype:tcp traddr:127.0.0.1"));
    }
}
