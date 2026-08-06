//! IoLoop - IO 循环 (固定数量, 每个 tokio task 管理一批连接的读写)
//!
//! 设计参考: BeeGFS StreamListenerV2 (epoll 多路复用) + 内核端 per-CPT scheduler
//!
//! 职责:
//!   - 从分配的连接读取帧 (tokio async read, epoll 驱动)
//!   - 解析帧为 NetMessage
//!   - 封装为 Work 推送到 WorkQueue
//!   - write_task 消费 outbound_rx, 将响应/通知帧写入 TCP
//!   - 连接断开时执行清理: registry.unregister (带身份校验) + handler.on_disconnect
//!
//! 不处理业务逻辑, 只做 IO 收发 + 断连清理.
//! 连接按 hash(client_id) % N 分配到 IO Loop.

use std::sync::Arc;

use log::{debug, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::client_conn::{ClientConn, CloseHandle, ConnRegistry, ConnState};
use crate::errors::{NetError, NetResult};
use crate::flow_control::FlowController;
use crate::protocol::{FrameFlags, FrameHeader, MsgType, NetMessage, PROTOCOL_VERSION, STATUS_OK};
use crate::server_connection::{NetHandler, ServerConnectionManager};
use crate::work::Work;

/// IO Loop (固定数量, 每个管理一批连接的读写)
pub struct IoLoop {
    pub id: usize,
    /// 推送到 WorkQueue 的发送端
    work_tx: mpsc::Sender<Work>,
    /// 连接注册表 (断连清理时注销)
    registry: Arc<ConnRegistry>,
    /// 业务处理器 (断连通知)
    handler: Arc<dyn NetHandler>,
    /// 会话管理器 (断连注销 session, 可选)
    manager: Option<Arc<ServerConnectionManager>>,
    /// 流控控制器 (断连时注销连接统计)
    flow_ctrl: Arc<FlowController>,
}

impl IoLoop {
    pub fn new(
        id: usize,
        work_tx: mpsc::Sender<Work>,
        registry: Arc<ConnRegistry>,
        handler: Arc<dyn NetHandler>,
        manager: Option<Arc<ServerConnectionManager>>,
        flow_ctrl: Arc<FlowController>,
    ) -> Self {
        Self {
            id,
            work_tx,
            registry,
            handler,
            manager,
            flow_ctrl,
        }
    }

    /// 管理一个连接 (spawn 一个 tokio task)
    ///
    /// 参数:
    ///   - stream: TCP 连接 (IoLoop 接管读写)
    ///   - conn: ClientConn (持有 outbound_tx, 供 Worker/notify 使用)
    ///   - outbound_rx: 出站帧接收端 (write_task 消费, 写入 TCP)
    pub fn manage(
        self: Arc<Self>,
        stream: TcpStream,
        conn: Arc<ClientConn>,
        outbound_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        let work_tx = self.work_tx.clone();
        let registry = self.registry.clone();
        let handler = self.handler.clone();
        let manager = self.manager.clone();
        let flow_ctrl = self.flow_ctrl.clone();

        tokio::spawn(async move {
            let peer = conn.addr;
            stream.set_nodelay(true).ok();
            let (read_half, write_half) = stream.into_split();

            // 设置 close_handle: disconnect() 通过此通道通知 read_task 退出
            let (shutdown_tx, shutdown_rx) = mpsc::channel::<()>(1);
            let handle = CloseHandle::new(shutdown_tx);
            conn.set_close_handle(handle).await;

            Self::run_connection(
                conn,
                read_half,
                write_half,
                work_tx,
                shutdown_rx,
                outbound_rx,
                peer,
                registry,
                handler,
                manager,
                flow_ctrl,
            )
            .await;
        });
    }

    /// 运行连接的读写循环 (内部方法)
    ///
    /// 完整流程:
    ///   1. spawn write_task: 消费 outbound_rx, 写入 write_half
    ///   2. spawn read_task: 读取帧 → 封装 Work → 推送 WorkQueue
    ///   3. 等待任一 task 结束
    ///   4. 标记 conn.state = Closed
    ///   5. 执行断连清理: registry.unregister (带身份校验) + handler.on_disconnect
    #[allow(clippy::too_many_arguments)]
    async fn run_connection(
        conn: Arc<ClientConn>,
        mut read_half: OwnedReadHalf,
        mut write_half: OwnedWriteHalf,
        work_tx: mpsc::Sender<Work>,
        mut shutdown_rx: mpsc::Receiver<()>,
        mut outbound_rx: mpsc::UnboundedReceiver<Vec<u8>>,
        peer: std::net::SocketAddr,
        registry: Arc<ConnRegistry>,
        handler: Arc<dyn NetHandler>,
        manager: Option<Arc<ServerConnectionManager>>, // 目前未使用, 保留以备将来扩展
        flow_ctrl: Arc<FlowController>,
    ) {
        let _ = &manager; // 避免未使用变量警告
        // write_task: 独占 write_half, 消费 outbound_rx (响应帧 + 通知帧)
        let write_task = tokio::spawn(async move {
            while let Some(frame) = outbound_rx.recv().await {
                if let Err(e) = write_half.write_all(&frame).await {
                    warn!("IoLoop write_task: write error: {:?}", e);
                    break;
                }
            }
        });

        // read_task: 读取帧 → Work → WorkQueue
        let read_conn = conn.clone();
        let read_flow_ctrl = flow_ctrl.clone();
        let read_task = tokio::spawn(async move {
            loop {
                // 检查关闭信号 (非阻塞)
                match shutdown_rx.try_recv() {
                    Ok(_) => break,
                    Err(mpsc::error::TryRecvError::Empty) => {}
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                }
                // 检查连接状态
                if *read_conn.state.read().await == ConnState::Closing {
                    break;
                }
                // 读取帧
                match Self::read_frame(&mut read_half).await {
                    Ok(msg) => {
                        // === P3.10 防错乱校验 (服务端稳定性第一) ===
                        // 1. protocol_ver 必须匹配 (版本升级一致性检查)
                        if msg.header.protocol_ver != PROTOCOL_VERSION {
                            warn!(
                                "IoLoop: protocol_ver mismatch from {}: got={} expected={}, closing",
                                peer, msg.header.protocol_ver, PROTOCOL_VERSION
                            );
                            read_conn.stats.write().await.error_count += 1;
                            break;
                        }
                        // 2. route_hash channel 位必须匹配连接 channel (防帧串连接)
                        let frame_channel = msg.header.route_hash & 0x01;
                        if frame_channel != read_conn.channel {
                            warn!(
                                "IoLoop: channel mismatch from {}: frame={} conn={}, closing",
                                peer, frame_channel, read_conn.channel
                            );
                            read_conn.stats.write().await.error_count += 1;
                            break;
                        }
                        // 3. route_hash 高7位校验 (route_hash=0 时跳过, 兼容发现阶段)
                        if msg.header.route_hash != 0 {
                            let frame_hash = msg.header.route_hash >> 1;
                            let conn_hash = read_conn.route_hash >> 1;
                            if frame_hash != conn_hash {
                                warn!(
                                    "IoLoop: route_hash mismatch from {}: frame=0x{:02x} conn=0x{:02x}, closing",
                                    peer, msg.header.route_hash, read_conn.route_hash
                                );
                                read_conn.stats.write().await.error_count += 1;
                                break;
                            }
                        }

                        read_conn.touch().await;
                        read_conn.stats.write().await.request_count += 1;

                        // 处理 Ping (控制帧, 直接回复, 不走 Worker)
                        if let Some(MsgType::Ping) = msg.msg_type() {
                            let lf = read_flow_ctrl.current_load_factor();
                            let mut resp_header = FrameHeader::new(
                                MsgType::Ping.as_u16(),
                                FrameFlags::new(FrameFlags::RESPONSE),
                                msg.header.seq,
                                0,
                            )
                            .with_status(STATUS_OK);
                            // Phase 2: stamp load_factor so clients can probe
                            // server load via Ping without sending real requests.
                            resp_header.set_load_factor(lf);
                            let resp = NetMessage::new(resp_header);
                            let _ = read_conn.send_response(&resp);
                            continue;
                        }

                        // 封装 Work 推送到 WorkQueue
                        let work = Work::new(read_conn.clone(), msg);
                        if work_tx.send(work).await.is_err() {
                            debug!("IoLoop: WorkQueue closed, stopping read loop");
                            break;
                        }
                    }
                    Err(e) => {
                        read_conn.stats.write().await.error_count += 1;
                        if e.is_eof() {
                            info!("IoLoop: client {} disconnected (EOF)", peer);
                        } else {
                            warn!("IoLoop: read_frame error from {}: {:?}", peer, e);
                        }
                        break;
                    }
                }
            }
        });

        // 等待任一 task 结束
        tokio::select! {
            _ = read_task => {
                debug!("IoLoop: read_task ended for {}", peer);
            }
            _ = write_task => {
                debug!("IoLoop: write_task ended for {}", peer);
            }
        }

        // 标记连接已关闭
        *conn.state.write().await = ConnState::Closed;

        // === 流控: 注销连接统计 (停止该连接的统计收集) ===
        flow_ctrl.unregister_conn(conn.id);

        // === 断连清理 ===
        // 从注册表注销 (带身份校验, 防止误删同 client_id 的其他连接).
        // 注意: 不再调用 mgr.unregister_session(), 因为它内部调用
        // registry.unregister(client_id, None) 不带身份校验, 会误删
        // 同 client_id 的 data/meta 通道连接.
        if let Some(removed_conn) = registry.unregister(conn.id, Some(&conn)).await {
            // 通知 handler 执行业务清理 (释放 lease 等)
            handler.on_disconnect(removed_conn.id).await;

            // 记录断连日志 (原 unregister_session 的功能)
            let stats = removed_conn.stats.read().await;
            info!(
                "[Server] Client disconnected: id={}, duration={}s, requests={}, errors={}",
                removed_conn.id,
                stats.connected_at.elapsed().as_secs(),
                stats.request_count,
                stats.error_count
            );
        }

        info!("IoLoop: connection {} closed and cleaned up", peer);
    }

    /// 读取一个完整的帧 (header + body + data)
    async fn read_frame(reader: &mut OwnedReadHalf) -> NetResult<NetMessage> {
        let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
        reader.read_exact(&mut hdr_buf).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                NetError::Protocol("client disconnected (EOF)".into())
            } else {
                NetError::Io(e)
            }
        })?;

        let header = FrameHeader::decode(&hdr_buf)
            .ok_or_else(|| NetError::Protocol("invalid frame header".into()))?;

        let total_len = header.data_len as usize;
        let body_len = header.body_len as usize;

        let mut payload = Vec::with_capacity(total_len);
        if total_len > 0 {
            payload.resize(total_len, 0u8);
            reader.read_exact(&mut payload).await?;
        }

        let body = payload[..body_len].to_vec();
        let data = payload[body_len..].to_vec();

        Ok(NetMessage::new(header).with_body(body).with_data(data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::ClientType;
    use crate::request_context::RequestContext;

    /// 简单的 Echo handler (用于测试)
    struct EchoHandler;

    #[async_trait::async_trait]
    impl NetHandler for EchoHandler {
        async fn handle(
            &self,
            _ctx: &mut RequestContext,
            msg: &NetMessage,
        ) -> NetResult<NetMessage> {
            let resp_header = FrameHeader::new(
                msg.header.msg_type,
                FrameFlags::new(FrameFlags::RESPONSE),
                msg.header.seq,
                msg.body.len() as u32,
            )
            .with_status(STATUS_OK);
            Ok(NetMessage::new(resp_header).with_body(msg.body.clone()))
        }
    }

    fn make_io_loop(work_tx: mpsc::Sender<Work>) -> Arc<IoLoop> {
        let registry = Arc::new(ConnRegistry::new());
        let handler = Arc::new(EchoHandler) as Arc<dyn NetHandler>;
        let flow_ctrl = Arc::new(FlowController::with_defaults());
        Arc::new(IoLoop::new(
            0,
            work_tx,
            registry,
            handler,
            None,
            flow_ctrl,
        ))
    }

    #[tokio::test]
    async fn test_io_loop_new() {
        let (tx, _rx) = mpsc::channel::<Work>(16);
        let io_loop = make_io_loop(tx);
        assert_eq!(io_loop.id, 0);
    }

    #[tokio::test]
    async fn test_read_frame_eof() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let connect_handle =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });

        let (server_stream, _) = listener.accept().await.unwrap();
        let client_stream = connect_handle.await.unwrap();
        drop(client_stream);

        let (mut read_half, _write_half) = server_stream.into_split();
        let result = IoLoop::read_frame(&mut read_half).await;
        assert!(result.is_err(), "read_frame should fail on EOF");
        assert!(result.unwrap_err().is_eof(), "error should be EOF");
    }

    #[tokio::test]
    async fn test_read_frame_valid() {
        use crate::protocol::{build_frame, HandshakeRequest};
        use tokio::io::AsyncWriteExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let connect_handle =
            tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });

        let (server_stream, _) = listener.accept().await.unwrap();
        let mut client_stream = connect_handle.await.unwrap();

        let req = HandshakeRequest::new(ClientType::Fuse, 42, 0);
        let mut req_buf = vec![0u8; HandshakeRequest::SIZE];
        req.encode(&mut req_buf);

        let frame = build_frame(
            MsgType::Handshake.as_u16(),
            FrameFlags::new(FrameFlags::REQUEST),
            1,
            &req_buf,
            &[],
        );
        client_stream.write_all(&frame).await.unwrap();
        drop(client_stream);

        let (mut read_half, _write_half) = server_stream.into_split();
        let msg = IoLoop::read_frame(&mut read_half).await.unwrap();
        assert_eq!(msg.msg_type(), Some(MsgType::Handshake));
        assert_eq!(msg.body.len(), HandshakeRequest::SIZE);
        assert!(msg.data.is_empty());
    }
}
