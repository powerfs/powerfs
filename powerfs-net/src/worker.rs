//! Worker - 业务处理 (固定并发数, 从 WorkQueue 取 Work 处理)
//!
//! 设计参考: BeeGFS Worker (PThread) + 内核端 pfs_scheduler_thread 处理部分
//!
//! 职责:
//!   - 从 WorkQueue 取 Work
//!   - 调用 handler.handle() 处理业务逻辑
//!   - 通过 conn.send_response() 写回响应
//!   - 跳过已关闭连接的 Work
//!   - 更新连接统计
//!
//! Worker 不负责断连清理 — IoLoop 在连接断开时统一处理
//! (registry.unregister + handler.on_disconnect + manager.unregister_session).

use std::sync::Arc;

use log::{debug, error, info};
use tokio::sync::mpsc;

use crate::client_conn::ConnState;
use crate::protocol::{FrameFlags, FrameHeader, NetMessage, STATUS_ERR_SERVER_ERROR};
use crate::server_connection::{NetHandler, ServerConnectionManager};
use crate::work::Work;

/// Worker - 业务处理器 (轻量结构, 可通过 Arc 共享)
///
/// 并发控制由调用方 (server.rs) 通过 Semaphore 实现,
/// Worker 本身只提供 process_work 方法.
pub struct Worker {
    pub id: usize,
    /// 业务处理器 (VolumeNetHandler / MasterHandler 等)
    handler: Arc<dyn NetHandler>,
    /// 会话管理器 (可选, 启用时通过 pipeline 处理请求)
    manager: Option<Arc<ServerConnectionManager>>,
}

impl Worker {
    pub fn new(
        id: usize,
        handler: Arc<dyn NetHandler>,
        manager: Option<Arc<ServerConnectionManager>>,
    ) -> Self {
        Self {
            id,
            handler,
            manager,
        }
    }

    /// 启动 Worker 循环 (顺序处理, 适用于单 Worker 场景)
    ///
    /// 多 Worker 并发场景: server.rs 使用 Semaphore + spawn,
    /// 每个并发任务调用 process_work.
    pub async fn run(self, mut work_rx: mpsc::Receiver<Work>) {
        info!("Worker {} started", self.id);

        while let Some(work) = work_rx.recv().await {
            self.process_work(work).await;
        }

        info!("Worker {} stopped (WorkQueue closed)", self.id);
    }

    /// 处理单个 Work
    pub async fn process_work(&self, work: Work) {
        let conn = &work.conn;
        let seq = work.msg.header.seq;
        let msg_type = work.msg.header.msg_type;

        // 跳过已关闭连接的请求 (IoLoop 可能在 Work 入队后才关闭连接)
        if *conn.state.read().await == ConnState::Closed {
            debug!(
                "Worker {}: skipping work for closed conn {} seq={}",
                self.id, conn.id, seq
            );
            return;
        }

        // 记录入队等待时长
        let queue_latency = work.queue_latency();
        if queue_latency.as_millis() > 10 {
            debug!(
                "Worker {}: high queue latency for conn={} seq={}: {:?}",
                self.id, conn.id, seq, queue_latency
            );
        }

        // 调用业务处理器
        // - 若存在会话管理器, 通过 pipeline 处理 (含中间件、metrics、session 校验)
        // - 否则构建最小 context 直接调用 handler.handle
        let result = if let Some(mgr) = &self.manager {
            mgr.process_with_pipeline(conn.id, &work.msg, self.handler.clone())
                .await
        } else {
            let session_info = crate::request_context::ClientInfo {
                client_id: conn.id,
                client_type: crate::ClientType::Fuse,
                address: std::net::SocketAddr::from(([0, 0, 0, 0], 0)),
            };
            let mut ctx = crate::request_context::RequestContext::new(&session_info, &work.msg);
            self.handler.handle(&mut ctx, &work.msg).await
        };

        match result {
            Ok(resp) => {
                // 发送响应 (通过 conn.outbound_tx → IoLoop write_task)
                if !conn.send_response(&resp) {
                    // 通道关闭 = 连接已断开, IoLoop 会处理清理
                    debug!(
                        "Worker {}: outbound channel closed for conn={} seq={}",
                        self.id, conn.id, seq
                    );
                } else {
                    debug!(
                        "Worker {}: request handled conn={} seq={} type={:#x} status={}",
                        self.id,
                        conn.id,
                        seq,
                        msg_type,
                        resp.header.status
                    );
                }
            }
            Err(e) => {
                error!(
                    "Worker {}: handler error for conn={} seq={} type={:#x}: {:?}",
                    self.id, conn.id, seq, msg_type, e
                );

                // 构造错误响应
                let error_resp = Self::build_error_response(&work.msg);
                let _ = conn.send_response(&error_resp);

                // 更新错误统计
                conn.stats.write().await.error_count += 1;
            }
        }

        // 更新活动时间
        conn.touch().await;
    }

    /// 构造错误响应 (handler 返回 Err 时使用)
    fn build_error_response(req: &NetMessage) -> NetMessage {
        let header = FrameHeader::new(
            req.header.msg_type,
            FrameFlags::new(FrameFlags::RESPONSE),
            req.header.seq,
            0,
        )
        .with_status(STATUS_ERR_SERVER_ERROR);
        NetMessage::new(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_conn::ClientConn;
    use crate::errors::{NetError, NetResult};
    use crate::protocol::{ClientType, FrameFlags, FrameHeader, MsgType, STATUS_OK};
    use crate::request_context::RequestContext;
    use async_trait::async_trait;

    /// 简单的 Echo handler (用于测试)
    struct EchoHandler;

    #[async_trait]
    impl NetHandler for EchoHandler {
        async fn handle(&self, _ctx: &mut RequestContext, msg: &NetMessage) -> NetResult<NetMessage> {
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

    fn make_conn(id: u64) -> Arc<ClientConn> {
        let (tx, _rx) = mpsc::unbounded_channel::<Vec<u8>>();
        ClientConn::new(
            id,
            "127.0.0.1:1234".parse().unwrap(),
            ClientType::Kernel,
            tx,
        )
    }

    fn make_request(seq: u32, body: &[u8]) -> NetMessage {
        let header = FrameHeader::new(
            MsgType::Lookup.as_u16(),
            FrameFlags::new(FrameFlags::REQUEST),
            seq,
            body.len() as u32,
        );
        NetMessage::new(header).with_body(body.to_vec())
    }

    #[tokio::test]
    async fn test_worker_processes_request() {
        let (work_tx, work_rx) = mpsc::channel::<Work>(16);
        let handler = Arc::new(EchoHandler) as Arc<dyn NetHandler>;
        let worker = Worker::new(0, handler, None);

        let conn = make_conn(42);
        let msg = make_request(1, b"hello");

        work_tx.send(Work::new(conn.clone(), msg)).await.unwrap();

        drop(work_tx);
        worker.run(work_rx).await;

        // Worker 不更新 request_count (由 IoLoop 更新)
        let stats = conn.stats.read().await;
        assert_eq!(stats.request_count, 0);
    }

    #[tokio::test]
    async fn test_worker_skips_closed_conn() {
        let (work_tx, work_rx) = mpsc::channel::<Work>(16);
        let handler = Arc::new(EchoHandler) as Arc<dyn NetHandler>;
        let worker = Worker::new(0, handler, None);

        let conn = make_conn(42);
        *conn.state.write().await = ConnState::Closed;

        let msg = make_request(1, b"hello");
        work_tx.send(Work::new(conn.clone(), msg)).await.unwrap();

        drop(work_tx);
        worker.run(work_rx).await;

        let stats = conn.stats.read().await;
        assert_eq!(stats.error_count, 0);
    }

    #[tokio::test]
    async fn test_worker_error_response() {
        struct FailHandler;

        #[async_trait]
        impl NetHandler for FailHandler {
            async fn handle(
                &self,
                _ctx: &mut RequestContext,
                _msg: &NetMessage,
            ) -> NetResult<NetMessage> {
                Err(NetError::ServerError("test error".into()))
            }
        }

        let (work_tx, work_rx) = mpsc::channel::<Work>(16);
        let handler = Arc::new(FailHandler) as Arc<dyn NetHandler>;
        let worker = Worker::new(0, handler, None);

        let conn = make_conn(42);
        let msg = make_request(1, b"hello");
        work_tx.send(Work::new(conn.clone(), msg)).await.unwrap();

        drop(work_tx);
        worker.run(work_rx).await;

        let stats = conn.stats.read().await;
        assert!(stats.error_count >= 1);
    }

    #[tokio::test]
    async fn test_worker_concurrent_processing() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        static CONCURRENT: AtomicUsize = AtomicUsize::new(0);
        static MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);

        struct SlowHandler;

        #[async_trait]
        impl NetHandler for SlowHandler {
            async fn handle(
                &self,
                _ctx: &mut RequestContext,
                msg: &NetMessage,
            ) -> NetResult<NetMessage> {
                let cur = CONCURRENT.fetch_add(1, Ordering::SeqCst) + 1;
                let mut max = MAX_CONCURRENT.load(Ordering::SeqCst);
                while cur > max {
                    match MAX_CONCURRENT.compare_exchange(max, cur, Ordering::SeqCst, Ordering::SeqCst)
                    {
                        Ok(_) => break,
                        Err(v) => max = v,
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                CONCURRENT.fetch_sub(1, Ordering::SeqCst);
                let resp_header = FrameHeader::new(
                    MsgType::Ping.as_u16(),
                    FrameFlags::new(FrameFlags::RESPONSE),
                    msg.header.seq,
                    0,
                )
                .with_status(STATUS_OK);
                Ok(NetMessage::new(resp_header))
            }
        }

        let handler = Arc::new(SlowHandler) as Arc<dyn NetHandler>;
        let worker = Arc::new(Worker::new(0, handler, None));

        // 模拟并发处理 (类似 server.rs 的 Semaphore 模式)
        let semaphore = Arc::new(tokio::sync::Semaphore::new(4));
        let mut handles = Vec::new();

        for i in 0..8 {
            let conn = make_conn(i as u64);
            let msg = make_request(i, b"req");
            let work = Work::new(conn, msg);
            let worker = worker.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            handles.push(tokio::spawn(async move {
                worker.process_work(work).await;
                drop(permit);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // 验证并发度被信号量限制
        assert!(MAX_CONCURRENT.load(Ordering::SeqCst) <= 4);
    }
}
