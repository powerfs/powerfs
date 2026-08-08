//! Work 封装 - IO Loop → Worker 的请求载体
//!
//! 设计参考: BeeGFS MultiWorkQueue + 内核端 powerfs_request
//!
//! IoLoop 读取完整帧后封装为 Work, 推送到 WorkQueue;
//! Worker 从 WorkQueue 取出 Work, 调用 handler 处理业务逻辑.
//! Work 持有 Arc<ClientConn> 引用, Worker 可直接:
//!   - 查询/修改连接状态 (state, stats)
//!   - 获取 holder_uuid (lease 校验)
//!   - 发送响应 (conn.send_response)
//!   - 添加/移除 lease (conn.add_lease / remove_lease)

use std::sync::Arc;
use std::time::Instant;

use crate::client_conn::ClientConn;
use crate::protocol::NetMessage;

/// IO Loop → Worker 的请求封装
#[derive(Debug)]
pub struct Work {
    /// 客户端连接 (Arc 引用, Worker 可查/改状态)
    pub conn: Arc<ClientConn>,
    /// 收到的请求消息
    pub msg: NetMessage,
    /// 接收时间 (用于延迟统计)
    pub recv_at: Instant,
}

impl Work {
    pub fn new(conn: Arc<ClientConn>, msg: NetMessage) -> Self {
        Self {
            conn,
            msg,
            recv_at: Instant::now(),
        }
    }

    /// 从接收到现在经历的时长 (用于延迟统计)
    pub fn queue_latency(&self) -> std::time::Duration {
        self.recv_at.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ClientType, FrameFlags, FrameHeader, MsgType, STATUS_OK};
    use tokio::sync::mpsc;

    fn make_conn() -> Arc<ClientConn> {
        let (tx, _rx) = mpsc::unbounded_channel::<Vec<u8>>();
        ClientConn::new(
            42,
            "127.0.0.1:1234".parse().unwrap(),
            ClientType::Kernel,
            crate::protocol::CHANNEL_DATA,
            0,
            tx,
        )
    }

    fn make_msg() -> NetMessage {
        let header = FrameHeader::new(
            MsgType::Ping.as_u16(),
            FrameFlags::new(FrameFlags::REQUEST),
            1,
            0,
        )
        .with_status(STATUS_OK);
        NetMessage::new(header)
    }

    #[test]
    fn test_work_new() {
        let conn = make_conn();
        let msg = make_msg();
        let work = Work::new(conn.clone(), msg);

        assert_eq!(work.conn.id, 42);
        assert_eq!(work.msg.header.seq, 1);
        assert!(work.queue_latency().as_nanos() < 1_000_000_000);
    }
}
