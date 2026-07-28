use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Instant;

use crate::client_identity::ClientIdentity;
use crate::request_id::RequestId;

/// 请求生命周期状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RequestState {
    /// 已创建，尚未发送
    Init,
    /// 已发送到服务端，等待响应
    Sent,
    /// 正在等待服务端响应 (与 Sent 类似，用于重试逻辑)
    Wait,
    /// 请求超时，准备重发
    Timeout,
    /// 请求正在重新发送
    Resent,
    /// 请求已完成 (成功或失败)
    Complete,
    /// 请求失败 (最终失败，不再重试)
    Failed,
    /// 请求被取消
    Cancelled,
}

impl RequestState {
    /// 是否为终态 (不会再改变)
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            RequestState::Complete | RequestState::Failed | RequestState::Cancelled
        )
    }

    /// 是否为活跃态 (还在进行中)
    pub fn is_active(&self) -> bool {
        !self.is_terminal()
    }

    /// 状态转换规则
    pub fn can_transition_to(&self, target: RequestState) -> bool {
        match self {
            RequestState::Init => matches!(target, RequestState::Sent | RequestState::Cancelled),
            RequestState::Sent => matches!(
                target,
                RequestState::Wait
                    | RequestState::Complete
                    | RequestState::Timeout
                    | RequestState::Cancelled
            ),
            RequestState::Wait => matches!(
                target,
                RequestState::Sent
                    | RequestState::Complete
                    | RequestState::Timeout
                    | RequestState::Cancelled
            ),
            RequestState::Timeout => matches!(
                target,
                RequestState::Resent | RequestState::Failed | RequestState::Cancelled
            ),
            RequestState::Resent => matches!(
                target,
                RequestState::Sent | RequestState::Failed | RequestState::Cancelled
            ),
            RequestState::Complete => false,  // 终态
            RequestState::Failed => false,    // 终态
            RequestState::Cancelled => false, // 终态
        }
    }
}

impl fmt::Display for RequestState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestState::Init => write!(f, "Init"),
            RequestState::Sent => write!(f, "Sent"),
            RequestState::Wait => write!(f, "Wait"),
            RequestState::Timeout => write!(f, "Timeout"),
            RequestState::Resent => write!(f, "Resent"),
            RequestState::Complete => write!(f, "Complete"),
            RequestState::Failed => write!(f, "Failed"),
            RequestState::Cancelled => write!(f, "Cancelled"),
        }
    }
}

/// 请求类型 (用于通道选择和优先级)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RequestKind {
    // ========== MetaShardClient 请求 ==========
    /// 元数据操作 (lookup, create, mkdir, unlink, statfs 等)
    Metadata,
    /// 控制请求 (心跳, 配置查询等)
    Control,

    // ========== VolumeClient 请求 ==========
    /// 数据读取
    Read,
    /// 数据写入
    Write,
    /// Lease 操作 (获取, 续约, 释放)
    Lease,
    /// 卷管理操作 (statfs, 状态查询等)
    Management,
}

impl RequestKind {
    /// 获取请求的优先级 (数值越小优先级越高)
    pub fn priority(&self) -> u8 {
        match self {
            RequestKind::Lease => 0,      // 最高优先级
            RequestKind::Management => 1, // 高优先级
            RequestKind::Control => 2,    // 中优先级
            RequestKind::Metadata => 3,   // 普通优先级
            RequestKind::Read => 4,       // 低优先级
            RequestKind::Write => 5,      // 最低优先级
        }
    }

    /// 是否为 Lease 相关请求
    pub fn is_lease(&self) -> bool {
        matches!(self, RequestKind::Lease)
    }

    /// 是否为数据请求 (读或写)
    pub fn is_data(&self) -> bool {
        matches!(self, RequestKind::Read | RequestKind::Write)
    }

    /// 是否为元数据请求
    pub fn is_metadata(&self) -> bool {
        matches!(self, RequestKind::Metadata)
    }
}

impl fmt::Display for RequestKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RequestKind::Metadata => write!(f, "Metadata"),
            RequestKind::Control => write!(f, "Control"),
            RequestKind::Read => write!(f, "Read"),
            RequestKind::Write => write!(f, "Write"),
            RequestKind::Lease => write!(f, "Lease"),
            RequestKind::Management => write!(f, "Management"),
        }
    }
}

/// 请求上下文 (追踪请求生命周期)
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// 请求 ID (全局唯一)
    pub request_id: RequestId,
    /// 客户端身份
    pub client_identity: ClientIdentity,
    /// 请求类型
    pub kind: RequestKind,
    /// 消息类型 (powerfs-net MsgType)
    pub msg_type: u16,
    /// 当前状态
    pub state: RequestState,
    /// 分片 ID (用于 MetaShardClient)
    pub shard_id: Option<u32>,
    /// 卷 ID (用于 VolumeClient)
    pub volume_id: Option<u32>,
    /// 分片哈希 (用于通道选择)
    pub stripe_hash: Option<u64>,
    /// 创建时间
    pub created_at: Instant,
    /// 最后一次状态变更时间
    pub last_state_change: Instant,
    /// 重试次数
    pub retry_count: u32,
    /// 最大重试次数
    pub max_retries: u32,
    /// 关联的 Lease ID (如果是数据请求)
    pub lease_id: Option<String>,
    /// 负载数据 (请求体)
    pub payload: Vec<u8>,
    /// 结果数据 (响应体)
    pub response: Option<Vec<u8>>,
    /// 错误信息 (如果失败)
    pub error: Option<String>,
}

impl RequestContext {
    /// 创建新的请求上下文
    pub fn new(
        client_identity: ClientIdentity,
        kind: RequestKind,
        msg_type: u16,
        payload: Vec<u8>,
    ) -> Self {
        let now = Instant::now();
        Self {
            request_id: RequestId::new(),
            client_identity,
            kind,
            msg_type,
            state: RequestState::Init,
            shard_id: None,
            volume_id: None,
            stripe_hash: None,
            created_at: now,
            last_state_change: now,
            retry_count: 0,
            max_retries: 3,
            lease_id: None,
            payload,
            response: None,
            error: None,
        }
    }

    /// 设置分片 ID
    pub fn with_shard_id(mut self, shard_id: u32) -> Self {
        self.shard_id = Some(shard_id);
        self
    }

    /// 设置卷 ID
    pub fn with_volume_id(mut self, volume_id: u32) -> Self {
        self.volume_id = Some(volume_id);
        self
    }

    /// 设置分片哈希
    pub fn with_stripe_hash(mut self, stripe_hash: u64) -> Self {
        self.stripe_hash = Some(stripe_hash);
        self
    }

    /// 设置请求 ID
    pub fn with_request_id(mut self, request_id: RequestId) -> Self {
        self.request_id = request_id;
        self
    }

    /// 设置最大重试次数
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// 设置 Lease ID
    pub fn with_lease_id(mut self, lease_id: String) -> Self {
        self.lease_id = Some(lease_id);
        self
    }

    /// 转换状态
    pub fn transition_to(&mut self, new_state: RequestState) -> Result<(), String> {
        if !self.state.can_transition_to(new_state) {
            return Err(format!(
                "Invalid state transition: {} -> {}",
                self.state, new_state
            ));
        }
        self.state = new_state;
        self.last_state_change = Instant::now();
        Ok(())
    }

    /// 标记为已发送
    pub fn mark_sent(&mut self) -> Result<(), String> {
        self.transition_to(RequestState::Sent)
    }

    /// 标记为完成
    pub fn mark_complete(&mut self, response: Vec<u8>) -> Result<(), String> {
        self.response = Some(response);
        self.transition_to(RequestState::Complete)
    }

    /// 标记为超时
    pub fn mark_timeout(&mut self) -> Result<(), String> {
        self.transition_to(RequestState::Timeout)
    }

    /// 标记为正在重试
    pub fn mark_resending(&mut self) -> Result<(), String> {
        self.retry_count += 1;
        self.transition_to(RequestState::Resent)
    }

    /// 标记为失败
    pub fn mark_failed(&mut self, error: String) -> Result<(), String> {
        self.error = Some(error);
        self.transition_to(RequestState::Failed)
    }

    /// 标记为取消
    pub fn mark_cancelled(&mut self) -> Result<(), String> {
        self.transition_to(RequestState::Cancelled)
    }

    /// 是否已达到最大重试次数
    pub fn max_retries_reached(&self) -> bool {
        self.retry_count >= self.max_retries
    }

    /// 是否已超时 (根据给定的超时时间判断)
    pub fn is_timed_out(&self, timeout: std::time::Duration) -> bool {
        self.last_state_change.elapsed() >= timeout
    }

    /// 请求耗时 (从创建到完成或当前)
    pub fn elapsed(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_context() -> RequestContext {
        let identity = ClientIdentity::new();
        RequestContext::new(
            identity,
            RequestKind::Read,
            0x0020, // Read
            vec![1, 2, 3],
        )
    }

    #[test]
    fn test_initial_state() {
        let ctx = create_test_context();
        assert_eq!(ctx.state, RequestState::Init);
        assert_eq!(ctx.retry_count, 0);
        assert!(!ctx.max_retries_reached());
    }

    #[test]
    fn test_valid_state_transitions() {
        let mut ctx = create_test_context();

        // Init -> Sent
        assert!(ctx.mark_sent().is_ok());
        assert_eq!(ctx.state, RequestState::Sent);

        // Sent -> Complete
        assert!(ctx.mark_complete(vec![4, 5, 6]).is_ok());
        assert_eq!(ctx.state, RequestState::Complete);
        assert!(ctx.response.is_some());
    }

    #[test]
    fn test_invalid_state_transition() {
        let mut ctx = create_test_context();

        // Init -> Complete 应该失败
        assert!(ctx.mark_complete(vec![]).is_err());
        assert_eq!(ctx.state, RequestState::Init);
    }

    #[test]
    fn test_timeout_and_retry() {
        let mut ctx = create_test_context();
        ctx.mark_sent().unwrap();

        // Sent -> Timeout
        assert!(ctx.mark_timeout().is_ok());
        assert_eq!(ctx.state, RequestState::Timeout);

        // Timeout -> Resent
        assert!(ctx.mark_resending().is_ok());
        assert_eq!(ctx.state, RequestState::Resent);
        assert_eq!(ctx.retry_count, 1);

        // Resent -> Sent
        ctx.transition_to(RequestState::Sent).unwrap();
        assert_eq!(ctx.state, RequestState::Sent);
    }

    #[test]
    fn test_max_retries() {
        let mut ctx = create_test_context().with_max_retries(2);

        assert!(!ctx.max_retries_reached());

        ctx.mark_sent().unwrap();
        ctx.mark_timeout().unwrap();
        ctx.mark_resending().unwrap(); // retry_count = 1

        assert!(!ctx.max_retries_reached());

        ctx.transition_to(RequestState::Sent).unwrap();
        ctx.mark_timeout().unwrap();
        ctx.mark_resending().unwrap(); // retry_count = 2

        assert!(ctx.max_retries_reached());
    }

    #[test]
    fn test_cancel() {
        let mut ctx = create_test_context();
        assert!(ctx.mark_cancelled().is_ok());
        assert_eq!(ctx.state, RequestState::Cancelled);
        assert!(ctx.state.is_terminal());
    }

    #[test]
    fn test_request_kind_priority() {
        assert!(RequestKind::Lease.priority() < RequestKind::Management.priority());
        assert!(RequestKind::Management.priority() < RequestKind::Control.priority());
        assert!(RequestKind::Control.priority() < RequestKind::Metadata.priority());
        assert!(RequestKind::Metadata.priority() < RequestKind::Read.priority());
        assert!(RequestKind::Read.priority() < RequestKind::Write.priority());
    }

    #[test]
    fn test_request_kind_methods() {
        assert!(RequestKind::Lease.is_lease());
        assert!(RequestKind::Read.is_data());
        assert!(RequestKind::Write.is_data());
        assert!(RequestKind::Metadata.is_metadata());
        assert!(!RequestKind::Control.is_data());
    }

    #[test]
    fn test_context_builder_methods() {
        let ctx = create_test_context()
            .with_shard_id(1)
            .with_volume_id(100)
            .with_stripe_hash(12345)
            .with_max_retries(5)
            .with_lease_id("lease-123".to_string());

        assert_eq!(ctx.shard_id, Some(1));
        assert_eq!(ctx.volume_id, Some(100));
        assert_eq!(ctx.stripe_hash, Some(12345));
        assert_eq!(ctx.max_retries, 5);
        assert_eq!(ctx.lease_id.as_deref(), Some("lease-123"));
    }

    #[test]
    fn test_state_display() {
        assert_eq!(format!("{}", RequestState::Init), "Init");
        assert_eq!(format!("{}", RequestState::Complete), "Complete");
        assert_eq!(format!("{}", RequestState::Failed), "Failed");
    }

    #[test]
    fn test_kind_display() {
        assert_eq!(format!("{}", RequestKind::Read), "Read");
        assert_eq!(format!("{}", RequestKind::Lease), "Lease");
        assert_eq!(format!("{}", RequestKind::Metadata), "Metadata");
    }
}
