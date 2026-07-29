//! 集成测试 - 验证 FuseClientFacade 的核心逻辑
//!
//! 这些测试验证了客户端的初始化、请求提交和状态管理功能。
//! 注意：这些测试不需要实际的服务器连接。

#![allow(unused_mut, unused_variables)]

use std::sync::Arc;
use std::time::Duration;

use powerfs_fuse_core::client_error::ClientError;
use powerfs_fuse_core::*;

/// 测试 FuseClientFacadeConfig 创建
#[test]
fn test_facade_config_creation() {
    let config = FuseClientFacadeConfig::default();

    // 验证默认值
    assert_eq!(config.master_addr, "127.0.0.1");
    assert_eq!(config.master_port, 9333);
    assert_eq!(config.filer_addr, "127.0.0.1");
    assert_eq!(config.filer_port, 9343);
    assert_eq!(config.request_timeout, Duration::from_secs(5));

    // 验证客户端身份已生成
    assert!(!config.client_identity.client_uuid.is_empty());
}

/// 测试 FuseClientFacadeConfig 自定义
#[test]
fn test_facade_config_custom_values() {
    let identity = ClientIdentity::new();
    let config = FuseClientFacadeConfig {
        master_addr: "192.168.1.100".to_string(),
        master_port: 8000,
        volume_net_port: 8002,
        volume_addrs: Vec::new(),
        filer_addr: "192.168.1.200".to_string(),
        filer_port: 8001,
        request_timeout: Duration::from_secs(10),
        client_identity: identity,
    };

    assert_eq!(config.master_addr, "192.168.1.100");
    assert_eq!(config.master_port, 8000);
    assert_eq!(config.filer_addr, "192.168.1.200");
    assert_eq!(config.filer_port, 8001);
    assert_eq!(config.request_timeout, Duration::from_secs(10));
}

/// 测试 MetaShardClient 初始化（不连接网络）
#[test]
fn test_meta_shard_client_initialization() {
    let topology_manager = Arc::new(ClusterTopologyManager::new());
    let config = MetaShardClientConfig::default();
    let mut client = MetaShardClient::new(config, topology_manager);

    // 验证初始状态
    assert_eq!(client.state(), MetaShardClientState::Init);

    // 初始化
    client.init();

    // 验证初始化后状态
    assert_eq!(client.state(), MetaShardClientState::Ready);
}

/// 测试 VolumeClient 初始化（不连接网络）
#[test]
fn test_volume_client_initialization() {
    let topology_manager = Arc::new(ClusterTopologyManager::new());
    let config = VolumeClientConfig::default();
    let mut client = VolumeClient::new(config, topology_manager);

    // 验证初始状态
    assert_eq!(client.state(), VolumeClientState::Init);

    // 初始化
    client.init();

    // 验证初始化后状态
    assert_eq!(client.state(), VolumeClientState::Ready);
}

/// 测试请求提交（使用空网络客户端）
#[tokio::test]
async fn test_request_submission_without_network() {
    let topology_manager = Arc::new(ClusterTopologyManager::new());
    let config = MetaShardClientConfig::default();
    let mut client = MetaShardClient::new(config, topology_manager.clone());
    client.init();

    // 创建请求上下文
    let request_id = RequestId::new();
    let identity = ClientIdentity::new();
    let context = RequestContext::new(
        identity,
        RequestKind::Metadata,
        powerfs_net::MsgType::Lookup as u16,
        vec![1, 2, 3],
    )
    .with_request_id(request_id);

    // 提交请求
    let result = client.submit_metadata_request(context, 1);
    assert!(result.is_ok());

    // 验证请求已入队
    let (data_len, control_len) = client.queue_stats();
    assert_eq!(data_len, 1);
    assert_eq!(control_len, 0);

    // 尝试处理请求（没有网络连接应该返回错误）
    let result = client.process_next_data_request().await;
    assert!(result.is_some());

    let result = result.unwrap();
    match result {
        Ok(_) => {
            // 成功（不太可能，因为没有网络连接）
        }
        Err(e) => {
            // 应该是网络错误（连接失败或路由未配置）
            assert!(matches!(
                e,
                ClientError::Network(_) | ClientError::VolumeNotFound(_)
            ));
        }
    }
}

/// 测试 VolumeClient 请求提交
#[tokio::test]
async fn test_volume_request_submission_without_network() {
    let topology_manager = Arc::new(ClusterTopologyManager::new());
    let config = VolumeClientConfig::default();
    let mut client = VolumeClient::new(config, topology_manager);
    client.init();

    // 创建数据请求
    let request_id = RequestId::new();
    let identity = ClientIdentity::new();
    let context = RequestContext::new(
        identity,
        RequestKind::Read,
        powerfs_net::MsgType::ReadNeedleBlob as u16,
        vec![1, 2, 3],
    )
    .with_request_id(request_id);

    // 提交请求
    let result = client.submit_data_request(context, 1);
    assert!(result.is_ok());

    // 验证请求已入队
    let (data_len, lease_len, mgmt_len) = client.queue_stats();
    assert_eq!(data_len, 1);
    assert_eq!(lease_len, 0);
    assert_eq!(mgmt_len, 0);

    // 尝试处理请求（没有网络连接应该返回错误）
    let result = client.process_next_data_request().await;
    assert!(result.is_some());

    let result = result.unwrap();
    match result {
        Ok(_) => {
            // 成功（不太可能，因为没有网络连接）
        }
        Err(e) => {
            // 应该是网络错误（连接失败或路由未配置）
            assert!(matches!(
                e,
                ClientError::Network(_) | ClientError::VolumeNotFound(_)
            ));
        }
    }
}

/// 测试 MasterClient 连接状态
#[test]
fn test_master_client_state_transitions() {
    let topology_manager = Arc::new(ClusterTopologyManager::new());
    let config = MasterClientConfig::default();
    let client = MasterClient::new(config, topology_manager);

    // 初始状态为 Disconnected
    assert_eq!(client.state(), MasterClientState::Disconnected);

    // 设置 Leader
    client.set_leader("127.0.0.1:9333".to_string());
    assert_eq!(client.state(), MasterClientState::Connected);
    assert_eq!(client.current_leader(), Some("127.0.0.1:9333".to_string()));

    // 断开连接
    client.disconnect();
    assert_eq!(client.state(), MasterClientState::Disconnected);
    assert_eq!(client.current_leader(), None);
}

/// 测试拓扑更新
#[test]
fn test_topology_update() {
    let topology_manager = Arc::new(ClusterTopologyManager::new());

    // 创建初始拓扑
    let initial = topology_manager.get_topology();
    assert!(initial.shards.is_empty());
    assert!(initial.volumes.is_empty());

    // 更新拓扑
    let mut new_topology = ClusterTopology::new();
    new_topology
        .shards
        .insert(1, ShardInfo::new(1, "127.0.0.1:9343".to_string()));
    new_topology.volumes.insert(
        1,
        VolumeInfo::new(1, "volume-1".to_string(), "127.0.0.1:9344".to_string()),
    );

    topology_manager.update_topology(new_topology);

    // 验证更新
    let updated = topology_manager.get_topology();
    assert_eq!(updated.shards.len(), 1);
    assert_eq!(updated.volumes.len(), 1);
    assert!(updated.shards.contains_key(&1));
    assert!(updated.volumes.contains_key(&1));
}

/// 测试请求状态转换
#[test]
fn test_request_state_transitions() {
    let identity = ClientIdentity::new();
    let context = RequestContext::new(
        identity,
        RequestKind::Metadata,
        powerfs_net::MsgType::Lookup as u16,
        vec![],
    );

    // 初始状态为 Init
    assert_eq!(context.state, RequestState::Init);

    // 转换到 Sent
    let mut ctx = context.clone();
    assert!(ctx.transition_to(RequestState::Sent).is_ok());
    assert_eq!(ctx.state, RequestState::Sent);

    // 转换到 Complete
    assert!(ctx.transition_to(RequestState::Complete).is_ok());
    assert_eq!(ctx.state, RequestState::Complete);

    // 无效转换
    let mut ctx2 = context.clone();
    assert!(ctx2.transition_to(RequestState::Complete).is_err());
}

/// 测试 RequestId 唯一性
#[test]
fn test_request_id_uniqueness() {
    let id1 = RequestId::new();
    let id2 = RequestId::new();

    // 两个 ID 应该不同
    assert_ne!(id1, id2);
    assert_ne!(id1.as_str(), id2.as_str());

    // 格式验证
    let id_str = id1.as_str();
    assert!(!id_str.is_empty());
    assert_eq!(id_str.len(), 36); // UUID v4 格式
}

/// 测试关闭和清理
#[test]
fn test_client_cleanup() {
    let topology_manager = Arc::new(ClusterTopologyManager::new());
    let meta_config = MetaShardClientConfig::default();
    let volume_config = VolumeClientConfig::default();

    let mut meta_client = MetaShardClient::new(meta_config, topology_manager.clone());
    let mut volume_client = VolumeClient::new(volume_config, topology_manager);

    meta_client.init();
    volume_client.init();

    assert_eq!(meta_client.state(), MetaShardClientState::Ready);
    assert_eq!(volume_client.state(), VolumeClientState::Ready);

    // 关闭
    meta_client.close();
    volume_client.close();

    assert_eq!(meta_client.state(), MetaShardClientState::Closed);
    assert_eq!(volume_client.state(), VolumeClientState::Closed);
}

/// 测试请求类型优先级
#[test]
fn test_request_kind_priority() {
    // 优先级数字越小表示优先级越高
    assert!(RequestKind::Lease.priority() < RequestKind::Metadata.priority());
    assert!(RequestKind::Management.priority() < RequestKind::Read.priority());
    assert!(RequestKind::Control.priority() < RequestKind::Write.priority());
}

/// 测试 MetaShardClient 队列操作
#[test]
fn test_meta_shard_client_queue_operations() {
    let topology_manager = Arc::new(ClusterTopologyManager::new());
    let config = MetaShardClientConfig::default();
    let mut client = MetaShardClient::new(config, topology_manager);
    client.init();

    // 创建多个请求
    for i in 0..5 {
        let request_id = RequestId::new();
        let identity = ClientIdentity::new();
        let context = RequestContext::new(
            identity,
            RequestKind::Metadata,
            powerfs_net::MsgType::Lookup as u16,
            vec![i as u8],
        )
        .with_request_id(request_id);

        client.submit_metadata_request(context, 1).unwrap();
    }

    // 验证队列大小
    let (data_len, control_len) = client.queue_stats();
    assert_eq!(data_len, 5);
    assert_eq!(control_len, 0);

    // 获取并处理请求
    for i in 0..3 {
        let req = client.next_data_request();
        assert!(req.is_some());

        // 处理 (不会真正发送)
        let _ = req.unwrap();
    }

    // 验证剩余队列大小
    let (data_len, _) = client.queue_stats();
    assert_eq!(data_len, 2);
}
