//! LeaseManager trait — 统一 lease 生命周期管理接口。
//!
//! read/write 通过 [`LeaseMode`] 区分，都走缓存复用：
//! 缓存命中（lease 仍在有效期内）时零 RPC 返回 token；缓存未命中或已过期时
//! 才向 Volume Server 发送 AcquireLease RPC 并写入缓存。
//!
//! 设计要点：
//! - trait 方法返回 `Pin<Box<dyn Future + Send + 'static>>`（而非 `'_`），
//!   以便调用方可以通过 [`crate::SyncFuseClientFacade::block_on`] 驱动 future
//!   （该方法要求 future 为 `'static`）。`VolumeLeaseManager` 内部所有状态都
//!   通过 `Arc` 共享，因此 future 可以按值捕获这些 `Arc` clone 而不借用
//!   `&self`，从而满足 `'static` 约束。
//! - 缓存 key 包含 `(volume_id, inode, stripe_start, stripe_count, exclusive)`，
//!   同一 stripe 范围的重复读可直接复用缓存 token。

use crate::fuse_client_facade::FuseClientFacade;
use powerfs_common::error::{PowerFsError, Result};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Lease 模式
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseMode {
    /// 读共享
    Shared,
    /// 写排他
    Exclusive,
}

impl LeaseMode {
    fn is_exclusive(self) -> bool {
        matches!(self, LeaseMode::Exclusive)
    }
}

/// 强类型 lease token，避免与普通字符串混淆。
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LeaseToken(String);

impl LeaseToken {
    pub fn new(s: String) -> Self {
        Self(s)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

/// Lease 状态信息（监控/查询用）。
///
/// 注意：这与 [`crate::volume_client::LeaseState`]（lease 生命周期 enum）同名
/// 但属于不同模块路径，互不冲突。
#[derive(Clone, Debug)]
pub struct LeaseState {
    pub token: LeaseToken,
    pub mode: LeaseMode,
    pub expire_at: Instant,
    pub volume_id: u64,
    pub inode: u64,
    pub stripe_start: u64,
    pub stripe_count: u64,
}

/// Lease 管理接口（统一 read/write 入口，缓存复用）。
pub trait LeaseManager: Send + Sync {
    /// 获取 lease（缓存复用）。
    /// - 命中有效缓存 → 零 RPC 返回
    /// - 未命中/过期 → RPC 获取并缓存
    ///
    /// 返回的 future 为 `'static`：调用方可通过 `SyncFuseClientFacade::block_on`
    /// 或任意 tokio runtime 驱动。
    fn acquire(
        &self,
        volume_id: u64,
        inode: u64,
        mode: LeaseMode,
        stripe_start: u64,
        stripe_count: u64,
        duration_ms: u64,
    ) -> Pin<Box<dyn Future<Output = Result<LeaseToken>> + Send + 'static>>;

    /// 释放 lease（发送 ReleaseLease RPC 并从缓存移除）。
    fn release(
        &self,
        volume_id: u64,
        inode: u64,
        token: &LeaseToken,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

    /// 查询 lease 状态（监控用）。返回首个匹配 `(volume_id, inode)` 的缓存项。
    fn state(&self, volume_id: u64, inode: u64) -> Option<LeaseState>;
}

/// Lease 缓存 key。
#[derive(Clone, PartialEq, Eq, Hash)]
struct LeaseKey {
    volume_id: u64,
    inode: u64,
    stripe_start: u64,
    stripe_count: u64,
    exclusive: bool,
}

/// Lease 缓存 entry。
struct LeaseCacheEntry {
    token: String,
    expire_at: Instant,
    mode: LeaseMode,
}

/// `VolumeLeaseManager` — 包装 [`FuseClientFacade`] 实现 [`LeaseManager`]，
/// 在其之上增加 stripe 粒度的 lease 缓存复用。
///
/// `FuseClientFacade::acquire_lease` 内部会更新 `VolumeClient` 的 (volume_id,
/// inode) lease 表，因此文件 `release()` 时既有的 `release_lease` 路径仍能
/// 找到 token 释放；本结构额外维护一份 stripe 粒度缓存用于读路径零 RPC 复用。
pub struct VolumeLeaseManager {
    facade: Arc<FuseClientFacade>,
    client_id: Arc<String>,
    cache: Arc<RwLock<HashMap<LeaseKey, LeaseCacheEntry>>>,
}

impl VolumeLeaseManager {
    pub fn new(facade: Arc<FuseClientFacade>, client_id: String) -> Self {
        Self {
            facade,
            client_id: Arc::new(client_id),
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 同步检查缓存中是否有有效 lease。命中且未过期则返回 token。
    fn check_cache(&self, key: &LeaseKey) -> Option<String> {
        let cache = self.cache.read().unwrap();
        if let Some(entry) = cache.get(key) {
            if Instant::now() < entry.expire_at {
                return Some(entry.token.clone());
            }
        }
        None
    }

    /// 失效指定 `(volume_id, inode)` 的全部缓存项。
    ///
    /// 供文件 `release()` 在 close 时调用，避免 close 后复用已释放的 token。
    pub fn invalidate(&self, volume_id: u64, inode: u64) {
        let mut cache = self.cache.write().unwrap();
        cache.retain(|k, _| !(k.volume_id == volume_id && k.inode == inode));
    }
}

impl LeaseManager for VolumeLeaseManager {
    fn acquire(
        &self,
        volume_id: u64,
        inode: u64,
        mode: LeaseMode,
        stripe_start: u64,
        stripe_count: u64,
        duration_ms: u64,
    ) -> Pin<Box<dyn Future<Output = Result<LeaseToken>> + Send + 'static>> {
        let key = LeaseKey {
            volume_id,
            inode,
            stripe_start,
            stripe_count,
            exclusive: mode.is_exclusive(),
        };

        // 1. 同步检查缓存：命中则零 RPC 返回。
        if let Some(token) = self.check_cache(&key) {
            log::debug!(
                "lease: cache hit volume={} inode={} stripe=[{},{}) exclusive={}",
                volume_id,
                inode,
                stripe_start,
                stripe_start + stripe_count,
                key.exclusive
            );
            return Box::pin(async move { Ok(LeaseToken::new(token)) });
        }

        // 2. 缓存未命中：clone Arc 以构造 'static future，发 RPC 并写入缓存。
        let facade = self.facade.clone();
        let client_id = self.client_id.clone();
        let cache = self.cache.clone();
        Box::pin(async move {
            let token = facade
                .acquire_lease(
                    volume_id,
                    inode,
                    stripe_start,
                    stripe_count,
                    &client_id,
                    mode.is_exclusive(),
                    duration_ms,
                )
                .await
                .map_err(PowerFsError::Internal)?;

            {
                let mut guard = cache.write().unwrap();
                guard.insert(
                    key.clone(),
                    LeaseCacheEntry {
                        token: token.clone(),
                        expire_at: Instant::now() + Duration::from_millis(duration_ms),
                        mode,
                    },
                );
            }

            log::debug!(
                "lease: acquired volume={} inode={} stripe=[{},{}) exclusive={}",
                volume_id,
                inode,
                stripe_start,
                stripe_start + stripe_count,
                key.exclusive
            );
            Ok(LeaseToken::new(token))
        })
    }

    fn release(
        &self,
        volume_id: u64,
        inode: u64,
        token: &LeaseToken,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        let facade = self.facade.clone();
        let client_id = self.client_id.clone();
        let cache = self.cache.clone();
        let token_str = token.as_str().to_string();
        Box::pin(async move {
            // 先从缓存移除（按 (volume_id, inode)），避免复用已释放的 token。
            {
                let mut guard = cache.write().unwrap();
                guard.retain(|k, _| !(k.volume_id == volume_id && k.inode == inode));
            }
            facade
                .release_lease(volume_id, inode, &client_id, &token_str)
                .await
                .map_err(PowerFsError::Internal)?;
            Ok(())
        })
    }

    fn state(&self, volume_id: u64, inode: u64) -> Option<LeaseState> {
        let cache = self.cache.read().unwrap();
        for (k, v) in cache.iter() {
            if k.volume_id == volume_id && k.inode == inode {
                return Some(LeaseState {
                    token: LeaseToken::new(v.token.clone()),
                    mode: v.mode,
                    expire_at: v.expire_at,
                    volume_id: k.volume_id,
                    inode: k.inode,
                    stripe_start: k.stripe_start,
                    stripe_count: k.stripe_count,
                });
            }
        }
        None
    }
}
