//! openraft 官方存储一致性套件验证。
//!
//! 参考 `openraft/examples/sm-rocks/src/test.rs`。
//! 用一个测试专用的 TypeConfig（`NodeId = u64`，满足 `From<u64>`），
//! `D = MasterRequest` / `R = MasterResponse` 复用 crate 的占位类型。

use openraft::testing::log::StoreBuilder;
use openraft::testing::log::Suite;
use openraft::type_config::TypeConfigExt;
use openraft::StorageError;
use powerfs_raft::store;
use powerfs_raft::{MasterRequest, MasterResponse};
use tempfile::TempDir;

// 测试专用 TypeConfig：用默认 `NodeId = u64`（满足 Suite 的 `From<u64>` 约束）。
openraft::declare_raft_types!(
    pub TestTypeConfig:
        D = MasterRequest,
        R = MasterResponse,
);

struct RocksBuilder {}

impl
    StoreBuilder<
        TestTypeConfig,
        store::RocksLogStore<TestTypeConfig>,
        store::RocksStateMachine,
        TempDir,
    > for RocksBuilder
{
    async fn build(
        &self,
    ) -> Result<
        (
            TempDir,
            store::RocksLogStore<TestTypeConfig>,
            store::RocksStateMachine,
        ),
        StorageError<TestTypeConfig>,
    > {
        let td =
            TempDir::new().map_err(|e| StorageError::read(TestTypeConfig::err_from_error(&e)))?;
        let (log_store, sm) = store::new::<TestTypeConfig, _>(td.path())
            .await
            .map_err(|e| StorageError::read(TestTypeConfig::err_from_error(&e)))?;
        Ok((td, log_store, sm))
    }
}

#[test]
pub fn test_rocks_store() {
    TestTypeConfig::run(async {
        Suite::test_all(RocksBuilder {}).await.unwrap();
    });
}
