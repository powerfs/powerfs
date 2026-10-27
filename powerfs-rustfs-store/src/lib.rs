//! PowerFS 到 RustFS 存储契约的适配层（P1c 阶段）。
//!
//! 本 crate 把 PowerFS 现有的 master / volume / 目录树接口适配成 RustFS 的
//! [`ExternalBackend`](rustfs_ecstore::storage_external::ExternalBackend) 契约，
//! 使 RustFS 的 S3 表面可以建立在 PowerFS 的 needle/volume 存储之上。
//!
//! P1c 只保证「能编译 + 基础 CRUD 语义正确」：
//! - 桶：创建 / 查询 / 列举 / 删除；
//! - 对象：PUT / GET / HEAD / DELETE / 前缀列举；
//! - 版本化、multipart、条件请求均未实现，能力声明见 [`capability`]。
//!
//! 路径约定：桶目录为 `/bucket`，对象条目 `directory = "/bucket"`、`name = key`，
//! 完整路径为 `/{bucket}/{key}`。

pub mod backend;
pub mod bucket;
pub mod capability;
pub mod config;
pub mod error;
pub mod filer;
pub mod list;
pub mod object;

pub use backend::PowerFsStore;
pub use capability::{capability, describe, Capability};
pub use config::StoreConfig;
