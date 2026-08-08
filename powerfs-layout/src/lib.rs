//! # powerfs-layout
//!
//! 文件数据布局统一设计: 三维正交模型
//!
//! ```text
//! FileLayout = Placement x Reliability x ChunkEncoding
//! ```
//!
//! - **Placement**: 数据如何分布到 volume (Inline/Flat/Stripe/WideStripe)
//! - **Reliability**: 数据如何保护 (SingleReplica/Replicated/EC) + 状态机
//! - **ChunkEncoding**: 元数据如何序列化 (InlineData/PerChunk/StripeDescriptor/Paginated)
//!
//! 设计文档: `docs/file-layout-design.md`
//!
//! ## 依赖关系
//!
//! 本 crate 只依赖 `powerfs-net` (FieldId/TlvEncoder/TlvDecoder),
//! 不依赖 filer/fuse/master, 避免循环依赖。
//!
//! 被以下 crate 依赖:
//! - `powerfs-filer`: 写入路径按 Placement 分配 volume
//! - `powerfs-fuse`: 读取路径按 `locate()` 找 volume
//! - `powerfs-master`: volume 选择 + anti-affinity
//! - `powerfs-coherence`: ChunkWire -> ChunkRef 演进

pub mod anti_affinity;
pub mod codec;
pub mod compat;
pub mod encoding;
pub mod error;
pub mod layout;
pub mod placement;
pub mod policy;
pub mod reliability;
pub mod xattr;

// ---- 便捷 re-export (常用类型可直接 `use powerfs_layout::*`) ----

pub use anti_affinity::{NodeId, VolumeInfo};
pub use encoding::{ChunkEncoding, ChunkRef};
pub use error::LayoutError;
pub use layout::FileLayout;
pub use placement::{Placement, PlacementSpec};
pub use policy::PlacementPolicy;
pub use reliability::{CompressionState, Reliability, ReliabilityState};
