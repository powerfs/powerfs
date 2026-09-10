//! Volume WAL 引擎（v2）底层构件。
//!
//! 模块划分：
//! - 记录帧编解码与哈希链校验（方案 §4.2 / §4.3）：[`frame`]。
//! - 段文件读写（段头、SegWriter、SegReader，方案 §4.1）：[`segment`]。
//! - 段清单（目录内段文件的内存索引）：[`manifest`]。

pub mod frame;
pub mod manifest;
pub mod segment;
