//! P1c 阶段的能力声明，供 Admin 界面展示。

/// 后端能力开关。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capability {
    /// 纠删码（EC）存储。
    pub ec: bool,
    /// 对象版本化。
    pub versioning: bool,
    /// 分段上传（multipart）。
    pub multipart: bool,
    /// 对象锁（Object Lock）。
    pub object_lock: bool,
    /// 生命周期（Lifecycle / ILM）。
    pub lifecycle: bool,
}

/// 当前阶段（P1c）的能力：仅基础 CRUD，上述高级能力全部不提供。
pub fn capability() -> Capability {
    Capability {
        ec: false,
        versioning: false,
        multipart: false,
        object_lock: false,
        lifecycle: false,
    }
}

/// 一行中文说明，供 Admin 展示。
pub fn describe() -> &'static str {
    "PowerFS 存储后端（P1c）：支持桶与对象的单版本基础增删改查/列举；暂不支持纠删码、版本化、分段上传、对象锁与生命周期"
}
