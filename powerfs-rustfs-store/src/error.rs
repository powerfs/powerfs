//! PowerFS 错误到 RustFS 存储契约错误的映射。

use powerfs_common::error::PowerFsError;
use rustfs_ecstore::api::error::Error;

/// 把 master 客户端（`PowerFsError`）返回的错误映射成契约错误。
///
/// 映射原则：资源不存在统一落到 `FileNotFound`（对象层），其余无法精确
/// 归类的失败落到 `Io`，保留原始错误链便于排障。
pub fn from_master(error: PowerFsError) -> Error {
    match error {
        PowerFsError::FileNotFound(_)
        | PowerFsError::DirectoryNotFound(_)
        | PowerFsError::NeedleNotFound(_)
        | PowerFsError::VolumeNotFound(_) => Error::FileNotFound,
        PowerFsError::PermissionDenied => Error::FileAccessDenied,
        PowerFsError::OutOfSpace => Error::StorageFull,
        PowerFsError::Timeout => Error::Timeout,
        other => Error::other(other),
    }
}

/// 把 Filer 的 gRPC 状态映射成契约错误。
///
/// Filer 的业务错误通常放在响应体里（由调用方判定），这里只处理传输层与
/// gRPC 状态码，避免把状态码语义丢失成笼统的 IO 错误。
pub fn from_status(status: tonic::Status) -> Error {
    match status.code() {
        tonic::Code::NotFound => Error::FileNotFound,
        tonic::Code::PermissionDenied => Error::FileAccessDenied,
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => Error::Timeout,
        _ => Error::other(status.to_string()),
    }
}

/// 把 volume 客户端返回的字符串错误映射成契约错误。
///
/// volume 客户端目前只回传 `String`，无法结构化区分「不存在」与「网络失败」，
/// 这里按错误文本粗略判定 not found，其余按 IO 失败处理。
pub fn from_volume(message: String) -> Error {
    if message.to_ascii_lowercase().contains("not found") {
        Error::FileNotFound
    } else {
        Error::other(message)
    }
}

/// 把 Filer 的桶操作错误文本映射成 S3 语义错误。
///
/// Filer 在响应体里用字符串表达「桶不存在 / 桶非空 / 非法名字」，
/// 直接透传会让 S3 层返回 500，这里转成对应错误码。
pub fn bucket_error(bucket: &str, error: Error) -> Error {
    let message = error.to_string();
    let lowered = message.to_ascii_lowercase();
    if lowered.contains("bucket not found") {
        return Error::BucketNotFound(bucket.to_string());
    }
    if lowered.contains("bucket not empty") {
        return Error::BucketNotEmpty(bucket.to_string());
    }
    if lowered.contains("invalid bucket") {
        return Error::BucketNameInvalid(bucket.to_string());
    }
    error
}
