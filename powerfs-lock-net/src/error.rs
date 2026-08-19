//! Codec errors for the lock wire protocol.

use thiserror::Error;

/// Errors returned by [`crate::codec`] encode/decode functions.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum CodecError {
    #[error("unexpected end of input (need {needed} bytes at offset {offset})")]
    Truncated { offset: usize, needed: usize },

    #[error("invalid msg_type: {0:#x}")]
    InvalidMsgType(u8),

    #[error("invalid field tag: {0:#x}")]
    InvalidFieldTag(u8),

    #[error("invalid mode byte: {0}")]
    InvalidMode(u8),

    #[error("invalid error code: {0}")]
    InvalidErrorCode(u8),

    #[error("invalid UTF-8 in string field {tag:#x}: {reason}")]
    InvalidUtf8 { tag: u8, reason: String },

    #[error("field length {len} exceeds remaining buffer at offset {offset}")]
    FieldTooLong { offset: usize, len: usize },

    #[error("duplicate field tag {0:#x}")]
    DuplicateField(u8),

    #[error("missing required field tag {0:#x}")]
    MissingField(u8),

    #[error("internal codec error: {0}")]
    Internal(String),
}
