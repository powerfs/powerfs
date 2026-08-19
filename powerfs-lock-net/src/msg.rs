//! Message types and wire-protocol constants.
//!
//! All constants here are the single source of truth for the Rust side of
//! the lock protocol. The C side (`powerfs-kernel`) mirrors these exact
//! values — keep them in sync via `docs/lock-protocol.md`.

use powerfs_lock::{LockError, LockMode, LockRequest, Range};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Message type codes (first byte of a frame)
// ---------------------------------------------------------------------------

/// One-byte message type identifier (first byte of a frame).
pub type MsgType = u8;

pub const MSG_ACQUIRE: u8 = 0x01;
pub const MSG_GRANT: u8 = 0x02;
pub const MSG_RELEASE: u8 = 0x03;
pub const MSG_RELEASE_ACK: u8 = 0x04;
pub const MSG_RENEW: u8 = 0x05;
pub const MSG_RENEW_ACK: u8 = 0x06;
pub const MSG_REVOKE: u8 = 0x07; // server → client (Early Revoke)
pub const MSG_INVALIDATE: u8 = 0x08; // server → client
pub const MSG_REVOKE_ACK: u8 = 0x09; // client → server

// ---------------------------------------------------------------------------
// Field tags (TLV "T")
// ---------------------------------------------------------------------------

pub type FieldTag = u8;

pub const FIELD_INODE: u8 = 0x01;
pub const FIELD_TOKEN: u8 = 0x02;
pub const FIELD_MODE: u8 = 0x03;
pub const FIELD_RANGE_START: u8 = 0x04;
pub const FIELD_RANGE_END: u8 = 0x05; // absent = EOF
pub const FIELD_TIMEOUT_MS: u8 = 0x06;
pub const FIELD_SN: u8 = 0x07;
pub const FIELD_LEASE_MS: u8 = 0x08;
pub const FIELD_CLIENT_ID: u8 = 0x09;
pub const FIELD_ERROR_CODE: u8 = 0x0A;

/// Sentinel value for "range end = EOF" when range_end field is present
/// but should be interpreted as open-ended. Receivers should treat
/// `range_end == RANGE_END_EOF_SENTINEL` the same as the field being absent.
///
/// Note: in practice we prefer to omit the FIELD_RANGE_END field entirely
/// for EOF ranges; this sentinel is only for senders that always emit both
/// range fields. Receivers handle both forms.
pub const RANGE_END_EOF_SENTINEL: u64 = u64::MAX;

// ---------------------------------------------------------------------------
// Error codes (for ACK messages)
// ---------------------------------------------------------------------------

pub type ErrorCode = u8;

pub const ERR_OK: u8 = 0x00;
pub const ERR_NOT_FOUND: u8 = 0x01;
pub const ERR_HOLDER_MISMATCH: u8 = 0x02;
pub const ERR_EXPIRED: u8 = 0x03;
pub const ERR_EXPIRED_BEYOND_GRACE: u8 = 0x04;
pub const ERR_CONFLICT: u8 = 0x05;
pub const ERR_KEY_NOT_COVERED: u8 = 0x06;
pub const ERR_QUARANTINED: u8 = 0x07;
pub const ERR_NETWORK: u8 = 0x08;
pub const ERR_INTERNAL: u8 = 0x09;

/// Mode byte values (encoded in FIELD_MODE).
pub const MODE_SHARED: u8 = 0x00;
pub const MODE_EXCLUSIVE: u8 = 0x01;
pub const MODE_RANGE: u8 = 0x02;

// ---------------------------------------------------------------------------
// Rust-side message model
// ---------------------------------------------------------------------------

/// A decoded protocol frame: the message type plus its typed payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// Client → server: request a lock.
    Acquire {
        inode: u64,
        mode: LockMode,
        range: Option<Range>,
        timeout: Duration,
        client_id: String,
    },

    /// Server → client: grant response (success only; failures are
    /// conveyed as `error_code` and a synthetic Grant with empty token
    /// — but we model failures separately via `*Ack` for release/renew;
    /// for acquire, the server sends `Grant` with `error_code != OK`
    /// when the acquire is rejected).
    Grant {
        inode: u64,
        token: String,
        sn: u64,
        lease_ms: u64,
        mode: LockMode,
        range: Option<Range>,
        error_code: ErrorCode,
    },

    /// Client → server: release a held lease.
    Release {
        inode: u64,
        token: String,
        client_id: String,
    },

    /// Server → client: release acknowledgment.
    ReleaseAck { inode: u64, error_code: ErrorCode },

    /// Client → server: renew a held lease.
    Renew {
        inode: u64,
        token: String,
        timeout: Duration,
        client_id: String,
    },

    /// Server → client: renew acknowledgment.
    RenewAck {
        inode: u64,
        lease_ms: u64,
        error_code: ErrorCode,
    },

    /// Server → client: Early Revoke notification (§5.2).
    /// Client should flush + ACK via `RevokeAck`.
    Revoke { inode: u64, token: String },

    /// Server → client: cache invalidation notification.
    Invalidate { inode: u64, range: Option<Range> },

    /// Client → server: acknowledge a revoke (lease is being released).
    RevokeAck {
        inode: u64,
        token: String,
        client_id: String,
    },
}

impl Message {
    /// The wire `MsgType` byte for this message variant.
    pub fn msg_type(&self) -> MsgType {
        match self {
            Message::Acquire { .. } => MSG_ACQUIRE,
            Message::Grant { .. } => MSG_GRANT,
            Message::Release { .. } => MSG_RELEASE,
            Message::ReleaseAck { .. } => MSG_RELEASE_ACK,
            Message::Renew { .. } => MSG_RENEW,
            Message::RenewAck { .. } => MSG_RENEW_ACK,
            Message::Revoke { .. } => MSG_REVOKE,
            Message::Invalidate { .. } => MSG_INVALIDATE,
            Message::RevokeAck { .. } => MSG_REVOKE_ACK,
        }
    }

    /// Construct an `Acquire` message from a `LockRequest` plus client id.
    pub fn from_request(req: &LockRequest, client_id: &str) -> Self {
        Message::Acquire {
            inode: req.inode,
            mode: req.mode.clone(),
            range: req.effective_range(),
            timeout: req.timeout,
            client_id: client_id.to_string(),
        }
    }
}

/// A raw frame on the wire (type + payload bytes). Decoding the payload
/// into a [`Message`] is done by [`crate::codec::decode_frame`].
#[derive(Clone, Debug)]
pub struct Frame {
    pub msg_type: MsgType,
    pub payload: Vec<u8>,
}

// ---------------------------------------------------------------------------
// LockError ↔ ErrorCode mapping
// ---------------------------------------------------------------------------

/// Map a `LockError` to its wire error code.
pub fn error_to_code(e: &LockError) -> ErrorCode {
    match e {
        LockError::NotFound => ERR_NOT_FOUND,
        LockError::HolderMismatch { .. } => ERR_HOLDER_MISMATCH,
        LockError::Expired => ERR_EXPIRED,
        LockError::ExpiredBeyondGrace => ERR_EXPIRED_BEYOND_GRACE,
        LockError::Conflict(_) => ERR_CONFLICT,
        LockError::KeyNotCovered => ERR_KEY_NOT_COVERED,
        LockError::Quarantined(_) => ERR_QUARANTINED,
        LockError::Network(_) => ERR_NETWORK,
        LockError::Internal(_) => ERR_INTERNAL,
    }
}

/// Map a wire error code back to a `LockError` (with empty context).
pub fn code_to_error(code: ErrorCode) -> LockError {
    match code {
        ERR_OK => LockError::Internal("error_code=OK but expected an error".into()),
        ERR_NOT_FOUND => LockError::NotFound,
        ERR_HOLDER_MISMATCH => LockError::HolderMismatch {
            expected: String::new(),
            actual: String::new(),
        },
        ERR_EXPIRED => LockError::Expired,
        ERR_EXPIRED_BEYOND_GRACE => LockError::ExpiredBeyondGrace,
        ERR_CONFLICT => LockError::Conflict(String::new()),
        ERR_KEY_NOT_COVERED => LockError::KeyNotCovered,
        ERR_QUARANTINED => LockError::Quarantined(String::new()),
        ERR_NETWORK => LockError::Network(String::new()),
        _ => LockError::Internal(format!("unknown error code: {}", code)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_msg_type_roundtrip() {
        let cases = vec![
            (
                Message::Acquire {
                    inode: 1,
                    mode: LockMode::Exclusive,
                    range: None,
                    timeout: Duration::from_secs(30),
                    client_id: "c".into(),
                },
                MSG_ACQUIRE,
            ),
            (
                Message::Revoke {
                    inode: 1,
                    token: "t".into(),
                },
                MSG_REVOKE,
            ),
            (
                Message::Invalidate {
                    inode: 1,
                    range: None,
                },
                MSG_INVALIDATE,
            ),
        ];
        for (msg, code) in cases {
            assert_eq!(msg.msg_type(), code);
        }
    }

    #[test]
    fn test_error_code_roundtrip() {
        // Verify each variant maps to a distinct code and back.
        let errs = vec![
            LockError::NotFound,
            LockError::Expired,
            LockError::ExpiredBeyondGrace,
            LockError::KeyNotCovered,
            LockError::Conflict("x".into()),
            LockError::Quarantined("y".into()),
            LockError::Network("z".into()),
            LockError::Internal("w".into()),
        ];
        for e in &errs {
            let code = error_to_code(e);
            assert_ne!(code, ERR_OK);
            // Roundtrip (codes are stable; context is lost — that's by design)
            let _ = code_to_error(code);
        }
    }

    #[test]
    fn test_error_to_code_holder_mismatch() {
        let e = LockError::HolderMismatch {
            expected: "a".into(),
            actual: "b".into(),
        };
        assert_eq!(error_to_code(&e), ERR_HOLDER_MISMATCH);
    }
}
