//! TLV encoder/decoder for the lock wire protocol.
//!
//! See `docs/lock-protocol.md` for the authoritative byte-level spec.
//! This module is the Rust reference implementation; the C kernel client
//! mirrors it independently.

use crate::error::CodecError;
use crate::msg::*;
use powerfs_lock::{LockMode, Range};
use std::collections::HashMap;
use std::time::Duration;

// ===========================================================================
// Low-level write helpers
// ===========================================================================

fn write_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn write_field(buf: &mut Vec<u8>, tag: u8, value: &[u8]) {
    buf.push(tag);
    write_u32(buf, value.len() as u32);
    buf.extend_from_slice(value);
}

fn write_u8_field(buf: &mut Vec<u8>, tag: u8, v: u8) {
    write_field(buf, tag, &[v]);
}

fn write_u64_field(buf: &mut Vec<u8>, tag: u8, v: u64) {
    write_field(buf, tag, &v.to_le_bytes());
}

fn write_string_field(buf: &mut Vec<u8>, tag: u8, s: &str) {
    write_field(buf, tag, s.as_bytes());
}

// ===========================================================================
// Low-level read helpers
// ===========================================================================

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn read_u8(&mut self) -> Result<u8, CodecError> {
        if self.remaining() < 1 {
            return Err(CodecError::Truncated {
                offset: self.pos,
                needed: 1,
            });
        }
        let v = self.data[self.pos];
        self.pos += 1;
        Ok(v)
    }

    fn read_u32(&mut self) -> Result<u32, CodecError> {
        if self.remaining() < 4 {
            return Err(CodecError::Truncated {
                offset: self.pos,
                needed: 4,
            });
        }
        let v = u32::from_le_bytes(self.data[self.pos..self.pos + 4].try_into().unwrap());
        self.pos += 4;
        Ok(v)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], CodecError> {
        if len > self.remaining() {
            return Err(CodecError::FieldTooLong {
                offset: self.pos,
                len,
            });
        }
        let v = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(v)
    }
}

// ===========================================================================
// Field map (tag → raw value bytes)
// ===========================================================================

type FieldMap = HashMap<u8, Vec<u8>>;

fn parse_fields(payload: &[u8]) -> Result<FieldMap, CodecError> {
    let mut r = Reader::new(payload);
    let mut map = HashMap::new();
    while r.remaining() > 0 {
        let tag = r.read_u8()?;
        let len = r.read_u32()? as usize;
        let value = r.read_bytes(len)?.to_vec();
        if map.insert(tag, value).is_some() {
            return Err(CodecError::DuplicateField(tag));
        }
    }
    Ok(map)
}

fn get_u8_field(map: &FieldMap, tag: u8) -> Result<Option<u8>, CodecError> {
    match map.get(&tag) {
        Some(v) if v.len() == 1 => Ok(Some(v[0])),
        Some(_) => Err(CodecError::Internal(format!(
            "field {:#x} expected 1 byte",
            tag
        ))),
        None => Ok(None),
    }
}

fn get_u64_field(map: &FieldMap, tag: u8) -> Result<Option<u64>, CodecError> {
    match map.get(&tag) {
        Some(v) if v.len() == 8 => Ok(Some(u64::from_le_bytes(v.as_slice().try_into().unwrap()))),
        Some(_) => Err(CodecError::Internal(format!(
            "field {:#x} expected 8 bytes",
            tag
        ))),
        None => Ok(None),
    }
}

fn get_string_field(map: &FieldMap, tag: u8) -> Result<Option<String>, CodecError> {
    match map.get(&tag) {
        Some(v) => {
            let s = String::from_utf8(v.clone()).map_err(|e| CodecError::InvalidUtf8 {
                tag,
                reason: e.to_string(),
            })?;
            Ok(Some(s))
        }
        None => Ok(None),
    }
}

fn require_u8_field(map: &FieldMap, tag: u8) -> Result<u8, CodecError> {
    get_u8_field(map, tag)?.ok_or(CodecError::MissingField(tag))
}

fn require_u64_field(map: &FieldMap, tag: u8) -> Result<u64, CodecError> {
    get_u64_field(map, tag)?.ok_or(CodecError::MissingField(tag))
}

fn require_string_field(map: &FieldMap, tag: u8) -> Result<String, CodecError> {
    get_string_field(map, tag)?.ok_or(CodecError::MissingField(tag))
}

// ===========================================================================
// Mode + range (de)serialization (shared by Acquire and Grant)
// ===========================================================================

fn encode_mode_and_range(buf: &mut Vec<u8>, mode: &LockMode, range: Option<Range>) {
    // For LockMode::Range(r), the embedded r is the canonical range; the
    // standalone `range` arg (if Some) overrides (matches LockRequest semantics
    // where explicit range takes precedence over the mode-embedded range).
    let (mode_byte, effective) = match mode {
        LockMode::Shared => (MODE_SHARED, range),
        LockMode::Exclusive => (MODE_EXCLUSIVE, range),
        LockMode::Range(r) => (MODE_RANGE, range.or(Some(*r))),
    };
    write_u8_field(buf, FIELD_MODE, mode_byte);
    if let Some(r) = effective {
        write_u64_field(buf, FIELD_RANGE_START, r.start);
        // EOF ranges omit FIELD_RANGE_END entirely (per wire spec).
        if let Some(end) = r.end {
            write_u64_field(buf, FIELD_RANGE_END, end);
        }
    }
}

fn decode_mode_and_range(map: &FieldMap) -> Result<(LockMode, Option<Range>), CodecError> {
    let mode_byte = get_u8_field(map, FIELD_MODE)?.ok_or(CodecError::MissingField(FIELD_MODE))?;
    let range_start = get_u64_field(map, FIELD_RANGE_START)?;
    let range_end = get_u64_field(map, FIELD_RANGE_END)?;

    let range = match (range_start, range_end) {
        (Some(start), Some(end)) if end != RANGE_END_EOF_SENTINEL => Some(Range {
            start,
            end: Some(end),
        }),
        (Some(start), _) => Some(Range { start, end: None }),
        (None, Some(_)) => {
            return Err(CodecError::Internal(
                "range_end present without range_start".into(),
            ))
        }
        (None, None) => None,
    };

    let mode = match mode_byte {
        MODE_SHARED => LockMode::Shared,
        MODE_EXCLUSIVE => LockMode::Exclusive,
        MODE_RANGE => {
            let r = range.ok_or(CodecError::MissingField(FIELD_RANGE_START))?;
            LockMode::Range(r)
        }
        _ => return Err(CodecError::InvalidMode(mode_byte)),
    };
    Ok((mode, range))
}

// ===========================================================================
// Public API: encode_frame / decode_frame
// ===========================================================================

/// Encode a `Message` into a full frame: `[msg_type:u8][len:u32 LE][payload]`.
pub fn encode_frame(msg: &Message) -> Result<Vec<u8>, CodecError> {
    let mut payload = Vec::new();
    match msg {
        Message::Acquire {
            inode,
            mode,
            range,
            timeout,
            client_id,
        } => {
            write_u64_field(&mut payload, FIELD_INODE, *inode);
            encode_mode_and_range(&mut payload, mode, *range);
            write_u64_field(&mut payload, FIELD_TIMEOUT_MS, timeout.as_millis() as u64);
            write_string_field(&mut payload, FIELD_CLIENT_ID, client_id);
        }
        Message::Grant {
            inode,
            token,
            sn,
            lease_ms,
            mode,
            range,
            error_code,
        } => {
            write_u64_field(&mut payload, FIELD_INODE, *inode);
            write_string_field(&mut payload, FIELD_TOKEN, token);
            // SN is reserved (0) during modularization; receivers must accept
            // its absence for forward compatibility with older servers.
            if *sn != 0 {
                write_u64_field(&mut payload, FIELD_SN, *sn);
            }
            write_u64_field(&mut payload, FIELD_LEASE_MS, *lease_ms);
            encode_mode_and_range(&mut payload, mode, *range);
            write_u8_field(&mut payload, FIELD_ERROR_CODE, *error_code);
        }
        Message::Release {
            inode,
            token,
            client_id,
        } => {
            write_u64_field(&mut payload, FIELD_INODE, *inode);
            write_string_field(&mut payload, FIELD_TOKEN, token);
            write_string_field(&mut payload, FIELD_CLIENT_ID, client_id);
        }
        Message::ReleaseAck { inode, error_code } => {
            write_u64_field(&mut payload, FIELD_INODE, *inode);
            write_u8_field(&mut payload, FIELD_ERROR_CODE, *error_code);
        }
        Message::Renew {
            inode,
            token,
            timeout,
            client_id,
        } => {
            write_u64_field(&mut payload, FIELD_INODE, *inode);
            write_string_field(&mut payload, FIELD_TOKEN, token);
            write_u64_field(&mut payload, FIELD_TIMEOUT_MS, timeout.as_millis() as u64);
            write_string_field(&mut payload, FIELD_CLIENT_ID, client_id);
        }
        Message::RenewAck {
            inode,
            lease_ms,
            error_code,
        } => {
            write_u64_field(&mut payload, FIELD_INODE, *inode);
            write_u64_field(&mut payload, FIELD_LEASE_MS, *lease_ms);
            write_u8_field(&mut payload, FIELD_ERROR_CODE, *error_code);
        }
        Message::Revoke { inode, token } => {
            write_u64_field(&mut payload, FIELD_INODE, *inode);
            write_string_field(&mut payload, FIELD_TOKEN, token);
        }
        Message::Invalidate { inode, range } => {
            write_u64_field(&mut payload, FIELD_INODE, *inode);
            if let Some(r) = range {
                write_u64_field(&mut payload, FIELD_RANGE_START, r.start);
                if let Some(end) = r.end {
                    write_u64_field(&mut payload, FIELD_RANGE_END, end);
                }
            }
        }
        Message::RevokeAck {
            inode,
            token,
            client_id,
        } => {
            write_u64_field(&mut payload, FIELD_INODE, *inode);
            write_string_field(&mut payload, FIELD_TOKEN, token);
            write_string_field(&mut payload, FIELD_CLIENT_ID, client_id);
        }
    }

    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(msg.msg_type());
    write_u32(&mut frame, payload.len() as u32);
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Decode a full frame `[msg_type:u8][len:u32 LE][payload]` into a `Message`.
///
/// Trailing bytes after the declared payload are tolerated (ignored) so
/// that future field additions stay forward-compatible.
pub fn decode_frame(bytes: &[u8]) -> Result<Message, CodecError> {
    let mut r = Reader::new(bytes);
    let msg_type = r.read_u8()?;
    let payload_len = r.read_u32()? as usize;
    let payload = r.read_bytes(payload_len)?;

    let map = parse_fields(payload)?;

    let msg = match msg_type {
        MSG_ACQUIRE => {
            let inode = require_u64_field(&map, FIELD_INODE)?;
            let (mode, range) = decode_mode_and_range(&map)?;
            let timeout_ms = require_u64_field(&map, FIELD_TIMEOUT_MS)?;
            let client_id = require_string_field(&map, FIELD_CLIENT_ID)?;
            Message::Acquire {
                inode,
                mode,
                range,
                timeout: Duration::from_millis(timeout_ms),
                client_id,
            }
        }
        MSG_GRANT => {
            let inode = require_u64_field(&map, FIELD_INODE)?;
            let token = require_string_field(&map, FIELD_TOKEN)?;
            // SN optional during modularization phase
            let sn = get_u64_field(&map, FIELD_SN)?.unwrap_or(0);
            let lease_ms = require_u64_field(&map, FIELD_LEASE_MS)?;
            let (mode, range) = decode_mode_and_range(&map)?;
            let error_code = get_u8_field(&map, FIELD_ERROR_CODE)?.unwrap_or(ERR_OK);
            Message::Grant {
                inode,
                token,
                sn,
                lease_ms,
                mode,
                range,
                error_code,
            }
        }
        MSG_RELEASE => {
            let inode = require_u64_field(&map, FIELD_INODE)?;
            let token = require_string_field(&map, FIELD_TOKEN)?;
            let client_id = require_string_field(&map, FIELD_CLIENT_ID)?;
            Message::Release {
                inode,
                token,
                client_id,
            }
        }
        MSG_RELEASE_ACK => {
            let inode = require_u64_field(&map, FIELD_INODE)?;
            let error_code = require_u8_field(&map, FIELD_ERROR_CODE)?;
            Message::ReleaseAck { inode, error_code }
        }
        MSG_RENEW => {
            let inode = require_u64_field(&map, FIELD_INODE)?;
            let token = require_string_field(&map, FIELD_TOKEN)?;
            let timeout_ms = require_u64_field(&map, FIELD_TIMEOUT_MS)?;
            let client_id = require_string_field(&map, FIELD_CLIENT_ID)?;
            Message::Renew {
                inode,
                token,
                timeout: Duration::from_millis(timeout_ms),
                client_id,
            }
        }
        MSG_RENEW_ACK => {
            let inode = require_u64_field(&map, FIELD_INODE)?;
            let lease_ms = require_u64_field(&map, FIELD_LEASE_MS)?;
            let error_code = require_u8_field(&map, FIELD_ERROR_CODE)?;
            Message::RenewAck {
                inode,
                lease_ms,
                error_code,
            }
        }
        MSG_REVOKE => {
            let inode = require_u64_field(&map, FIELD_INODE)?;
            let token = require_string_field(&map, FIELD_TOKEN)?;
            Message::Revoke { inode, token }
        }
        MSG_INVALIDATE => {
            let inode = require_u64_field(&map, FIELD_INODE)?;
            let range_start = get_u64_field(&map, FIELD_RANGE_START)?;
            let range_end = get_u64_field(&map, FIELD_RANGE_END)?;
            let range = match (range_start, range_end) {
                (Some(start), Some(end)) if end != RANGE_END_EOF_SENTINEL => Some(Range {
                    start,
                    end: Some(end),
                }),
                (Some(start), _) => Some(Range { start, end: None }),
                (None, _) => None,
            };
            Message::Invalidate { inode, range }
        }
        MSG_REVOKE_ACK => {
            let inode = require_u64_field(&map, FIELD_INODE)?;
            let token = require_string_field(&map, FIELD_TOKEN)?;
            let client_id = require_string_field(&map, FIELD_CLIENT_ID)?;
            Message::RevokeAck {
                inode,
                token,
                client_id,
            }
        }
        _ => return Err(CodecError::InvalidMsgType(msg_type)),
    };
    Ok(msg)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use powerfs_lock::{LockMode, Range};

    fn roundtrip(msg: Message) {
        let bytes = encode_frame(&msg).expect("encode");
        let decoded = decode_frame(&bytes).expect("decode");
        assert_eq!(msg, decoded, "roundtrip mismatch for {:?}", msg.msg_type());
    }

    #[test]
    fn test_roundtrip_acquire_inode_level() {
        roundtrip(Message::Acquire {
            inode: 42,
            mode: LockMode::Exclusive,
            range: None,
            timeout: Duration::from_secs(30),
            client_id: "client-A".into(),
        });
    }

    #[test]
    fn test_roundtrip_acquire_range_level_eof() {
        roundtrip(Message::Acquire {
            inode: 42,
            mode: LockMode::Range(Range::new(0, None)),
            range: Some(Range::new(0, None)),
            timeout: Duration::from_secs(30),
            client_id: "client-A".into(),
        });
    }

    #[test]
    fn test_roundtrip_acquire_range_level_bounded() {
        roundtrip(Message::Acquire {
            inode: 42,
            mode: LockMode::Shared,
            range: Some(Range::new(4096, Some(8192))),
            timeout: Duration::from_millis(5000),
            client_id: "c".into(),
        });
    }

    #[test]
    fn test_roundtrip_grant_success() {
        roundtrip(Message::Grant {
            inode: 7,
            token: "lease-0-abc".into(),
            sn: 0,
            lease_ms: 30_000,
            mode: LockMode::Exclusive,
            range: None,
            error_code: ERR_OK,
        });
    }

    #[test]
    fn test_roundtrip_grant_with_sn() {
        // Forward-compat: SN present
        roundtrip(Message::Grant {
            inode: 7,
            token: "lease-1-def".into(),
            sn: 12345,
            lease_ms: 30_000,
            mode: LockMode::Range(Range::new(0, Some(4096))),
            range: Some(Range::new(0, Some(4096))),
            error_code: ERR_OK,
        });
    }

    #[test]
    fn test_roundtrip_grant_error() {
        roundtrip(Message::Grant {
            inode: 7,
            token: "".into(),
            sn: 0,
            lease_ms: 0,
            mode: LockMode::Exclusive,
            range: None,
            error_code: ERR_CONFLICT,
        });
    }

    #[test]
    fn test_roundtrip_release() {
        roundtrip(Message::Release {
            inode: 1,
            token: "t".into(),
            client_id: "c".into(),
        });
    }

    #[test]
    fn test_roundtrip_release_ack() {
        roundtrip(Message::ReleaseAck {
            inode: 1,
            error_code: ERR_OK,
        });
    }

    #[test]
    fn test_roundtrip_renew() {
        roundtrip(Message::Renew {
            inode: 1,
            token: "t".into(),
            timeout: Duration::from_secs(60),
            client_id: "c".into(),
        });
    }

    #[test]
    fn test_roundtrip_renew_ack() {
        roundtrip(Message::RenewAck {
            inode: 1,
            lease_ms: 60_000,
            error_code: ERR_OK,
        });
    }

    #[test]
    fn test_roundtrip_revoke() {
        roundtrip(Message::Revoke {
            inode: 1,
            token: "t".into(),
        });
    }

    #[test]
    fn test_roundtrip_revoke_ack() {
        roundtrip(Message::RevokeAck {
            inode: 1,
            token: "t".into(),
            client_id: "c".into(),
        });
    }

    #[test]
    fn test_roundtrip_invalidate_full_inode() {
        roundtrip(Message::Invalidate {
            inode: 1,
            range: None,
        });
    }

    #[test]
    fn test_roundtrip_invalidate_eof_range() {
        roundtrip(Message::Invalidate {
            inode: 1,
            range: Some(Range::new(0, None)),
        });
    }

    #[test]
    fn test_roundtrip_invalidate_bounded_range() {
        roundtrip(Message::Invalidate {
            inode: 1,
            range: Some(Range::new(100, Some(200))),
        });
    }

    // --- Error paths ---

    #[test]
    fn test_decode_truncated_header() {
        // Only 2 bytes — not enough for msg_type + len
        assert!(matches!(
            decode_frame(&[0x01, 0x00]),
            Err(CodecError::Truncated { .. })
        ));
    }

    #[test]
    fn test_decode_invalid_msg_type() {
        // msg_type=0xFF, len=0
        let bytes = [0xFF, 0x00, 0x00, 0x00, 0x00];
        assert!(matches!(
            decode_frame(&bytes),
            Err(CodecError::InvalidMsgType(0xFF))
        ));
    }

    #[test]
    fn test_decode_payload_too_long() {
        // msg_type=ACQUIRE, len=999 but no payload
        let bytes = [0x01, 0xE7, 0x03, 0x00, 0x00];
        assert!(matches!(
            decode_frame(&bytes),
            Err(CodecError::FieldTooLong { .. })
        ));
    }

    #[test]
    fn test_decode_missing_required_field() {
        // Acquire with only inode field, missing mode/timeout/client_id
        let mut payload = Vec::new();
        write_u64_field(&mut payload, FIELD_INODE, 42);
        let mut frame = vec![MSG_ACQUIRE];
        write_u32(&mut frame, payload.len() as u32);
        frame.extend_from_slice(&payload);
        assert!(matches!(
            decode_frame(&frame),
            Err(CodecError::MissingField(tag)) if tag == FIELD_MODE
        ));
    }

    #[test]
    fn test_decode_duplicate_field() {
        // Two FIELD_INODE entries
        let mut payload = Vec::new();
        write_u64_field(&mut payload, FIELD_INODE, 1);
        write_u64_field(&mut payload, FIELD_INODE, 2);
        let mut frame = vec![MSG_ACQUIRE];
        write_u32(&mut frame, payload.len() as u32);
        frame.extend_from_slice(&payload);
        assert!(matches!(
            decode_frame(&frame),
            Err(CodecError::DuplicateField(tag)) if tag == FIELD_INODE
        ));
    }

    #[test]
    fn test_decode_invalid_mode_byte() {
        // Acquire with mode=0xFF (invalid)
        let mut payload = Vec::new();
        write_u64_field(&mut payload, FIELD_INODE, 1);
        write_u8_field(&mut payload, FIELD_MODE, 0xFF);
        write_u64_field(&mut payload, FIELD_TIMEOUT_MS, 1000);
        write_string_field(&mut payload, FIELD_CLIENT_ID, "c");
        let mut frame = vec![MSG_ACQUIRE];
        write_u32(&mut frame, payload.len() as u32);
        frame.extend_from_slice(&payload);
        assert!(matches!(
            decode_frame(&frame),
            Err(CodecError::InvalidMode(0xFF))
        ));
    }

    #[test]
    fn test_decode_invalid_utf8() {
        // Acquire with invalid UTF-8 in client_id
        let mut payload = Vec::new();
        write_u64_field(&mut payload, FIELD_INODE, 1);
        write_u8_field(&mut payload, FIELD_MODE, MODE_EXCLUSIVE);
        write_u64_field(&mut payload, FIELD_TIMEOUT_MS, 1000);
        write_field(&mut payload, FIELD_CLIENT_ID, &[0xFF, 0xFE]);
        let mut frame = vec![MSG_ACQUIRE];
        write_u32(&mut frame, payload.len() as u32);
        frame.extend_from_slice(&payload);
        assert!(matches!(
            decode_frame(&frame),
            Err(CodecError::InvalidUtf8 { tag, .. }) if tag == FIELD_CLIENT_ID
        ));
    }

    #[test]
    fn test_decode_trailing_bytes_ignored() {
        // Append trailing bytes after a valid frame — must be ignored.
        let msg = Message::Release {
            inode: 1,
            token: "t".into(),
            client_id: "c".into(),
        };
        let mut bytes = encode_frame(&msg).unwrap();
        bytes.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        let decoded = decode_frame(&bytes).expect("trailing tolerated");
        assert_eq!(decoded, msg);
    }

    #[test]
    fn test_encode_grant_sn_zero_omitted() {
        // Grant with sn=0 must NOT include FIELD_SN (forward compat).
        let msg = Message::Grant {
            inode: 1,
            token: "t".into(),
            sn: 0,
            lease_ms: 1000,
            mode: LockMode::Exclusive,
            range: None,
            error_code: ERR_OK,
        };
        let bytes = encode_frame(&msg).unwrap();
        // Verify FIELD_SN (tag 0x07) is not present in payload
        // Frame layout: [msg_type][len:4][payload]
        let payload = &bytes[5..];
        let mut r = Reader::new(payload);
        let mut found_sn = false;
        while r.remaining() > 0 {
            let tag = r.read_u8().unwrap();
            let len = r.read_u32().unwrap() as usize;
            let _ = r.read_bytes(len).unwrap();
            if tag == FIELD_SN {
                found_sn = true;
            }
        }
        assert!(!found_sn, "FIELD_SN must be omitted when sn=0");
    }
}
