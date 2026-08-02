//! TLV (Type-Length-Value) serialization/deserialization
//!
//! This module provides a simple binary serialization format that is
//! compatible with both Rust and C implementations.
//!
//! Format per field:
//!   field_id (1B) | length (4B, big-endian u32) | value (length bytes)
//!
//! Supports up to 4GB per value field, suitable for both metadata
//! operations and large data payloads.

use crate::errors::NetError;
use crate::protocol::{FieldId, MAX_TLV_VALUE_LEN};

/// Result of decode_setattr_req: (ino, mode, uid, gid, size)
pub type SetattrResult = (u64, Option<u32>, Option<u32>, Option<u32>, Option<u64>);

/// TLV encoder for building request/response bodies
pub struct TlvEncoder {
    buf: Vec<u8>,
}

impl TlvEncoder {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(256),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap),
        }
    }

    /// Get the encoded bytes
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    // ========================================================================
    // Encode methods for basic types
    // ========================================================================

    /// Add a uint8 field
    pub fn add_u8(&mut self, field: FieldId, value: u8) -> &mut Self {
        self.write_header(field, 1);
        self.buf.push(value);
        self
    }

    /// Add a uint16 field
    pub fn add_u16(&mut self, field: FieldId, value: u16) -> &mut Self {
        self.write_header(field, 2);
        self.buf.extend_from_slice(&value.to_le_bytes());
        self
    }

    /// Add a uint32 field
    pub fn add_u32(&mut self, field: FieldId, value: u32) -> &mut Self {
        self.write_header(field, 4);
        self.buf.extend_from_slice(&value.to_le_bytes());
        self
    }

    /// Add a uint64 field
    pub fn add_u64(&mut self, field: FieldId, value: u64) -> &mut Self {
        self.write_header(field, 8);
        self.buf.extend_from_slice(&value.to_le_bytes());
        self
    }

    /// Add a string field
    pub fn add_string(&mut self, field: FieldId, value: &str) -> Result<&mut Self, NetError> {
        let bytes = value.as_bytes();
        if bytes.len() > MAX_TLV_VALUE_LEN as usize {
            return Err(NetError::Serialize(format!(
                "string too long: {} > {}",
                bytes.len(),
                MAX_TLV_VALUE_LEN
            )));
        }
        self.write_header(field, bytes.len() as u32);
        self.buf.extend_from_slice(bytes);
        Ok(self)
    }

    /// Add raw bytes field
    pub fn add_bytes(&mut self, field: FieldId, value: &[u8]) -> Result<&mut Self, NetError> {
        if value.len() > MAX_TLV_VALUE_LEN as usize {
            return Err(NetError::Serialize(format!(
                "bytes too long: {} > {}",
                value.len(),
                MAX_TLV_VALUE_LEN
            )));
        }
        self.write_header(field, value.len() as u32);
        self.buf.extend_from_slice(value);
        Ok(self)
    }

    // ========================================================================
    // New helper methods for extended fields
    // ========================================================================

    /// Add a RequestId (UUID string)
    pub fn add_request_id(&mut self, request_id: &str) -> Result<&mut Self, NetError> {
        self.add_string(FieldId::RequestId, request_id)
    }

    /// Add a ClientUuid
    pub fn add_client_uuid(&mut self, client_uuid: &str) -> Result<&mut Self, NetError> {
        self.add_string(FieldId::ClientUuid, client_uuid)
    }

    /// Add a ChannelId
    pub fn add_channel_id(&mut self, channel_id: u16) -> &mut Self {
        self.add_u16(FieldId::ChannelId, channel_id)
    }

    /// Add a ShardHash
    pub fn add_shard_hash(&mut self, shard_hash: u64) -> &mut Self {
        self.add_u64(FieldId::ShardHash, shard_hash)
    }

    /// Add a ShardId
    pub fn add_shard_id(&mut self, shard_id: u32) -> &mut Self {
        self.add_u32(FieldId::ShardId, shard_id)
    }

    /// Add a ShardLeader address
    pub fn add_shard_leader(&mut self, leader: &str) -> Result<&mut Self, NetError> {
        self.add_string(FieldId::ShardLeader, leader)
    }

    /// Add a VolumeListPayload (raw serialized topology data)
    pub fn add_volume_list_payload(&mut self, payload: &[u8]) -> Result<&mut Self, NetError> {
        self.add_bytes(FieldId::VolumeListPayload, payload)
    }

    /// Add a TopologyVersion
    pub fn add_topology_version(&mut self, version: u64) -> &mut Self {
        self.add_u64(FieldId::TopologyVersion, version)
    }

    /// Add a LeaseToken
    pub fn add_lease_token(&mut self, token: &str) -> Result<&mut Self, NetError> {
        self.add_string(FieldId::LeaseToken, token)
    }

    /// Add a LeaseRangeOffset
    pub fn add_lease_range_offset(&mut self, offset: u64) -> &mut Self {
        self.add_u64(FieldId::LeaseRangeOffset, offset)
    }

    /// Add a LeaseRangeLength
    pub fn add_lease_range_length(&mut self, length: u64) -> &mut Self {
        self.add_u64(FieldId::LeaseRangeLength, length)
    }

    /// Add a nested TLV-encoded field (for repeated/complex types)
    pub fn add_nested(&mut self, field: FieldId, value: &[u8]) -> Result<&mut Self, NetError> {
        if value.len() > MAX_TLV_VALUE_LEN as usize {
            return Err(NetError::Serialize(format!(
                "nested too long: {} > {}",
                value.len(),
                MAX_TLV_VALUE_LEN
            )));
        }
        self.write_header(field, value.len() as u32);
        self.buf.extend_from_slice(value);
        Ok(self)
    }

    // ========================================================================
    // Helper: write TLV header
    // ========================================================================

    fn write_header(&mut self, field: FieldId, length: u32) {
        self.buf.push(field.as_u8());
        self.buf.extend_from_slice(&length.to_be_bytes());
    }
}

impl Default for TlvEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// TLV decoder for parsing request/response bodies
pub struct TlvDecoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> TlvDecoder<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    // ========================================================================
    // Decode methods for basic types
    // ========================================================================

    /// Peek at the next field ID without consuming it
    pub fn peek_field(&self) -> Option<FieldId> {
        if self.pos >= self.buf.len() {
            return None;
        }
        FieldId::from_u8(self.buf[self.pos])
    }

    /// Get the next field ID and length, skipping unknown fields
    pub fn next_field(&mut self) -> Option<(FieldId, u32)> {
        while self.pos + 5 <= self.buf.len() {
            let field_id = self.buf[self.pos];
            let length = u32::from_be_bytes([
                self.buf[self.pos + 1],
                self.buf[self.pos + 2],
                self.buf[self.pos + 3],
                self.buf[self.pos + 4],
            ]);
            self.pos += 5;

            if let Some(field) = FieldId::from_u8(field_id) {
                return Some((field, length));
            }

            // Unknown field - skip it
            let end = self.pos + length as usize;
            if end > self.buf.len() {
                return None; // Malformed
            }
            self.pos = end;
        }
        None
    }

    /// Skip the current field
    pub fn skip(&mut self, length: u32) -> Result<(), NetError> {
        let end = self.pos + length as usize;
        if end > self.buf.len() {
            return Err(NetError::Serialize("unexpected end of data".into()));
        }
        self.pos = end;
        Ok(())
    }

    /// Decode a uint8 value (current position should be at the value)
    pub fn read_u8(&mut self, length: u32) -> Result<u8, NetError> {
        if length != 1 {
            return Err(NetError::Serialize(format!(
                "expected 1 byte, got {}",
                length
            )));
        }
        if self.pos + 1 > self.buf.len() {
            return Err(NetError::Serialize("unexpected end of data".into()));
        }
        let val = self.buf[self.pos];
        self.pos += 1;
        Ok(val)
    }

    /// Decode a uint16 value
    pub fn read_u16(&mut self, length: u32) -> Result<u16, NetError> {
        if length != 2 {
            return Err(NetError::Serialize(format!(
                "expected 2 bytes, got {}",
                length
            )));
        }
        if self.pos + 2 > self.buf.len() {
            return Err(NetError::Serialize("unexpected end of data".into()));
        }
        let val = u16::from_le_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(val)
    }

    /// Decode a uint32 value
    pub fn read_u32(&mut self, length: u32) -> Result<u32, NetError> {
        if length != 4 {
            return Err(NetError::Serialize(format!(
                "expected 4 bytes, got {}",
                length
            )));
        }
        if self.pos + 4 > self.buf.len() {
            return Err(NetError::Serialize("unexpected end of data".into()));
        }
        let bytes: [u8; 4] = self.buf[self.pos..self.pos + 4]
            .try_into()
            .map_err(|_| NetError::Serialize("buffer too short".into()))?;
        let val = u32::from_le_bytes(bytes);
        self.pos += 4;
        Ok(val)
    }

    /// Decode a uint64 value
    pub fn read_u64(&mut self, length: u32) -> Result<u64, NetError> {
        if length != 8 {
            return Err(NetError::Serialize(format!(
                "expected 8 bytes, got {}",
                length
            )));
        }
        if self.pos + 8 > self.buf.len() {
            return Err(NetError::Serialize("unexpected end of data".into()));
        }
        let bytes: [u8; 8] = self.buf[self.pos..self.pos + 8]
            .try_into()
            .map_err(|_| NetError::Serialize("buffer too short".into()))?;
        let val = u64::from_le_bytes(bytes);
        self.pos += 8;
        Ok(val)
    }

    /// Decode a string value
    pub fn read_string(&mut self, length: u32) -> Result<&'a str, NetError> {
        if self.pos + length as usize > self.buf.len() {
            return Err(NetError::Serialize("unexpected end of data".into()));
        }
        let val = std::str::from_utf8(&self.buf[self.pos..self.pos + length as usize])
            .map_err(|_| NetError::Serialize("invalid UTF-8".into()))?;
        self.pos += length as usize;
        Ok(val)
    }

    /// Decode raw bytes
    pub fn read_bytes(&mut self, length: u32) -> Result<&'a [u8], NetError> {
        if self.pos + length as usize > self.buf.len() {
            return Err(NetError::Serialize("unexpected end of data".into()));
        }
        let val = &self.buf[self.pos..self.pos + length as usize];
        self.pos += length as usize;
        Ok(val)
    }

    /// Decode a nested TLV structure as raw bytes
    pub fn read_nested(&mut self, length: u32) -> Result<&'a [u8], NetError> {
        self.read_bytes(length)
    }

    // ========================================================================
    // New helper methods for extended fields
    // ========================================================================

    /// Read a RequestId (as String)
    pub fn next_request_id(&mut self) -> Result<String, NetError> {
        self.next_string(FieldId::RequestId)
    }

    /// Read a ClientUuid (as String)
    pub fn next_client_uuid(&mut self) -> Result<String, NetError> {
        self.next_string(FieldId::ClientUuid)
    }

    /// Read a ChannelId
    pub fn next_channel_id(&mut self) -> Result<u16, NetError> {
        self.next_u16(FieldId::ChannelId)
    }

    /// Read a ShardHash
    pub fn next_shard_hash(&mut self) -> Result<u64, NetError> {
        self.next_u64(FieldId::ShardHash)
    }

    /// Read a ShardId
    pub fn next_shard_id(&mut self) -> Result<u32, NetError> {
        self.next_u32(FieldId::ShardId)
    }

    /// Read a ShardLeader (as String)
    pub fn next_shard_leader(&mut self) -> Result<String, NetError> {
        self.next_string(FieldId::ShardLeader)
    }

    /// Read a VolumeListPayload (as Vec<u8>)
    pub fn next_volume_list_payload(&mut self) -> Result<Vec<u8>, NetError> {
        self.next_bytes(FieldId::VolumeListPayload)
    }

    /// Read a TopologyVersion
    pub fn next_topology_version(&mut self) -> Result<u64, NetError> {
        self.next_u64(FieldId::TopologyVersion)
    }

    /// Read a LeaseToken (as String)
    pub fn next_lease_token(&mut self) -> Result<String, NetError> {
        self.next_string(FieldId::LeaseToken)
    }

    /// Read a LeaseRangeOffset
    pub fn next_lease_range_offset(&mut self) -> Result<u64, NetError> {
        self.next_u64(FieldId::LeaseRangeOffset)
    }

    /// Read a LeaseRangeLength
    pub fn next_lease_range_length(&mut self) -> Result<u64, NetError> {
        self.next_u64(FieldId::LeaseRangeLength)
    }

    // ========================================================================
    // Convenience methods: read a named field in one call
    // ========================================================================

    /// Check if the next field has the given FieldId without consuming it
    pub fn has_field(&self, field: FieldId) -> bool {
        self.peek_field() == Some(field)
    }

    /// Read a u64 value for the given field (consumes the field)
    pub fn next_u64(&mut self, field: FieldId) -> Result<u64, NetError> {
        match self.next_field() {
            Some((f, length)) => {
                if f != field {
                    return Err(NetError::Serialize(format!(
                        "expected field {:?}, got {:?}",
                        field, f
                    )));
                }
                self.read_u64(length)
            }
            None => Err(NetError::Serialize(format!(
                "no more fields, expected {:?}",
                field
            ))),
        }
    }

    /// Read a u32 value for the given field (consumes the field)
    pub fn next_u32(&mut self, field: FieldId) -> Result<u32, NetError> {
        match self.next_field() {
            Some((f, length)) => {
                if f != field {
                    return Err(NetError::Serialize(format!(
                        "expected field {:?}, got {:?}",
                        field, f
                    )));
                }
                self.read_u32(length)
            }
            None => Err(NetError::Serialize(format!(
                "no more fields, expected {:?}",
                field
            ))),
        }
    }

    /// Read a u16 value for the given field (consumes the field)
    pub fn next_u16(&mut self, field: FieldId) -> Result<u16, NetError> {
        match self.next_field() {
            Some((f, length)) => {
                if f != field {
                    return Err(NetError::Serialize(format!(
                        "expected field {:?}, got {:?}",
                        field, f
                    )));
                }
                self.read_u16(length)
            }
            None => Err(NetError::Serialize(format!(
                "no more fields, expected {:?}",
                field
            ))),
        }
    }

    /// Read a u8 value for the given field (consumes the field)
    pub fn next_u8(&mut self, field: FieldId) -> Result<u8, NetError> {
        match self.next_field() {
            Some((f, length)) => {
                if f != field {
                    return Err(NetError::Serialize(format!(
                        "expected field {:?}, got {:?}",
                        field, f
                    )));
                }
                self.read_u8(length)
            }
            None => Err(NetError::Serialize(format!(
                "no more fields, expected {:?}",
                field
            ))),
        }
    }

    /// Read a string value for the given field (consumes the field, returns owned String)
    pub fn next_string(&mut self, field: FieldId) -> Result<String, NetError> {
        match self.next_field() {
            Some((f, length)) => {
                if f != field {
                    return Err(NetError::Serialize(format!(
                        "expected field {:?}, got {:?}",
                        field, f
                    )));
                }
                let s = self.read_string(length)?;
                Ok(s.to_string())
            }
            None => Err(NetError::Serialize(format!(
                "no more fields, expected {:?}",
                field
            ))),
        }
    }

    /// Read raw bytes for the given field (consumes the field, returns owned Vec<u8>)
    pub fn next_bytes(&mut self, field: FieldId) -> Result<Vec<u8>, NetError> {
        match self.next_field() {
            Some((f, length)) => {
                if f != field {
                    return Err(NetError::Serialize(format!(
                        "expected field {:?}, got {:?}",
                        field, f
                    )));
                }
                let data = self.read_bytes(length)?;
                Ok(data.to_vec())
            }
            None => Err(NetError::Serialize(format!(
                "no more fields, expected {:?}",
                field
            ))),
        }
    }
}

// ============================================================================
// High-level encoding/decoding helpers for common message patterns
// ============================================================================

/// Encode a lookup request: parent_ino + name
pub fn encode_lookup_req(parent_ino: u64, name: &str) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::ParentIno, parent_ino);
    enc.add_string(FieldId::Name, name)?;
    Ok(enc.into_bytes())
}

/// Decode a lookup request
pub fn decode_lookup_req(body: &[u8]) -> Result<(u64, String), NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut parent_ino = 0u64;
    let mut name = String::new();

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::ParentIno => parent_ino = dec.read_u64(length)?,
            FieldId::Name => name = dec.read_string(length)?.to_string(),
            _ => dec.skip(length)?,
        }
    }

    Ok((parent_ino, name))
}

/// Encode a create request: parent_ino + name + mode + uid + gid
pub fn encode_create_req(
    parent_ino: u64,
    name: &str,
    mode: u32,
    uid: u32,
    gid: u32,
) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::ParentIno, parent_ino);
    enc.add_string(FieldId::Name, name)?;
    enc.add_u32(FieldId::Mode, mode);
    enc.add_u32(FieldId::Uid, uid);
    enc.add_u32(FieldId::Gid, gid);
    Ok(enc.into_bytes())
}

/// Decode a create request
pub fn decode_create_req(body: &[u8]) -> Result<(u64, String, u32, u32, u32), NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut parent_ino = 0u64;
    let mut name = String::new();
    let mut mode = 0u32;
    let mut uid = 0u32;
    let mut gid = 0u32;

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::ParentIno => parent_ino = dec.read_u64(length)?,
            FieldId::Name => name = dec.read_string(length)?.to_string(),
            FieldId::Mode => mode = dec.read_u32(length)?,
            FieldId::Uid => uid = dec.read_u32(length)?,
            FieldId::Gid => gid = dec.read_u32(length)?,
            _ => dec.skip(length)?,
        }
    }

    Ok((parent_ino, name, mode, uid, gid))
}

/// Encode an entry response (common pattern for many operations)
#[allow(clippy::too_many_arguments)]
pub fn encode_entry_resp(
    ino: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    nlink: u32,
    mtime: u64,
    atime: u64,
    ctime: u64,
    is_dir: bool,
    symlink_target: Option<&str>,
) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, ino);
    enc.add_u32(FieldId::Mode, mode);
    enc.add_u32(FieldId::Uid, uid);
    enc.add_u32(FieldId::Gid, gid);
    enc.add_u64(FieldId::Size, size);
    enc.add_u32(FieldId::Nlink, nlink);
    enc.add_u64(FieldId::Mtime, mtime);
    enc.add_u64(FieldId::Atime, atime);
    enc.add_u64(FieldId::Ctime, ctime);
    enc.add_u8(FieldId::IsDir, is_dir as u8);
    if let Some(target) = symlink_target {
        enc.add_string(FieldId::SymlinkTarget, target)?;
    }
    Ok(enc.into_bytes())
}

/// Decode an entry response
pub struct EntryInfo {
    pub ino: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub nlink: u32,
    pub mtime: u64,
    pub atime: u64,
    pub ctime: u64,
    pub is_dir: bool,
    pub symlink_target: Option<String>,
    pub name: String,
    pub version: u64,
}

impl EntryInfo {
    pub fn default_entry() -> Self {
        Self {
            ino: 0,
            mode: 0,
            uid: 0,
            gid: 0,
            size: 0,
            nlink: 0,
            mtime: 0,
            atime: 0,
            ctime: 0,
            is_dir: false,
            symlink_target: None,
            name: String::new(),
            version: 0,
        }
    }
}

pub fn decode_entry_resp(body: &[u8]) -> Result<EntryInfo, NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut entry = EntryInfo::default_entry();

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::Ino => entry.ino = dec.read_u64(length)?,
            FieldId::Mode => entry.mode = dec.read_u32(length)?,
            FieldId::Uid => entry.uid = dec.read_u32(length)?,
            FieldId::Gid => entry.gid = dec.read_u32(length)?,
            FieldId::Size => entry.size = dec.read_u64(length)?,
            FieldId::Nlink => entry.nlink = dec.read_u32(length)?,
            FieldId::Mtime => entry.mtime = dec.read_u64(length)?,
            FieldId::Atime => entry.atime = dec.read_u64(length)?,
            FieldId::Ctime => entry.ctime = dec.read_u64(length)?,
            FieldId::IsDir => entry.is_dir = dec.read_u8(length)? != 0,
            FieldId::SymlinkTarget => {
                entry.symlink_target = Some(dec.read_string(length)?.to_string());
            }
            FieldId::Name => {
                entry.name = dec.read_string(length)?.to_string();
            }
            FieldId::Version => entry.version = dec.read_u64(length)?,
            _ => dec.skip(length)?,
        }
    }

    Ok(entry)
}

/// Encode a rename request
pub fn encode_rename_req(
    old_parent_ino: u64,
    old_name: &str,
    new_parent_ino: u64,
    new_name: &str,
) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::ParentIno, old_parent_ino);
    enc.add_string(FieldId::Name, old_name)?;
    enc.add_u64(FieldId::NewParentIno, new_parent_ino);
    enc.add_string(FieldId::NewName, new_name)?;
    Ok(enc.into_bytes())
}

/// Decode a rename request
pub fn decode_rename_req(body: &[u8]) -> Result<(u64, String, u64, String), NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut old_parent_ino = 0u64;
    let mut old_name = String::new();
    let mut new_parent_ino = 0u64;
    let mut new_name = String::new();

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::ParentIno => old_parent_ino = dec.read_u64(length)?,
            FieldId::Name => old_name = dec.read_string(length)?.to_string(),
            FieldId::NewParentIno => new_parent_ino = dec.read_u64(length)?,
            FieldId::NewName => new_name = dec.read_string(length)?.to_string(),
            _ => dec.skip(length)?,
        }
    }

    Ok((old_parent_ino, old_name, new_parent_ino, new_name))
}

/// Encode an unlink/rmdir request
pub fn encode_delete_req(ino: u64, is_dir: bool) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, ino);
    enc.add_u8(FieldId::IsDir, is_dir as u8);
    Ok(enc.into_bytes())
}

/// Decode a delete request
pub fn decode_delete_req(body: &[u8]) -> Result<(u64, bool), NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut ino = 0u64;
    let mut is_dir = false;

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::Ino => ino = dec.read_u64(length)?,
            FieldId::IsDir => is_dir = dec.read_u8(length)? != 0,
            _ => dec.skip(length)?,
        }
    }

    Ok((ino, is_dir))
}

/// Encode a readdir request
pub fn encode_readdir_req(ino: u64, offset: u64, count: u32) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, ino);
    enc.add_u64(FieldId::Offset, offset);
    enc.add_u32(FieldId::Count, count);
    Ok(enc.into_bytes())
}

/// Decode a readdir request
pub fn decode_readdir_req(body: &[u8]) -> Result<(u64, u64, u32), NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut ino = 0u64;
    let mut offset = 0u64;
    let mut count = 0u32;

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::Ino => ino = dec.read_u64(length)?,
            FieldId::Offset => offset = dec.read_u64(length)?,
            FieldId::Count => count = dec.read_u32(length)?,
            _ => dec.skip(length)?,
        }
    }

    Ok((ino, offset, count))
}

/// Encode a data request (read/write)
pub fn encode_data_req(ino: u64, offset: u64, data_len: u32) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, ino);
    enc.add_u64(FieldId::Offset, offset);
    enc.add_u32(FieldId::DataLen, data_len);
    Ok(enc.into_bytes())
}

/// Decode a data request
pub fn decode_data_req(body: &[u8]) -> Result<(u64, u64, u32), NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut ino = 0u64;
    let mut offset = 0u64;
    let mut data_len = 0u32;

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::Ino => ino = dec.read_u64(length)?,
            FieldId::Offset => offset = dec.read_u64(length)?,
            FieldId::DataLen => data_len = dec.read_u32(length)?,
            _ => dec.skip(length)?,
        }
    }

    Ok((ino, offset, data_len))
}

// ============================================================================
// Symlink / Readlink / Link operations
// ============================================================================

/// Encode a symlink request
pub fn encode_symlink_req(parent_ino: u64, name: &str, target: &str) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::ParentIno, parent_ino);
    enc.add_string(FieldId::Name, name)?;
    enc.add_string(FieldId::SymlinkTarget, target)?;
    Ok(enc.into_bytes())
}

/// Decode a symlink request
pub fn decode_symlink_req(body: &[u8]) -> Result<(u64, String, String), NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut parent_ino = 0u64;
    let mut name = String::new();
    let mut target = String::new();

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::ParentIno => parent_ino = dec.read_u64(length)?,
            FieldId::Name => name = dec.read_string(length)?.to_string(),
            FieldId::SymlinkTarget => target = dec.read_string(length)?.to_string(),
            _ => dec.skip(length)?,
        }
    }

    Ok((parent_ino, name, target))
}

/// Encode a readlink request
pub fn encode_readlink_req(ino: u64) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, ino);
    Ok(enc.into_bytes())
}

/// Decode a readlink request
pub fn decode_readlink_req(body: &[u8]) -> Result<u64, NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut ino = 0u64;

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::Ino => ino = dec.read_u64(length)?,
            _ => dec.skip(length)?,
        }
    }

    Ok(ino)
}

/// Decode a readlink response (returns the symlink target string)
pub fn decode_readlink_resp(body: &[u8]) -> Result<String, NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut target = String::new();

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::SymlinkTarget => target = dec.read_string(length)?.to_string(),
            _ => dec.skip(length)?,
        }
    }

    Ok(target)
}

/// Encode a hard link request
pub fn encode_link_req(ino: u64, parent_ino: u64, name: &str) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, ino);
    enc.add_u64(FieldId::ParentIno, parent_ino);
    enc.add_string(FieldId::Name, name)?;
    Ok(enc.into_bytes())
}

/// Decode a hard link request
pub fn decode_link_req(body: &[u8]) -> Result<(u64, u64, String), NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut ino = 0u64;
    let mut parent_ino = 0u64;
    let mut name = String::new();

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::Ino => ino = dec.read_u64(length)?,
            FieldId::ParentIno => parent_ino = dec.read_u64(length)?,
            FieldId::Name => name = dec.read_string(length)?.to_string(),
            _ => dec.skip(length)?,
        }
    }

    Ok((ino, parent_ino, name))
}

// ============================================================================
// Getattr / Setattr operations
// ============================================================================

/// Encode a getattr request
pub fn encode_getattr_req(ino: u64) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, ino);
    Ok(enc.into_bytes())
}

/// Decode a getattr request
pub fn decode_getattr_req(body: &[u8]) -> Result<u64, NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut ino = 0u64;

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::Ino => ino = dec.read_u64(length)?,
            _ => dec.skip(length)?,
        }
    }

    Ok(ino)
}

/// Encode a setattr request
pub fn encode_setattr_req(
    ino: u64,
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::Ino, ino);
    if let Some(m) = mode {
        enc.add_u32(FieldId::Mode, m);
    }
    if let Some(u) = uid {
        enc.add_u32(FieldId::Uid, u);
    }
    if let Some(g) = gid {
        enc.add_u32(FieldId::Gid, g);
    }
    if let Some(s) = size {
        enc.add_u64(FieldId::Size, s);
    }
    Ok(enc.into_bytes())
}

/// Decode a setattr request
pub fn decode_setattr_req(body: &[u8]) -> Result<SetattrResult, NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut ino = 0u64;
    let mut mode: Option<u32> = None;
    let mut uid: Option<u32> = None;
    let mut gid: Option<u32> = None;
    let mut size: Option<u64> = None;

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::Ino => ino = dec.read_u64(length)?,
            FieldId::Mode => mode = Some(dec.read_u32(length)?),
            FieldId::Uid => uid = Some(dec.read_u32(length)?),
            FieldId::Gid => gid = Some(dec.read_u32(length)?),
            FieldId::Size => size = Some(dec.read_u64(length)?),
            _ => dec.skip(length)?,
        }
    }

    Ok((ino, mode, uid, gid, size))
}

// ============================================================================
// Readdir response
// ============================================================================

/// A directory entry returned by readdir
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub ino: u64,
    pub name: String,
    pub mode: u32,
    pub offset: u64,
}

/// Encode a readdir response
pub fn encode_readdir_resp(entries: &[DirEntry]) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u32(FieldId::Count, entries.len() as u32);

    for entry in entries {
        let mut entry_enc = TlvEncoder::new();
        entry_enc.add_u64(FieldId::Ino, entry.ino);
        entry_enc.add_string(FieldId::Name, &entry.name)?;
        entry_enc.add_u32(FieldId::Mode, entry.mode);
        entry_enc.add_u64(FieldId::Offset, entry.offset);
        enc.add_nested(FieldId::Entry, &entry_enc.into_bytes())?;
    }

    Ok(enc.into_bytes())
}

/// Decode a readdir response
pub fn decode_readdir_resp(body: &[u8]) -> Result<Vec<DirEntry>, NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut count = 0u32;
    let mut entries = Vec::new();

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::Count => count = dec.read_u32(length)?,
            FieldId::Entry => {
                let entry_data = dec.read_bytes(length)?;
                let mut entry_dec = TlvDecoder::new(entry_data);
                let mut entry = DirEntry {
                    ino: 0,
                    name: String::new(),
                    mode: 0,
                    offset: 0,
                };

                while let Some((ef, el)) = entry_dec.next_field() {
                    match ef {
                        FieldId::Ino => entry.ino = entry_dec.read_u64(el)?,
                        FieldId::Name => entry.name = entry_dec.read_string(el)?.to_string(),
                        FieldId::Mode => entry.mode = entry_dec.read_u32(el)?,
                        FieldId::Offset => entry.offset = entry_dec.read_u64(el)?,
                        _ => entry_dec.skip(el)?,
                    }
                }

                entries.push(entry);
            }
            _ => dec.skip(length)?,
        }
    }

    // If no count field was present, just return what we decoded
    if count > 0 && entries.len() != count as usize {
        // Not an error, just a hint
    }

    Ok(entries)
}

// ============================================================================
// Mkdir / Unlink / Rmdir request encode/decode
// ============================================================================

/// Encode a mkdir request
/// Filer expects: ParentIno(u64) / Name(string) / Mode(u64) / Uid(u64) / Gid(u64)
pub fn encode_mkdir_req(
    parent_ino: u64,
    name: &str,
    mode: u32,
    uid: u32,
    gid: u32,
) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::ParentIno, parent_ino);
    enc.add_string(FieldId::Name, name)?;
    enc.add_u64(FieldId::Mode, mode as u64);
    enc.add_u64(FieldId::Uid, uid as u64);
    enc.add_u64(FieldId::Gid, gid as u64);
    Ok(enc.into_bytes())
}

/// Decode a mkdir request (server side)
pub fn decode_mkdir_req(body: &[u8]) -> Result<(u64, String, u32, u32, u32), NetError> {
    let mut dec = TlvDecoder::new(body);
    let parent_ino = dec.next_u64(FieldId::ParentIno)?;
    let name = dec.next_string(FieldId::Name)?;
    let mode = dec.next_u64(FieldId::Mode)? as u32;
    let uid = dec.next_u64(FieldId::Uid)? as u32;
    let gid = dec.next_u64(FieldId::Gid)? as u32;
    Ok((parent_ino, name, mode, uid, gid))
}

/// Encode an unlink request
/// Filer expects: ParentIno(u64) / Name(string)
pub fn encode_unlink_req(parent_ino: u64, name: &str) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::ParentIno, parent_ino);
    enc.add_string(FieldId::Name, name)?;
    Ok(enc.into_bytes())
}

/// Decode an unlink request (server side)
pub fn decode_unlink_req(body: &[u8]) -> Result<(u64, String), NetError> {
    let mut dec = TlvDecoder::new(body);
    let parent_ino = dec.next_u64(FieldId::ParentIno)?;
    let name = dec.next_string(FieldId::Name)?;
    Ok((parent_ino, name))
}

/// Encode an rmdir request
/// Filer expects: ParentIno(u64) / Name(string)
pub fn encode_rmdir_req(parent_ino: u64, name: &str) -> Result<Vec<u8>, NetError> {
    let mut enc = TlvEncoder::new();
    enc.add_u64(FieldId::ParentIno, parent_ino);
    enc.add_string(FieldId::Name, name)?;
    Ok(enc.into_bytes())
}

/// Decode an rmdir request (server side)
pub fn decode_rmdir_req(body: &[u8]) -> Result<(u64, String), NetError> {
    let mut dec = TlvDecoder::new(body);
    let parent_ino = dec.next_u64(FieldId::ParentIno)?;
    let name = dec.next_string(FieldId::Name)?;
    Ok((parent_ino, name))
}

// ============================================================================
// Common attr response decode (shared by lookup/getattr/mkdir/create/symlink/link/rename)
// ============================================================================

/// Common attr response fields
#[derive(Debug, Clone, Default)]
pub struct AttrResponse {
    pub ino: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub nlink: u32,
    pub mtime: u64,
    pub atime: u64,
    pub ctime: u64,
    pub name: String,
    pub rdev: u64,
}

/// Decode a common attr response (lookup/getattr return TLV)
/// Field order: Ino / Mode / Uid / Gid / Size / Nlink / Mtime / Atime / Ctime / Name / ...
/// Some fields may be missing (mkdir response only has Ino/Mode/Name); they stay at default.
pub fn decode_attr_resp(body: &[u8]) -> Result<AttrResponse, NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut resp = AttrResponse::default();

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::Ino => resp.ino = dec.read_u64(length)?,
            FieldId::Mode => resp.mode = dec.read_u32(length)?,
            FieldId::Uid => resp.uid = dec.read_u32(length)?,
            FieldId::Gid => resp.gid = dec.read_u32(length)?,
            FieldId::Size => resp.size = dec.read_u64(length)?,
            FieldId::Nlink => resp.nlink = dec.read_u32(length)?,
            FieldId::Mtime => resp.mtime = dec.read_u64(length)?,
            FieldId::Atime => resp.atime = dec.read_u64(length)?,
            FieldId::Ctime => resp.ctime = dec.read_u64(length)?,
            FieldId::Name => resp.name = dec.read_string(length)?.to_string(),
            FieldId::Rdev => resp.rdev = dec.read_u64(length)?,
            _ => dec.skip(length)?,
        }
    }

    Ok(resp)
}

/// Encode a statfs request (empty body, shard_id in routing)
pub fn encode_statfs_req() -> Result<Vec<u8>, NetError> {
    Ok(Vec::new())
}

/// Decode a statfs response
pub fn decode_statfs_resp(body: &[u8]) -> Result<(u64, u64, u64, u64, u32), NetError> {
    let mut dec = TlvDecoder::new(body);
    let mut total = 0u64;
    let mut free = 0u64;
    let mut total_inodes = 0u64;
    let mut free_inodes = 0u64;
    let mut block_size = 4096u32;

    while let Some((field, length)) = dec.next_field() {
        match field {
            FieldId::Size => total = dec.read_u64(length)?,
            FieldId::Free => free = dec.read_u64(length)?,
            FieldId::Nlink => total_inodes = dec.read_u64(length)?,
            FieldId::FreeInodes => free_inodes = dec.read_u64(length)?,
            FieldId::BlockSize => block_size = dec.read_u32(length)?,
            _ => dec.skip(length)?,
        }
    }

    Ok((total, free, total_inodes, free_inodes, block_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_lookup() {
        let body = encode_lookup_req(12345, "test.txt").unwrap();
        let (parent_ino, name) = decode_lookup_req(&body).unwrap();
        assert_eq!(parent_ino, 12345);
        assert_eq!(name, "test.txt");
    }

    #[test]
    fn test_encode_decode_create() {
        let body = encode_create_req(1, "hello.txt", 0o644, 1000, 1000).unwrap();
        let (parent_ino, name, mode, uid, gid) = decode_create_req(&body).unwrap();
        assert_eq!(parent_ino, 1);
        assert_eq!(name, "hello.txt");
        assert_eq!(mode, 0o644);
        assert_eq!(uid, 1000);
        assert_eq!(gid, 1000);
    }

    #[test]
    fn test_encode_decode_entry() {
        let body = encode_entry_resp(
            99, 0o644, 1000, 1000, 1024, 1, 1234567890, 1234567890, 1234567890, false, None,
        )
        .unwrap();
        let entry = decode_entry_resp(&body).unwrap();
        assert_eq!(entry.ino, 99);
        assert_eq!(entry.mode, 0o644);
        assert_eq!(entry.size, 1024);
        assert!(!entry.is_dir);
        assert!(entry.symlink_target.is_none());
    }

    #[test]
    fn test_encode_decode_symlink_entry() {
        let body = encode_entry_resp(
            100,
            0o777,
            1000,
            1000,
            0,
            1,
            0,
            0,
            0,
            false,
            Some("/tmp/target"),
        )
        .unwrap();
        let entry = decode_entry_resp(&body).unwrap();
        assert_eq!(entry.ino, 100);
        assert_eq!(entry.symlink_target.as_deref(), Some("/tmp/target"));
    }

    #[test]
    fn test_tlv_encoder_decoder() {
        let mut enc = TlvEncoder::new();
        enc.add_u8(FieldId::IsDir, 1);
        enc.add_u16(FieldId::Mode, 0o755);
        enc.add_u32(FieldId::Uid, 1000);
        enc.add_u64(FieldId::Size, 123456789);
        enc.add_string(FieldId::Name, "test").unwrap();
        let bytes = enc.into_bytes();

        let mut dec = TlvDecoder::new(&bytes);
        assert_eq!(dec.remaining(), bytes.len());

        let (field, len) = dec.next_field().unwrap();
        assert_eq!(field, FieldId::IsDir);
        assert_eq!(dec.read_u8(len).unwrap(), 1);

        let (field, len) = dec.next_field().unwrap();
        assert_eq!(field, FieldId::Mode);
        assert_eq!(dec.read_u16(len).unwrap(), 0o755);

        let (field, len) = dec.next_field().unwrap();
        assert_eq!(field, FieldId::Uid);
        assert_eq!(dec.read_u32(len).unwrap(), 1000);

        let (field, len) = dec.next_field().unwrap();
        assert_eq!(field, FieldId::Size);
        assert_eq!(dec.read_u64(len).unwrap(), 123456789);

        let (field, len) = dec.next_field().unwrap();
        assert_eq!(field, FieldId::Name);
        assert_eq!(dec.read_string(len).unwrap(), "test");

        assert!(dec.is_empty());
    }

    #[test]
    fn test_unknown_field_skipped() {
        let mut enc = TlvEncoder::new();
        enc.add_u32(FieldId::Mode, 42);
        // Use an unknown field ID (0xFE) with 4-byte big-endian length
        enc.buf.push(0xFE);
        enc.buf.extend_from_slice(&3u32.to_be_bytes());
        enc.buf.extend_from_slice(&[1u8, 2, 3]);
        let bytes = enc.into_bytes();

        let mut dec = TlvDecoder::new(&bytes);
        let (field, len) = dec.next_field().unwrap();
        assert_eq!(field, FieldId::Mode);
        assert_eq!(dec.read_u32(len).unwrap(), 42);

        // Unknown field 0xFE should be automatically skipped by next_field()
        // Since it's the last field, next_field() should return None
        assert!(dec.next_field().is_none());

        assert!(dec.is_empty());
    }

    #[test]
    fn test_extended_fields_roundtrip() {
        let mut enc = TlvEncoder::new();
        enc.add_request_id("req-12345").unwrap();
        enc.add_client_uuid("client-uuid-67890").unwrap();
        enc.add_channel_id(42);
        enc.add_shard_hash(123456789);
        enc.add_shard_id(1);
        enc.add_shard_leader("192.168.1.1:9334").unwrap();
        enc.add_topology_version(100);
        enc.add_lease_token("token-abcde").unwrap();
        enc.add_lease_range_offset(0);
        enc.add_lease_range_length(65536);
        let bytes = enc.into_bytes();

        let mut dec = TlvDecoder::new(&bytes);

        // Read and verify each field
        assert_eq!(dec.next_request_id().unwrap(), "req-12345");
        assert_eq!(dec.next_client_uuid().unwrap(), "client-uuid-67890");
        assert_eq!(dec.next_channel_id().unwrap(), 42);
        assert_eq!(dec.next_shard_hash().unwrap(), 123456789);
        assert_eq!(dec.next_shard_id().unwrap(), 1);
        assert_eq!(dec.next_shard_leader().unwrap(), "192.168.1.1:9334");
        assert_eq!(dec.next_topology_version().unwrap(), 100);
        assert_eq!(dec.next_lease_token().unwrap(), "token-abcde");
        assert_eq!(dec.next_lease_range_offset().unwrap(), 0);
        assert_eq!(dec.next_lease_range_length().unwrap(), 65536);

        assert!(dec.is_empty());
    }
}
