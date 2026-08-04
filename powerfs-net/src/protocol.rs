//! Wire protocol definitions for powerfs-net
//!
//! This module defines the binary protocol format used for communication
//! between PowerFS clients (FUSE, kernel) and servers (Master, Volume).

/// Protocol magic: "PFSN"
pub const PROTOCOL_MAGIC: &[u8; 4] = b"PFSN";

/// Current protocol version
pub const PROTOCOL_VERSION: u8 = 0x01;

/// Header size in bytes
pub const HEADER_SIZE: usize = 28;

/// Maximum frame size (header + data)
pub const MAX_FRAME_SIZE: u32 = 4 * 1024 * 1024; // 4MB

/// Maximum TLV value length (4GB - 1, using u32 length field)
pub const MAX_TLV_VALUE_LEN: u32 = 0xFFFFFFFF;

// ============================================================================
// Frame Flags
// ============================================================================

/// Frame flags
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FrameFlags(u8);

impl FrameFlags {
    pub const REQUEST: u8 = 0x01;
    pub const RESPONSE: u8 = 0x02;
    pub const NOTIFY: u8 = 0x04;
    pub const BATCH: u8 = 0x08;
    pub const ACK: u8 = 0x10;

    pub fn new(bits: u8) -> Self {
        Self(bits)
    }

    pub fn bits(&self) -> u8 {
        self.0
    }

    pub fn is_request(&self) -> bool {
        self.0 & Self::REQUEST != 0
    }

    pub fn is_response(&self) -> bool {
        self.0 & Self::RESPONSE != 0
    }

    pub fn is_notify(&self) -> bool {
        self.0 & Self::NOTIFY != 0
    }

    pub fn is_batch(&self) -> bool {
        self.0 & Self::BATCH != 0
    }

    pub fn with(self, flag: u8) -> Self {
        Self(self.0 | flag)
    }

    pub fn without(self, flag: u8) -> Self {
        Self(self.0 & !flag)
    }
}

// ============================================================================
// Connection Types
// ============================================================================

/// Client type for handshake
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ClientType {
    Fuse = 0x01,
    Kernel = 0x02,
    Admin = 0x03,
}

impl ClientType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Fuse),
            0x02 => Some(Self::Kernel),
            0x03 => Some(Self::Admin),
            _ => None,
        }
    }
}

// ============================================================================
// Handshake
// ============================================================================

/// Handshake request (18 bytes)
#[derive(Debug, Clone)]
pub struct HandshakeRequest {
    pub magic: [u8; 4],  // "PFSN"
    pub version: u8,     // 0x01
    pub client_type: u8, // ClientType
    pub client_id: u64,  // Unique client identifier
    pub features: u32,   // Supported features
}

impl HandshakeRequest {
    pub const SIZE: usize = 18;

    pub fn new(client_type: ClientType, client_id: u64) -> Self {
        Self {
            magic: *PROTOCOL_MAGIC,
            version: PROTOCOL_VERSION,
            client_type: client_type as u8,
            client_id,
            features: 0,
        }
    }

    pub fn encode(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.magic);
        buf[4] = self.version;
        buf[5] = self.client_type;
        buf[6..14].copy_from_slice(&self.client_id.to_le_bytes());
        buf[14..18].copy_from_slice(&self.features.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&buf[0..4]);
        if magic != *PROTOCOL_MAGIC {
            return None;
        }
        Some(Self {
            magic,
            version: buf[4],
            client_type: buf[5],
            client_id: u64::from_le_bytes(buf[6..14].try_into().ok()?),
            features: u32::from_le_bytes(buf[14..18].try_into().ok()?),
        })
    }
}

/// Handshake response (18 bytes)
#[derive(Debug, Clone)]
pub struct HandshakeResponse {
    pub magic: [u8; 4], // "PFSN"
    pub version: u8,    // 0x01
    pub status: u8,     // 0=OK, 1=REJECT
    pub server_id: u64, // Server identifier
    pub features: u32,  // Supported features
}

impl HandshakeResponse {
    pub const SIZE: usize = 18;

    pub fn ok(server_id: u64) -> Self {
        Self {
            magic: *PROTOCOL_MAGIC,
            version: PROTOCOL_VERSION,
            status: 0,
            server_id,
            features: 0,
        }
    }

    pub fn reject() -> Self {
        Self {
            magic: *PROTOCOL_MAGIC,
            version: PROTOCOL_VERSION,
            status: 1,
            server_id: 0,
            features: 0,
        }
    }

    pub fn encode(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.magic);
        buf[4] = self.version;
        buf[5] = self.status;
        buf[6..14].copy_from_slice(&self.server_id.to_le_bytes());
        buf[14..18].copy_from_slice(&self.features.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&buf[0..4]);
        if magic != *PROTOCOL_MAGIC {
            return None;
        }
        Some(Self {
            magic,
            version: buf[4],
            status: buf[5],
            server_id: u64::from_le_bytes(buf[6..14].try_into().ok()?),
            features: u32::from_le_bytes(buf[14..18].try_into().ok()?),
        })
    }

    pub fn is_ok(&self) -> bool {
        self.status == 0
    }
}

// ============================================================================
// Frame Header
// ============================================================================

/// Frame header (28 bytes)
///
/// Layout:
///   magic: 4B   - "PFSN"
///   version: 1B - Protocol version
///   flags: 1B   - FrameFlags
///   seq: 4B     - Sequence number
///   msg_type: 2B - Message type
///   status: 2B  - Response status code (0=OK)
///   data_len: 4B - Total data length (body + data segment)
///   reserved: 6B - Reserved for future use
///   header_crc: 4B - CRC32C of header (fields before this)
#[derive(Debug, Clone)]
pub struct FrameHeader {
    pub magic: [u8; 4],
    pub version: u8,
    pub flags: u8,
    pub seq: u32,
    pub msg_type: u16,
    pub status: u16,
    pub data_len: u32,
    pub reserved: [u8; 6],
    pub header_crc: u32,
}

impl FrameHeader {
    pub const SIZE: usize = 28;

    pub fn new(msg_type: u16, flags: FrameFlags, seq: u32, data_len: u32) -> Self {
        let mut hdr = Self {
            magic: *PROTOCOL_MAGIC,
            version: PROTOCOL_VERSION,
            flags: flags.bits(),
            seq,
            msg_type,
            status: 0,
            data_len,
            reserved: [0u8; 6],
            header_crc: 0,
        };
        hdr.header_crc = hdr.calc_header_crc();
        hdr
    }

    pub fn with_status(mut self, status: u16) -> Self {
        self.status = status;
        self.header_crc = self.calc_header_crc();
        self
    }

    fn calc_header_crc(&self) -> u32 {
        // crc32c_append(prev, data) 的 prev 是"已完成的标准 CRC32C 值",
        // 空数据 CRC32C = 0xFFFFFFFF ^ 0xFFFFFFFF = 0, 故初始值为 0.
        // 逐步 append 后 crc 已是标准 CRC32C (初始 0xFFFFFFFF, 末尾 XOR 0xFFFFFFFF),
        // 无需再 XOR. 之前用 0xFFFFFFFF 初始值 + 末尾 XOR 是双重错误, 实际算出
        // raw_crc(0, data) (非标准), 与内核标准 CRC32C 不一致, 导致 Filer 报
        // "invalid frame header".
        let mut crc: u32 = 0;
        crc = crc32c::crc32c_append(crc, &self.magic);
        crc = crc32c::crc32c_append(crc, &[self.version]);
        crc = crc32c::crc32c_append(crc, &[self.flags]);
        crc = crc32c::crc32c_append(crc, &self.seq.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &self.msg_type.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &self.status.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &self.data_len.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &self.reserved);
        crc
    }

    pub fn verify_crc(&self) -> bool {
        self.header_crc == self.calc_header_crc()
    }

    /// Check if this frame is a NOTIFY (server-pushed notification)
    pub fn is_notify(&self) -> bool {
        self.flags & FrameFlags::NOTIFY != 0
    }

    pub fn encode(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.magic);
        buf[4] = self.version;
        buf[5] = self.flags;
        buf[6..10].copy_from_slice(&self.seq.to_le_bytes());
        buf[10..12].copy_from_slice(&self.msg_type.to_le_bytes());
        buf[12..14].copy_from_slice(&self.status.to_le_bytes());
        buf[14..18].copy_from_slice(&self.data_len.to_le_bytes());
        buf[18..24].copy_from_slice(&self.reserved);
        buf[24..28].copy_from_slice(&self.header_crc.to_le_bytes());
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < Self::SIZE {
            return None;
        }
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&buf[0..4]);
        if magic != *PROTOCOL_MAGIC {
            return None;
        }
        let hdr = Self {
            magic,
            version: buf[4],
            flags: buf[5],
            seq: u32::from_le_bytes(buf[6..10].try_into().ok()?),
            msg_type: u16::from_le_bytes(buf[10..12].try_into().ok()?),
            status: u16::from_le_bytes(buf[12..14].try_into().ok()?),
            data_len: u32::from_le_bytes(buf[14..18].try_into().ok()?),
            reserved: buf[18..24].try_into().ok()?,
            header_crc: u32::from_le_bytes(buf[24..28].try_into().ok()?),
        };
        if !hdr.verify_crc() {
            return None;
        }
        Some(hdr)
    }
}

// ============================================================================
// Message Types
// ============================================================================

/// Message type identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum MsgType {
    // Control messages
    Ping = 0x0001,
    Handshake = 0x0002,

    // Metadata operations
    Lookup = 0x0010,
    GetAttr = 0x0011,
    SetAttr = 0x0012,
    Create = 0x0013,
    Mkdir = 0x0014,
    Unlink = 0x0015,
    Rmdir = 0x0016,
    Rename = 0x0017,
    ReadDir = 0x0018,
    Symlink = 0x0019,
    Readlink = 0x001A,
    Link = 0x001B,
    SetAttrData = 0x001C, // Strong-consistency SetAttr (size/chunks)
    SetAttrMeta = 0x001D, // Eventually-consistency SetAttr (mode/uid/gid)

    // Data operations
    Read = 0x0020,
    Write = 0x0021,

    // Consistency operations
    PushDelta = 0x0030,
    PullDelta = 0x0031,
    Invalidate = 0x0032,
    AllocInodeBatch = 0x0033,
    UpdateInodeSizeChunks = 0x0034,
    OpenCountInc = 0x0035,
    OpenCountDec = 0x0036,

    // Status
    StatFs = 0x0040,

    // Master operations
    Assign = 0x0050,
    LookupVolume = 0x0051,
    Heartbeat = 0x0052,
    KeepConnected = 0x0053,
    VolumeList = 0x0054,

    // Volume operations
    CreateVolume = 0x0060,
    DeleteVolume = 0x0061,
    WriteNeedle = 0x0062,
    ReadNeedle = 0x0063,
    DeleteNeedle = 0x0064,
    BatchWriteNeedle = 0x0065,
    ReadNeedleBlob = 0x0066,
    RangeLease = 0x0067,
    VolumeStatus = 0x0068,

    // Master topology & discovery operations
    GetTopology = 0x0070,
    WatchTopology = 0x0071,
    TopologyChanged = 0x0072,
    AssignVolumeV2 = 0x0073,

    // Extended Lease operations
    AcquireLease = 0x0080,
    ReleaseLease = 0x0081,
    RenewLease = 0x0082,
    LeaseStatus = 0x0083,
    AcquireLeaseBatch = 0x0084,
}

impl MsgType {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            0x0001 => Some(Self::Ping),
            0x0002 => Some(Self::Handshake),
            0x0010 => Some(Self::Lookup),
            0x0011 => Some(Self::GetAttr),
            0x0012 => Some(Self::SetAttr),
            0x0013 => Some(Self::Create),
            0x0014 => Some(Self::Mkdir),
            0x0015 => Some(Self::Unlink),
            0x0016 => Some(Self::Rmdir),
            0x0017 => Some(Self::Rename),
            0x0018 => Some(Self::ReadDir),
            0x0019 => Some(Self::Symlink),
            0x001A => Some(Self::Readlink),
            0x001B => Some(Self::Link),
            0x001C => Some(Self::SetAttrData),
            0x001D => Some(Self::SetAttrMeta),
            0x0020 => Some(Self::Read),
            0x0021 => Some(Self::Write),
            0x0030 => Some(Self::PushDelta),
            0x0031 => Some(Self::PullDelta),
            0x0032 => Some(Self::Invalidate),
            0x0033 => Some(Self::AllocInodeBatch),
            0x0034 => Some(Self::UpdateInodeSizeChunks),
            0x0035 => Some(Self::OpenCountInc),
            0x0036 => Some(Self::OpenCountDec),
            0x0040 => Some(Self::StatFs),
            0x0050 => Some(Self::Assign),
            0x0051 => Some(Self::LookupVolume),
            0x0052 => Some(Self::Heartbeat),
            0x0053 => Some(Self::KeepConnected),
            0x0054 => Some(Self::VolumeList),
            0x0060 => Some(Self::CreateVolume),
            0x0061 => Some(Self::DeleteVolume),
            0x0062 => Some(Self::WriteNeedle),
            0x0063 => Some(Self::ReadNeedle),
            0x0064 => Some(Self::DeleteNeedle),
            0x0065 => Some(Self::BatchWriteNeedle),
            0x0066 => Some(Self::ReadNeedleBlob),
            0x0067 => Some(Self::RangeLease),
            0x0068 => Some(Self::VolumeStatus),
            0x0070 => Some(Self::GetTopology),
            0x0071 => Some(Self::WatchTopology),
            0x0072 => Some(Self::TopologyChanged),
            0x0073 => Some(Self::AssignVolumeV2),
            0x0080 => Some(Self::AcquireLease),
            0x0081 => Some(Self::ReleaseLease),
            0x0082 => Some(Self::RenewLease),
            0x0084 => Some(Self::AcquireLeaseBatch),
            0x0083 => Some(Self::LeaseStatus),
            _ => None,
        }
    }

    pub fn as_u16(self) -> u16 {
        self as u16
    }

    pub fn is_metadata(self) -> bool {
        let v = self.as_u16();
        (0x0010..=0x001D).contains(&v)
    }

    pub fn is_data(self) -> bool {
        let v = self.as_u16();
        v == 0x0020 || v == 0x0021
    }
}

// ============================================================================
// Response Status Codes
// ============================================================================

/// Response status codes
pub const STATUS_OK: u16 = 0;
pub const STATUS_ERR_NOT_FOUND: u16 = 1;
pub const STATUS_ERR_ALREADY_EXISTS: u16 = 2;
pub const STATUS_ERR_PERMISSION_DENIED: u16 = 3;
pub const STATUS_ERR_IO: u16 = 4;
pub const STATUS_ERR_INVALID_ARG: u16 = 5;
pub const STATUS_ERR_NOT_DIR: u16 = 6;
pub const STATUS_ERR_IS_DIR: u16 = 7;
pub const STATUS_ERR_NO_SPACE: u16 = 8;
pub const STATUS_ERR_BAD_FD: u16 = 9;
pub const STATUS_ERR_SERVER_ERROR: u16 = 10;
pub const STATUS_ERR_REDIRECT: u16 = 11;

// ============================================================================
// TLV Field IDs
// ============================================================================

/// TLV field identifiers (1 byte)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum FieldId {
    // Common fields
    ParentIno = 0x01,
    Name = 0x02,
    Mode = 0x03,
    Uid = 0x04,
    Gid = 0x05,
    Size = 0x06,
    Ino = 0x07,
    Nlink = 0x08,
    Mtime = 0x09,
    Atime = 0x0A,
    Ctime = 0x0B,
    SymlinkTarget = 0x0C,
    IsDir = 0x0D,
    Offset = 0x0E,
    DataLen = 0x0F,

    // Extended fields
    Rdev = 0x10,
    Blksize = 0x11,
    Blocks = 0x12,
    ContentSize = 0x13,
    DiskSize = 0x14,
    Generation = 0x15,
    HardLinkId = 0x16,
    Owner = 0x17,
    Backend = 0x18,
    Version = 0x19,

    // Statfs fields
    Free = 0x1A,
    FreeInodes = 0x1B,
    BlockSize = 0x1C,

    // List fields
    Limit = 0x20,
    LastName = 0x21,
    HasMore = 0x22,
    Entries = 0x23,
    Count = 0x24,
    Entry = 0x25,

    // Delta sync fields
    ClientId = 0x30,
    Seq = 0x31,
    VclockEntries = 0x32,
    DeltaOps = 0x33,

    // Lease fields
    LeaseId = 0x40,
    LeaseDuration = 0x41,
    LeaseEpoch = 0x42,

    // Rename fields
    NewParentIno = 0x50,
    NewName = 0x51,

    // Request tracking fields (for Exactly-Once)
    RequestId = 0x60,
    ClientUuid = 0x61,
    ChannelId = 0x62,
    ShardHash = 0x63,

    // Master topology fields
    ShardId = 0x70,
    ShardLeader = 0x71,
    VolumeListPayload = 0x72,
    TopologyVersion = 0x73,

    // Lease extended fields
    LeaseToken = 0x80,
    LeaseRangeOffset = 0x81,
    LeaseRangeLength = 0x82,
    /// Batch lease specs: flat byte array of (stripe_start: u64 LE, stripe_count: u64 LE) pairs.
    LeaseBatchSpecs = 0x83,

    // AssignVolume fields
    Collection = 0x90,
    Replication = 0x91,
    VolumeId = 0x92,
    Cookie = 0x93,
    /// Chunk-level storage key (needle_id on volume server).
    /// Used in Write/Read/Delete/BatchWrite TLV to identify the physical needle.
    FileKey = 0x94,
    Fid = 0x95,
    /// 完整 chunks 列表（JSON 序列化的 Vec<ChunkWire>）。
    /// 用于 GetAttr/Lookup/ReadDir 返回多 chunk 文件的完整数据布局。
    Chunks = 0x96,
    /// Inode for lease validation (lease is registered per-inode, not per-needle).
    /// Used in Write/BatchWrite TLV alongside FileKey.
    Inode = 0x97,
}

impl FieldId {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::ParentIno),
            0x02 => Some(Self::Name),
            0x03 => Some(Self::Mode),
            0x04 => Some(Self::Uid),
            0x05 => Some(Self::Gid),
            0x06 => Some(Self::Size),
            0x07 => Some(Self::Ino),
            0x08 => Some(Self::Nlink),
            0x09 => Some(Self::Mtime),
            0x0A => Some(Self::Atime),
            0x0B => Some(Self::Ctime),
            0x0C => Some(Self::SymlinkTarget),
            0x0D => Some(Self::IsDir),
            0x0E => Some(Self::Offset),
            0x0F => Some(Self::DataLen),
            0x10 => Some(Self::Rdev),
            0x11 => Some(Self::Blksize),
            0x12 => Some(Self::Blocks),
            0x13 => Some(Self::ContentSize),
            0x14 => Some(Self::DiskSize),
            0x15 => Some(Self::Generation),
            0x16 => Some(Self::HardLinkId),
            0x17 => Some(Self::Owner),
            0x18 => Some(Self::Backend),
            0x19 => Some(Self::Version),
            0x1A => Some(Self::Free),
            0x1B => Some(Self::FreeInodes),
            0x1C => Some(Self::BlockSize),
            0x20 => Some(Self::Limit),
            0x21 => Some(Self::LastName),
            0x22 => Some(Self::HasMore),
            0x23 => Some(Self::Entries),
            0x24 => Some(Self::Count),
            0x25 => Some(Self::Entry),
            0x30 => Some(Self::ClientId),
            0x31 => Some(Self::Seq),
            0x32 => Some(Self::VclockEntries),
            0x33 => Some(Self::DeltaOps),
            0x40 => Some(Self::LeaseId),
            0x41 => Some(Self::LeaseDuration),
            0x42 => Some(Self::LeaseEpoch),
            0x50 => Some(Self::NewParentIno),
            0x51 => Some(Self::NewName),
            0x60 => Some(Self::RequestId),
            0x61 => Some(Self::ClientUuid),
            0x62 => Some(Self::ChannelId),
            0x63 => Some(Self::ShardHash),
            0x70 => Some(Self::ShardId),
            0x71 => Some(Self::ShardLeader),
            0x72 => Some(Self::VolumeListPayload),
            0x73 => Some(Self::TopologyVersion),
            0x80 => Some(Self::LeaseToken),
            0x81 => Some(Self::LeaseRangeOffset),
            0x82 => Some(Self::LeaseRangeLength),
            0x90 => Some(Self::Collection),
            0x91 => Some(Self::Replication),
            0x92 => Some(Self::VolumeId),
            0x93 => Some(Self::Cookie),
            0x94 => Some(Self::FileKey),
            0x95 => Some(Self::Fid),
            0x96 => Some(Self::Chunks),
            0x97 => Some(Self::Inode),
            _ => None,
        }
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

// ============================================================================
// Runtime Message (decoded frame with typed payload)
// ============================================================================

/// A decoded message with header and body
#[derive(Debug, Clone)]
pub struct NetMessage {
    pub header: FrameHeader,
    pub body: Vec<u8>,
    pub data: Vec<u8>,
}

impl NetMessage {
    pub fn new(header: FrameHeader) -> Self {
        Self {
            header,
            body: Vec::new(),
            data: Vec::new(),
        }
    }

    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }

    pub fn with_data(mut self, data: Vec<u8>) -> Self {
        self.data = data;
        self
    }

    pub fn total_data_len(&self) -> u32 {
        self.body.len() as u32 + self.data.len() as u32
    }

    pub fn is_request(&self) -> bool {
        self.header.flags & FrameFlags::REQUEST != 0
    }

    pub fn is_response(&self) -> bool {
        self.header.flags & FrameFlags::RESPONSE != 0
    }

    pub fn is_ok(&self) -> bool {
        self.is_response() && self.header.status == STATUS_OK
    }

    pub fn msg_type(&self) -> Option<MsgType> {
        MsgType::from_u16(self.header.msg_type)
    }
}

// ============================================================================
// Frame Construction
// ============================================================================

/// Build a frame from message components
pub fn build_frame(
    msg_type: u16,
    flags: FrameFlags,
    seq: u32,
    body: &[u8],
    data: &[u8],
) -> Vec<u8> {
    let data_len = body.len() as u32 + data.len() as u32;
    let header = FrameHeader::new(msg_type, flags, seq, data_len);

    let mut frame = Vec::with_capacity(FrameHeader::SIZE + body.len() + data.len());
    let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
    header.encode(&mut hdr_buf);
    frame.extend_from_slice(&hdr_buf);
    frame.extend_from_slice(body);
    frame.extend_from_slice(data);

    frame
}

/// Parse a frame header from buffer
pub fn parse_header(buf: &[u8]) -> Option<FrameHeader> {
    FrameHeader::decode(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_encode_decode() {
        let req = HandshakeRequest::new(ClientType::Fuse, 12345);
        let mut buf = vec![0u8; HandshakeRequest::SIZE];
        req.encode(&mut buf);

        let decoded = HandshakeRequest::decode(&buf).unwrap();
        assert_eq!(decoded.client_type, 0x01);
        assert_eq!(decoded.client_id, 12345);
        assert_eq!(decoded.version, PROTOCOL_VERSION);
    }

    #[test]
    fn test_handshake_response() {
        let resp = HandshakeResponse::ok(99);
        let mut buf = vec![0u8; HandshakeResponse::SIZE];
        resp.encode(&mut buf);

        let decoded = HandshakeResponse::decode(&buf).unwrap();
        assert!(decoded.is_ok());
        assert_eq!(decoded.server_id, 99);
    }

    #[test]
    fn test_frame_header_crc() {
        let hdr = FrameHeader::new(
            MsgType::Lookup.as_u16(),
            FrameFlags::new(FrameFlags::REQUEST),
            42,
            100,
        );
        assert!(hdr.verify_crc());

        let mut buf = vec![0u8; FrameHeader::SIZE];
        hdr.encode(&mut buf);

        let decoded = FrameHeader::decode(&buf).unwrap();
        assert_eq!(decoded.seq, 42);
        assert_eq!(decoded.msg_type, MsgType::Lookup.as_u16());
        assert_eq!(decoded.data_len, 100);
        assert!(decoded.verify_crc());
    }

    #[test]
    fn test_bad_crc() {
        let hdr = FrameHeader::new(
            MsgType::Lookup.as_u16(),
            FrameFlags::new(FrameFlags::REQUEST),
            1,
            0,
        );
        let mut buf = vec![0u8; FrameHeader::SIZE];
        hdr.encode(&mut buf);

        // Corrupt data
        buf[10] ^= 0xFF;

        assert!(FrameHeader::decode(&buf).is_none());
    }

    #[test]
    fn test_msg_type_roundtrip() {
        for v in 0x0001..=0x0042 {
            if let Some(mt) = MsgType::from_u16(v) {
                assert_eq!(mt.as_u16(), v);
            }
        }
    }

    #[test]
    fn test_field_id_roundtrip() {
        for v in [0x01u8, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08] {
            if let Some(fid) = FieldId::from_u8(v) {
                assert_eq!(fid.as_u8(), v);
            }
        }
    }
}
