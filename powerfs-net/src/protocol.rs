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

    /// Bits 6-7: server load_factor (Phase 2).
    /// Encoded as 2-bit level (0-3) for backward-compatible piggyback on
    /// response frames. Old clients ignore these bits; old servers fill 0.
    pub const LOAD_FACTOR_SHIFT: u8 = 6;
    pub const LOAD_FACTOR_MASK: u8 = 0xC0;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ClientType {
    Fuse = 0x01,
    Kernel = 0x02,
    Admin = 0x03,
    Volume = 0x04,
    Filer = 0x05,
    Master = 0x06,
}

impl ClientType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::Fuse),
            0x02 => Some(Self::Kernel),
            0x03 => Some(Self::Admin),
            0x04 => Some(Self::Volume),
            0x05 => Some(Self::Filer),
            0x06 => Some(Self::Master),
            _ => None,
        }
    }
}

// ============================================================================
// Handshake
// ============================================================================

/// Handshake request (20 bytes)
#[derive(Debug, Clone)]
pub struct HandshakeRequest {
    pub magic: [u8; 4],  // "PFSN"
    pub version: u8,     // 0x01
    pub client_type: u8, // ClientType
    pub channel: u8,     // 0=data, 1=meta (通路类型, 服务端登记+收帧校验)
    pub reserved: u8,    // 对齐
    pub client_id: u64,  // Unique client identifier
    pub features: u32,   // Supported features
}

/// Channel constants (与内核 POWERFS_NET_CHANNEL_DATA/META 一致)
pub const CHANNEL_DATA: u8 = 0;
pub const CHANNEL_META: u8 = 1;

impl HandshakeRequest {
    pub const SIZE: usize = 20;

    pub fn new(client_type: ClientType, client_id: u64, channel: u8) -> Self {
        Self {
            magic: *PROTOCOL_MAGIC,
            version: PROTOCOL_VERSION,
            client_type: client_type as u8,
            channel,
            reserved: 0,
            client_id,
            features: 0,
        }
    }

    pub fn encode(&self, buf: &mut [u8]) {
        buf[0..4].copy_from_slice(&self.magic);
        buf[4] = self.version;
        buf[5] = self.client_type;
        buf[6] = self.channel;
        buf[7] = self.reserved;
        buf[8..16].copy_from_slice(&self.client_id.to_le_bytes());
        buf[16..20].copy_from_slice(&self.features.to_le_bytes());
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
            channel: buf[6],
            reserved: buf[7],
            client_id: u64::from_le_bytes(buf[8..16].try_into().ok()?),
            features: u32::from_le_bytes(buf[16..20].try_into().ok()?),
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
///   magic: 4B    - "PFSN"
///   version: 1B  - Protocol version
///   flags: 1B    - FrameFlags
///   seq: 4B      - Sequence number
///   msg_type: 2B - Message type
///   status: 2B   - Response status code (0=OK)
///   data_len: 4B - Total data length (body + data segment)
///   body_len: 4B - Body segment length (data segment = data_len - body_len)
///   route_hash: 1B - 高7位=client_id hash, 低1位=channel (防错乱校验)
///   protocol_ver: 1B - 协议版本 (版本升级一致性检查)
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
    pub body_len: u32,
    pub route_hash: u8,
    pub protocol_ver: u8,
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
            body_len: 0,
            route_hash: 0,
            protocol_ver: PROTOCOL_VERSION,
            header_crc: 0,
        };
        hdr.header_crc = hdr.calc_header_crc();
        hdr
    }

    /// Set body_len and data_len, then recompute CRC.
    /// Called by build_frame before encoding to ensure body/data boundary
    /// is correctly recorded in the header.
    pub fn set_body_data_len(&mut self, body_len: u32, data_len: u32) {
        self.body_len = body_len;
        self.data_len = data_len;
        self.header_crc = self.calc_header_crc();
    }

    pub fn with_status(mut self, status: u16) -> Self {
        self.status = status;
        self.header_crc = self.calc_header_crc();
        self
    }

    /// Stamp server load_factor (0-3) into flags bits 6-7, recompute CRC.
    ///
    /// Phase 2: called by Worker before sending a response so the client can
    /// adapt its admission concurrency. Values >3 are clamped to 3.
    pub fn set_load_factor(&mut self, lf: u8) {
        let level = lf.min(3);
        self.flags = (self.flags & !FrameFlags::LOAD_FACTOR_MASK)
            | (level << FrameFlags::LOAD_FACTOR_SHIFT);
        self.header_crc = self.calc_header_crc();
    }

    /// Extract server load_factor (0-3) from flags bits 6-7.
    ///
    /// Phase 2: called by kernel client on response receipt to adjust
    /// admission concurrency.
    pub fn load_factor(&self) -> u8 {
        (self.flags & FrameFlags::LOAD_FACTOR_MASK) >> FrameFlags::LOAD_FACTOR_SHIFT
    }

    fn calc_header_crc(&self) -> u32 {
        let mut crc: u32 = 0;
        crc = crc32c::crc32c_append(crc, &self.magic);
        crc = crc32c::crc32c_append(crc, &[self.version]);
        crc = crc32c::crc32c_append(crc, &[self.flags]);
        crc = crc32c::crc32c_append(crc, &self.seq.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &self.msg_type.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &self.status.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &self.data_len.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &self.body_len.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &[self.route_hash, self.protocol_ver]);
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
        buf[18..22].copy_from_slice(&self.body_len.to_le_bytes());
        buf[22] = self.route_hash;
        buf[23] = self.protocol_ver;
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
            body_len: u32::from_le_bytes(buf[18..22].try_into().ok()?),
            route_hash: buf[22],
            protocol_ver: buf[23],
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
    /// Assign a new needle_id within a volume.
    /// Filer → Volume Server: requests allocation of a needle_id.
    /// Response: volume_id + needle_id.
    /// This is a metadata-only operation (no data transfer).
    AssignNeedle = 0x0069,
    /// Register Filer with Master to get a Zone assignment.
    /// Filer → Master: requests Zone allocation.
    /// Response: zone_id + [(volume_id, addr, size, used), ...].
    RegisterFiler = 0x006A,

    // Master topology & discovery operations
    GetTopology = 0x0070,
    WatchTopology = 0x0071,
    TopologyChanged = 0x0072,
    AssignVolumeV2 = 0x0073,
    /// List registered filers (addr + net_port + health + shard_ids).
    /// Used by kernel client on mount to discover filer nodes from Master.
    ListFilers = 0x0074,

    // Extended Lease operations
    AcquireLease = 0x0080,
    ReleaseLease = 0x0081,
    RenewLease = 0x0082,
    LeaseStatus = 0x0083,
    AcquireLeaseBatch = 0x0084,

    /// Inode Metadata Lease (Phase 2 / 方案 A):
    /// Managed by Filer, not Volume Server. Used when backend doesn't support
    /// range lease (e.g., NVMe-oF target). FUSE client → Filer.
    AcquireInodeLease = 0x0085,
    ReleaseInodeLease = 0x0086,
    RenewInodeLease = 0x0087,

    // Raft inter-node operations
    /// Filer → Filer: forward a Raft protocol message (eraftpb::Message)
    /// to the peer that leads the target shard group.
    /// Request: ShardId + RaftPayload. Response: STATUS_OK / STATUS_ERR.
    RaftMessage = 0x0090,
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
            0x0069 => Some(Self::AssignNeedle),
            0x006A => Some(Self::RegisterFiler),
            0x0070 => Some(Self::GetTopology),
            0x0071 => Some(Self::WatchTopology),
            0x0072 => Some(Self::TopologyChanged),
            0x0073 => Some(Self::AssignVolumeV2),
            0x0074 => Some(Self::ListFilers),
            0x0080 => Some(Self::AcquireLease),
            0x0081 => Some(Self::ReleaseLease),
            0x0082 => Some(Self::RenewLease),
            0x0084 => Some(Self::AcquireLeaseBatch),
            0x0083 => Some(Self::LeaseStatus),
            0x0085 => Some(Self::AcquireInodeLease),
            0x0086 => Some(Self::ReleaseInodeLease),
            0x0087 => Some(Self::RenewInodeLease),
            0x0090 => Some(Self::RaftMessage),
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
    /// Used space in bytes (for GetTopology volume status).
    UsedSpace = 0x98,
    /// File/needle count (for GetTopology volume status).
    FileCount = 0x99,
    /// Zone ID (for RegisterFiler response).
    ZoneId = 0x9A,
    /// Packed u64 LE array of shard ids (for RegisterFiler request — filer node discovery).
    ShardIdList = 0x9B,
    /// Filer advertise address (string, "ip:net_port" — for RegisterFiler request).
    FilerAddress = 0x9C,
    /// Volume server powerfs-net port (for Heartbeat — so Master knows the TLV port).
    NetPort = 0x9D,
    /// Serialized Raft protocol message (eraftpb::Message protobuf bytes).
    /// Used by MsgType::RaftMessage for Filer inter-node Raft transport.
    RaftPayload = 0x9E,
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
            0x98 => Some(Self::UsedSpace),
            0x99 => Some(Self::FileCount),
            0x9A => Some(Self::ZoneId),
            0x9B => Some(Self::ShardIdList),
            0x9C => Some(Self::FilerAddress),
            0x9D => Some(Self::NetPort),
            0x9E => Some(Self::RaftPayload),
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

    /// Build a response for the given request.
    ///
    /// Copies `msg_type` and `seq` from `req`, sets the RESPONSE flag and the
    /// provided `status`, and attaches the supplied `body` and `data` segments.
    ///
    /// Upper-layer handlers should use this instead of reaching into
    /// `FrameHeader`/`FrameFlags` directly.
    pub fn response(req: &NetMessage, status: u16, body: Vec<u8>, data: Vec<u8>) -> Self {
        let data_len = body.len() as u32 + data.len() as u32;
        let header = FrameHeader::new(
            req.header.msg_type,
            FrameFlags::new(FrameFlags::RESPONSE),
            req.header.seq,
            data_len,
        )
        .with_status(status);
        // body_len is recorded at serialization time by `to_frame`, which
        // calls `set_body_data_len` before encoding so the receiver can
        // split the payload into body and data segments correctly.
        let mut msg = Self::new(header);
        msg.body = body;
        msg.data = data;
        msg
    }

    /// Convenience wrapper for a successful response (STATUS_OK).
    pub fn ok_response(req: &NetMessage, body: Vec<u8>, data: Vec<u8>) -> Self {
        Self::response(req, STATUS_OK, body, data)
    }

    /// Build a server-pushed notification message.
    ///
    /// Uses the NOTIFY flag with `seq = 0` (notifications are fire-and-forget)
    /// and attaches the supplied `body` and `data` segments.
    pub fn notification(msg_type: MsgType, body: Vec<u8>, data: Vec<u8>) -> Self {
        let data_len = body.len() as u32 + data.len() as u32;
        let header = FrameHeader::new(
            msg_type.as_u16(),
            FrameFlags::new(FrameFlags::NOTIFY),
            0,
            data_len,
        );
        let mut msg = Self::new(header);
        msg.body = body;
        msg.data = data;
        msg
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

    /// Serialize this message to a wire frame (header + body + data).
    ///
    /// Sets `body_len` and `data_len` on a cloned header so the receiver
    /// can split the payload into body and data segments correctly.
    pub fn to_frame(&self) -> Vec<u8> {
        let mut hdr = self.header.clone();
        hdr.set_body_data_len(self.body.len() as u32, self.total_data_len());

        let mut frame = Vec::with_capacity(FrameHeader::SIZE + self.body.len() + self.data.len());
        let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
        hdr.encode(&mut hdr_buf);
        frame.extend_from_slice(&hdr_buf);
        frame.extend_from_slice(&self.body);
        frame.extend_from_slice(&self.data);
        frame
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
    let mut header = FrameHeader::new(msg_type, flags, seq, data_len);
    header.set_body_data_len(body.len() as u32, data_len);

    let mut frame = Vec::with_capacity(FrameHeader::SIZE + body.len() + data.len());
    let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
    header.encode(&mut hdr_buf);
    frame.extend_from_slice(&hdr_buf);
    frame.extend_from_slice(body);
    frame.extend_from_slice(data);

    frame
}

/// Build a frame with `route_hash` set (client → server requests).
///
/// `route_hash` is computed from `client_id` and `channel`:
/// - high 7 bits = hash of `client_id` (identifies the client)
/// - low 1 bit = `channel` (0=data, 1=meta, identifies the physical path)
///
/// The server validates `route_hash` to detect frames arriving on the wrong
/// connection (e.g. a lease frame on a data connection). Without this, the
/// server's channel-mismatch check in `io_loop.rs` would close meta-channel
/// connections because `build_frame` leaves `route_hash=0`.
///
/// Mirrors the kernel-side `pfs_route_hash` computation.
pub fn build_frame_with_route_hash(
    msg_type: u16,
    flags: FrameFlags,
    seq: u32,
    body: &[u8],
    data: &[u8],
    client_id: u64,
    channel: u8,
) -> Vec<u8> {
    let data_len = body.len() as u32 + data.len() as u32;
    let mut header = FrameHeader::new(msg_type, flags, seq, data_len);
    header.set_body_data_len(body.len() as u32, data_len);
    header.route_hash = calc_route_hash(client_id, channel);
    header.header_crc = header.calc_header_crc();

    let mut frame = Vec::with_capacity(FrameHeader::SIZE + body.len() + data.len());
    let mut hdr_buf = vec![0u8; FrameHeader::SIZE];
    header.encode(&mut hdr_buf);
    frame.extend_from_slice(&hdr_buf);
    frame.extend_from_slice(body);
    frame.extend_from_slice(data);

    frame
}

/// Compute `route_hash` from `client_id` and `channel`.
///
/// Layout (1 byte):
/// - bit 0: `channel` (0=data, 1=meta)
/// - bits 1-7: hash of `client_id` (high 7 bits of a 64-bit mix)
///
/// `route_hash=0` is reserved as "unset" — the server skips validation
/// for frames with `route_hash=0` (backward compat with discovery-phase
/// frames that have no client_id yet).
///
/// Mirrors the kernel-side `pfs_route_hash` computation in `powerfs_net.h`.
pub fn calc_route_hash(client_id: u64, channel: u8) -> u8 {
    let mut h = client_id;
    h ^= h >> 32;
    h = h.wrapping_mul(0x9E3779B97F4A7C15);
    h ^= h >> 32;
    ((h >> 25) as u8) << 1 | (channel & 0x01)
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
        let req = HandshakeRequest::new(ClientType::Fuse, 12345, 0);
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

    fn make_request_msg(seq: u32, body: &[u8]) -> NetMessage {
        let header = FrameHeader::new(
            MsgType::Lookup.as_u16(),
            FrameFlags::new(FrameFlags::REQUEST),
            seq,
            body.len() as u32,
        );
        NetMessage::new(header).with_body(body.to_vec())
    }

    #[test]
    fn test_response_builder_carries_seq_and_type() {
        let req = make_request_msg(42, b"req-body");
        let resp = NetMessage::response(&req, STATUS_OK, b"ok".to_vec(), Vec::new());

        assert!(resp.is_response());
        assert!(resp.is_ok());
        assert_eq!(resp.header.seq, 42);
        assert_eq!(resp.header.msg_type, MsgType::Lookup.as_u16());
        assert_eq!(resp.body, b"ok");
        assert!(resp.data.is_empty());
    }

    #[test]
    fn test_response_builder_with_error_status() {
        let req = make_request_msg(7, b"");
        let resp = NetMessage::response(&req, STATUS_ERR_NOT_FOUND, Vec::new(), Vec::new());

        assert!(resp.is_response());
        assert!(!resp.is_ok());
        assert_eq!(resp.header.status, STATUS_ERR_NOT_FOUND);
        assert_eq!(resp.header.seq, 7);
    }

    #[test]
    fn test_ok_response_builder_shorthand() {
        let req = make_request_msg(99, b"");
        let resp = NetMessage::ok_response(&req, b"body".to_vec(), b"data".to_vec());

        assert!(resp.is_ok());
        assert_eq!(resp.body, b"body");
        assert_eq!(resp.data, b"data");
        assert_eq!(resp.total_data_len(), 8);
    }

    #[test]
    fn test_notification_builder_sets_notify_flag() {
        let msg = NetMessage::notification(MsgType::Invalidate, b"payload".to_vec(), Vec::new());

        assert!(msg.header.is_notify());
        assert!(!msg.is_request());
        assert!(!msg.is_response());
        assert_eq!(msg.header.seq, 0); // notifications are fire-and-forget
        assert_eq!(msg.msg_type(), Some(MsgType::Invalidate));
        assert_eq!(msg.body, b"payload");
    }

    #[test]
    fn test_notification_builder_roundtrips_through_frame() {
        // The notification must serialize cleanly so IoLoop can write it
        // and the FUSE/kernel client can decode the header.
        let msg = NetMessage::notification(MsgType::TopologyChanged, Vec::new(), Vec::new());
        let frame = msg.to_frame();

        assert!(frame.len() >= FrameHeader::SIZE);
        let decoded = FrameHeader::decode(&frame[..FrameHeader::SIZE]).unwrap();
        assert!(decoded.is_notify());
        assert_eq!(decoded.msg_type, MsgType::TopologyChanged.as_u16());
        assert!(decoded.verify_crc());
    }

    // ----- Phase 2: load_factor flag encoding -----

    #[test]
    fn test_set_and_get_load_factor() {
        let mut hdr = FrameHeader::new(
            MsgType::Lookup.as_u16(),
            FrameFlags::new(FrameFlags::RESPONSE),
            1,
            0,
        );
        assert_eq!(hdr.load_factor(), 0); // default

        for lf in 0..=3 {
            hdr.set_load_factor(lf);
            assert_eq!(hdr.load_factor(), lf);
            assert!(hdr.verify_crc(), "CRC must be valid after set_load_factor({})", lf);
        }
    }

    #[test]
    fn test_load_factor_clamped() {
        let mut hdr = FrameHeader::new(
            MsgType::Ping.as_u16(),
            FrameFlags::new(FrameFlags::RESPONSE),
            1,
            0,
        );
        hdr.set_load_factor(255);
        assert_eq!(hdr.load_factor(), 3);
        hdr.set_load_factor(5);
        assert_eq!(hdr.load_factor(), 3);
    }

    #[test]
    fn test_load_factor_preserves_other_flags() {
        let mut hdr = FrameHeader::new(
            MsgType::Lookup.as_u16(),
            FrameFlags::new(FrameFlags::RESPONSE | FrameFlags::BATCH),
            42,
            100,
        );
        hdr.set_load_factor(2);
        // RESPONSE and BATCH bits must survive
        assert!(hdr.flags & FrameFlags::RESPONSE != 0);
        assert!(hdr.flags & FrameFlags::BATCH != 0);
        assert_eq!(hdr.load_factor(), 2);
        assert!(hdr.verify_crc());
    }

    #[test]
    fn test_load_factor_survives_encode_decode() {
        let mut hdr = FrameHeader::new(
            MsgType::WriteNeedle.as_u16(),
            FrameFlags::new(FrameFlags::RESPONSE),
            7,
            2048,
        );
        hdr.set_load_factor(3);

        let mut buf = vec![0u8; FrameHeader::SIZE];
        hdr.encode(&mut buf);
        let decoded = FrameHeader::decode(&buf).unwrap();
        assert_eq!(decoded.load_factor(), 3);
        assert!(decoded.verify_crc());
    }

    #[test]
    fn test_load_factor_backward_compat_zero() {
        // Old server fills flags=RESPONSE (0x02), load_factor bits = 00.
        // New client reads load_factor=0 (idle). No breakage.
        let hdr = FrameHeader::new(
            MsgType::Lookup.as_u16(),
            FrameFlags::new(FrameFlags::RESPONSE),
            1,
            0,
        );
        assert_eq!(hdr.load_factor(), 0);
    }
}
