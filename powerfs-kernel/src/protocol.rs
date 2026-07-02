use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum PowerFSOpcode {
    Lookup = 1,
    Getattr = 2,
    Setattr = 3,
    Readlink = 4,
    Symlink = 5,
    Link = 6,
    Unlink = 7,
    Rmdir = 8,
    Mkdir = 9,
    Rename = 10,
    Open = 11,
    Read = 12,
    Write = 13,
    Release = 14,
    Fsync = 15,
    Readdir = 16,
    Statfs = 17,
    Access = 18,
    Create = 19,
    Ioctl = 20,
}

impl PowerFSOpcode {
    pub fn from_u32(op: u32) -> Option<Self> {
        match op {
            1 => Some(Self::Lookup),
            2 => Some(Self::Getattr),
            3 => Some(Self::Setattr),
            4 => Some(Self::Readlink),
            5 => Some(Self::Symlink),
            6 => Some(Self::Link),
            7 => Some(Self::Unlink),
            8 => Some(Self::Rmdir),
            9 => Some(Self::Mkdir),
            10 => Some(Self::Rename),
            11 => Some(Self::Open),
            12 => Some(Self::Read),
            13 => Some(Self::Write),
            14 => Some(Self::Release),
            15 => Some(Self::Fsync),
            16 => Some(Self::Readdir),
            17 => Some(Self::Statfs),
            18 => Some(Self::Access),
            19 => Some(Self::Create),
            20 => Some(Self::Ioctl),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelRequest {
    pub unique: u64,
    pub opcode: u32,
    pub inode: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KernelResponse {
    pub unique: u64,
    pub error: i32,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InodeAttr {
    pub inode: u64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub blksize: u32,
    pub blocks: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub nlink: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntry {
    pub inode: u64,
    pub name: String,
    pub type_: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatFS {
    pub blocks: u64,
    pub bfree: u64,
    pub bavail: u64,
    pub files: u64,
    pub ffree: u64,
    pub bsize: u32,
}
