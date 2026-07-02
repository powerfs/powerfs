#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KernelOp {
    Lookup,
    Getattr,
    Setattr,
    Readlink,
    Symlink,
    Link,
    Unlink,
    Rmdir,
    Mkdir,
    Rename,
    Open,
    Read,
    Write,
    Release,
    Fsync,
    Readdir,
    Statfs,
    Access,
    Create,
    Ioctl,
    Getlk,
    Setlk,
    Setlkw,
}

impl KernelOp {
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
            21 => Some(Self::Getlk),
            22 => Some(Self::Setlk),
            23 => Some(Self::Setlkw),
            _ => None,
        }
    }

    pub fn as_u32(&self) -> u32 {
        match self {
            Self::Lookup => 1,
            Self::Getattr => 2,
            Self::Setattr => 3,
            Self::Readlink => 4,
            Self::Symlink => 5,
            Self::Link => 6,
            Self::Unlink => 7,
            Self::Rmdir => 8,
            Self::Mkdir => 9,
            Self::Rename => 10,
            Self::Open => 11,
            Self::Read => 12,
            Self::Write => 13,
            Self::Release => 14,
            Self::Fsync => 15,
            Self::Readdir => 16,
            Self::Statfs => 17,
            Self::Access => 18,
            Self::Create => 19,
            Self::Ioctl => 20,
            Self::Getlk => 21,
            Self::Setlk => 22,
            Self::Setlkw => 23,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KernelRequest {
    pub unique: u64,
    pub opcode: KernelOp,
    pub inode: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct KernelResponse {
    pub unique: u64,
    pub error: i32,
    pub data: Vec<u8>,
}

pub trait KernelBackend {
    fn submit_request(&self, req: KernelRequest) -> Result<(), String>;
    fn poll_response(&self) -> Result<Option<KernelResponse>, String>;
}

#[derive(Debug, Clone)]
pub struct DAXConfig {
    pub enabled: bool,
    pub mapping_size: u64,
    pub hugepages: bool,
}

impl Default for DAXConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mapping_size: 2 * 1024 * 1024 * 1024,
            hugepages: false,
        }
    }
}
