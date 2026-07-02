use std::io;
use std::os::unix::io::RawFd;

use thiserror::Error;

const NETLINK_POWERFS: u32 = 31;
const POWERFS_MULTICAST_GROUP: u32 = 1;

#[derive(Debug, Error)]
pub enum NetlinkError {
    #[error("socket creation failed: {0}")]
    Socket(String),
    #[error("bind failed: {0}")]
    Bind(String),
    #[error("send failed: {0}")]
    Send(String),
    #[error("recv failed: {0}")]
    Recv(String),
    #[error("buffer too small")]
    BufferTooSmall,
    #[error("invalid message")]
    InvalidMessage,
}

fn last_error() -> i32 {
    io::Error::last_os_error().raw_os_error().unwrap_or(-1)
}

pub struct PowerFSNetlink {
    fd: RawFd,
}

impl PowerFSNetlink {
    pub fn new() -> Result<Self, NetlinkError> {
        unsafe {
            let fd = libc::socket(
                libc::AF_NETLINK,
                libc::SOCK_RAW | libc::SOCK_CLOEXEC,
                NETLINK_POWERFS as i32,
            );
            if fd < 0 {
                return Err(NetlinkError::Socket(format!("errno: {}", -last_error())));
            }

            let mut addr: libc::sockaddr_nl = std::mem::zeroed();
            addr.nl_family = libc::AF_NETLINK as u16;
            addr.nl_pid = libc::getpid() as u32;
            addr.nl_groups = POWERFS_MULTICAST_GROUP;

            let ret = libc::bind(
                fd,
                &addr as *const _ as *const libc::sockaddr,
                std::mem::size_of::<libc::sockaddr_nl>() as u32,
            );
            if ret < 0 {
                libc::close(fd);
                return Err(NetlinkError::Bind(format!("errno: {}", -last_error())));
            }

            Ok(Self { fd })
        }
    }

    pub fn fd(&self) -> RawFd {
        self.fd
    }

    pub fn send(&self, data: &[u8]) -> Result<usize, NetlinkError> {
        unsafe {
            let mut hdr: libc::nlmsghdr = std::mem::zeroed();
            hdr.nlmsg_len = (std::mem::size_of::<libc::nlmsghdr>() + data.len()) as u32;
            hdr.nlmsg_type = 1;
            hdr.nlmsg_flags = libc::NLM_F_REQUEST as u16;

            let mut buf = Vec::with_capacity(hdr.nlmsg_len as usize);
            buf.extend_from_slice(&hdr.nlmsg_len.to_le_bytes());
            buf.extend_from_slice(&hdr.nlmsg_type.to_le_bytes());
            buf.extend_from_slice(&hdr.nlmsg_flags.to_le_bytes());
            buf.extend_from_slice(&hdr.nlmsg_seq.to_le_bytes());
            buf.extend_from_slice(&hdr.nlmsg_pid.to_le_bytes());
            buf.extend_from_slice(data);

            let ret = libc::send(self.fd, buf.as_ptr() as *const libc::c_void, buf.len(), 0);
            if ret < 0 {
                Err(NetlinkError::Send(format!("errno: {}", -last_error())))
            } else {
                Ok(ret as usize)
            }
        }
    }

    pub fn recv(&self, buf: &mut [u8]) -> Result<usize, NetlinkError> {
        unsafe {
            let ret = libc::recv(self.fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0);
            if ret < 0 {
                Err(NetlinkError::Recv(format!("errno: {}", -last_error())))
            } else {
                Ok(ret as usize)
            }
        }
    }
}

impl Drop for PowerFSNetlink {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}
