use crate::io_uring::AsyncIORing;
use crate::protocol::*;

#[derive(Debug, thiserror::Error)]
pub enum KernelClientError {
    #[error("io_uring error: {0}")]
    IoUring(String),
    #[error("protocol error: {0}")]
    Protocol(String),
}

pub struct KernelClient {
    io_uring: AsyncIORing,
    dax_config: DAXConfig,
    mount_point: String,
}

impl KernelClient {
    pub fn new(mount_point: &str, queue_depth: u32) -> Result<Self, KernelClientError> {
        let io_uring = AsyncIORing::new(queue_depth).map_err(KernelClientError::IoUring)?;

        Ok(Self {
            io_uring,
            dax_config: DAXConfig::default(),
            mount_point: mount_point.to_string(),
        })
    }

    pub fn mount_point(&self) -> &str {
        &self.mount_point
    }

    pub fn with_dax(mut self, config: DAXConfig) -> Self {
        self.dax_config = config;
        self
    }

    pub fn dax_config(&self) -> &DAXConfig {
        &self.dax_config
    }

    pub async fn lookup(&self, parent: u64, name: &str) -> Result<u64, KernelClientError> {
        let unique = self.io_uring.backend().next_unique();
        let mut data = Vec::new();
        data.extend_from_slice(&parent.to_le_bytes());
        data.extend_from_slice(name.as_bytes());

        let req = KernelRequest {
            unique,
            opcode: KernelOp::Lookup,
            inode: parent,
            data,
        };

        let resp = self
            .io_uring
            .submit_and_wait(req)
            .await
            .map_err(KernelClientError::IoUring)?;

        if resp.error != 0 {
            return Err(KernelClientError::Protocol(format!(
                "lookup failed: errno={}",
                resp.error
            )));
        }

        if resp.data.len() >= 8 {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&resp.data[..8]);
            Ok(u64::from_le_bytes(buf))
        } else {
            Err(KernelClientError::Protocol(
                "invalid lookup response".to_string(),
            ))
        }
    }

    pub async fn read(
        &self,
        inode: u64,
        offset: u64,
        size: u32,
    ) -> Result<Vec<u8>, KernelClientError> {
        let unique = self.io_uring.backend().next_unique();
        let mut data = Vec::with_capacity(16);
        data.extend_from_slice(&offset.to_le_bytes());
        data.extend_from_slice(&size.to_le_bytes());

        let req = KernelRequest {
            unique,
            opcode: KernelOp::Read,
            inode,
            data,
        };

        let resp = self
            .io_uring
            .submit_and_wait(req)
            .await
            .map_err(KernelClientError::IoUring)?;

        if resp.error != 0 {
            return Err(KernelClientError::Protocol(format!(
                "read failed: errno={}",
                resp.error
            )));
        }

        Ok(resp.data)
    }

    pub async fn write(
        &self,
        inode: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<u32, KernelClientError> {
        let unique = self.io_uring.backend().next_unique();
        let mut buf = Vec::with_capacity(16 + data.len());
        buf.extend_from_slice(&offset.to_le_bytes());
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(data);

        let req = KernelRequest {
            unique,
            opcode: KernelOp::Write,
            inode,
            data: buf,
        };

        let resp = self
            .io_uring
            .submit_and_wait(req)
            .await
            .map_err(KernelClientError::IoUring)?;

        if resp.error != 0 {
            return Err(KernelClientError::Protocol(format!(
                "write failed: errno={}",
                resp.error
            )));
        }

        if resp.data.len() >= 4 {
            let mut buf = [0u8; 4];
            buf.copy_from_slice(&resp.data[..4]);
            Ok(u32::from_le_bytes(buf))
        } else {
            Err(KernelClientError::Protocol(
                "invalid write response".to_string(),
            ))
        }
    }

    pub async fn getattr(&self, inode: u64) -> Result<(), KernelClientError> {
        let unique = self.io_uring.backend().next_unique();
        let req = KernelRequest {
            unique,
            opcode: KernelOp::Getattr,
            inode,
            data: Vec::new(),
        };

        let resp = self
            .io_uring
            .submit_and_wait(req)
            .await
            .map_err(KernelClientError::IoUring)?;

        if resp.error != 0 {
            return Err(KernelClientError::Protocol(format!(
                "getattr failed: errno={}",
                resp.error
            )));
        }

        Ok(())
    }

    pub fn io_uring(&self) -> &AsyncIORing {
        &self.io_uring
    }
}

pub struct KernelClientConfig {
    pub mount_point: String,
    pub queue_depth: u32,
    pub use_io_uring: bool,
    pub dax: DAXConfig,
}

impl Default for KernelClientConfig {
    fn default() -> Self {
        Self {
            mount_point: "/mnt/powerfs".to_string(),
            queue_depth: 1024,
            use_io_uring: true,
            dax: DAXConfig::default(),
        }
    }
}
