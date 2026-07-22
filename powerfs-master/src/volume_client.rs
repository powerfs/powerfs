use crate::volume_proto::powerfs::volume_service_client::VolumeServiceClient;
use crate::volume_proto::powerfs::{
    CreateVolumeRequest, DeleteNeedleRequest, ReadNeedleRequest, RestoreNeedleRequest,
    WormLockRequest, WriteNeedleRequest,
};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::transport::Channel;

pub struct VolumeClientPool {
    channels: RwLock<HashMap<String, Channel>>,
}

impl VolumeClientPool {
    pub fn new() -> Self {
        Self::default()
    }

    async fn get_or_create_channel(&self, address: &str) -> Result<Channel, String> {
        {
            let channels = self.channels.read().await;
            if let Some(ch) = channels.get(address) {
                return Ok(ch.clone());
            }
        }

        let addr = format!("http://{}", address);
        let channel = Channel::from_shared(addr)
            .map_err(|e| format!("invalid address: {}", e))?
            .connect()
            .await
            .map_err(|e| format!("connect failed: {}", e))?;

        let mut channels = self.channels.write().await;
        channels.insert(address.to_string(), channel.clone());

        Ok(channel)
    }

    async fn invalidate_channel(&self, address: &str) {
        let mut channels = self.channels.write().await;
        channels.remove(address);
    }

    /// 通知 Volume Server 创建 volume，带重试。
    /// Master 在 apply_assign_volume 中调用此方法，确保 Volume Server 上
    /// 实际存在对应 volume，避免后续 write_needle 报 `volume not found`。
    pub async fn create_volume_with_retry(
        &self,
        address: &str,
        volume_id: u32,
        size: u64,
        max_retries: u32,
        retry_delay: std::time::Duration,
    ) -> Result<(), String> {
        for retry in 0..max_retries {
            let channel = match self.get_or_create_channel(address).await {
                Ok(ch) => ch,
                Err(e) => {
                    log::warn!(
                        "create_volume: connect to {} failed (retry {}): {}",
                        address,
                        retry,
                        e
                    );
                    tokio::time::sleep(retry_delay).await;
                    continue;
                }
            };

            let mut service = VolumeServiceClient::new(channel);
            let request = CreateVolumeRequest { volume_id, size };

            match service.create_volume(tonic::Request::new(request)).await {
                Ok(resp) => {
                    let inner = resp.into_inner();
                    if inner.success {
                        log::info!("create_volume: volume {} created on {}", volume_id, address);
                        return Ok(());
                    } else {
                        log::warn!(
                            "create_volume: volume {} on {} returned success=false (retry {})",
                            volume_id,
                            address,
                            retry
                        );
                    }
                }
                Err(e) => {
                    self.invalidate_channel(address).await;
                    log::warn!(
                        "create_volume: volume {} on {} failed (retry {}): {}",
                        volume_id,
                        address,
                        retry,
                        e
                    );
                }
            }

            if retry + 1 < max_retries {
                tokio::time::sleep(retry_delay).await;
            }
        }
        Err(format!(
            "create_volume: failed after {} retries on {} for volume {}",
            max_retries, address, volume_id
        ))
    }

    pub async fn write_needle(
        &self,
        address: &str,
        volume_id: u32,
        file_key: u64,
        data: &[u8],
    ) -> Result<(), String> {
        let channel = match self.get_or_create_channel(address).await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut service = VolumeServiceClient::new(channel);
        let request = WriteNeedleRequest {
            volume_id,
            file_key,
            data: data.to_vec(),
            cookie: 0,
            ttl: "".to_string(),
        };

        match service.write_needle(tonic::Request::new(request)).await {
            Ok(response) => {
                let result = response.into_inner();
                if result.success {
                    Ok(())
                } else {
                    Err("write_needle failed".to_string())
                }
            }
            Err(e) => {
                self.invalidate_channel(address).await;
                Err(format!("write_needle failed: {}", e))
            }
        }
    }

    pub async fn read_needle(
        &self,
        address: &str,
        volume_id: u32,
        file_key: u64,
    ) -> Result<Vec<u8>, String> {
        let channel = match self.get_or_create_channel(address).await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut service = VolumeServiceClient::new(channel);
        let request = ReadNeedleRequest {
            volume_id,
            file_key,
            cookie: 0,
        };

        match service.read_needle(tonic::Request::new(request)).await {
            Ok(response) => {
                let result = response.into_inner();
                if result.success {
                    Ok(result.data)
                } else {
                    Err("read_needle failed".to_string())
                }
            }
            Err(e) => {
                self.invalidate_channel(address).await;
                Err(format!("read_needle failed: {}", e))
            }
        }
    }

    pub async fn delete_needle(
        &self,
        address: &str,
        volume_id: u32,
        file_key: u64,
    ) -> Result<(), String> {
        let channel = match self.get_or_create_channel(address).await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut service = VolumeServiceClient::new(channel);
        let request = DeleteNeedleRequest {
            volume_id,
            file_key,
            cookie: 0,
        };

        match service.delete_needle(tonic::Request::new(request)).await {
            Ok(response) => {
                let result = response.into_inner();
                if result.success {
                    Ok(())
                } else {
                    Err("delete_needle failed".to_string())
                }
            }
            Err(e) => {
                self.invalidate_channel(address).await;
                Err(format!("delete_needle failed: {}", e))
            }
        }
    }

    pub async fn restore_needle(
        &self,
        address: &str,
        volume_id: u32,
        file_key: u64,
    ) -> Result<(), String> {
        let channel = match self.get_or_create_channel(address).await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut service = VolumeServiceClient::new(channel);
        let request = RestoreNeedleRequest {
            volume_id,
            file_key,
            cookie: 0,
        };

        match service.restore_needle(tonic::Request::new(request)).await {
            Ok(response) => {
                let result = response.into_inner();
                if result.success {
                    Ok(())
                } else {
                    Err("restore_needle failed".to_string())
                }
            }
            Err(e) => {
                self.invalidate_channel(address).await;
                Err(format!("restore_needle failed: {}", e))
            }
        }
    }

    pub async fn worm_lock(
        &self,
        address: &str,
        volume_id: u32,
        file_key: u64,
        retention_days: i64,
    ) -> Result<String, String> {
        let channel = match self.get_or_create_channel(address).await {
            Ok(ch) => ch,
            Err(e) => return Err(e),
        };

        let mut service = VolumeServiceClient::new(channel);
        let request = WormLockRequest {
            volume_id,
            file_key,
            cookie: 0,
            retention_days,
        };

        match service.worm_lock(tonic::Request::new(request)).await {
            Ok(response) => {
                let result = response.into_inner();
                if result.success {
                    Ok(result.retention_until)
                } else {
                    Err("worm_lock failed".to_string())
                }
            }
            Err(e) => {
                self.invalidate_channel(address).await;
                Err(format!("worm_lock failed: {}", e))
            }
        }
    }
}

impl Default for VolumeClientPool {
    fn default() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
        }
    }
}
