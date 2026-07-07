use log::info;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
pub struct EcConfig {
    pub k: usize,
    pub m: usize,
    pub data_shards: usize,
    pub parity_shards: usize,
}

impl Default for EcConfig {
    fn default() -> Self {
        EcConfig {
            k: 4,
            m: 2,
            data_shards: 4,
            parity_shards: 2,
        }
    }
}

pub struct EcEncoder {
    rs: Arc<ReedSolomon>,
    config: EcConfig,
}

impl EcEncoder {
    pub fn new(config: EcConfig) -> Self {
        let rs = ReedSolomon::new(config.data_shards, config.parity_shards).unwrap();

        EcEncoder {
            rs: Arc::new(rs),
            config,
        }
    }

    pub fn encode(&self, data: &[u8]) -> Vec<Vec<u8>> {
        let shard_size = data.len().div_ceil(self.config.data_shards);

        let mut shards: Vec<Vec<u8>> =
            Vec::with_capacity(self.config.data_shards + self.config.parity_shards);

        for i in 0..self.config.data_shards {
            let start = i * shard_size;
            let end = std::cmp::min(start + shard_size, data.len());
            let mut shard = Vec::with_capacity(shard_size);
            shard.extend_from_slice(&data[start..end]);
            while shard.len() < shard_size {
                shard.push(0);
            }
            shards.push(shard);
        }

        for _ in 0..self.config.parity_shards {
            shards.push(vec![0u8; shard_size]);
        }

        self.rs.encode(&mut shards).unwrap();

        shards
    }

    pub fn decode(&self, shards: &[Vec<u8>]) -> Vec<u8> {
        let shard_size = shards[0].len();

        let mut option_shards: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();
        if self.rs.reconstruct(&mut option_shards).is_ok() {
            let mut data = Vec::with_capacity(shard_size * self.config.data_shards);

            for shard in option_shards.iter().take(self.config.data_shards).flatten() {
                data.extend_from_slice(shard);
            }

            data
        } else {
            Vec::new()
        }
    }

    pub fn can_recover(&self, available_shards: &[bool]) -> bool {
        let mut available_count = 0;
        for &available in available_shards {
            if available {
                available_count += 1;
            }
        }
        available_count >= self.config.data_shards
    }
}

pub enum EcTask {
    Encode {
        data: Vec<u8>,
        config: EcConfig,
        response_tx: oneshot::Sender<Result<Vec<Vec<u8>>, ()>>,
    },
    Decode {
        shards: Vec<Vec<u8>>,
        config: EcConfig,
        response_tx: oneshot::Sender<Result<Vec<u8>, ()>>,
    },
}

pub struct EcThread {
    tx: mpsc::Sender<EcTask>,
}

impl EcThread {
    pub fn start(config: EcConfig) -> Self {
        let (tx, mut rx) = mpsc::channel(100);

        tokio::spawn(async move {
            info!("EC thread started with k={}, m={}", config.k, config.m);

            while let Some(task) = rx.recv().await {
                match task {
                    EcTask::Encode {
                        data,
                        config,
                        response_tx,
                    } => {
                        let encoder = EcEncoder::new(config);
                        let shards = encoder.encode(&data);
                        let _ = response_tx.send(Ok(shards));
                    }
                    EcTask::Decode {
                        shards,
                        config,
                        response_tx,
                    } => {
                        let encoder = EcEncoder::new(config);
                        let data = encoder.decode(&shards);
                        if data.is_empty() {
                            let _ = response_tx.send(Err(()));
                        } else {
                            let _ = response_tx.send(Ok(data));
                        }
                    }
                }
            }

            info!("EC thread shutting down");
        });

        EcThread { tx }
    }

    pub async fn encode(&self, data: Vec<u8>, config: EcConfig) -> Result<Vec<Vec<u8>>, ()> {
        let (response_tx, response_rx) = oneshot::channel();

        if self
            .tx
            .send(EcTask::Encode {
                data,
                config,
                response_tx,
            })
            .await
            .is_err()
        {
            return Err(());
        }

        response_rx.await.map_err(|_| ())?
    }

    pub async fn decode(&self, shards: Vec<Vec<u8>>, config: EcConfig) -> Result<Vec<u8>, ()> {
        let (response_tx, response_rx) = oneshot::channel();

        if self
            .tx
            .send(EcTask::Decode {
                shards,
                config,
                response_tx,
            })
            .await
            .is_err()
        {
            return Err(());
        }

        response_rx.await.map_err(|_| ())?
    }
}
