use log::{info, warn};
use rayon::prelude::*;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

#[derive(Clone, Copy)]
struct EncodeBlockParams<'a> {
    block_size: usize,
    shard_size: usize,
    data_shards: usize,
    parity_shards: usize,
    shards: &'a [Vec<u8>],
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SimdBackend {
    None,
    Sse41,
    Avx2,
    Neon,
    #[default]
    Auto,
}

impl SimdBackend {
    pub fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                return SimdBackend::Avx2;
            } else if is_x86_feature_detected!("sse4.1") {
                return SimdBackend::Sse41;
            }
        }
        #[cfg(target_arch = "aarch64")]
        {
            if is_aarch64_feature_detected!("neon") {
                return SimdBackend::Neon;
            }
        }
        SimdBackend::None
    }

    pub fn effective_backend(&self) -> Self {
        match self {
            SimdBackend::Auto => Self::detect(),
            other => other.clone(),
        }
    }

    pub fn supports_simd(&self) -> bool {
        !matches!(self.effective_backend(), SimdBackend::None)
    }
}

#[derive(Debug, Clone)]
pub struct EcConfig {
    pub k: usize,
    pub m: usize,
    pub data_shards: usize,
    pub parity_shards: usize,
    pub min_small_file_size: usize,
    pub simd_backend: SimdBackend,
    pub parallel_encoding: bool,
}

impl Default for EcConfig {
    fn default() -> Self {
        EcConfig {
            k: 4,
            m: 2,
            data_shards: 4,
            parity_shards: 2,
            min_small_file_size: 64 * 1024,
            simd_backend: SimdBackend::Auto,
            parallel_encoding: true,
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

    pub fn should_skip_ec(&self, data_size: usize) -> bool {
        data_size < self.config.min_small_file_size
    }

    pub fn encode(&self, data: &[u8]) -> Vec<Vec<u8>> {
        if self.should_skip_ec(data.len()) {
            return vec![data.to_vec()];
        }

        let shard_size = data.len().div_ceil(self.config.data_shards);
        let total_shards = self.config.data_shards + self.config.parity_shards;

        let mut shards: Vec<Vec<u8>> = Vec::with_capacity(total_shards);

        if self.config.parallel_encoding {
            self.split_data_into_shards_parallel(data, shard_size, &mut shards);
        } else {
            self.split_data_into_shards_serial(data, shard_size, &mut shards);
        }

        for _ in 0..self.config.parity_shards {
            shards.push(vec![0u8; shard_size]);
        }

        self.rs.encode(&mut shards).unwrap();

        shards
    }

    fn split_data_into_shards_parallel(
        &self,
        data: &[u8],
        shard_size: usize,
        shards: &mut Vec<Vec<u8>>,
    ) {
        let data_shards = self.config.data_shards;

        let temp_shards: Vec<Vec<u8>> = (0..data_shards)
            .into_par_iter()
            .map(|i| {
                let start = i * shard_size;
                let end = std::cmp::min(start + shard_size, data.len());
                let mut shard = Vec::with_capacity(shard_size);
                shard.extend_from_slice(&data[start..end]);
                while shard.len() < shard_size {
                    shard.push(0);
                }
                shard
            })
            .collect();

        shards.extend(temp_shards);
    }

    fn split_data_into_shards_serial(
        &self,
        data: &[u8],
        shard_size: usize,
        shards: &mut Vec<Vec<u8>>,
    ) {
        let data_shards = self.config.data_shards;

        for i in 0..data_shards {
            let start = i * shard_size;
            let end = std::cmp::min(start + shard_size, data.len());
            let mut shard = Vec::with_capacity(shard_size);
            shard.extend_from_slice(&data[start..end]);
            while shard.len() < shard_size {
                shard.push(0);
            }
            shards.push(shard);
        }
    }

    #[allow(dead_code)]
    fn encode_with_simd(&self, shards: &mut [Vec<u8>]) {
        self.rs.encode(shards).unwrap();
    }

    #[allow(dead_code)]
    fn encode_generic_parallel(&self, shards: &mut [Vec<u8>]) {
        let shard_size = shards[0].len();
        let data_shards = self.config.data_shards;
        let parity_shards = self.config.parity_shards;

        let block_size = 64;
        let num_blocks = shard_size.div_ceil(block_size);

        let mut all_parity_blocks: Vec<Vec<Vec<u8>>> =
            vec![vec![vec![0u8; block_size]; parity_shards]; num_blocks];

        let params = EncodeBlockParams {
            block_size,
            shard_size,
            data_shards,
            parity_shards,
            shards,
        };

        if self.config.parallel_encoding {
            all_parity_blocks
                .par_iter_mut()
                .enumerate()
                .for_each(|(block_idx, parity_blocks)| {
                    self.encode_block(block_idx, params, parity_blocks);
                });
        } else {
            for (block_idx, parity_blocks) in all_parity_blocks.iter_mut().enumerate() {
                self.encode_block(block_idx, params, parity_blocks);
            }
        }

        for (block_idx, parity_blocks) in all_parity_blocks.iter().enumerate() {
            let start = block_idx * block_size;
            let end = std::cmp::min(start + block_size, shard_size);
            let block_len = end - start;

            for (i, parity_block) in parity_blocks.iter().enumerate() {
                shards[data_shards + i][start..end].copy_from_slice(&parity_block[0..block_len]);
            }
        }
    }

    #[allow(dead_code)]
    fn encode_block(
        &self,
        block_idx: usize,
        params: EncodeBlockParams,
        parity_blocks: &mut [Vec<u8>],
    ) {
        let start = block_idx * params.block_size;
        let end = std::cmp::min(start + params.block_size, params.shard_size);
        let block_len = end - start;

        let data_block: Vec<&[u8]> = params
            .shards
            .iter()
            .take(params.data_shards)
            .map(|s| &s[start..end])
            .collect();

        for (i, parity_block) in parity_blocks
            .iter_mut()
            .enumerate()
            .take(params.parity_shards)
        {
            for (j, data_slice) in data_block.iter().enumerate().take(params.data_shards) {
                let coef = self.get_coefficient(i, j);
                for k in 0..block_len {
                    parity_block[k] ^= self.gf_mul(data_slice[k], coef);
                }
            }
        }
    }

    #[allow(dead_code)]
    fn get_coefficient(&self, parity_idx: usize, data_idx: usize) -> u8 {
        let x = (data_idx + 1) as u8;
        let power = (parity_idx + 1) as u32;
        let mut result = 1u8;
        let mut base = x;
        let mut exp = power;

        while exp > 0 {
            if exp & 1 == 1 {
                result = self.gf_mul(result, base);
            }
            base = self.gf_mul(base, base);
            exp >>= 1;
        }

        result
    }

    #[allow(dead_code)]
    fn gf_mul(&self, a: u8, b: u8) -> u8 {
        let mut p: u16 = 0;
        let mut a = a as u16;
        let mut b = b as u16;

        for _ in 0..8 {
            if b & 1 == 1 {
                p ^= a;
            }
            let carry = a & 0x80;
            a <<= 1;
            if carry != 0 {
                a ^= 0x1d;
            }
            b >>= 1;
        }

        p as u8
    }

    #[allow(dead_code)]
    fn gf_mul_add_block(&self, dst: &mut [u8], src: &[u8], coef: u8, len: usize) {
        for i in 0..len {
            dst[i] ^= self.gf_mul(src[i], coef);
        }
    }

    pub fn decode(&self, shards: &[Vec<u8>]) -> Vec<u8> {
        if shards.len() == 1 {
            return shards[0].clone();
        }

        let shard_size = shards[0].len();

        let mut option_shards: Vec<Option<Vec<u8>>> = shards.iter().cloned().map(Some).collect();

        if self.rs.reconstruct(&mut option_shards).is_ok() {
            let mut data = Vec::with_capacity(shard_size * self.config.data_shards);

            for shard in option_shards.iter().take(self.config.data_shards).flatten() {
                data.extend_from_slice(shard);
            }

            data
        } else {
            warn!("EC decode failed: unable to reconstruct shards");
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

pub struct EcThreadPool {
    tx: mpsc::Sender<EcTask>,
}

impl EcThreadPool {
    pub fn start(_config: EcConfig) -> Self {
        let (tx, mut rx) = mpsc::channel(100);

        info!("EC thread pool started with parallel encoding");

        tokio::spawn(async move {
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
        });

        EcThreadPool { tx }
    }

    pub async fn encode(&self, data: Vec<u8>, config: EcConfig) -> Result<Vec<Vec<u8>>, ()> {
        let (response_tx, response_rx) = oneshot::channel();

        let task = EcTask::Encode {
            data,
            config,
            response_tx,
        };

        if self.tx.send(task).await.is_err() {
            return Err(());
        }

        response_rx.await.map_err(|_| ())?
    }

    pub async fn decode(&self, shards: Vec<Vec<u8>>, config: EcConfig) -> Result<Vec<u8>, ()> {
        let (response_tx, response_rx) = oneshot::channel();

        let task = EcTask::Decode {
            shards,
            config,
            response_tx,
        };

        if self.tx.send(task).await.is_err() {
            return Err(());
        }

        response_rx.await.map_err(|_| ())?
    }
}
