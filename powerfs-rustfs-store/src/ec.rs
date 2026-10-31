//! P6: EC 对象读路径。
//!
//! scrubber 把对象从平铺 chunk 转成 EC(4+2) 分片后，Filer 元数据里的
//! `Entry.chunks` 变成 **group-major** 分片列表：
//!
//! ```text
//! chunks[g * (data + parity) + i]  i < data    → 数据分片 i
//! chunks[g * (data + parity) + i]  i >= data   → 校验分片 i - data
//! ```
//!
//! 每个 group 覆盖对象内 `[g * data * shard_size, ...)`，最后不足一组的部分
//! 在编码时零填充到完整 stripe。`Entry.content_size` 始终是对象原始长度。
//!
//! 读路径策略：
//! 1. **快路径**：只读与请求区间相交的数据分片，拼接后裁剪；任一读失败
//!    （长度不符 / CRC32 不符）即放弃本组快路径。
//! 2. **降级路径**：读满本组 `data + parity` 个分片，CRC 不符视为缺失，
//!    用 Reed-Solomon 重建；可用分片数不足 `data` 时返回错误（绝不返回错数据）。
//!
//! 校验分片的 `offset` 与数据分片 0 重叠（都是 group 基址），因此**不能**
//! 按 offset 判断分片身份，只能按 group 内索引。

use std::future::Future;

use powerfs_core::ec_thread::{EcConfig, EcEncoder};
use powerfs_master::filer_proto::powerfs::{EcLayout, Entry, FileChunk};
use rustfs_ecstore::api::error::{Error, Result};

use crate::backend::PowerFsStore;

/// EC 读逻辑错误（与 RustFS 契约错误分开，便于单测直接断言）。
#[derive(Debug)]
pub(crate) enum EcReadError {
    /// 布局参数非法（data/parity 为 0 或分片大小为 0）。
    InvalidLayout { data: usize, parity: usize },
    /// 分片数量与布局不符。
    ShardCountMismatch { expected: usize, actual: usize },
    /// 可用分片不足以重建。
    InsufficientShards { available: usize, need: usize },
    /// Reed-Solomon 重建失败。
    DecodeFailed(String),
}

impl std::fmt::Display for EcReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EcReadError::InvalidLayout { data, parity } => {
                write!(f, "invalid EC layout: data={data}, parity={parity}")
            }
            EcReadError::ShardCountMismatch { expected, actual } => {
                write!(
                    f,
                    "EC shard count mismatch: expected {expected}, actual {actual}"
                )
            }
            EcReadError::InsufficientShards { available, need } => {
                write!(
                    f,
                    "not enough EC shards to reconstruct: available {available}, need {need}"
                )
            }
            EcReadError::DecodeFailed(message) => write!(f, "EC reconstruction failed: {message}"),
        }
    }
}

impl std::error::Error for EcReadError {}

/// 用 `data + parity` 个分片槽还原一个 group 的完整数据（长度 `data * shard_size`）。
///
/// 前 `data` 个槽位是数据分片、后 `parity` 个是校验分片；`None` 表示缺失。
/// 全部数据分片命中时直接拼接（不构造编码器）；否则要求可用分片数 ≥ `data`，
/// 用 Reed-Solomon 重建缺失槽位。返回长度含最后一组的零填充，调用方按对象
/// 实际长度截断。
pub(crate) fn assemble_group(
    shards: &[Option<Vec<u8>>],
    data: usize,
    parity: usize,
) -> std::result::Result<Vec<u8>, EcReadError> {
    let total = data + parity;
    if data == 0 || parity == 0 {
        return Err(EcReadError::InvalidLayout { data, parity });
    }
    if shards.len() != total {
        return Err(EcReadError::ShardCountMismatch {
            expected: total,
            actual: shards.len(),
        });
    }

    let available = shards.iter().filter(|shard| shard.is_some()).count();
    if available < data {
        return Err(EcReadError::InsufficientShards {
            available,
            need: data,
        });
    }

    // 所有在位分片必须等长：既用于重建，也用于拒绝对不齐的「全数据分片」拼接。
    let shard_size = shards
        .iter()
        .find_map(|shard| shard.as_ref().map(Vec::len))
        .ok_or(EcReadError::InsufficientShards {
            available,
            need: data,
        })?;
    if shards
        .iter()
        .flatten()
        .any(|shard| shard.len() != shard_size)
    {
        return Err(EcReadError::DecodeFailed(
            "present shards have inconsistent lengths".to_string(),
        ));
    }

    // 快路径：所有数据分片都在，直接按索引拼接，无需 RS 重建。
    if shards[..data].iter().all(Option::is_some) {
        let mut out = Vec::with_capacity(data * shard_size);
        for shard in &shards[..data] {
            match shard {
                Some(shard) => out.extend_from_slice(shard),
                None => unreachable!("checked all data shards present"),
            }
        }
        return Ok(out);
    }

    // 降级路径：用 Reed-Solomon 重建缺失槽位。
    let encoder = EcEncoder::new(EcConfig {
        data_shards: data,
        parity_shards: parity,
        // 读路径必须对任意分片大小解码，不能沿用 scrubber 的最小文件阈值。
        min_small_file_size: 0,
        ..Default::default()
    });
    let mut slots = shards.to_vec();
    encoder
        .decode_missing(&mut slots)
        .map_err(EcReadError::DecodeFailed)
}

/// 由后端提供 needle 读取能力，读取单个 EC 对象区间。
impl PowerFsStore {
    pub(crate) async fn read_ec_range(
        &self,
        entry: &Entry,
        ec: &EcLayout,
        want_start: u64,
        want_end: u64,
    ) -> Result<Vec<u8>> {
        let this = self;
        read_ec_range_with(
            entry,
            ec,
            want_start,
            want_end,
            move |volume_id, needle_id| async move {
                this.read_needle_raw(volume_id, needle_id).await
            },
        )
        .await
    }

    /// 读取一个 needle 的原始字节（不含任何校验），校验由调用方统一处理。
    pub(crate) async fn read_needle_raw(&self, volume_id: u64, needle_id: u64) -> Result<Vec<u8>> {
        let address = self.volume_address(volume_id).await?;
        self.volumes
            .read_needle(&address, volume_id, needle_id)
            .await
            .map_err(crate::error::from_volume)
    }
}

/// `read_ec_range` 的可测试内核：needle 读取由 `load_needle` 注入。
///
/// 长度/CRC32 校验、快路径与降级路径都在这里完成，单测用内存映射替换
/// 真实 volume 即可覆盖全部读语义。
pub(crate) async fn read_ec_range_with<F, Fut>(
    entry: &Entry,
    ec: &EcLayout,
    want_start: u64,
    want_end: u64,
    mut load_needle: F,
) -> Result<Vec<u8>>
where
    F: FnMut(u64, u64) -> Fut,
    Fut: Future<Output = Result<Vec<u8>>>,
{
    let data = ec.data_shards as usize;
    let parity = ec.parity_shards as usize;
    let shard_size = ec.shard_size as usize;
    let total = data + parity;

    if data == 0 || parity == 0 || shard_size == 0 {
        return Err(Error::other(EcReadError::InvalidLayout { data, parity }));
    }
    // P7: EC 分片绝不应被压缩（scrubber 只对 Replicated 平铺 chunk 压缩，
    // EC 转换与压缩互斥）。若遇到压缩 chunk 说明元数据不自洽，直接拒绝，
    // 避免把压缩字节当 RS 分片输入导致解码乱码。
    if entry.chunks.iter().any(|c| c.compression != 0) {
        return Err(Error::other(EcReadError::InvalidLayout { data, parity }));
    }
    if !entry.chunks.len().is_multiple_of(total) {
        return Err(Error::other(EcReadError::ShardCountMismatch {
            expected: entry.chunks.len() / total * total,
            actual: entry.chunks.len(),
        }));
    }

    let object_size = entry.content_size;
    if want_start >= want_end || object_size == 0 {
        return Ok(Vec::new());
    }
    let want_end = want_end.min(object_size);

    let group_data_size = data as u64 * shard_size as u64;
    let first_group = want_start / group_data_size;
    let last_group = (want_end - 1) / group_data_size;
    let group_count = entry.chunks.len() / total;
    if last_group as usize >= group_count {
        return Err(Error::other(format!(
            "EC read out of range: group {} >= chunks 组数 {} (inode chunk 数 {}, data+parity={})",
            last_group,
            group_count,
            entry.chunks.len(),
            total
        )));
    }

    let capacity = usize::try_from(want_end - want_start).unwrap_or(0);
    let mut body = Vec::with_capacity(capacity);

    for group in first_group..=last_group {
        let base = group as usize * total;
        let group_start = group * group_data_size;
        let group_end = (group_start + group_data_size).min(object_size);
        let start = want_start.max(group_start);
        let end = want_end.min(group_end);
        if start >= end {
            continue;
        }

        // 快路径：只读与 [start, end) 相交的数据分片。
        let mut fast_ok = true;
        let mut fast = Vec::new();
        for i in 0..data {
            let shard_start = group_start + i as u64 * shard_size as u64;
            let shard_end = shard_start + shard_size as u64;
            if shard_end <= start || shard_start >= end {
                continue;
            }
            let Some(shard) =
                load_checked_shard(&entry.chunks[base + i], shard_size, &mut load_needle).await
            else {
                fast_ok = false;
                break;
            };
            let from = usize::try_from(start.saturating_sub(shard_start)).unwrap_or(0);
            let to = usize::try_from(end.min(shard_end) - shard_start)
                .unwrap_or(shard.len())
                .min(shard.len());
            if from < to {
                fast.extend_from_slice(&shard[from..to]);
            }
        }
        if fast_ok {
            body.extend_from_slice(&fast);
            continue;
        }

        // 降级路径：读满本组全部分片，CRC 不符视为缺失后重建。
        tracing::warn!(
            bucket = entry.directory.as_str(),
            object = entry.name.as_str(),
            group = group,
            "EC 快路径失败，降级读满整组并重建"
        );
        let mut slots = Vec::with_capacity(total);
        for i in 0..total {
            slots.push(
                load_checked_shard(&entry.chunks[base + i], shard_size, &mut load_needle).await,
            );
        }
        let group_data = assemble_group(&slots, data, parity).map_err(|error| {
            tracing::error!(
                bucket = entry.directory.as_str(),
                object = entry.name.as_str(),
                group = group,
                error = %error,
                "EC 重建失败"
            );
            Error::other(error)
        })?;

        // 去掉末组零填充，再裁出本组内的请求区间。
        let group_len = usize::try_from(group_end - group_start)
            .unwrap_or(group_data.len())
            .min(group_data.len());
        let full = &group_data[..group_len];
        let from = usize::try_from(start - group_start)
            .unwrap_or(full.len())
            .min(full.len());
        let to = usize::try_from(end - group_start)
            .unwrap_or(full.len())
            .min(full.len());
        body.extend_from_slice(&full[from..to]);
    }

    Ok(body)
}

/// 读取并校验单个分片：长度必须等于 `shard_size`，`crc32 != 0` 时还必须匹配。
/// 任一校验失败返回 `None`，由调用方决定走降级路径。
async fn load_checked_shard<F, Fut>(
    chunk: &FileChunk,
    shard_size: usize,
    load_needle: &mut F,
) -> Option<Vec<u8>>
where
    F: FnMut(u64, u64) -> Fut,
    Fut: Future<Output = Result<Vec<u8>>>,
{
    let shard = match load_needle(chunk.volume_id, chunk.needle_id).await {
        Ok(shard) => shard,
        Err(error) => {
            tracing::warn!(
                volume_id = chunk.volume_id,
                needle_id = chunk.needle_id,
                error = %error,
                "EC 分片读取失败"
            );
            return None;
        }
    };
    if shard.len() != shard_size {
        tracing::warn!(
            volume_id = chunk.volume_id,
            needle_id = chunk.needle_id,
            expected = shard_size,
            actual = shard.len(),
            "EC 分片长度不符"
        );
        return None;
    }
    if chunk.crc32 != 0 {
        let actual = crc32fast::hash(&shard);
        if actual != chunk.crc32 {
            tracing::warn!(
                volume_id = chunk.volume_id,
                needle_id = chunk.needle_id,
                expected = chunk.crc32,
                actual = actual,
                "EC 分片 CRC32 不符"
            );
            return None;
        }
    }
    Some(shard)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::future::{ready, Ready};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use powerfs_core::ec_thread::{EcConfig, EcEncoder};
    use powerfs_master::filer_proto::powerfs::FileChunk;

    use super::*;

    const DATA: usize = 4;
    const PARITY: usize = 2;
    const SHARD: usize = 4096;
    const GROUP: usize = DATA * SHARD;

    struct Fixture {
        entry: Entry,
        ec: EcLayout,
        object: Vec<u8>,
        needles: HashMap<(u64, u64), Vec<u8>>,
    }

    /// 确定性伪随机字节（避免引入 rand dev-dependency）。
    fn pseudo_random_bytes(len: usize) -> Vec<u8> {
        let mut state: u64 = 0x2545_F491_4F6C_DD1D;
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            out.push((state >> 33) as u8);
        }
        out
    }

    /// 按 scrubber 的真实布局编码：每 group 4 MiB 等价的 `GROUP` 字节，
    /// 末组零填充；分片 group-major 排列，每个分片独立 volume/needle。
    fn build_fixture(object_size: usize) -> Fixture {
        let encoder = EcEncoder::new(EcConfig {
            data_shards: DATA,
            parity_shards: PARITY,
            min_small_file_size: 0,
            ..Default::default()
        });
        let object = pseudo_random_bytes(object_size);
        let groups = object_size.div_ceil(GROUP);
        let mut chunks = Vec::with_capacity(groups * (DATA + PARITY));
        let mut needles = HashMap::new();

        for group in 0..groups {
            let start = group * GROUP;
            let end = (start + GROUP).min(object_size);
            let mut padded = object[start..end].to_vec();
            padded.resize(GROUP, 0);
            let shards = encoder.encode(&padded);
            assert_eq!(shards.len(), DATA + PARITY);

            for (index, shard) in shards.into_iter().enumerate() {
                let volume_id = 10 + index as u64;
                let needle_id = 0xEC00 + group as u64 * 100 + index as u64;
                needles.insert((volume_id, needle_id), shard.clone());
                chunks.push(FileChunk {
                    // 数据分片 offset = group 基址 + i*shard_size；
                    // 校验分片 offset = group 基址（与数据分片 0 重叠）。
                    offset: if index < DATA {
                        (start + index * SHARD) as u64
                    } else {
                        start as u64
                    },
                    size: shard.len() as u64,
                    mtime: 0,
                    needle_id,
                    volume_id,
                    crc32: crc32fast::hash(&shard),
                    // EC 与压缩互斥：分片永不压缩
                    compression: 0,
                });
            }
        }

        Fixture {
            entry: Entry {
                name: "k".to_string(),
                directory: "/b".to_string(),
                content_size: object_size as u64,
                chunks,
                ..Default::default()
            },
            ec: EcLayout {
                data_shards: DATA as u32,
                parity_shards: PARITY as u32,
                shard_size: SHARD as u32,
            },
            object,
            needles,
        }
    }

    fn loader(
        needles: &HashMap<(u64, u64), Vec<u8>>,
    ) -> impl FnMut(u64, u64) -> Ready<Result<Vec<u8>>> + '_ {
        move |volume_id, needle_id| {
            ready(match needles.get(&(volume_id, needle_id)) {
                Some(shard) => Ok(shard.clone()),
                None => Err(Error::other("needle missing")),
            })
        }
    }

    fn counting_loader(
        needles: &HashMap<(u64, u64), Vec<u8>>,
        loads: Arc<AtomicUsize>,
    ) -> impl FnMut(u64, u64) -> Ready<Result<Vec<u8>>> + '_ {
        move |volume_id, needle_id| {
            loads.fetch_add(1, Ordering::Relaxed);
            ready(match needles.get(&(volume_id, needle_id)) {
                Some(shard) => Ok(shard.clone()),
                None => Err(Error::other("needle missing")),
            })
        }
    }

    // 2 组 + 末组不足一组：覆盖末组零填充截断。
    const OBJECT_SIZE: usize = GROUP * 2 - 100;

    #[tokio::test]
    async fn all_data_shards_present_reads_object() {
        let fixture = build_fixture(OBJECT_SIZE);
        let body = read_ec_range_with(
            &fixture.entry,
            &fixture.ec,
            0,
            fixture.object.len() as u64,
            loader(&fixture.needles),
        )
        .await
        .unwrap();
        assert_eq!(body, fixture.object);
    }

    #[tokio::test]
    async fn one_missing_data_shard_is_reconstructed() {
        let mut fixture = build_fixture(OBJECT_SIZE);
        // 第 0 组数据分片 1 丢失
        fixture.needles.remove(&(11, 0xEC01));
        let body = read_ec_range_with(
            &fixture.entry,
            &fixture.ec,
            0,
            fixture.object.len() as u64,
            loader(&fixture.needles),
        )
        .await
        .unwrap();
        assert_eq!(body, fixture.object);
    }

    #[tokio::test]
    async fn two_missing_data_shards_are_reconstructed() {
        let mut fixture = build_fixture(OBJECT_SIZE);
        // 第 1 组数据分片 0、2 丢失（EC(4+2) 最多容忍 2 个）
        fixture.needles.remove(&(10, 0xEC00 + 100));
        fixture.needles.remove(&(12, 0xEC02 + 100));
        let body = read_ec_range_with(
            &fixture.entry,
            &fixture.ec,
            0,
            fixture.object.len() as u64,
            loader(&fixture.needles),
        )
        .await
        .unwrap();
        assert_eq!(body, fixture.object);
    }

    #[tokio::test]
    async fn three_missing_shards_fail_without_returning_bad_data() {
        let mut fixture = build_fixture(OBJECT_SIZE);
        // 第 0 组丢失 3 个分片（2 数据 + 1 校验），可用 3 < data 4
        fixture.needles.remove(&(10, 0xEC00));
        fixture.needles.remove(&(11, 0xEC01));
        fixture.needles.remove(&(14, 0xEC04));
        let result = read_ec_range_with(
            &fixture.entry,
            &fixture.ec,
            0,
            GROUP as u64,
            loader(&fixture.needles),
        )
        .await;
        assert!(result.is_err(), "可用分片不足时必须返回错误");
    }

    #[tokio::test]
    async fn crc_mismatch_falls_back_to_reconstruction() {
        let mut fixture = build_fixture(OBJECT_SIZE);
        // 篡改第 0 组数据分片 0 的 CRC：读到的字节不再可信，应视为缺失
        fixture.entry.chunks[0].crc32 ^= 0xFFFF_FFFF;
        let body = read_ec_range_with(
            &fixture.entry,
            &fixture.ec,
            0,
            fixture.object.len() as u64,
            loader(&fixture.needles),
        )
        .await
        .unwrap();
        assert_eq!(body, fixture.object, "CRC 不符的分片必须被重建替换");
    }

    #[tokio::test]
    async fn range_across_group_boundary_is_sliced_correctly() {
        let fixture = build_fixture(OBJECT_SIZE);
        let start = GROUP as u64 - 100;
        let end = GROUP as u64 + 100;
        let body = read_ec_range_with(
            &fixture.entry,
            &fixture.ec,
            start,
            end,
            loader(&fixture.needles),
        )
        .await
        .unwrap();
        assert_eq!(&body[..], &fixture.object[start as usize..end as usize]);
    }

    #[tokio::test]
    async fn small_range_only_reads_intersecting_data_shard() {
        let fixture = build_fixture(OBJECT_SIZE);
        let loads = Arc::new(AtomicUsize::new(0));
        let body = read_ec_range_with(
            &fixture.entry,
            &fixture.ec,
            0,
            100,
            counting_loader(&fixture.needles, loads.clone()),
        )
        .await
        .unwrap();
        assert_eq!(&body[..], &fixture.object[..100]);
        assert_eq!(
            loads.load(Ordering::Relaxed),
            1,
            "区间落在单个数据分片内时只应读取 1 个 needle"
        );
    }

    #[tokio::test]
    async fn last_group_is_not_padded_in_result() {
        let fixture = build_fixture(OBJECT_SIZE);
        let body = read_ec_range_with(
            &fixture.entry,
            &fixture.ec,
            0,
            fixture.object.len() as u64,
            loader(&fixture.needles),
        )
        .await
        .unwrap();
        assert_eq!(body.len(), OBJECT_SIZE, "末组零填充必须被截断");
        assert_eq!(&body[GROUP..], &fixture.object[GROUP..]);
    }

    #[test]
    fn assemble_group_rejects_inconsistent_shard_sizes() {
        // 4 个数据分片都在，但其中一个长度不对 → 不能直接拼接。
        let shards = vec![
            Some(vec![0u8; SHARD]),
            Some(vec![0u8; SHARD]),
            Some(vec![0u8; SHARD - 1]),
            Some(vec![0u8; SHARD]),
            None,
            None,
        ];
        assert!(matches!(
            assemble_group(&shards, DATA, PARITY),
            Err(EcReadError::DecodeFailed(_))
        ));
    }

    /// P7 Task 4: EC 分片若被标记压缩（元数据自洽失败），读路径必须拒绝，
    /// 避免把压缩字节当 RS 分片输入。
    #[tokio::test]
    async fn ec_read_rejects_compressed_shards() {
        let fixture = build_fixture(GROUP * 2);
        // 篡改第一个 chunk 的 compression tag
        let mut entry = fixture.entry.clone();
        entry.chunks[0].compression = 1; // Zstd

        let result = read_ec_range_with(
            &entry,
            &fixture.ec,
            0,
            fixture.object.len() as u64,
            loader(&fixture.needles),
        )
        .await;

        assert!(result.is_err(), "压缩 EC 分片必须被拒绝");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("invalid EC layout"),
            "应返回 InvalidLayout 类错误，实际: {msg}"
        );
    }
}
