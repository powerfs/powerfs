//! 数据管理器
//!
//! 封装文件数据读写相关的缓存和管理：
//! - ChunkCache：chunk 数据缓存（带 LRU 字节限制）
//! - WriteBuffer：写缓冲（按 inode 聚合）
//! - file_sizes：文件大小本地维护（修复 write 不更新 size 的历史问题）
//! - dirty_chunks：脏 chunk 标记
//!
//! 历史问题修复：
//! 1. ChunkCache LRU：之前 `_max_chunks` 被忽略，现在按字节数淘汰
//! 2. write 文件大小：之前每次 write 都通过 gRPC 更新 Master，现在本地维护
//! 3. truncate 清理 chunks：之前只改 size 不清 chunks，现在清理超过 new_size 的 chunk

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use log::debug;

use crate::cache::ChunkCache;

/// 写缓冲条目
#[derive(Clone, Debug)]
pub struct WriteBufferEntry {
    pub offset: u64,
    pub data: Vec<u8>,
}

/// 写缓冲（按 inode 聚合）
pub struct WriteBuffer {
    buffers: RwLock<HashMap<u64, Vec<WriteBufferEntry>>>,
    max_entries: usize,
}

impl WriteBuffer {
    pub fn new(max_entries: usize) -> Self {
        Self {
            buffers: RwLock::new(HashMap::new()),
            max_entries,
        }
    }

    /// 添加写缓冲条目，返回是否达到 flush 阈值
    pub fn add(&self, inode: u64, offset: u64, data: &[u8]) -> bool {
        let mut buffers = self.buffers.write().unwrap();
        let entries = buffers.entry(inode).or_default();
        entries.push(WriteBufferEntry {
            offset,
            data: data.to_vec(),
        });
        entries.len() >= self.max_entries
    }

    /// 取出并清空指定 inode 的写缓冲
    pub fn take(&self, inode: u64) -> Vec<WriteBufferEntry> {
        let mut buffers = self.buffers.write().unwrap();
        buffers.remove(&inode).unwrap_or_default()
    }

    /// 获取指定 inode 的最大写入偏移
    pub fn get_max_write_offset(&self, inode: u64) -> u64 {
        let buffers = self.buffers.read().unwrap();
        if let Some(entries) = buffers.get(&inode) {
            entries
                .iter()
                .map(|e| e.offset + e.data.len() as u64)
                .max()
                .unwrap_or(0)
        } else {
            0
        }
    }

    /// 是否有待 flush 的缓冲
    pub fn has_pending(&self, inode: u64) -> bool {
        let buffers = self.buffers.read().unwrap();
        buffers.get(&inode).map(|e| !e.is_empty()).unwrap_or(false)
    }
}

/// 数据管理器
pub struct DataManager {
    /// chunk 数据缓存（带 LRU 字节限制）
    chunk_cache: Arc<ChunkCache>,
    /// 写缓冲
    write_buffer: Arc<WriteBuffer>,
    /// 脏 chunk 标记：(inode, chunk_index)
    dirty_chunks: RwLock<HashSet<(u64, u64)>>,
    /// 文件大小缓存（write 时本地维护，修复历史问题）
    file_sizes: RwLock<HashMap<u64, u64>>,
}

impl DataManager {
    pub fn new(chunk_cache: Arc<ChunkCache>, write_buffer: Arc<WriteBuffer>) -> Self {
        Self {
            chunk_cache,
            write_buffer,
            dirty_chunks: RwLock::new(HashSet::new()),
            file_sizes: RwLock::new(HashMap::new()),
        }
    }

    /// 获取 chunk 缓存引用
    pub fn chunk_cache(&self) -> &Arc<ChunkCache> {
        &self.chunk_cache
    }

    /// 获取写缓冲引用
    pub fn write_buffer(&self) -> &Arc<WriteBuffer> {
        &self.write_buffer
    }

    /// 读取文件数据
    ///
    /// 优先从 chunk 缓存读取，未命中返回 None（由调用方从 Volume 拉取）。
    /// 读取范围受文件大小限制，超出部分返回短读。
    /// 支持跨 chunk 读取。
    pub fn read(&self, ino: u64, offset: u64, size: usize) -> Option<Vec<u8>> {
        // 受文件大小限制
        let file_size = self.current_file_size(ino);
        if offset >= file_size {
            return Some(vec![]);
        }
        let max_readable = (file_size - offset) as usize;
        let read_size = size.min(max_readable);

        let mut result = Vec::with_capacity(read_size);
        let mut current_offset = offset;
        let mut remaining = read_size;

        while remaining > 0 {
            let chunk = self.chunk_cache.get(ino, current_offset)?;
            let chunk_offset = self.chunk_cache.get_chunk_offset(current_offset);
            let available = chunk.data.len().saturating_sub(chunk_offset as usize);
            if available == 0 {
                break;
            }
            let take = remaining.min(available);
            result.extend_from_slice(
                &chunk.data[chunk_offset as usize..chunk_offset as usize + take],
            );
            remaining -= take;
            current_offset += take as u64;
        }

        Some(result)
    }

    /// 写入文件数据
    ///
    /// 写入 chunk 缓存 + write buffer，并更新本地文件大小。
    /// 返回实际写入的字节数。
    pub fn write(&self, ino: u64, offset: u64, data: &[u8]) -> u64 {
        let end = offset + data.len() as u64;

        // 修复：本地维护文件大小，无需 RPC
        self.update_file_size_if_larger(ino, end);

        let chunk_size = self.chunk_cache.chunk_size();
        let mtime = crate::orset::now_unix();

        // 将数据按 chunk 边界切分写入
        let mut remaining = data;
        let mut current_offset = offset;

        while !remaining.is_empty() {
            let chunk_index = self.chunk_cache.get_chunk_index(current_offset);
            let chunk_offset = self.chunk_cache.get_chunk_offset(current_offset);
            let space_in_chunk = (chunk_size - chunk_offset) as usize;
            let write_len = remaining.len().min(space_in_chunk);

            // 尝试修改现有 chunk，不存在则创建
            let chunk_start = chunk_index * chunk_size + chunk_offset;
            let written = self.chunk_cache.modify(ino, chunk_start, |chunk| {
                let end_in_chunk = chunk_offset as usize + write_len;
                if chunk.data.len() < end_in_chunk {
                    chunk.data.resize(end_in_chunk, 0);
                }
                chunk.data[chunk_offset as usize..end_in_chunk]
                    .copy_from_slice(&remaining[..write_len]);
            });

            if !written {
                // chunk 不存在，创建新 chunk
                let mut new_data = vec![0u8; chunk_size as usize];
                new_data[chunk_offset as usize..chunk_offset as usize + write_len]
                    .copy_from_slice(&remaining[..write_len]);
                self.chunk_cache
                    .put(ino, chunk_index * chunk_size, new_data, mtime, 0);
                debug!(
                    "DataManager::write: created new chunk ino={}, chunk_idx={}, chunk_offset={}, cache_len={}, cache_bytes={}",
                    ino,
                    chunk_index,
                    chunk_index * chunk_size,
                    self.chunk_cache.len(),
                    self.chunk_cache.current_bytes(),
                );
            }

            // 标记脏 chunk
            self.dirty_chunks
                .write()
                .unwrap()
                .insert((ino, chunk_index));

            current_offset += write_len as u64;
            remaining = &remaining[write_len..];
        }

        // 添加到 write buffer
        self.write_buffer.add(ino, offset, data);

        data.len() as u64
    }

    /// 获取文件当前大小（本地维护）
    pub fn current_file_size(&self, ino: u64) -> u64 {
        let sizes = self.file_sizes.read().unwrap();
        *sizes.get(&ino).unwrap_or(&0)
    }

    /// 设置文件大小（用于 setattr/truncate）
    pub fn set_file_size(&self, ino: u64, size: u64) {
        self.file_sizes.write().unwrap().insert(ino, size);
    }

    /// 更新文件大小（仅当 end > 当前 size 时）
    fn update_file_size_if_larger(&self, ino: u64, end: u64) {
        let mut sizes = self.file_sizes.write().unwrap();
        let current = sizes.entry(ino).or_insert(0);
        if end > *current {
            *current = end;
        }
    }

    /// 截断文件（修复历史问题：清理 chunks）
    ///
    /// 1. 更新本地文件大小
    /// 2. 清理超过 new_size 的 chunk 缓存
    /// 3. 清理 dirty_chunks 中超过 new_size 的标记
    pub fn truncate(&self, ino: u64, new_size: u64) {
        // 1. 更新本地大小
        self.set_file_size(ino, new_size);

        // 2. 清理超过 new_size 的 chunk 缓存
        self.chunk_cache.remove_after(ino, new_size);

        // 3. 清理 dirty_chunks
        let max_chunk_index = if new_size == 0 {
            0
        } else {
            self.chunk_cache.get_chunk_index(new_size - 1) + 1
        };
        let mut dirty = self.dirty_chunks.write().unwrap();
        dirty.retain(|(i, idx)| *i != ino || *idx < max_chunk_index);
    }

    /// 获取指定 inode 的所有脏 chunk 索引
    pub fn get_dirty_chunks(&self, ino: u64) -> Vec<u64> {
        let dirty = self.dirty_chunks.read().unwrap();
        dirty
            .iter()
            .filter(|(i, _)| *i == ino)
            .map(|(_, idx)| *idx)
            .collect()
    }

    /// 标记 chunk 为脏
    pub fn mark_dirty(&self, ino: u64, chunk_index: u64) {
        self.dirty_chunks
            .write()
            .unwrap()
            .insert((ino, chunk_index));
    }

    /// 清除指定 inode 的脏标记
    pub fn clear_dirty(&self, ino: u64) {
        let mut dirty = self.dirty_chunks.write().unwrap();
        dirty.retain(|(i, _)| *i != ino);
    }

    /// 是否有脏数据待 flush
    pub fn has_dirty(&self, ino: u64) -> bool {
        let dirty = self.dirty_chunks.read().unwrap();
        dirty.iter().any(|(i, _)| *i == ino)
    }

    /// 释放 inode 相关的所有资源（release 时调用）
    pub fn release_inode(&self, ino: u64) {
        // 注意：不清理 chunk_cache（其他打开的 fd 可能还在用）
        // 只清理 write_buffer 和 dirty 标记
        self.write_buffer.take(ino);
        self.clear_dirty(ino);
    }

    /// 删除 inode 的所有缓存数据（unlink 时调用）
    pub fn remove_inode(&self, ino: u64) {
        self.chunk_cache.remove(ino);
        self.write_buffer.take(ino);
        self.clear_dirty(ino);
        self.file_sizes.write().unwrap().remove(&ino);
    }

    /// 预读：返回缺失的 chunk 列表
    pub fn prefetch(&self, ino: u64, offset: u64, size: u64) -> Vec<(u64, u64)> {
        self.chunk_cache.prefetch(ino, offset, offset + size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_data_manager(chunk_size: u64, max_bytes: usize) -> DataManager {
        let chunk_cache = Arc::new(ChunkCache::with_max_bytes(chunk_size, max_bytes));
        let write_buffer = Arc::new(WriteBuffer::new(100));
        DataManager::new(chunk_cache, write_buffer)
    }

    #[test]
    fn test_write_updates_file_size() {
        let dm = create_data_manager(1024, 10240);
        let ino = 100u64;

        assert_eq!(dm.current_file_size(ino), 0);

        // 写入 512 字节到 offset 0
        dm.write(ino, 0, &[0u8; 512]);
        assert_eq!(dm.current_file_size(ino), 512);

        // 写入 256 字节到 offset 1024（扩容）
        dm.write(ino, 1024, &[1u8; 256]);
        assert_eq!(dm.current_file_size(ino), 1280); // 1024 + 256

        // 写入 100 字节到 offset 500（不扩容，覆盖中间）
        dm.write(ino, 500, &[2u8; 100]);
        assert_eq!(dm.current_file_size(ino), 1280); // 不变
    }

    #[test]
    fn test_truncate_shrinks_and_clears_chunks() {
        let dm = create_data_manager(1024, 10240);
        let ino = 100u64;

        // 写入 4 个 chunk（offset 0-4096）
        dm.write(ino, 0, &[0u8; 4096]);
        assert_eq!(dm.current_file_size(ino), 4096);
        assert_eq!(dm.get_dirty_chunks(ino).len(), 4);

        // truncate 到 2048
        dm.truncate(ino, 2048);
        assert_eq!(dm.current_file_size(ino), 2048);

        // chunk 0 和 1 应保留，2 和 3 应被清除
        let dirty = dm.get_dirty_chunks(ino);
        assert!(dirty.contains(&0), "chunk 0 should be dirty");
        assert!(dirty.contains(&1), "chunk 1 should be dirty");
        assert!(!dirty.contains(&2), "chunk 2 should be cleared");
        assert!(!dirty.contains(&3), "chunk 3 should be cleared");

        // chunk 缓存也应清理
        assert!(dm.chunk_cache.get(ino, 0).is_some());
        assert!(dm.chunk_cache.get(ino, 1024).is_some());
        assert!(dm.chunk_cache.get(ino, 2048).is_none());
        assert!(dm.chunk_cache.get(ino, 3072).is_none());
    }

    #[test]
    fn test_truncate_to_zero() {
        let dm = create_data_manager(1024, 10240);
        let ino = 100u64;

        dm.write(ino, 0, &[0u8; 2048]);
        assert_eq!(dm.current_file_size(ino), 2048);

        dm.truncate(ino, 0);
        assert_eq!(dm.current_file_size(ino), 0);
        assert!(dm.get_dirty_chunks(ino).is_empty());
    }

    #[test]
    fn test_read_from_cache() {
        let dm = create_data_manager(1024, 10240);
        let ino = 100u64;

        // 写入数据
        let data = vec![42u8; 512];
        dm.write(ino, 0, &data);

        // 读取
        let read = dm.read(ino, 0, 512).unwrap();
        assert_eq!(read, data);

        // 读取超出文件大小范围（offset >= file_size 返回空 Vec，而非 None）
        // 文件大小为 512，offset 2048 超出范围
        let read = dm.read(ino, 2048, 512).unwrap();
        assert!(read.is_empty(), "out-of-range read should return empty vec");
    }

    #[test]
    fn test_read_partial_chunk() {
        let dm = create_data_manager(1024, 10240);
        let ino = 100u64;

        // 写入 100 字节
        dm.write(ino, 0, &[1u8; 100]);

        // 读取 50 字节
        let read = dm.read(ino, 0, 50).unwrap();
        assert_eq!(read.len(), 50);
        assert!(read.iter().all(|&b| b == 1));

        // 读取超出已写部分（应只返回已写的数据）
        let read = dm.read(ino, 0, 200).unwrap();
        assert_eq!(read.len(), 100);
    }

    #[test]
    fn test_release_inode() {
        let dm = create_data_manager(1024, 10240);
        let ino = 100u64;

        dm.write(ino, 0, &[0u8; 512]);
        assert!(dm.has_dirty(ino));

        dm.release_inode(ino);
        assert!(!dm.has_dirty(ino));
        assert!(!dm.write_buffer.has_pending(ino));
        // chunk_cache 保留（可能其他 fd 在用）
        assert!(dm.chunk_cache.get(ino, 0).is_some());
    }

    #[test]
    fn test_remove_inode() {
        let dm = create_data_manager(1024, 10240);
        let ino = 100u64;

        dm.write(ino, 0, &[0u8; 512]);
        assert_eq!(dm.current_file_size(ino), 512);

        dm.remove_inode(ino);
        assert_eq!(dm.current_file_size(ino), 0);
        assert!(dm.chunk_cache.get(ino, 0).is_none());
        assert!(!dm.has_dirty(ino));
    }

    #[test]
    fn test_write_spanning_multiple_chunks() {
        let dm = create_data_manager(1024, 10240);
        let ino = 100u64;

        // 写入 2500 字节，跨 3 个 chunk（1024 * 3 = 3072）
        let data = vec![7u8; 2500];
        let written = dm.write(ino, 0, &data);
        assert_eq!(written, 2500);
        assert_eq!(dm.current_file_size(ino), 2500);

        // 验证可以读回
        let read = dm.read(ino, 0, 2500).unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn test_write_at_offset() {
        let dm = create_data_manager(1024, 10240);
        let ino = 100u64;

        // 在 offset 2048 写入
        dm.write(ino, 2048, &[9u8; 100]);
        assert_eq!(dm.current_file_size(ino), 2148); // 2048 + 100

        // 读取写入的数据
        let read = dm.read(ino, 2048, 100).unwrap();
        assert_eq!(read, vec![9u8; 100]);
    }

    #[test]
    fn test_dirty_chunk_tracking() {
        let dm = create_data_manager(1024, 10240);
        let ino = 100u64;

        dm.write(ino, 0, &[0u8; 1024]);
        dm.write(ino, 2048, &[1u8; 512]);

        let dirty = dm.get_dirty_chunks(ino);
        assert!(dirty.contains(&0), "chunk 0 should be dirty");
        assert!(dirty.contains(&2), "chunk 2 should be dirty");
        assert_eq!(dirty.len(), 2);

        dm.clear_dirty(ino);
        assert!(dm.get_dirty_chunks(ino).is_empty());
    }

    #[test]
    fn test_prefetch() {
        let dm = create_data_manager(1024, 10240);
        let ino = 100u64;

        dm.write(ino, 0, &[0u8; 1024]); // chunk 0 有数据

        // 预读 0-3072 范围，chunk 1 和 2 缺失
        let missing = dm.prefetch(ino, 0, 3072);
        assert_eq!(missing.len(), 2);
    }
}
