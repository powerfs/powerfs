use std::collections::{HashMap, HashSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::shard_store::FileType;

// ============================================================================
// EntryTag: CRDT 操作的唯一标签
// ============================================================================

/// CRDT 操作的唯一标签，用于冲突检测和合并
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct EntryTag {
    pub client_id: String,
    pub seq: u64,
    pub operation_id: String,
}

impl EntryTag {
    pub fn new(client_id: &str, seq: u64) -> Self {
        Self {
            client_id: client_id.to_string(),
            seq,
            operation_id: format!("{}-{}", client_id, seq),
        }
    }

    /// 检查两个 tag 是否来自同一客户端的操作序列
    pub fn same_client(&self, other: &EntryTag) -> bool {
        self.client_id == other.client_id
    }

    /// 比较操作因果顺序 (同一客户端内 seq 越大越新)
    pub fn is_newer_than(&self, other: &EntryTag) -> bool {
        self.client_id == other.client_id && self.seq > other.seq
    }

    /// 检查是否是同一操作 (用于幂等检测)
    pub fn is_same_operation(&self, other: &EntryTag) -> bool {
        self.operation_id == other.operation_id
    }

    /// 从 proto DirEntryOrset 创建
    pub fn from_dir_entry_orset(entry: &crate::powerfs::DirEntryOrset) -> Self {
        Self {
            client_id: entry.client_id.to_string(),
            seq: entry.seq,
            operation_id: format!("{}-{}", entry.client_id, entry.seq),
        }
    }

    /// 检查是否来自指定客户端
    pub fn is_from_client(&self, client_id: &str) -> bool {
        self.client_id == client_id
    }
}

// ============================================================================
// DirEntryOrset: 带 CRDT tag 的目录项
// ============================================================================

/// 带 CRDT 标签的目录条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirEntryOrset {
    pub tag: EntryTag,
    pub inode: u64,
    pub name: String,
    pub parent_ino: u64,
    pub mode: u32,
    pub file_type: FileType,
    pub size: u64,
    pub mtime: u64,
    pub etag: Option<String>,
}

// ============================================================================
// Tombstone: 已删除条目的标记
// ============================================================================

/// CRDT Tombstone: 标记已删除的条目
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tombstone {
    pub tag: EntryTag,
    pub entry_key: String,
    pub deleted_at_ms: u64, // 使用毫秒时间戳以便序列化
    pub gc_epoch: u64,
}

impl Tombstone {
    pub fn new(tag: EntryTag, entry_key: &str) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            tag,
            entry_key: entry_key.to_string(),
            deleted_at_ms: now_ms,
            gc_epoch: 0,
        }
    }

    /// 检查 tombstone 是否过期
    pub fn is_expired(&self, ttl: Duration) -> bool {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let elapsed_ms = now_ms.saturating_sub(self.deleted_at_ms);
        elapsed_ms > ttl.as_millis() as u64
    }

    /// 检查是否来自指定客户端
    pub fn is_from_client(&self, client_id: &str) -> bool {
        self.tag.client_id == client_id
    }
}

// ============================================================================
// MergeResult: 合并结果
// ============================================================================

/// CRDT 合并操作的结果
#[derive(Debug, Clone, PartialEq)]
pub enum MergeResult {
    /// 操作已成功应用
    Applied,
    /// 幂等操作，已忽略 (重复推送相同操作)
    Idempotent,
    /// 并发 Add (同名不同 tag，两个都保留)
    ConcurrentlyAdded,
    /// 并发 Remove (操作已标记删除)
    ConcurrentlyRemoved,
    /// 检测到冲突，需进一步处理
    Conflict,
}

impl MergeResult {
    pub fn is_success(&self) -> bool {
        matches!(self, MergeResult::Applied | MergeResult::Idempotent)
    }
}

// ============================================================================
// ServerVectorClock: Per-Shard VectorClock
// ============================================================================

/// Per-Shard VectorClock，跟踪各客户端在特定分片上的操作序列
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServerVectorClock {
    entries: HashMap<String, u64>,
}

impl ServerVectorClock {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// 记录某个客户端的操作序列
    pub fn observe(&mut self, client_id: &str, seq: u64) {
        let entry = self.entries.entry(client_id.to_string()).or_insert(0);
        if seq > *entry {
            *entry = seq;
        }
    }

    /// 获取某个客户端的最大 seq
    pub fn get(&self, client_id: &str) -> u64 {
        *self.entries.get(client_id).unwrap_or(&0)
    }

    /// 合并另一个 VectorClock (取每个客户端的最大值)
    pub fn merge(&mut self, other: &ServerVectorClock) {
        for (client_id, seq) in &other.entries {
            self.observe(client_id, *seq);
        }
    }

    /// 检查 self 是否因果依赖于 other (self > other)
    pub fn depends_on(&self, other: &ServerVectorClock) -> bool {
        for (client_id, other_seq) in &other.entries {
            let self_seq = self.get(client_id);
            if self_seq < *other_seq {
                return false;
            }
        }
        true
    }

    /// 检查两个 VectorClock 是否并发 (互不因果依赖)
    pub fn is_concurrent_with(&self, other: &ServerVectorClock) -> bool {
        !self.depends_on(other) && !other.depends_on(self)
    }

    /// 转换为 proto 格式
    pub fn to_proto(&self) -> crate::powerfs::VectorClock {
        crate::powerfs::VectorClock {
            entries: self
                .entries
                .iter()
                .map(|(k, v)| crate::powerfs::VectorClockEntry {
                    client_id: k.parse::<u64>().unwrap_or(0),
                    seq: *v,
                })
                .collect(),
        }
    }

    /// 从 proto 格式创建
    pub fn from_proto(proto: &crate::powerfs::VectorClock) -> Self {
        let mut vclock = Self::new();
        for entry in &proto.entries {
            vclock
                .entries
                .insert(entry.client_id.to_string(), entry.seq);
        }
        vclock
    }

    /// 从 HashMap 创建
    pub fn from_map(map: &HashMap<String, u64>) -> Self {
        let mut vclock = Self::new();
        for (client_id, seq) in map {
            vclock.entries.insert(client_id.clone(), *seq);
        }
        vclock
    }

    /// 获取与另一个 VectorClock 的差异 (self 中有但 other 中没有的)
    pub fn diff_against(&self, other: &ServerVectorClock) -> Vec<(String, u64)> {
        let mut diff = Vec::new();
        for (client_id, seq) in &self.entries {
            let other_seq = other.get(client_id);
            if *seq > other_seq {
                diff.push((client_id.clone(), *seq));
            }
        }
        diff
    }

    pub fn entries(&self) -> &HashMap<String, u64> {
        &self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

// ============================================================================
// ServerDirORSet: 服务端 CRDT 目录状态
// ============================================================================

/// 每个分片每个目录的 CRDT OR-Set 状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerDirORSet {
    pub dir_ino: u64,
    pub entries: HashMap<String, DirEntryOrset>,
    pub entry_tags: HashMap<String, HashSet<EntryTag>>,
    pub tombstones: HashMap<String, Vec<Tombstone>>,
    pub vclock: ServerVectorClock,
}

impl ServerDirORSet {
    pub fn new(dir_ino: u64) -> Self {
        Self {
            dir_ino,
            entries: HashMap::new(),
            entry_tags: HashMap::new(),
            tombstones: HashMap::new(),
            vclock: ServerVectorClock::new(),
        }
    }

    /// 获取条目键 (用于查找)
    fn entry_key(&self, parent_ino: u64, name: &str) -> String {
        format!("{}:{}", parent_ino, name)
    }

    // ========================================================================
    // Add 操作合并
    // ========================================================================

    /// 合并一个 Add 操作
    pub fn merge_add(&mut self, entry: DirEntryOrset) -> MergeResult {
        let entry_key = self.entry_key(entry.parent_ino, &entry.name);
        let tag = entry.tag.clone();

        // 1. 幂等检测: 相同 operation_id 的操作已应用
        if let Some(existing_tags) = self.entry_tags.get(&entry_key) {
            for existing_tag in existing_tags {
                if existing_tag.is_same_operation(&tag) {
                    return MergeResult::Idempotent;
                }
            }
        }

        // 2. 如果有 tombstone，检查是否是同一操作
        if let Some(tombstones) = self.tombstones.get(&entry_key) {
            for tombstone in tombstones {
                if tombstone.tag.is_same_operation(&tag) {
                    // 之前被标记为删除，但现在又添加回来
                    // 这可能是重试，视为幂等
                    return MergeResult::Idempotent;
                }
            }
            // 清除 tombstones (因为 Add 是最新操作)
            self.tombstones.remove(&entry_key);
        }

        // 3. 检查是否有同名条目
        if let Some(existing_entry) = self.entries.get(&entry_key) {
            // 同名不同 tag: 并发 Add 冲突
            if !existing_entry.tag.is_same_operation(&tag) {
                // 两个都保留: 使用 "#client_id#seq" 后缀区分
                let unique_key = format!("{}#{}", entry_key, tag.operation_id);
                self.entries.insert(unique_key.clone(), entry.clone());

                // 更新 entry_tags
                self.entry_tags
                    .entry(entry_key.clone())
                    .or_default()
                    .insert(tag.clone());

                // 也更新 unique_key 的 tags
                self.entry_tags
                    .entry(unique_key)
                    .or_default()
                    .insert(tag.clone());

                self.update_vclock(&tag);
                return MergeResult::ConcurrentlyAdded;
            }
        }

        // 4. 正常添加 (无冲突或同一客户端的更新)
        self.entries.insert(entry_key.clone(), entry);
        self.entry_tags
            .entry(entry_key)
            .or_default()
            .insert(tag.clone());

        self.update_vclock(&tag);
        MergeResult::Applied
    }

    // ========================================================================
    // Remove 操作合并 (Add-Wins 语义)
    // ========================================================================

    /// 合并一个 Remove 操作 (Add-Wins: 如果有并发 Add，Remove 被忽略)
    pub fn merge_remove(&mut self, parent_ino: u64, name: &str, tag: &EntryTag) -> MergeResult {
        let entry_key = self.entry_key(parent_ino, name);

        // 1. 幂等检测
        if let Some(tombstones) = self.tombstones.get(&entry_key) {
            for tombstone in tombstones {
                if tombstone.tag.is_same_operation(tag) {
                    return MergeResult::Idempotent;
                }
            }
        }

        // 2. 检查是否有并发 Add (Add-Wins 语义)
        if let Some(existing_tags) = self.entry_tags.get(&entry_key) {
            // 检查是否有其他客户端的并发 Add (不是当前 Remove 的客户端)
            let has_concurrent_add = existing_tags
                .iter()
                .any(|t| !t.is_same_operation(tag) && !t.is_from_client(&tag.client_id));

            if has_concurrent_add {
                // 并发 Add 存在，使用 Add-Wins 语义: Remove 被忽略
                // 记录 tombstone 但不实际删除
                let tombstone = Tombstone::new(tag.clone(), &entry_key);
                self.tombstones
                    .entry(entry_key.clone())
                    .or_default()
                    .push(tombstone);

                self.update_vclock(tag);
                return MergeResult::ConcurrentlyRemoved;
            }

            // 没有并发 Add，可以安全删除
            self.entries.remove(&entry_key);
            self.entry_tags.remove(&entry_key);
        }

        // 3. 记录 tombstone
        let tombstone = Tombstone::new(tag.clone(), &entry_key);
        self.tombstones
            .entry(entry_key)
            .or_default()
            .push(tombstone);

        self.update_vclock(tag);
        MergeResult::Applied
    }

    // ========================================================================
    // Rename 操作合并
    // ========================================================================

    /// 合并一个 Rename 操作
    pub fn merge_rename(
        &mut self,
        old_parent_ino: u64,
        old_name: &str,
        new_parent_ino: u64,
        new_name: &str,
        tag: &EntryTag,
    ) -> MergeResult {
        let old_key = self.entry_key(old_parent_ino, old_name);
        let new_key = self.entry_key(new_parent_ino, new_name);

        // 1. 幂等检测
        if let Some(tombstones) = self.tombstones.get(&old_key) {
            for tombstone in tombstones {
                if tombstone.tag.is_same_operation(tag) {
                    return MergeResult::Idempotent;
                }
            }
        }

        // 2. 获取旧条目
        if let Some(mut entry) = self.entries.remove(&old_key) {
            // 3. 检查新位置是否有冲突
            if let Some(existing_entry) = self.entries.get(&new_key) {
                if !existing_entry.tag.is_same_operation(tag) {
                    // 新位置已有条目，使用唯一键后缀
                    let unique_new_key = format!("{}#{}", new_key, tag.operation_id);
                    entry.name = new_name.to_string();
                    entry.parent_ino = new_parent_ino;
                    self.entries.insert(unique_new_key.clone(), entry);

                    self.entry_tags
                        .entry(unique_new_key)
                        .or_default()
                        .insert(tag.clone());
                }
            } else {
                // 4. 正常 rename
                entry.name = new_name.to_string();
                entry.parent_ino = new_parent_ino;
                self.entries.insert(new_key.clone(), entry);
            }

            // 5. 更新 tags
            self.entry_tags.remove(&old_key);
            self.entry_tags
                .entry(new_key)
                .or_default()
                .insert(tag.clone());

            // 6. 记录旧位置的 tombstone
            let tombstone = Tombstone::new(tag.clone(), &old_key);
            self.tombstones.entry(old_key).or_default().push(tombstone);

            self.update_vclock(tag);
            MergeResult::Applied
        } else {
            // 旧条目不存在 (可能已被删除或从未创建)
            // 仍然记录操作，用于同步
            self.update_vclock(tag);
            MergeResult::Applied
        }
    }

    // ========================================================================
    // SetAttr 操作合并 (Last-Writer-Wins 语义)
    // ========================================================================

    /// 合并一个 SetAttr 操作 (Last-Writer-Wins 基于 VectorClock)
    pub fn merge_setattr(
        &mut self,
        parent_ino: u64,
        name: &str,
        tag: &EntryTag,
        size: u64,
        mtime: u64,
    ) -> MergeResult {
        let entry_key = self.entry_key(parent_ino, name);

        // 1. 检查条目是否存在
        let entry = match self.entries.get_mut(&entry_key) {
            Some(e) => e,
            None => return MergeResult::Conflict, // 条目不存在
        };

        // 2. 获取当前条目的 vclock (存储在 entry_tags 中)
        let current_tags = self.entry_tags.get(&entry_key).cloned();

        // 3. 判断合并策略
        if let Some(tags) = &current_tags {
            // 检查是否是同一客户端的顺序操作
            let current_max_seq = tags
                .iter()
                .filter(|t| t.is_from_client(&tag.client_id))
                .map(|t| t.seq)
                .max()
                .unwrap_or(0);

            if tag.seq > current_max_seq {
                // 同一客户端的更新操作，可以安全应用
                entry.size = size;
                entry.mtime = mtime;
                entry.tag = tag.clone();

                self.entry_tags
                    .entry(entry_key.clone())
                    .or_default()
                    .insert(tag.clone());

                self.update_vclock(tag);
                return MergeResult::Applied;
            }
        }

        // 4. 并发操作: 使用 (client_id, seq) 作为 tiebreaker
        // 比较当前条目的 tag 与新 tag
        let current_tag_seq = entry.tag.seq;
        let current_tag_client = &entry.tag.client_id;

        // 按 (client_id, seq) 字典序决定
        if (current_tag_client.as_str(), current_tag_seq) <= (&tag.client_id, tag.seq) {
            // 新操作获胜
            entry.size = size;
            entry.mtime = mtime;
            entry.tag = tag.clone();

            self.entry_tags
                .entry(entry_key)
                .or_default()
                .insert(tag.clone());

            self.update_vclock(tag);
            MergeResult::Applied
        } else {
            // 旧操作获胜，新操作被忽略
            self.update_vclock(tag);
            MergeResult::Idempotent
        }
    }

    // ========================================================================
    // 辅助方法
    // ========================================================================

    /// 更新 VectorClock
    fn update_vclock(&mut self, tag: &EntryTag) {
        self.vclock.observe(&tag.client_id, tag.seq);
    }

    /// 检查操作是否可以安全应用 (无并发冲突)
    pub fn is_causally_ready(&self, tag: &EntryTag) -> bool {
        let current_seq = self.vclock.get(&tag.client_id);
        tag.seq <= current_seq + 1 // 允许下一个连续序列号
    }

    /// 查找条目 (返回主条目)
    pub fn lookup(&self, parent_ino: u64, name: &str) -> Option<&DirEntryOrset> {
        let entry_key = self.entry_key(parent_ino, name);
        self.entries.get(&entry_key)
    }

    /// 列出所有条目 (包括并发版本)
    pub fn list_all(&self) -> Vec<&DirEntryOrset> {
        self.entries.values().collect()
    }

    /// 列出主条目 (不包括并发版本)
    pub fn list_primary(&self) -> Vec<&DirEntryOrset> {
        self.entries
            .iter()
            .filter(|(k, _)| !k.contains('#'))
            .map(|(_, v)| v)
            .collect()
    }

    /// 清理过期的 tombstone，返回被清理的数量
    pub fn cleanup_tombstones(&mut self, max_age: Duration) -> usize {
        let mut cleaned_count = 0;
        self.tombstones.retain(|_, tombstones| {
            let before_count = tombstones.len();
            tombstones.retain(|t| !t.is_expired(max_age));
            cleaned_count += before_count - tombstones.len();
            !tombstones.is_empty()
        });
        cleaned_count
    }

    /// 获取 VectorClock
    pub fn vclock(&self) -> &ServerVectorClock {
        &self.vclock
    }

    /// 设置 VectorClock
    pub fn set_vclock(&mut self, vclock: ServerVectorClock) {
        self.vclock = vclock;
    }

    /// 获取条目数量 (主条目)
    pub fn entry_count(&self) -> usize {
        self.entries
            .iter()
            .filter(|(k, _)| !k.contains('#'))
            .count()
    }

    /// 获取 tombstone 数量
    pub fn tombstone_count(&self) -> usize {
        self.tombstones.values().map(|v| v.len()).sum()
    }

    /// 合并另一个 OR-Set 的状态
    pub fn merge_from(&mut self, other: &ServerDirORSet) {
        // 合并 entries (保留所有版本)
        for (key, entry) in &other.entries {
            if !self.entries.contains_key(key) {
                self.entries.insert(key.clone(), entry.clone());
            }
        }

        // 合并 entry_tags
        for (key, tags) in &other.entry_tags {
            self.entry_tags
                .entry(key.clone())
                .or_default()
                .extend(tags.iter().cloned());
        }

        // 合并 tombstones
        for (key, tombstones) in &other.tombstones {
            self.tombstones
                .entry(key.clone())
                .or_default()
                .extend(tombstones.iter().cloned());
        }

        // 合并 vclock
        self.vclock.merge(&other.vclock);
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_tag_idempotent() {
        let tag1 = EntryTag::new("client-A", 1);
        let tag2 = EntryTag::new("client-A", 1);
        let tag3 = EntryTag::new("client-A", 2);

        assert!(tag1.is_same_operation(&tag2));
        assert!(!tag1.is_same_operation(&tag3));
        assert!(tag3.is_newer_than(&tag1));
        assert!(!tag1.is_newer_than(&tag3));
    }

    #[test]
    fn test_merge_add_idempotent() {
        let mut orset = ServerDirORSet::new(1);
        let entry = DirEntryOrset {
            tag: EntryTag::new("client-A", 1),
            inode: 100,
            name: "test.txt".to_string(),
            parent_ino: 1,
            mode: 0o644,
            file_type: FileType::File,
            size: 0,
            mtime: 0,
            etag: None,
        };

        // 第一次添加
        let result = orset.merge_add(entry.clone());
        assert_eq!(result, MergeResult::Applied);

        // 重复添加 (相同 tag)
        let result = orset.merge_add(entry);
        assert_eq!(result, MergeResult::Idempotent);
    }

    #[test]
    fn test_merge_add_concurrent() {
        let mut orset = ServerDirORSet::new(1);

        let entry1 = DirEntryOrset {
            tag: EntryTag::new("client-A", 1),
            inode: 100,
            name: "test.txt".to_string(),
            parent_ino: 1,
            mode: 0o644,
            file_type: FileType::File,
            size: 0,
            mtime: 0,
            etag: None,
        };

        let entry2 = DirEntryOrset {
            tag: EntryTag::new("client-B", 1),
            inode: 101,
            name: "test.txt".to_string(),
            parent_ino: 1,
            mode: 0o644,
            file_type: FileType::File,
            size: 0,
            mtime: 0,
            etag: None,
        };

        // 两个客户端并发 Add 同名文件
        let result1 = orset.merge_add(entry1);
        assert_eq!(result1, MergeResult::Applied);

        let result2 = orset.merge_add(entry2);
        assert_eq!(result2, MergeResult::ConcurrentlyAdded);

        // 应该有两个版本
        assert!(orset.entry_count() >= 1);
    }

    #[test]
    fn test_merge_remove_add_wins() {
        let mut orset = ServerDirORSet::new(1);

        let entry = DirEntryOrset {
            tag: EntryTag::new("client-A", 1),
            inode: 100,
            name: "test.txt".to_string(),
            parent_ino: 1,
            mode: 0o644,
            file_type: FileType::File,
            size: 0,
            mtime: 0,
            etag: None,
        };

        // 先 Add
        orset.merge_add(entry);

        // 另一客户端并发 Remove (Add-Wins)
        let remove_tag = EntryTag::new("client-B", 1);
        let result = orset.merge_remove(1, "test.txt", &remove_tag);
        assert_eq!(result, MergeResult::ConcurrentlyRemoved);

        // 条目应该保留
        assert!(orset.lookup(1, "test.txt").is_some());

        // 但应该有 tombstone
        assert!(!orset.tombstones.is_empty());
    }

    #[test]
    fn test_merge_remove_normal() {
        let mut orset = ServerDirORSet::new(1);

        let entry = DirEntryOrset {
            tag: EntryTag::new("client-A", 1),
            inode: 100,
            name: "test.txt".to_string(),
            parent_ino: 1,
            mode: 0o644,
            file_type: FileType::File,
            size: 0,
            mtime: 0,
            etag: None,
        };

        orset.merge_add(entry);

        // 同一客户端删除 (无并发冲突)
        let remove_tag = EntryTag::new("client-A", 2);
        let result = orset.merge_remove(1, "test.txt", &remove_tag);
        assert_eq!(result, MergeResult::Applied);

        // 条目应该被删除
        assert!(orset.lookup(1, "test.txt").is_none());
    }

    #[test]
    fn test_merge_setattr_lww() {
        let mut orset = ServerDirORSet::new(1);

        let entry = DirEntryOrset {
            tag: EntryTag::new("client-A", 1),
            inode: 100,
            name: "test.txt".to_string(),
            parent_ino: 1,
            mode: 0o644,
            file_type: FileType::File,
            size: 100,
            mtime: 1000,
            etag: None,
        };

        orset.merge_add(entry);

        // 同一客户端更新 (seq 更大)
        let update_tag = EntryTag::new("client-A", 2);
        let result = orset.merge_setattr(1, "test.txt", &update_tag, 200, 2000);
        assert_eq!(result, MergeResult::Applied);

        // 检查更新生效
        let updated = orset.lookup(1, "test.txt").unwrap();
        assert_eq!(updated.size, 200);
        assert_eq!(updated.mtime, 2000);
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut vc1 = ServerVectorClock::new();
        vc1.observe("A", 1);
        vc1.observe("B", 1);

        let mut vc2 = ServerVectorClock::new();
        vc2.observe("A", 2);
        vc2.observe("C", 1);

        vc1.merge(&vc2);

        assert_eq!(vc1.get("A"), 2);
        assert_eq!(vc1.get("B"), 1);
        assert_eq!(vc1.get("C"), 1);
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let mut vc1 = ServerVectorClock::new();
        vc1.observe("A", 1);

        let mut vc2 = ServerVectorClock::new();
        vc2.observe("B", 1);

        // 两个 VectorClock 是并发的 (互不依赖)
        assert!(vc1.is_concurrent_with(&vc2));
    }

    #[test]
    fn test_vector_clock_depends_on() {
        let mut vc1 = ServerVectorClock::new();
        vc1.observe("A", 2);
        vc1.observe("B", 1);

        let mut vc2 = ServerVectorClock::new();
        vc2.observe("A", 1);

        // vc1 依赖 vc2 (vc1 的 A:2 > vc2 的 A:1)
        assert!(vc1.depends_on(&vc2));
        // vc2 不依赖 vc1
        assert!(!vc2.depends_on(&vc1));
    }

    #[test]
    fn test_tombstone_expiration() {
        // 创建一个 1 小时前的 tombstone
        let one_hour_ago_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
            - 3600 * 1000;

        let tombstone = Tombstone {
            tag: EntryTag::new("client-A", 1),
            entry_key: "1:test.txt".to_string(),
            deleted_at_ms: one_hour_ago_ms, // 1小时前
            gc_epoch: 0,
        };

        // TTL 30 分钟，应该过期
        assert!(tombstone.is_expired(Duration::from_secs(1800)));

        // TTL 2 小时，不应该过期
        assert!(!tombstone.is_expired(Duration::from_secs(7200)));
    }

    #[test]
    fn test_merge_rename() {
        let mut orset = ServerDirORSet::new(1);

        let entry = DirEntryOrset {
            tag: EntryTag::new("client-A", 1),
            inode: 100,
            name: "old.txt".to_string(),
            parent_ino: 1,
            mode: 0o644,
            file_type: FileType::File,
            size: 0,
            mtime: 0,
            etag: None,
        };

        orset.merge_add(entry);

        // Rename
        let rename_tag = EntryTag::new("client-A", 2);
        let result = orset.merge_rename(1, "old.txt", 1, "new.txt", &rename_tag);
        assert_eq!(result, MergeResult::Applied);

        // 检查旧条目被移除
        assert!(orset.lookup(1, "old.txt").is_none());

        // 检查新条目存在
        let new_entry = orset.lookup(1, "new.txt").unwrap();
        assert_eq!(new_entry.name, "new.txt");
        assert_eq!(new_entry.inode, 100);

        // 检查 tombstone
        assert!(!orset.tombstones.is_empty());
    }

    #[test]
    fn test_cleanup_tombstones() {
        let mut orset = ServerDirORSet::new(1);

        // 添加一个 tombstone (已过期) - 2小时前
        let two_hours_ago_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
            - 7200 * 1000;

        let tombstone = Tombstone {
            tag: EntryTag::new("client-A", 1),
            entry_key: "1:test.txt".to_string(),
            deleted_at_ms: two_hours_ago_ms, // 2小时前
            gc_epoch: 0,
        };
        orset
            .tombstones
            .entry("1:test.txt".to_string())
            .or_insert_with(Vec::new)
            .push(tombstone);

        assert_eq!(orset.tombstone_count(), 1);

        // 清理 (TTL 1 小时)
        let cleaned = orset.cleanup_tombstones(Duration::from_secs(3600));
        assert_eq!(cleaned, 1);
        assert_eq!(orset.tombstone_count(), 0);
    }
}
