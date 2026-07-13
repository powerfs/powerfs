//! OR-Set (Observed-Remove Set) 核心数据结构
//!
//! 用于目录条目的弱一致缓存。每个条目由 (name, client_id, seq) 唯一标识，
//! 避免并发写覆盖。配合 VectorClock 判定因果顺序与并发冲突。

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::cache::CachedFileChunk;

/// 返回当前 Unix 时间戳（秒）
pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 文件类型
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
}

impl FileType {
    pub fn is_dir(self) -> bool {
        matches!(self, FileType::Directory)
    }

    pub fn is_regular(self) -> bool {
        matches!(self, FileType::RegularFile)
    }

    /// 转为 libc d_type 值
    pub fn to_d_type(self) -> u32 {
        match self {
            FileType::RegularFile => libc::DT_REG as u32,
            FileType::Directory => libc::DT_DIR as u32,
            FileType::Symlink => libc::DT_LNK as u32,
        }
    }

    /// 从 mode 构造文件类型
    pub fn from_mode(mode: u32) -> Self {
        match mode & libc::S_IFMT {
            m if m == libc::S_IFDIR => FileType::Directory,
            m if m == libc::S_IFLNK => FileType::Symlink,
            _ => FileType::RegularFile,
        }
    }
}

/// 条目唯一标识：(name + client_id + seq)
///
/// 同名文件在不同客户端并发创建时，EntryId 不同，全部保留，永不覆盖。
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntryId {
    pub name: String,
    pub client_id: u64,
    pub seq: u64,
}

impl EntryId {
    pub fn new(name: impl Into<String>, client_id: u64, seq: u64) -> Self {
        Self {
            name: name.into(),
            client_id,
            seq,
        }
    }
}

impl fmt::Display for EntryId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}#{}#{}", self.name, self.client_id, self.seq)
    }
}

/// 目录条目（OR-Set 中的一个元素）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirEntry {
    pub id: EntryId,
    pub inode: u64,
    pub file_type: FileType,
    pub mode: u32,
    pub size: u64,
    pub mtime: u64,
    pub atime: u64,
    pub ctime: u64,
    pub parent_ino: u64,
    pub chunks: Vec<CachedFileChunk>,
    pub symlink_target: Option<String>,
}

impl DirEntry {
    pub fn new_file(id: EntryId, inode: u64, parent_ino: u64, mode: u32) -> Self {
        let now = now_unix();
        Self {
            id,
            inode,
            file_type: FileType::RegularFile,
            mode,
            size: 0,
            mtime: now,
            atime: now,
            ctime: now,
            parent_ino,
            chunks: vec![],
            symlink_target: None,
        }
    }

    pub fn new_dir(id: EntryId, inode: u64, parent_ino: u64, mode: u32) -> Self {
        let now = now_unix();
        Self {
            id,
            inode,
            file_type: FileType::Directory,
            mode,
            size: 0,
            mtime: now,
            atime: now,
            ctime: now,
            parent_ino,
            chunks: vec![],
            symlink_target: None,
        }
    }

    pub fn new_symlink(
        id: EntryId,
        inode: u64,
        parent_ino: u64,
        mode: u32,
        target: String,
    ) -> Self {
        let now = now_unix();
        Self {
            id,
            inode,
            file_type: FileType::Symlink,
            mode,
            size: 0,
            mtime: now,
            atime: now,
            ctime: now,
            parent_ino,
            chunks: vec![],
            symlink_target: Some(target),
        }
    }

    pub fn is_dir(&self) -> bool {
        self.file_type.is_dir()
    }

    pub fn is_regular(&self) -> bool {
        self.file_type.is_regular()
    }

    pub fn name(&self) -> &str {
        &self.id.name
    }
}

/// 因果顺序判定结果
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CausalOrder {
    /// self 在 other 之前（self → other）
    Before,
    /// self 在 other 之后（other → self）
    After,
    /// 两者相等
    Equal,
    /// 并发（无法判定因果顺序）
    Concurrent,
}

/// 向量时钟
///
/// 记录每个客户端已观察到的最大序列号，用于判定两个操作的因果顺序。
/// - `compare`: 返回两个时钟的因果关系
/// - `merge`: 合并两个时钟（取各客户端的最大值）
/// - `is_concurrent`: 判定是否并发
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VectorClock {
    counters: HashMap<u64, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// 递增指定客户端的计数器，返回新的序列号
    pub fn increment(&mut self, client_id: u64) -> u64 {
        let counter = self.counters.entry(client_id).or_insert(0);
        *counter += 1;
        *counter
    }

    /// 观察指定客户端的序列号（取最大值）
    pub fn observe(&mut self, client_id: u64, seq: u64) {
        let counter = self.counters.entry(client_id).or_insert(0);
        if seq > *counter {
            *counter = seq;
        }
    }

    /// 获取指定客户端的计数
    pub fn get(&self, client_id: u64) -> u64 {
        self.counters.get(&client_id).copied().unwrap_or(0)
    }

    /// 比较两个时钟的因果顺序
    pub fn compare(&self, other: &Self) -> CausalOrder {
        let all_keys: HashSet<u64> = self
            .counters
            .keys()
            .chain(other.counters.keys())
            .copied()
            .collect();

        let mut self_le = true; // self <= other
        let mut self_ge = true; // self >= other

        for key in all_keys {
            let s = self.get(key);
            let o = other.get(key);
            if s > o {
                self_le = false;
            }
            if s < o {
                self_ge = false;
            }
        }

        match (self_le, self_ge) {
            (true, true) => CausalOrder::Equal,
            (true, false) => CausalOrder::Before,
            (false, true) => CausalOrder::After,
            (false, false) => CausalOrder::Concurrent,
        }
    }

    /// 判定是否并发
    pub fn is_concurrent(&self, other: &Self) -> bool {
        self.compare(other) == CausalOrder::Concurrent
    }

    /// 判定 self 是否支配 other（self >= other 且至少一个 >）
    pub fn dominates(&self, other: &Self) -> bool {
        let all_keys: HashSet<u64> = self
            .counters
            .keys()
            .chain(other.counters.keys())
            .copied()
            .collect();

        let mut self_ge = true;
        let mut has_strict = false;

        for key in all_keys {
            let s = self.get(key);
            let o = other.get(key);
            if s < o {
                self_ge = false;
                break;
            }
            if s > o {
                has_strict = true;
            }
        }

        self_ge && has_strict
    }

    /// 合并另一个时钟（取各客户端的最大值）
    pub fn merge(&mut self, other: &Self) {
        for (&client_id, &seq) in &other.counters {
            self.observe(client_id, seq);
        }
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    /// 返回所有已知的客户端 ID
    pub fn known_clients(&self) -> Vec<u64> {
        self.counters.keys().copied().collect()
    }
}

impl PartialEq for VectorClock {
    fn eq(&self, other: &Self) -> bool {
        self.compare(other) == CausalOrder::Equal
    }
}

impl Eq for VectorClock {}

/// Delta 操作（客户端 → Master 增量同步的最小单元）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DeltaOp {
    /// 新增条目
    Add {
        entry: DirEntry,
        vclock: VectorClock,
    },
    /// 删除条目（加入墓碑）
    Remove { id: EntryId, vclock: VectorClock },
    /// 重命名（Remove old + Add new 原子组合）
    Rename {
        old_id: EntryId,
        new_entry: DirEntry,
        vclock: VectorClock,
    },
    /// 修改属性（mode/size/mtime 等）
    SetAttr {
        inode: u64,
        mode: Option<u32>,
        size: Option<u64>,
        mtime: Option<u64>,
        vclock: VectorClock,
    },
}

impl DeltaOp {
    /// 获取该 delta 操作关联的 vclock
    pub fn vclock(&self) -> &VectorClock {
        match self {
            DeltaOp::Add { vclock, .. }
            | DeltaOp::Remove { vclock, .. }
            | DeltaOp::Rename { vclock, .. }
            | DeltaOp::SetAttr { vclock, .. } => vclock,
        }
    }

    /// 获取该 delta 操作涉及的目录 inode（parent_ino）
    pub fn dir_ino(&self) -> Option<u64> {
        match self {
            DeltaOp::Add { entry, .. } => Some(entry.parent_ino),
            DeltaOp::Remove { .. } => None, // 需要调用方提供
            DeltaOp::Rename { new_entry, .. } => Some(new_entry.parent_ino),
            DeltaOp::SetAttr { .. } => None,
        }
    }

    /// 获取该 delta 涉及的条目名
    pub fn name(&self) -> Option<&str> {
        match self {
            DeltaOp::Add { entry, .. } => Some(&entry.id.name),
            DeltaOp::Remove { id, .. } => Some(&id.name),
            DeltaOp::Rename { new_entry, .. } => Some(&new_entry.id.name),
            DeltaOp::SetAttr { .. } => None,
        }
    }
}

/// 目录 OR-Set
///
/// 基于 Observed-Remove Set 模型：
/// - `entries`: 当前有效条目集合，key 为 EntryId
/// - `tombstones`: 已删除条目的墓碑集合，防止并发删除后复活
/// - `vclock`: 该目录的向量时钟
///
/// 合并规则：
/// - Add: 直接插入 entries（同名多份全部保留）
/// - Remove: 从 entries 移除，加入 tombstones
/// - 并发 Add + Remove 同一 EntryId: Add 失效（已被删除）
/// - 因果顺序 Remove → Add: Add 保留（删除后重建）
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DirORSet {
    pub dir_ino: u64,
    pub entries: HashMap<EntryId, DirEntry>,
    pub tombstones: HashSet<EntryId>,
    pub vclock: VectorClock,
}

impl DirORSet {
    pub fn new(dir_ino: u64) -> Self {
        Self {
            dir_ino,
            entries: HashMap::new(),
            tombstones: HashSet::new(),
            vclock: VectorClock::new(),
        }
    }

    /// 新增条目（本地操作）
    ///
    /// 如果该 EntryId 已在 tombstones 中（被删除后用相同 client+seq 重建），则不插入。
    pub fn add(&mut self, entry: DirEntry) {
        if self.tombstones.contains(&entry.id) {
            // 已删除的 EntryId 不会复活
            return;
        }
        self.entries.insert(entry.id.clone(), entry);
    }

    /// 删除条目（本地操作）
    pub fn remove(&mut self, id: &EntryId) {
        self.entries.remove(id);
        self.tombstones.insert(id.clone());
    }

    /// 按文件名查找所有同名条目（可能多份，Phase 2 冲突场景）
    pub fn get_by_name(&self, name: &str) -> Vec<&DirEntry> {
        self.entries
            .values()
            .filter(|e| e.id.name == name)
            .collect()
    }

    /// 按 inode 查找条目
    pub fn get_by_inode(&self, inode: u64) -> Option<&DirEntry> {
        self.entries.values().find(|e| e.inode == inode)
    }

    /// 按 inode 查找可变条目
    pub fn get_by_inode_mut(&mut self, inode: u64) -> Option<&mut DirEntry> {
        self.entries.values_mut().find(|e| e.inode == inode)
    }

    /// 列出所有有效条目
    pub fn list_all(&self) -> Vec<&DirEntry> {
        self.entries.values().collect()
    }

    /// 获取所有唯一文件名（去重）
    pub fn list_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.entries.values().map(|e| e.id.name.clone()).collect();
        names.sort();
        names.dedup();
        names
    }

    /// 条目数量
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 应用一个 delta 操作（用于合并远程/本地 delta）
    ///
    /// 注意：此方法不进行冲突检测（Phase 2 实现），仅做基础的 add/remove。
    pub fn apply_delta(&mut self, delta: &DeltaOp) {
        match delta {
            DeltaOp::Add { entry, vclock } => {
                self.add(entry.clone());
                self.vclock.merge(vclock);
            }
            DeltaOp::Remove { id, vclock } => {
                self.remove(id);
                self.vclock.merge(vclock);
            }
            DeltaOp::Rename {
                old_id,
                new_entry,
                vclock,
            } => {
                self.remove(old_id);
                self.add(new_entry.clone());
                self.vclock.merge(vclock);
            }
            DeltaOp::SetAttr {
                inode,
                mode,
                size,
                mtime,
                vclock,
            } => {
                if let Some(entry) = self.get_by_inode_mut(*inode) {
                    if let Some(m) = mode {
                        entry.mode = *m;
                    }
                    if let Some(s) = size {
                        entry.size = *s;
                    }
                    if let Some(t) = mtime {
                        entry.mtime = *t;
                    }
                }
                self.vclock.merge(vclock);
            }
        }
    }

    /// 合并另一个 DirORSet（用于全量对齐）
    ///
    /// 合并规则：
    /// - entries: 取并集（同名多份全部保留）
    /// - tombstones: 取并集
    /// - vclock: 取并集（merge）
    pub fn merge(&mut self, other: &Self) {
        for (id, entry) in &other.entries {
            if !self.tombstones.contains(id) {
                self.entries.insert(id.clone(), entry.clone());
            }
        }
        for id in &other.tombstones {
            self.entries.remove(id);
            self.tombstones.insert(id.clone());
        }
        self.vclock.merge(&other.vclock);
    }

    /// 清理过期墓碑（Phase 4 实现 GC，Phase 1 空操作）
    pub fn gc_tombstones(&mut self, _max_age_secs: u64) {
        // Phase 4: 清理超过 max_age 的墓碑
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_clock_basic() {
        let mut vc1 = VectorClock::new();
        assert_eq!(vc1.increment(1), 1);
        assert_eq!(vc1.increment(1), 2);
        assert_eq!(vc1.increment(2), 1);
        assert_eq!(vc1.get(1), 2);
        assert_eq!(vc1.get(2), 1);
        assert_eq!(vc1.get(3), 0);
    }

    #[test]
    fn test_vector_clock_equal() {
        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        vc1.increment(2);

        let mut vc2 = VectorClock::new();
        vc2.observe(1, 1);
        vc2.observe(2, 1);

        assert_eq!(vc1.compare(&vc2), CausalOrder::Equal);
        assert!(!vc1.is_concurrent(&vc2));
    }

    #[test]
    fn test_vector_clock_before_after() {
        let mut vc1 = VectorClock::new();
        vc1.increment(1); // vc1 = {1: 1}

        let mut vc2 = VectorClock::new();
        vc2.increment(1); // vc2 = {1: 1}
        vc2.increment(2); // vc2 = {1: 1, 2: 1}

        // vc1 < vc2
        assert_eq!(vc1.compare(&vc2), CausalOrder::Before);
        assert_eq!(vc2.compare(&vc1), CausalOrder::After);
        assert!(!vc1.is_concurrent(&vc2));
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let mut vc1 = VectorClock::new();
        vc1.increment(1); // vc1 = {1: 1}

        let mut vc2 = VectorClock::new();
        vc2.increment(2); // vc2 = {2: 1}

        assert_eq!(vc1.compare(&vc2), CausalOrder::Concurrent);
        assert!(vc1.is_concurrent(&vc2));
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut vc1 = VectorClock::new();
        vc1.increment(1); // {1: 1}

        let mut vc2 = VectorClock::new();
        vc2.increment(2); // {2: 1}

        vc1.merge(&vc2);
        assert_eq!(vc1.get(1), 1);
        assert_eq!(vc1.get(2), 1);
    }

    #[test]
    fn test_vector_clock_dominates() {
        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        vc1.increment(2);

        let mut vc2 = VectorClock::new();
        vc2.increment(1);

        assert!(vc1.dominates(&vc2));
        assert!(!vc2.dominates(&vc1));
    }

    #[test]
    fn test_vector_clock_observe() {
        let mut vc = VectorClock::new();
        vc.observe(1, 5);
        assert_eq!(vc.get(1), 5);
        vc.observe(1, 3); // 不降低
        assert_eq!(vc.get(1), 5);
    }

    #[test]
    fn test_entry_id_uniqueness() {
        let id1 = EntryId::new("file.txt", 1, 1);
        let id2 = EntryId::new("file.txt", 2, 1);
        let id3 = EntryId::new("file.txt", 1, 2);
        let id4 = EntryId::new("file.txt", 1, 1);

        assert_ne!(id1, id2); // 不同 client
        assert_ne!(id1, id3); // 不同 seq
        assert_eq!(id1, id4); // 完全相同
    }

    #[test]
    fn test_file_type_from_mode() {
        assert_eq!(
            FileType::from_mode(libc::S_IFREG | 0o644),
            FileType::RegularFile
        );
        assert_eq!(
            FileType::from_mode(libc::S_IFDIR | 0o755),
            FileType::Directory
        );
        assert_eq!(
            FileType::from_mode(libc::S_IFLNK | 0o777),
            FileType::Symlink
        );
    }

    #[test]
    fn test_dir_entry_new_file() {
        let id = EntryId::new("test.txt", 1, 1);
        let entry = DirEntry::new_file(id.clone(), 100, 1, 0o644 | libc::S_IFREG);
        assert_eq!(entry.id, id);
        assert_eq!(entry.inode, 100);
        assert_eq!(entry.parent_ino, 1);
        assert!(entry.is_regular());
        assert!(!entry.is_dir());
        assert_eq!(entry.size, 0);
        assert!(entry.chunks.is_empty());
    }

    #[test]
    fn test_dir_entry_new_dir() {
        let id = EntryId::new("subdir", 1, 2);
        let entry = DirEntry::new_dir(id, 200, 1, 0o755 | libc::S_IFDIR);
        assert!(entry.is_dir());
        assert_eq!(entry.parent_ino, 1);
    }

    #[test]
    fn test_dir_orset_add_remove() {
        let mut orset = DirORSet::new(1);
        let id = EntryId::new("file.txt", 1, 1);
        let entry = DirEntry::new_file(id.clone(), 100, 1, 0o644);

        orset.add(entry);
        assert_eq!(orset.len(), 1);
        assert!(orset.get_by_name("file.txt").len() == 1);

        orset.remove(&id);
        assert_eq!(orset.len(), 0);
        assert!(orset.get_by_name("file.txt").is_empty());
        assert!(orset.tombstones.contains(&id));
    }

    #[test]
    fn test_dir_orset_same_name_multiple_entries() {
        let mut orset = DirORSet::new(1);

        // 两个客户端并发创建同名文件
        let id1 = EntryId::new("file.txt", 1, 1);
        let id2 = EntryId::new("file.txt", 2, 1);
        let entry1 = DirEntry::new_file(id1.clone(), 100, 1, 0o644);
        let entry2 = DirEntry::new_file(id2.clone(), 200, 1, 0o644);

        orset.add(entry1);
        orset.add(entry2);

        // 同名两份，全部保留
        let found = orset.get_by_name("file.txt");
        assert_eq!(found.len(), 2);
        assert_eq!(orset.len(), 2);
    }

    #[test]
    fn test_dir_orset_tombstone_prevents_revival() {
        let mut orset = DirORSet::new(1);
        let id = EntryId::new("file.txt", 1, 1);
        let entry = DirEntry::new_file(id.clone(), 100, 1, 0o644);

        orset.add(entry);
        orset.remove(&id);

        // 尝试用相同 EntryId 重新添加（应该被墓碑阻止）
        let entry2 = DirEntry::new_file(id.clone(), 100, 1, 0o644);
        orset.add(entry2);
        assert_eq!(orset.len(), 0);
    }

    #[test]
    fn test_dir_orset_get_by_inode() {
        let mut orset = DirORSet::new(1);
        let id = EntryId::new("file.txt", 1, 1);
        let entry = DirEntry::new_file(id, 100, 1, 0o644);
        orset.add(entry);

        assert!(orset.get_by_inode(100).is_some());
        assert!(orset.get_by_inode(999).is_none());
    }

    #[test]
    fn test_dir_orset_list_names_dedup() {
        let mut orset = DirORSet::new(1);
        orset.add(DirEntry::new_file(
            EntryId::new("a.txt", 1, 1),
            100,
            1,
            0o644,
        ));
        orset.add(DirEntry::new_file(
            EntryId::new("a.txt", 2, 1),
            200,
            1,
            0o644,
        ));
        orset.add(DirEntry::new_file(
            EntryId::new("b.txt", 1, 2),
            300,
            1,
            0o644,
        ));

        let names = orset.list_names();
        assert_eq!(names, vec!["a.txt", "b.txt"]);
    }

    #[test]
    fn test_dir_orset_apply_delta_add() {
        let mut orset = DirORSet::new(1);
        let mut vc = VectorClock::new();
        vc.increment(1);

        let entry = DirEntry::new_file(EntryId::new("file.txt", 1, 1), 100, 1, 0o644);
        let delta = DeltaOp::Add { entry, vclock: vc };

        orset.apply_delta(&delta);
        assert_eq!(orset.len(), 1);
        assert_eq!(orset.vclock.get(1), 1);
    }

    #[test]
    fn test_dir_orset_apply_delta_remove() {
        let mut orset = DirORSet::new(1);
        let id = EntryId::new("file.txt", 1, 1);
        orset.add(DirEntry::new_file(id.clone(), 100, 1, 0o644));

        let mut vc = VectorClock::new();
        vc.increment(1);
        let delta = DeltaOp::Remove {
            id: id.clone(),
            vclock: vc,
        };

        orset.apply_delta(&delta);
        assert_eq!(orset.len(), 0);
        assert!(orset.tombstones.contains(&id));
    }

    #[test]
    fn test_dir_orset_apply_delta_rename() {
        let mut orset = DirORSet::new(1);
        let old_id = EntryId::new("old.txt", 1, 1);
        orset.add(DirEntry::new_file(old_id.clone(), 100, 1, 0o644));

        let mut vc = VectorClock::new();
        vc.increment(1);
        let new_entry = DirEntry::new_file(EntryId::new("new.txt", 1, 2), 100, 1, 0o644);
        let delta = DeltaOp::Rename {
            old_id,
            new_entry,
            vclock: vc,
        };

        orset.apply_delta(&delta);
        assert!(orset.get_by_name("old.txt").is_empty());
        assert!(orset.get_by_name("new.txt").len() == 1);
        assert_eq!(orset.len(), 1);
    }

    #[test]
    fn test_dir_orset_apply_delta_setattr() {
        let mut orset = DirORSet::new(1);
        let id = EntryId::new("file.txt", 1, 1);
        orset.add(DirEntry::new_file(id, 100, 1, 0o644));

        let mut vc = VectorClock::new();
        vc.increment(1);
        let delta = DeltaOp::SetAttr {
            inode: 100,
            mode: Some(0o600 | libc::S_IFREG),
            size: Some(1024),
            mtime: Some(99999),
            vclock: vc,
        };

        orset.apply_delta(&delta);
        let entry = orset.get_by_inode(100).unwrap();
        assert_eq!(entry.mode & 0o777, 0o600);
        assert_eq!(entry.size, 1024);
        assert_eq!(entry.mtime, 99999);
    }

    #[test]
    fn test_dir_orset_merge_union() {
        let mut orset1 = DirORSet::new(1);
        orset1.add(DirEntry::new_file(
            EntryId::new("a.txt", 1, 1),
            100,
            1,
            0o644,
        ));

        let mut orset2 = DirORSet::new(1);
        orset2.add(DirEntry::new_file(
            EntryId::new("b.txt", 2, 1),
            200,
            1,
            0o644,
        ));

        orset1.merge(&orset2);
        assert_eq!(orset1.len(), 2);
        assert!(orset1.get_by_name("a.txt").len() == 1);
        assert!(orset1.get_by_name("b.txt").len() == 1);
    }

    #[test]
    fn test_dir_orset_merge_with_tombstone() {
        let mut orset1 = DirORSet::new(1);
        let id = EntryId::new("file.txt", 1, 1);
        orset1.add(DirEntry::new_file(id.clone(), 100, 1, 0o644));

        let mut orset2 = DirORSet::new(1);
        orset2.remove(&id);

        // 合并后 file.txt 应被删除
        orset1.merge(&orset2);
        assert_eq!(orset1.len(), 0);
        assert!(orset1.tombstones.contains(&id));
    }

    #[test]
    fn test_delta_op_accessors() {
        let mut vc = VectorClock::new();
        vc.increment(1);

        let entry = DirEntry::new_file(EntryId::new("file.txt", 1, 1), 100, 1, 0o644);
        let delta = DeltaOp::Add {
            entry: entry.clone(),
            vclock: vc.clone(),
        };

        assert_eq!(delta.name(), Some("file.txt"));
        assert_eq!(delta.dir_ino(), Some(1));
        assert_eq!(delta.vclock().get(1), 1);
    }

    #[test]
    fn test_entry_id_display() {
        let id = EntryId::new("file.txt", 42, 7);
        assert_eq!(format!("{}", id), "file.txt#42#7");
    }
}
