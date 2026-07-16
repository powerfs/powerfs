//! OR-Set (Observed-Remove Set) 核心数据结构
//!
//! 用于目录条目的弱一致缓存。每个条目由 (name, `client_id`, seq) 唯一标识，
//! 避免并发写覆盖。配合 VectorClock 判定因果顺序与并发冲突。

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// 返回当前 Unix 时间戳（秒）
pub fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// 生成冲突 ID（UUID）
pub fn generate_conflict_id() -> String {
    uuid::Uuid::new_v4().to_string()
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

/// 文件块信息
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CachedFileChunk {
    pub offset: u64,
    pub size: u64,
    pub mtime: u64,
    pub fid: String,
    pub cookie: u32,
    pub crc32: u32,
}

/// 目录条目（OR-Set 中的一个元素）
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirEntry {
    pub id: EntryId,
    pub inode: u64,
    pub generation: u64,
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
            generation: 0,
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
            generation: 0,
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
            generation: 0,
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
    Before,
    After,
    Equal,
    Concurrent,
}

/// 冲突类型
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictType {
    CreateCreate,
    WriteWrite,
    WriteUnlink,
    DeleteCreate,
    RenameConflict,
}

/// 冲突记录
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConflictStats {
    pub total_count: u64,
    pub create_create_count: u64,
    pub write_write_count: u64,
    pub write_unlink_count: u64,
    pub delete_create_count: u64,
    pub rename_conflict_count: u64,
}

pub struct ConflictStatsFull {
    pub total_count: u64,
    pub resolved_count: u64,
    pub unresolved_count: u64,
    pub create_create_count: u64,
    pub create_create_resolved: u64,
    pub write_write_count: u64,
    pub write_write_resolved: u64,
    pub write_unlink_count: u64,
    pub write_unlink_resolved: u64,
    pub delete_create_count: u64,
    pub delete_create_resolved: u64,
    pub rename_conflict_count: u64,
    pub rename_conflict_resolved: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConflictRecord {
    pub id: String,
    pub conflict_type: ConflictType,
    pub base: Option<DirEntry>,
    pub branches: Vec<DirEntry>,
    pub create_time: u64,
    pub resolved: bool,
    pub resolved_time: Option<u64>,
    pub resolution: Option<ConflictResolution>,
}

/// 冲突解决方式
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictResolution {
    KeepFirst,
    KeepLast,
    KeepAll,
    Merge,
}

/// 向量时钟
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VectorClock {
    counters: HashMap<u64, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn increment(&mut self, client_id: u64) -> u64 {
        let counter = self.counters.entry(client_id).or_insert(0);
        *counter += 1;
        *counter
    }

    pub fn observe(&mut self, client_id: u64, seq: u64) {
        let counter = self.counters.entry(client_id).or_insert(0);
        if seq > *counter {
            *counter = seq;
        }
    }

    pub fn get(&self, client_id: u64) -> u64 {
        self.counters.get(&client_id).copied().unwrap_or(0)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&u64, &u64)> {
        self.counters.iter()
    }

    pub fn compare(&self, other: &Self) -> CausalOrder {
        let all_keys: HashSet<u64> = self
            .counters
            .keys()
            .chain(other.counters.keys())
            .copied()
            .collect();

        let mut is_less_or_equal = true;
        let mut is_greater_or_equal = true;

        for key in all_keys {
            let s = self.get(key);
            let o = other.get(key);
            if s > o {
                is_less_or_equal = false;
            }
            if s < o {
                is_greater_or_equal = false;
            }
        }

        match (is_less_or_equal, is_greater_or_equal) {
            (true, true) => CausalOrder::Equal,
            (true, false) => CausalOrder::Before,
            (false, true) => CausalOrder::After,
            (false, false) => CausalOrder::Concurrent,
        }
    }

    pub fn is_concurrent(&self, other: &Self) -> bool {
        self.compare(other) == CausalOrder::Concurrent
    }

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

    pub fn merge(&mut self, other: &Self) {
        for (&client_id, &seq) in &other.counters {
            self.observe(client_id, seq);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

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

/// Delta 操作
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DeltaOp {
    Add {
        entry: DirEntry,
        vclock: VectorClock,
    },
    Remove {
        id: EntryId,
        vclock: VectorClock,
    },
    Rename {
        old_id: EntryId,
        new_entry: DirEntry,
        vclock: VectorClock,
    },
    SetAttr {
        inode: u64,
        mode: Option<u32>,
        size: Option<u64>,
        mtime: Option<u64>,
        vclock: VectorClock,
    },
}

impl DeltaOp {
    pub fn vclock(&self) -> &VectorClock {
        match self {
            DeltaOp::Add { vclock, .. }
            | DeltaOp::Remove { vclock, .. }
            | DeltaOp::Rename { vclock, .. }
            | DeltaOp::SetAttr { vclock, .. } => vclock,
        }
    }

    pub fn dir_ino(&self) -> Option<u64> {
        match self {
            DeltaOp::Add { entry, .. } => Some(entry.parent_ino),
            DeltaOp::Remove { .. } => None,
            DeltaOp::Rename { new_entry, .. } => Some(new_entry.parent_ino),
            DeltaOp::SetAttr { .. } => None,
        }
    }

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
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DirORSet {
    pub dir_ino: u64,
    pub entries: HashMap<EntryId, DirEntry>,
    pub tombstones: HashSet<EntryId>,
    pub vclock: VectorClock,
    pub conflicts: Vec<ConflictRecord>,
    pub policy: MergePolicy,
    pub delta_log: Vec<DeltaOp>,
}

/// 合并策略
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergePolicy {
    #[default]
    LwwTime,
    ContentHash,
    WeightBased,
    KeepAll,
    WritePriority,
    DeletePriority,
    Aggressive,
    Conservative,
    Manual,
}

impl DirORSet {
    pub fn new(dir_ino: u64) -> Self {
        Self {
            dir_ino,
            entries: HashMap::new(),
            tombstones: HashSet::new(),
            vclock: VectorClock::new(),
            conflicts: Vec::new(),
            policy: MergePolicy::default(),
            delta_log: Vec::new(),
        }
    }

    pub fn add(&mut self, entry: DirEntry) {
        if self.tombstones.contains(&entry.id) {
            return;
        }
        self.vclock.increment(entry.id.client_id);
        let vclock = self.vclock.clone();

        self.entries.insert(entry.id.clone(), entry.clone());
        self.delta_log.push(DeltaOp::Add { entry, vclock });
    }

    pub fn remove(&mut self, id: &EntryId) {
        self.vclock.increment(id.client_id);
        let vclock = self.vclock.clone();

        self.entries.remove(id);
        self.tombstones.insert(id.clone());
        self.delta_log.push(DeltaOp::Remove {
            id: id.clone(),
            vclock,
        });
    }

    pub fn update_attr(
        &mut self,
        inode: u64,
        mode: Option<u32>,
        size: Option<u64>,
        mtime: Option<u64>,
        client_id: u64,
    ) {
        self.vclock.increment(client_id);
        let vclock = self.vclock.clone();

        if let Some(entry) = self.get_by_inode_mut(inode) {
            if let Some(m) = mode {
                entry.mode = m;
            }
            if let Some(s) = size {
                entry.size = s;
            }
            if let Some(t) = mtime {
                entry.mtime = t;
            }
        }

        self.delta_log.push(DeltaOp::SetAttr {
            inode,
            mode,
            size,
            mtime,
            vclock,
        });
    }

    pub fn get_by_name(&self, name: &str) -> Vec<&DirEntry> {
        self.entries
            .values()
            .filter(|e| e.id.name == name)
            .collect()
    }

    pub fn get_by_inode(&self, inode: u64) -> Option<&DirEntry> {
        self.entries.values().find(|e| e.inode == inode)
    }

    pub fn get_all_by_inode(&self, inode: u64) -> Vec<&DirEntry> {
        self.entries.values().filter(|e| e.inode == inode).collect()
    }

    pub fn get_by_inode_mut(&mut self, inode: u64) -> Option<&mut DirEntry> {
        self.entries.values_mut().find(|e| e.inode == inode)
    }

    pub fn list_all(&self) -> Vec<&DirEntry> {
        self.entries.values().collect()
    }

    pub fn list_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.entries.values().map(|e| e.id.name.clone()).collect();
        names.sort();
        names.dedup();
        names
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn apply_delta(&mut self, delta: &DeltaOp) {
        match delta {
            DeltaOp::Add { entry, vclock } => {
                self.detect_create_conflict(entry, vclock);
                if self.tombstones.contains(&entry.id) {
                    return;
                }
                self.entries.insert(entry.id.clone(), entry.clone());
                self.vclock.merge(vclock);
            }
            DeltaOp::Remove { id, vclock } => {
                self.detect_remove_conflict(id, vclock);
                self.entries.remove(id);
                self.tombstones.insert(id.clone());
                self.vclock.merge(vclock);
            }
            DeltaOp::Rename {
                old_id,
                new_entry,
                vclock,
            } => {
                self.detect_rename_conflict(old_id, new_entry, vclock);
                self.entries.remove(old_id);
                self.tombstones.insert(old_id.clone());
                if !self.tombstones.contains(&new_entry.id) {
                    self.entries.insert(new_entry.id.clone(), new_entry.clone());
                }
                self.vclock.merge(vclock);
            }
            DeltaOp::SetAttr {
                inode,
                mode,
                size,
                mtime,
                vclock,
            } => {
                self.detect_write_write_conflict(*inode, vclock);
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

    fn detect_create_conflict(&mut self, entry: &DirEntry, vclock: &VectorClock) {
        if self.vclock.is_concurrent(vclock) {
            let existing = self.get_by_name(&entry.id.name);
            let mut conflicts_to_record: Vec<Vec<DirEntry>> = Vec::new();
            for e in existing {
                if e.id.client_id != entry.id.client_id {
                    conflicts_to_record.push(vec![e.clone(), entry.clone()]);
                }
            }
            for branches in conflicts_to_record {
                self.record_conflict(ConflictType::CreateCreate, None, branches);
            }
        }

        if self.tombstones.iter().any(|t| t.name == entry.id.name) {
            let mut branches: Vec<DirEntry> = self
                .get_by_name(&entry.id.name)
                .iter()
                .cloned()
                .cloned()
                .collect();
            branches.push(entry.clone());
            self.record_conflict(ConflictType::DeleteCreate, None, branches);
        }
    }

    fn detect_remove_conflict(&mut self, id: &EntryId, vclock: &VectorClock) {
        if self.vclock.is_concurrent(vclock) {
            if let Some(entry) = self.entries.get(id) {
                let branches = vec![entry.clone()];
                self.record_conflict(ConflictType::WriteUnlink, None, branches);
            } else if self.tombstones.contains(id) {
                let branches = vec![];
                self.record_conflict(ConflictType::WriteUnlink, None, branches);
            }
        }
    }

    fn detect_rename_conflict(
        &mut self,
        _old_id: &EntryId,
        new_entry: &DirEntry,
        vclock: &VectorClock,
    ) {
        if self.vclock.is_concurrent(vclock) {
            let existing = self.get_by_name(&new_entry.id.name);
            let mut conflicts_to_record: Vec<Vec<DirEntry>> = Vec::new();
            for e in existing {
                if e.id.client_id != new_entry.id.client_id {
                    conflicts_to_record.push(vec![e.clone(), new_entry.clone()]);
                }
            }
            for branches in conflicts_to_record {
                self.record_conflict(ConflictType::RenameConflict, None, branches);
            }
        }
    }

    fn detect_write_write_conflict(&mut self, inode: u64, vclock: &VectorClock) {
        if self.vclock.is_concurrent(vclock) {
            let entries = self.get_all_by_inode(inode);
            if !entries.is_empty() {
                let branches: Vec<_> = entries.into_iter().cloned().collect();
                self.record_conflict(ConflictType::WriteWrite, None, branches);
            } else {
                for id in self.entries.keys() {
                    if self.tombstones.contains(id) {
                        self.record_conflict(ConflictType::WriteUnlink, None, vec![]);
                        break;
                    }
                }
            }
        }
    }

    fn chunks_content_equal(a: &[CachedFileChunk], b: &[CachedFileChunk]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        for (ca, cb) in a.iter().zip(b.iter()) {
            if ca.crc32 != cb.crc32 || ca.offset != cb.offset || ca.size != cb.size {
                return false;
            }
        }
        true
    }

    fn can_auto_merge_by_content_hash(&self, branches: &[DirEntry]) -> bool {
        if branches.len() < 2 {
            return false;
        }
        let first = &branches[0];
        for branch in branches.iter().skip(1) {
            if first.file_type != branch.file_type {
                return false;
            }
            if !Self::chunks_content_equal(&first.chunks, &branch.chunks) {
                return false;
            }
        }
        true
    }

    fn record_conflict(
        &mut self,
        conflict_type: ConflictType,
        base: Option<DirEntry>,
        mut branches: Vec<DirEntry>,
    ) {
        if branches.len() < 2
            && conflict_type != ConflictType::WriteUnlink
            && conflict_type != ConflictType::WriteWrite
            && conflict_type != ConflictType::DeleteCreate
        {
            return;
        }

        match self.policy {
            MergePolicy::Conservative | MergePolicy::Aggressive => {
                if self.can_auto_merge_by_content_hash(&branches) {
                    branches.sort_by_key(|b| std::cmp::Reverse(b.mtime));
                    for branch in branches.iter().skip(1) {
                        self.tombstones.insert(branch.id.clone());
                    }
                    return;
                }
            }
            MergePolicy::Manual => {}
            _ => {}
        }

        let branch_ids: Vec<_> = branches.iter().map(|b| &b.id).collect();
        let has_existing = self.conflicts.iter().any(|c| {
            if c.resolved || c.conflict_type != conflict_type {
                return false;
            }
            let c_branch_ids: Vec<_> = c.branches.iter().map(|b| &b.id).collect();
            branch_ids.len() == c_branch_ids.len()
                && branch_ids.iter().all(|id| c_branch_ids.contains(id))
        });

        if has_existing {
            return;
        }

        let conflict = ConflictRecord {
            id: generate_conflict_id(),
            conflict_type,
            base,
            branches,
            create_time: now_unix(),
            resolved: false,
            resolved_time: None,
            resolution: None,
        };
        self.conflicts.push(conflict);
    }

    pub fn unresolved_conflicts(&self) -> Vec<&ConflictRecord> {
        self.conflicts.iter().filter(|c| !c.resolved).collect()
    }

    pub fn resolve_conflict(&mut self, conflict_id: &str, resolution: ConflictResolution) {
        if let Some(conflict) = self.conflicts.iter_mut().find(|c| c.id == conflict_id) {
            conflict.resolved = true;
            conflict.resolved_time = Some(now_unix());
            conflict.resolution = Some(resolution);
        }
    }

    pub fn conflict_count(&self) -> usize {
        self.conflicts.iter().filter(|c| !c.resolved).count()
    }

    pub fn has_unresolved_conflicts(&self) -> bool {
        self.conflicts.iter().any(|c| !c.resolved)
    }

    pub fn conflicts(&self) -> Vec<ConflictRecord> {
        self.conflicts.clone()
    }

    pub fn set_policy(&mut self, policy: MergePolicy) {
        self.policy = policy;
    }

    pub fn auto_resolve_all(&mut self) -> u64 {
        let mut resolved_count = 0;
        for conflict in self.conflicts.iter_mut() {
            if !conflict.resolved {
                conflict.resolved = true;
                conflict.resolved_time = Some(now_unix());
                conflict.resolution = Some(ConflictResolution::KeepLast);
                resolved_count += 1;
            }
        }
        resolved_count
    }

    pub fn get_conflict_stats_by_dir(&self, dir_ino: u64, recursive: bool) -> ConflictStats {
        let mut stats = ConflictStats {
            total_count: 0,
            create_create_count: 0,
            write_write_count: 0,
            write_unlink_count: 0,
            delete_create_count: 0,
            rename_conflict_count: 0,
        };

        for conflict in &self.conflicts {
            let inode = conflict.branches.first().map(|b| b.inode).unwrap_or(0);
            if !self.is_in_dir(inode, dir_ino, recursive) {
                continue;
            }

            stats.total_count += 1;
            match conflict.conflict_type {
                ConflictType::CreateCreate => stats.create_create_count += 1,
                ConflictType::WriteWrite => stats.write_write_count += 1,
                ConflictType::WriteUnlink => stats.write_unlink_count += 1,
                ConflictType::DeleteCreate => stats.delete_create_count += 1,
                ConflictType::RenameConflict => stats.rename_conflict_count += 1,
            }
        }

        stats
    }

    pub fn get_conflict_stats_full_by_dir(
        &self,
        dir_ino: u64,
        recursive: bool,
    ) -> ConflictStatsFull {
        let mut stats = ConflictStatsFull {
            total_count: 0,
            resolved_count: 0,
            unresolved_count: 0,
            create_create_count: 0,
            create_create_resolved: 0,
            write_write_count: 0,
            write_write_resolved: 0,
            write_unlink_count: 0,
            write_unlink_resolved: 0,
            delete_create_count: 0,
            delete_create_resolved: 0,
            rename_conflict_count: 0,
            rename_conflict_resolved: 0,
        };

        for conflict in &self.conflicts {
            let inode = conflict.branches.first().map(|b| b.inode).unwrap_or(0);
            if !self.is_in_dir(inode, dir_ino, recursive) {
                continue;
            }

            stats.total_count += 1;
            if conflict.resolved {
                stats.resolved_count += 1;
            } else {
                stats.unresolved_count += 1;
            }

            match conflict.conflict_type {
                ConflictType::CreateCreate => {
                    stats.create_create_count += 1;
                    if conflict.resolved {
                        stats.create_create_resolved += 1;
                    }
                }
                ConflictType::WriteWrite => {
                    stats.write_write_count += 1;
                    if conflict.resolved {
                        stats.write_write_resolved += 1;
                    }
                }
                ConflictType::WriteUnlink => {
                    stats.write_unlink_count += 1;
                    if conflict.resolved {
                        stats.write_unlink_resolved += 1;
                    }
                }
                ConflictType::DeleteCreate => {
                    stats.delete_create_count += 1;
                    if conflict.resolved {
                        stats.delete_create_resolved += 1;
                    }
                }
                ConflictType::RenameConflict => {
                    stats.rename_conflict_count += 1;
                    if conflict.resolved {
                        stats.rename_conflict_resolved += 1;
                    }
                }
            }
        }

        stats
    }

    fn is_in_dir(&self, inode: u64, dir_ino: u64, recursive: bool) -> bool {
        if inode == dir_ino {
            return true;
        }
        if !recursive {
            return false;
        }
        let entry = self.entries.values().find(|e| e.inode == inode);
        entry.is_some_and(|e| {
            let parent_ino = e.parent_ino;
            parent_ino == dir_ino || self.is_in_dir(parent_ino, dir_ino, true)
        })
    }

    pub fn batch_resolve_by_dir(
        &mut self,
        dir_ino: u64,
        recursive: bool,
        conflict_type: i32,
    ) -> u64 {
        let mut resolved_count = 0;

        let entries_snapshot: Vec<_> = self.entries.values().cloned().collect();

        for conflict in self.conflicts.iter_mut() {
            if conflict.resolved {
                continue;
            }

            let inode = conflict.branches.first().map(|b| b.inode).unwrap_or(0);
            if !Self::is_in_dir_snapshot(inode, dir_ino, recursive, &entries_snapshot) {
                continue;
            }

            let conflict_type_matches = if conflict_type < 0 {
                true
            } else {
                let ct = match conflict_type {
                    0 => ConflictType::CreateCreate,
                    1 => ConflictType::WriteWrite,
                    2 => ConflictType::WriteUnlink,
                    3 => ConflictType::DeleteCreate,
                    4 => ConflictType::RenameConflict,
                    _ => continue,
                };
                conflict.conflict_type == ct
            };

            if conflict_type_matches {
                conflict.resolved = true;
                conflict.resolved_time = Some(now_unix());
                conflict.resolution = Some(ConflictResolution::KeepLast);
                resolved_count += 1;
            }
        }

        resolved_count
    }

    pub fn batch_ignore_by_dir(&mut self, dir_ino: u64, conflict_type: i32) -> u64 {
        let mut ignored_count = 0;

        let entries_snapshot: Vec<_> = self.entries.values().cloned().collect();

        for conflict in self.conflicts.iter_mut() {
            let inode = conflict.branches.first().map(|b| b.inode).unwrap_or(0);
            if !Self::is_in_dir_snapshot(inode, dir_ino, true, &entries_snapshot) {
                continue;
            }

            let conflict_type_matches = if conflict_type < 0 {
                true
            } else {
                let ct = match conflict_type {
                    0 => ConflictType::CreateCreate,
                    1 => ConflictType::WriteWrite,
                    2 => ConflictType::WriteUnlink,
                    3 => ConflictType::DeleteCreate,
                    4 => ConflictType::RenameConflict,
                    _ => continue,
                };
                conflict.conflict_type == ct
            };

            if conflict_type_matches {
                conflict.resolved = true;
                conflict.resolved_time = Some(now_unix());
                conflict.resolution = Some(ConflictResolution::KeepLast);
                ignored_count += 1;
            }
        }

        ignored_count
    }

    fn is_in_dir_snapshot(inode: u64, dir_ino: u64, recursive: bool, entries: &[DirEntry]) -> bool {
        if inode == dir_ino {
            return true;
        }
        if !recursive {
            return false;
        }
        let entry = entries.iter().find(|e| e.inode == inode);
        entry.is_some_and(|e| {
            let parent_ino = e.parent_ino;
            parent_ino == dir_ino || Self::is_in_dir_snapshot(parent_ino, dir_ino, true, entries)
        })
    }

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

    pub fn gc_tombstones(&mut self, _max_age_secs: u64) {}

    pub fn get_deltas_since(&self, since_vclock: &VectorClock) -> Vec<DeltaOp> {
        self.delta_log
            .iter()
            .filter(|delta| !since_vclock.dominates(delta.vclock()))
            .cloned()
            .collect()
    }

    pub fn clear_delta_log(&mut self) {
        self.delta_log.clear();
    }
}

use std::sync::{Arc, RwLock};

pub trait DirCacheProvider: Sync + Send + 'static {
    fn get(&self, dir_ino: u64) -> Option<Arc<RwLock<DirORSet>>>;
    fn insert(&self, dir_ino: u64, orset: Arc<RwLock<DirORSet>>);
    fn remove(&self, dir_ino: u64) -> Option<Arc<RwLock<DirORSet>>>;
    fn ensure_dir_cache(&self, dir_ino: u64) -> Arc<RwLock<DirORSet>>;
    #[allow(clippy::result_unit_err)]
    fn try_read(&self, dir_ino: u64) -> Result<Option<Arc<RwLock<DirORSet>>>, ()>;
    fn shard_index(&self, dir_ino: u64) -> usize;
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
        vc1.increment(1);

        let mut vc2 = VectorClock::new();
        vc2.increment(1);
        vc2.increment(2);

        assert_eq!(vc1.compare(&vc2), CausalOrder::Before);
        assert_eq!(vc2.compare(&vc1), CausalOrder::After);
        assert!(!vc1.is_concurrent(&vc2));
    }

    #[test]
    fn test_vector_clock_concurrent() {
        let mut vc1 = VectorClock::new();
        vc1.increment(1);

        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        assert_eq!(vc1.compare(&vc2), CausalOrder::Concurrent);
        assert!(vc1.is_concurrent(&vc2));
    }

    #[test]
    fn test_vector_clock_merge() {
        let mut vc1 = VectorClock::new();
        vc1.increment(1);

        let mut vc2 = VectorClock::new();
        vc2.increment(2);

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
        vc.observe(1, 3);
        assert_eq!(vc.get(1), 5);
    }

    #[test]
    fn test_entry_id_uniqueness() {
        let id1 = EntryId::new("file.txt", 1, 1);
        let id2 = EntryId::new("file.txt", 2, 1);
        let id3 = EntryId::new("file.txt", 1, 2);
        let id4 = EntryId::new("file.txt", 1, 1);

        assert_ne!(id1, id2);
        assert_ne!(id1, id3);
        assert_eq!(id1, id4);
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

        let id1 = EntryId::new("file.txt", 1, 1);
        let id2 = EntryId::new("file.txt", 2, 1);
        let entry1 = DirEntry::new_file(id1.clone(), 100, 1, 0o644);
        let entry2 = DirEntry::new_file(id2.clone(), 200, 1, 0o644);

        orset.add(entry1);
        orset.add(entry2);

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

    #[test]
    fn test_conflict_detection_create_create() {
        let mut orset = DirORSet::new(1);

        let entry1 = DirEntry::new_file(EntryId::new("file.txt", 1, 1), 100, 1, 0o644);
        let entry2 = DirEntry::new_file(EntryId::new("file.txt", 2, 1), 200, 1, 0o644);

        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::Add {
            entry: entry1,
            vclock: vc1,
        });
        orset.apply_delta(&DeltaOp::Add {
            entry: entry2,
            vclock: vc2,
        });

        assert_eq!(orset.conflict_count(), 1);
        assert!(orset.has_unresolved_conflicts());
        let conflicts = orset.unresolved_conflicts();
        assert_eq!(conflicts[0].conflict_type, ConflictType::CreateCreate);
        assert_eq!(conflicts[0].branches.len(), 2);
    }

    #[test]
    fn test_conflict_detection_write_write() {
        let mut orset = DirORSet::new(1);

        let entry = DirEntry::new_file(EntryId::new("file.txt", 1, 1), 100, 1, 0o644);
        orset.add(entry);

        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::SetAttr {
            inode: 100,
            mode: None,
            size: Some(100),
            mtime: Some(1000),
            vclock: vc1,
        });
        orset.apply_delta(&DeltaOp::SetAttr {
            inode: 100,
            mode: None,
            size: Some(200),
            mtime: Some(2000),
            vclock: vc2,
        });

        assert_eq!(orset.conflict_count(), 1);
        let conflicts = orset.unresolved_conflicts();
        assert_eq!(conflicts[0].conflict_type, ConflictType::WriteWrite);
    }

    #[test]
    fn test_conflict_detection_write_unlink() {
        let mut orset = DirORSet::new(1);

        let id = EntryId::new("file.txt", 1, 1);
        let entry = DirEntry::new_file(id.clone(), 100, 1, 0o644);
        orset.add(entry);

        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::SetAttr {
            inode: 100,
            mode: None,
            size: Some(100),
            mtime: Some(1000),
            vclock: vc1,
        });
        orset.apply_delta(&DeltaOp::Remove { id, vclock: vc2 });

        assert_eq!(orset.conflict_count(), 1);
        let conflicts = orset.unresolved_conflicts();
        assert_eq!(conflicts[0].conflict_type, ConflictType::WriteUnlink);
    }

    #[test]
    fn test_conflict_detection_delete_create() {
        let mut orset = DirORSet::new(1);

        let id1 = EntryId::new("file.txt", 1, 1);
        let entry1 = DirEntry::new_file(id1.clone(), 100, 1, 0o644);

        let mut vc1 = VectorClock::new();
        vc1.increment(1);

        orset.apply_delta(&DeltaOp::Add {
            entry: entry1,
            vclock: vc1.clone(),
        });

        orset.apply_delta(&DeltaOp::Remove {
            id: id1,
            vclock: vc1,
        });

        let entry2 = DirEntry::new_file(EntryId::new("file.txt", 2, 1), 200, 1, 0o644);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::Add {
            entry: entry2,
            vclock: vc2,
        });

        assert_eq!(orset.conflict_count(), 1);
        let conflicts = orset.unresolved_conflicts();
        assert_eq!(conflicts[0].conflict_type, ConflictType::DeleteCreate);
    }

    #[test]
    fn test_conflict_detection_rename_conflict() {
        let mut orset = DirORSet::new(1);

        let entry1 = DirEntry::new_file(EntryId::new("a.txt", 1, 1), 100, 1, 0o644);
        orset.apply_delta(&DeltaOp::Add {
            entry: entry1,
            vclock: VectorClock::new(),
        });

        let entry2 = DirEntry::new_file(EntryId::new("target.txt", 2, 1), 200, 1, 0o644);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);
        orset.apply_delta(&DeltaOp::Add {
            entry: entry2,
            vclock: vc2,
        });

        let mut vc1 = VectorClock::new();
        vc1.increment(1);

        let new_entry = DirEntry::new_file(EntryId::new("target.txt", 1, 2), 100, 1, 0o644);
        orset.apply_delta(&DeltaOp::Rename {
            old_id: EntryId::new("a.txt", 1, 1),
            new_entry,
            vclock: vc1,
        });

        assert_eq!(orset.conflict_count(), 1);
        let conflicts = orset.unresolved_conflicts();
        assert_eq!(conflicts[0].conflict_type, ConflictType::RenameConflict);
    }

    #[test]
    fn test_conflict_resolution() {
        let mut orset = DirORSet::new(1);

        let entry1 = DirEntry::new_file(EntryId::new("file.txt", 1, 1), 100, 1, 0o644);
        let entry2 = DirEntry::new_file(EntryId::new("file.txt", 2, 1), 200, 1, 0o644);

        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::Add {
            entry: entry1,
            vclock: vc1,
        });
        orset.apply_delta(&DeltaOp::Add {
            entry: entry2,
            vclock: vc2,
        });

        assert_eq!(orset.conflict_count(), 1);

        let conflict_id = orset.conflicts[0].id.clone();
        orset.resolve_conflict(&conflict_id, ConflictResolution::KeepFirst);

        assert_eq!(orset.conflict_count(), 0);
        assert!(!orset.has_unresolved_conflicts());
        let conflict = orset
            .conflicts
            .iter()
            .find(|c| c.id == conflict_id)
            .unwrap();
        assert!(conflict.resolved);
        assert_eq!(conflict.resolution, Some(ConflictResolution::KeepFirst));
    }

    #[test]
    fn test_content_hash_auto_merge_same_content() {
        let mut orset = DirORSet::new(1);
        orset.policy = MergePolicy::Aggressive;

        let mut entry1 = DirEntry::new_file(EntryId::new("file.txt", 1, 1), 100, 1, 0o644);
        entry1.chunks = vec![CachedFileChunk {
            offset: 0,
            size: 10,
            mtime: 1000,
            fid: "fid1".to_string(),
            cookie: 0,
            crc32: 12345,
        }];

        let mut entry2 = DirEntry::new_file(EntryId::new("file.txt", 2, 1), 200, 1, 0o644);
        entry2.chunks = vec![CachedFileChunk {
            offset: 0,
            size: 10,
            mtime: 2000,
            fid: "fid2".to_string(),
            cookie: 0,
            crc32: 12345,
        }];

        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::Add {
            entry: entry1,
            vclock: vc1,
        });
        orset.apply_delta(&DeltaOp::Add {
            entry: entry2,
            vclock: vc2,
        });

        assert_eq!(orset.conflict_count(), 0);
        assert!(!orset.has_unresolved_conflicts());
        assert!(orset.tombstones.contains(&EntryId::new("file.txt", 2, 1)));
    }

    #[test]
    fn test_content_hash_no_merge_different_content() {
        let mut orset = DirORSet::new(1);
        orset.policy = MergePolicy::Aggressive;

        let mut entry1 = DirEntry::new_file(EntryId::new("file.txt", 1, 1), 100, 1, 0o644);
        entry1.chunks = vec![CachedFileChunk {
            offset: 0,
            size: 10,
            mtime: 1000,
            fid: "fid1".to_string(),
            cookie: 0,
            crc32: 12345,
        }];

        let mut entry2 = DirEntry::new_file(EntryId::new("file.txt", 2, 1), 200, 1, 0o644);
        entry2.chunks = vec![CachedFileChunk {
            offset: 0,
            size: 10,
            mtime: 2000,
            fid: "fid2".to_string(),
            cookie: 0,
            crc32: 67890,
        }];

        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::Add {
            entry: entry1,
            vclock: vc1,
        });
        orset.apply_delta(&DeltaOp::Add {
            entry: entry2,
            vclock: vc2,
        });

        assert_eq!(orset.conflict_count(), 1);
        assert!(orset.has_unresolved_conflicts());
    }

    #[test]
    fn test_conservative_policy_auto_merge() {
        let mut orset = DirORSet::new(1);
        orset.policy = MergePolicy::Conservative;

        let mut entry1 = DirEntry::new_file(EntryId::new("file.txt", 1, 1), 100, 1, 0o644);
        entry1.chunks = vec![CachedFileChunk {
            offset: 0,
            size: 10,
            mtime: 1000,
            fid: "fid1".to_string(),
            cookie: 0,
            crc32: 12345,
        }];

        let mut entry2 = DirEntry::new_file(EntryId::new("file.txt", 2, 1), 200, 1, 0o644);
        entry2.chunks = vec![CachedFileChunk {
            offset: 0,
            size: 10,
            mtime: 2000,
            fid: "fid2".to_string(),
            cookie: 0,
            crc32: 12345,
        }];

        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::Add {
            entry: entry1,
            vclock: vc1,
        });
        orset.apply_delta(&DeltaOp::Add {
            entry: entry2,
            vclock: vc2,
        });

        assert_eq!(orset.conflict_count(), 0);
        assert!(orset.tombstones.contains(&EntryId::new("file.txt", 2, 1)));
    }

    #[test]
    fn test_aggressive_policy_auto_merge() {
        let mut orset = DirORSet::new(1);
        orset.policy = MergePolicy::Aggressive;

        let mut entry1 = DirEntry::new_file(EntryId::new("file.txt", 1, 1), 100, 1, 0o644);
        entry1.chunks = vec![CachedFileChunk {
            offset: 0,
            size: 10,
            mtime: 1000,
            fid: "fid1".to_string(),
            cookie: 0,
            crc32: 12345,
        }];

        let mut entry2 = DirEntry::new_file(EntryId::new("file.txt", 2, 1), 200, 1, 0o644);
        entry2.chunks = vec![CachedFileChunk {
            offset: 0,
            size: 10,
            mtime: 2000,
            fid: "fid2".to_string(),
            cookie: 0,
            crc32: 12345,
        }];

        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::Add {
            entry: entry1,
            vclock: vc1,
        });
        orset.apply_delta(&DeltaOp::Add {
            entry: entry2,
            vclock: vc2,
        });

        assert_eq!(orset.conflict_count(), 0);
        assert!(orset.tombstones.contains(&EntryId::new("file.txt", 2, 1)));
    }

    #[test]
    fn test_manual_policy_no_auto_merge() {
        let mut orset = DirORSet::new(1);
        orset.policy = MergePolicy::Manual;

        let mut entry1 = DirEntry::new_file(EntryId::new("file.txt", 1, 1), 100, 1, 0o644);
        entry1.chunks = vec![CachedFileChunk {
            offset: 0,
            size: 10,
            mtime: 1000,
            fid: "fid1".to_string(),
            cookie: 0,
            crc32: 12345,
        }];

        let mut entry2 = DirEntry::new_file(EntryId::new("file.txt", 2, 1), 200, 1, 0o644);
        entry2.chunks = vec![CachedFileChunk {
            offset: 0,
            size: 10,
            mtime: 2000,
            fid: "fid2".to_string(),
            cookie: 0,
            crc32: 12345,
        }];

        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::Add {
            entry: entry1,
            vclock: vc1,
        });
        orset.apply_delta(&DeltaOp::Add {
            entry: entry2,
            vclock: vc2,
        });

        assert_eq!(orset.conflict_count(), 1);
        assert!(orset.has_unresolved_conflicts());
        assert!(!orset.tombstones.contains(&EntryId::new("file.txt", 2, 1)));
    }

    #[test]
    fn test_lww_policy_selects_latest() {
        let mut orset = DirORSet::new(1);
        orset.policy = MergePolicy::LwwTime;

        let mut entry1 = DirEntry::new_file(EntryId::new("file.txt", 1, 1), 100, 1, 0o644);
        entry1.mtime = 1000;

        let mut entry2 = DirEntry::new_file(EntryId::new("file.txt", 2, 1), 200, 1, 0o644);
        entry2.mtime = 2000;

        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::Add {
            entry: entry1,
            vclock: vc1,
        });
        orset.apply_delta(&DeltaOp::Add {
            entry: entry2,
            vclock: vc2,
        });

        let entries = orset.get_by_name("file.txt");
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_write_priority_policy() {
        let mut orset = DirORSet::new(1);
        orset.policy = MergePolicy::WritePriority;

        let id = EntryId::new("file.txt", 1, 1);
        let entry = DirEntry::new_file(id.clone(), 100, 1, 0o644);
        orset.add(entry);

        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::SetAttr {
            inode: 100,
            mode: None,
            size: Some(100),
            mtime: Some(1000),
            vclock: vc1,
        });
        orset.apply_delta(&DeltaOp::Remove { id, vclock: vc2 });

        assert_eq!(orset.conflict_count(), 1);
        let conflicts = orset.unresolved_conflicts();
        assert_eq!(conflicts[0].conflict_type, ConflictType::WriteUnlink);
    }

    #[test]
    fn test_delete_priority_policy() {
        let mut orset = DirORSet::new(1);
        orset.policy = MergePolicy::DeletePriority;

        let id = EntryId::new("file.txt", 1, 1);
        let entry = DirEntry::new_file(id.clone(), 100, 1, 0o644);
        orset.add(entry);

        let mut vc1 = VectorClock::new();
        vc1.increment(1);
        let mut vc2 = VectorClock::new();
        vc2.increment(2);

        orset.apply_delta(&DeltaOp::SetAttr {
            inode: 100,
            mode: None,
            size: Some(100),
            mtime: Some(1000),
            vclock: vc1,
        });
        orset.apply_delta(&DeltaOp::Remove { id, vclock: vc2 });

        assert_eq!(orset.conflict_count(), 1);
        let conflicts = orset.unresolved_conflicts();
        assert_eq!(conflicts[0].conflict_type, ConflictType::WriteUnlink);
    }
}
