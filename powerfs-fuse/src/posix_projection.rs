//! POSIX 投影层
//!
//! 将 OR-Set（可能包含同名多份条目）投影为 POSIX 兼容的目录视图：
//! - 每个文件名只返回一个"主版本"（readdir/lookup 用）
//! - 冲突副本通过 `.conflicts/` 虚拟目录访问（Phase 1B 实现）
//!
//! Phase 1B：支持冲突检测、主版本选择、.conflicts/ 虚拟目录。

use std::collections::HashSet;

use crate::orset::{DirEntry, DirORSet, FileType, MergePolicy};

/// 投影后的可见条目（readdir 返回用）
#[derive(Clone, Debug)]
pub struct VisibleEntry {
    pub name: String,
    pub inode: u64,
    pub file_type: FileType,
    pub has_conflict: bool,
}

/// 冲突目录中的条目展示
#[derive(Clone, Debug)]
pub struct ConflictEntry {
    pub display_name: String,
    pub original_name: String,
    pub inode: u64,
    pub client_id: u64,
    pub seq: u64,
    pub file_type: FileType,
    pub mtime: u64,
}

/// `.conflicts/` 虚拟目录的 inode 分配器
///
/// 为每个真实目录分配一个虚拟的 .conflicts/ 目录 inode。
/// 使用特殊范围（0xFFFF000000000000 + dir_ino）避免与真实 inode 冲突。
pub struct ConflictDirInodeMapper {
    base: u64,
}

impl ConflictDirInodeMapper {
    pub fn new() -> Self {
        Self {
            base: 0xFFFF000000000000,
        }
    }

    /// 从真实目录 inode 获取对应的 .conflicts/ 虚拟目录 inode
    pub fn get_conflict_dir_inode(&self, dir_ino: u64) -> u64 {
        self.base | (dir_ino & 0xFFFFFFFF)
    }

    /// 判断一个 inode 是否为 .conflicts/ 虚拟目录
    pub fn is_conflict_dir_inode(&self, ino: u64) -> bool {
        (ino & 0xFFFF000000000000) == self.base
    }

    /// 从 .conflicts/ inode 还原出真实目录 inode
    pub fn get_real_dir_inode(&self, conflict_dir_ino: u64) -> u64 {
        conflict_dir_ino & 0xFFFFFFFF
    }
}

impl Default for ConflictDirInodeMapper {
    fn default() -> Self {
        Self::new()
    }
}

/// POSIX 投影器
pub struct PosixProjection {
    default_policy: MergePolicy,
    conflict_dir_mapper: ConflictDirInodeMapper,
}

impl Default for PosixProjection {
    fn default() -> Self {
        Self::new()
    }
}

impl PosixProjection {
    pub fn new() -> Self {
        Self {
            default_policy: MergePolicy::default(),
            conflict_dir_mapper: ConflictDirInodeMapper::new(),
        }
    }

    pub fn with_policy(policy: MergePolicy) -> Self {
        Self {
            default_policy: policy,
            conflict_dir_mapper: ConflictDirInodeMapper::new(),
        }
    }

    /// 获取 .conflicts/ 目录的 inode
    pub fn get_conflict_dir_inode(&self, dir_ino: u64) -> u64 {
        self.conflict_dir_mapper.get_conflict_dir_inode(dir_ino)
    }

    /// 判断一个 inode 是否为 .conflicts/ 虚拟目录
    pub fn is_conflict_dir_inode(&self, ino: u64) -> bool {
        self.conflict_dir_mapper.is_conflict_dir_inode(ino)
    }

    /// 从 .conflicts/ inode 获取真实目录 inode
    pub fn get_real_dir_inode(&self, conflict_dir_ino: u64) -> u64 {
        self.conflict_dir_mapper
            .get_real_dir_inode(conflict_dir_ino)
    }

    /// 判断目录是否应该显示 .conflicts/ 条目
    pub fn should_show_conflict_dir(&self, orset: &DirORSet) -> bool {
        self.has_conflicts(orset)
    }

    /// 投影目录列表（readdir 用）
    ///
    /// 每个 name 只返回一个主版本。同名多份时按策略选主，has_conflict=true。
    pub fn project_listing(&self, orset: &DirORSet) -> Vec<VisibleEntry> {
        let mut visible: Vec<VisibleEntry> = Vec::new();
        let mut seen_names: HashSet<String> = HashSet::new();

        // 先按 name 分组
        let mut groups: std::collections::HashMap<String, Vec<&DirEntry>> =
            std::collections::HashMap::new();
        for entry in orset.entries.values() {
            groups.entry(entry.id.name.clone()).or_default().push(entry);
        }

        for (name, entries) in groups {
            if seen_names.contains(&name) {
                continue;
            }
            seen_names.insert(name.clone());

            let has_conflict = entries.len() > 1;
            let primary = self.select_primary(&entries);

            visible.push(VisibleEntry {
                name,
                inode: primary.inode,
                file_type: primary.file_type,
                has_conflict,
            });
        }

        // 按名称排序，保证 readdir 输出稳定
        visible.sort_by(|a, b| a.name.cmp(&b.name));
        visible
    }

    /// 投影单文件查找（lookup 用）
    pub fn project_lookup(&self, orset: &DirORSet, name: &str) -> Option<DirEntry> {
        let candidates = orset.get_by_name(name);
        if candidates.is_empty() {
            return None;
        }
        if candidates.len() == 1 {
            return Some(candidates[0].clone());
        }
        // 多版本：选主
        Some(self.select_primary(&candidates).clone())
    }

    /// 获取同名所有冲突条目（.conflicts/ 目录用）
    pub fn get_conflict_entries(&self, orset: &DirORSet, name: &str) -> Vec<DirEntry> {
        orset.get_by_name(name).into_iter().cloned().collect()
    }

    /// 列出 .conflicts/ 目录中的所有冲突副本
    ///
    /// 仅当某 name 有多份时才列出。展示名格式：{name}.{client_id}.{seq}
    pub fn list_conflict_dir(&self, orset: &DirORSet) -> Vec<ConflictEntry> {
        let mut result = Vec::new();

        let mut groups: std::collections::HashMap<String, Vec<&DirEntry>> =
            std::collections::HashMap::new();
        for entry in orset.entries.values() {
            groups.entry(entry.id.name.clone()).or_default().push(entry);
        }

        for (name, entries) in groups {
            if entries.len() <= 1 {
                continue; // 无冲突
            }
            for entry in entries {
                result.push(ConflictEntry {
                    display_name: format!(
                        "{}.{}.{}",
                        entry.id.name, entry.id.client_id, entry.id.seq
                    ),
                    original_name: name.clone(),
                    inode: entry.inode,
                    client_id: entry.id.client_id,
                    seq: entry.id.seq,
                    file_type: entry.file_type,
                    mtime: entry.mtime,
                });
            }
        }

        result.sort_by(|a, b| a.display_name.cmp(&b.display_name));
        result
    }

    /// 判断目录是否有冲突条目
    pub fn has_conflicts(&self, orset: &DirORSet) -> bool {
        let mut groups: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for entry in orset.entries.values() {
            *groups.entry(entry.id.name.clone()).or_default() += 1;
        }
        groups.values().any(|&count| count > 1)
    }

    /// 主版本选择
    fn select_primary<'a>(&self, entries: &'a [&DirEntry]) -> &'a DirEntry {
        match self.default_policy {
            MergePolicy::LwwTime => entries
                .iter()
                .max_by(|a, b| {
                    a.mtime
                        .cmp(&b.mtime)
                        .then_with(|| a.id.client_id.cmp(&b.id.client_id))
                        .then_with(|| a.id.seq.cmp(&b.id.seq))
                })
                .copied()
                .expect("select_primary called with empty entries"),
            MergePolicy::KeepAll => entries
                .iter()
                .min_by_key(|e| e.inode)
                .copied()
                .expect("select_primary called with empty entries"),
            MergePolicy::ContentHash => entries
                .iter()
                .min_by_key(|e| e.inode)
                .copied()
                .expect("select_primary called with empty entries"),
            MergePolicy::WeightBased => entries
                .iter()
                .min_by_key(|e| e.inode)
                .copied()
                .expect("select_primary called with empty entries"),
            MergePolicy::WritePriority => entries
                .iter()
                .max_by(|a, b| {
                    a.mtime
                        .cmp(&b.mtime)
                        .then_with(|| a.id.client_id.cmp(&b.id.client_id))
                        .then_with(|| a.id.seq.cmp(&b.id.seq))
                })
                .copied()
                .expect("select_primary called with empty entries"),
            MergePolicy::DeletePriority => entries
                .iter()
                .min_by_key(|e| e.inode)
                .copied()
                .expect("select_primary called with empty entries"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orset::{DirEntry, DirORSet, EntryId, FileType};

    fn make_entry(name: &str, client_id: u64, seq: u64, inode: u64, mtime: u64) -> DirEntry {
        let mut entry = DirEntry::new_file(
            EntryId::new(name, client_id, seq),
            inode,
            1,
            0o644 | libc::S_IFREG,
        );
        entry.mtime = mtime;
        entry
    }

    #[test]
    fn test_project_listing_single_version() {
        let mut orset = DirORSet::new(1);
        orset.add(make_entry("a.txt", 1, 1, 100, 1000));
        orset.add(make_entry("b.txt", 1, 2, 101, 2000));

        let proj = PosixProjection::new();
        let listing = proj.project_listing(&orset);

        assert_eq!(listing.len(), 2);
        assert_eq!(listing[0].name, "a.txt");
        assert_eq!(listing[0].inode, 100);
        assert!(!listing[0].has_conflict);
        assert_eq!(listing[1].name, "b.txt");
        assert_eq!(listing[1].inode, 101);
    }

    #[test]
    fn test_project_listing_multiple_versions() {
        let mut orset = DirORSet::new(1);
        // 两个客户端并发创建同名文件
        orset.add(make_entry("file.txt", 1, 1, 100, 1000)); // client 1
        orset.add(make_entry("file.txt", 2, 1, 200, 2000)); // client 2, 更新

        let proj = PosixProjection::new();
        let listing = proj.project_listing(&orset);

        assert_eq!(listing.len(), 1); // 只显示一个主版本
        assert_eq!(listing[0].name, "file.txt");
        assert!(listing[0].has_conflict);
        // LwwTime 策略：mtime=2000 的版本为主
        assert_eq!(listing[0].inode, 200);
    }

    #[test]
    fn test_project_lookup_single() {
        let mut orset = DirORSet::new(1);
        orset.add(make_entry("file.txt", 1, 1, 100, 1000));

        let proj = PosixProjection::new();
        let entry = proj.project_lookup(&orset, "file.txt").unwrap();
        assert_eq!(entry.inode, 100);
    }

    #[test]
    fn test_project_lookup_not_found() {
        let orset = DirORSet::new(1);
        let proj = PosixProjection::new();
        assert!(proj.project_lookup(&orset, "nonexistent").is_none());
    }

    #[test]
    fn test_project_lookup_conflict_selects_lww() {
        let mut orset = DirORSet::new(1);
        orset.add(make_entry("file.txt", 1, 1, 100, 1000));
        orset.add(make_entry("file.txt", 2, 1, 200, 2000));

        let proj = PosixProjection::new();
        let entry = proj.project_lookup(&orset, "file.txt").unwrap();
        assert_eq!(entry.inode, 200); // mtime=2000 的版本
    }

    #[test]
    fn test_list_conflict_dir_no_conflicts() {
        let mut orset = DirORSet::new(1);
        orset.add(make_entry("a.txt", 1, 1, 100, 1000));
        orset.add(make_entry("b.txt", 1, 2, 101, 2000));

        let proj = PosixProjection::new();
        let conflicts = proj.list_conflict_dir(&orset);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn test_list_conflict_dir_with_conflicts() {
        let mut orset = DirORSet::new(1);
        orset.add(make_entry("file.txt", 1, 1, 100, 1000));
        orset.add(make_entry("file.txt", 2, 1, 200, 2000));

        let proj = PosixProjection::new();
        let conflicts = proj.list_conflict_dir(&orset);
        assert_eq!(conflicts.len(), 2);

        // 按展示名排序
        assert_eq!(conflicts[0].display_name, "file.txt.1.1");
        assert_eq!(conflicts[0].inode, 100);
        assert_eq!(conflicts[1].display_name, "file.txt.2.1");
        assert_eq!(conflicts[1].inode, 200);
    }

    #[test]
    fn test_has_conflicts() {
        let mut orset = DirORSet::new(1);
        orset.add(make_entry("a.txt", 1, 1, 100, 1000));

        let proj = PosixProjection::new();
        assert!(!proj.has_conflicts(&orset));

        orset.add(make_entry("a.txt", 2, 1, 200, 2000));
        assert!(proj.has_conflicts(&orset));
    }

    #[test]
    fn test_select_primary_keep_all_policy() {
        let mut orset = DirORSet::new(1);
        orset.add(make_entry("file.txt", 1, 1, 200, 2000));
        orset.add(make_entry("file.txt", 2, 1, 100, 1000)); // inode 更小但 mtime 更旧

        let proj = PosixProjection::with_policy(MergePolicy::KeepAll);
        let listing = proj.project_listing(&orset);
        assert_eq!(listing.len(), 1);
        // KeepAll 策略：取 inode 最小的作为主版本
        assert_eq!(listing[0].inode, 100);
    }

    #[test]
    fn test_project_listing_sorted() {
        let mut orset = DirORSet::new(1);
        orset.add(make_entry("zeta.txt", 1, 1, 100, 1000));
        orset.add(make_entry("alpha.txt", 1, 2, 101, 2000));
        orset.add(make_entry("mid.txt", 1, 3, 102, 3000));

        let proj = PosixProjection::new();
        let listing = proj.project_listing(&orset);

        assert_eq!(listing[0].name, "alpha.txt");
        assert_eq!(listing[1].name, "mid.txt");
        assert_eq!(listing[2].name, "zeta.txt");
    }

    #[test]
    fn test_project_listing_empty() {
        let orset = DirORSet::new(1);
        let proj = PosixProjection::new();
        let listing = proj.project_listing(&orset);
        assert!(listing.is_empty());
    }

    #[test]
    fn test_project_listing_with_directory() {
        let mut orset = DirORSet::new(1);
        orset.add(make_entry("file.txt", 1, 1, 100, 1000));
        let mut dir_entry =
            DirEntry::new_dir(EntryId::new("subdir", 1, 2), 101, 1, 0o755 | libc::S_IFDIR);
        dir_entry.mtime = 2000;
        orset.add(dir_entry);

        let proj = PosixProjection::new();
        let listing = proj.project_listing(&orset);

        assert_eq!(listing.len(), 2);
        let dir_visible = listing.iter().find(|e| e.name == "subdir").unwrap();
        assert_eq!(dir_visible.file_type, FileType::Directory);
    }
}
