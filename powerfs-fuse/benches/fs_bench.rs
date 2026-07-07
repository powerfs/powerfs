use powerfs_fuse::cache::{CachedEntry, MetadataCache, UpdateAttrParams, ROOT_INODE};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn make_file_entry(inode: u64, parent: u64, name: &str) -> CachedEntry {
    CachedEntry {
        inode,
        parent,
        name: name.to_string(),
        is_dir: false,
        is_symlink: false,
        symlink_target: None,
        nlink: 1,
        fid: None,
        size: 0,
        mode: 0o100644,
        uid: 1000,
        gid: 1000,
        atime: 1234567890,
        mtime: 1234567890,
        ctime: 1234567890,
        xattrs: std::collections::HashMap::new(),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        content_size: 0,
        disk_size: 0,
    }
}

fn make_dir_entry(inode: u64, parent: u64, name: &str) -> CachedEntry {
    CachedEntry {
        inode,
        parent,
        name: name.to_string(),
        is_dir: true,
        is_symlink: false,
        symlink_target: None,
        nlink: 2,
        fid: None,
        size: 0,
        mode: 0o040755,
        uid: 1000,
        gid: 1000,
        atime: 1234567890,
        mtime: 1234567890,
        ctime: 1234567890,
        xattrs: std::collections::HashMap::new(),
        chunks: Vec::new(),
        hard_link_id: String::new(),
        hard_link_counter: 0,
        content_size: 0,
        disk_size: 0,
    }
}

struct BenchResult {
    name: String,
    ops: u64,
    duration: Duration,
}

impl BenchResult {
    fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / self.duration.as_secs_f64()
    }
}

fn run_bench<F>(name: &str, ops: u64, mut f: F) -> BenchResult
where
    F: FnMut(u64),
{
    let start = Instant::now();
    for i in 0..ops {
        f(i);
    }
    let duration = start.elapsed();
    BenchResult {
        name: name.to_string(),
        ops,
        duration,
    }
}

fn print_result(result: &BenchResult) {
    println!(
        "{:<40} {:>10} ops  {:>12.3}s  {:>10.0} ops/s",
        result.name,
        result.ops,
        result.duration.as_secs_f64(),
        result.ops_per_sec()
    );
}

fn main() {
    println!("=== PowerFS FUSE 缓存性能基准测试 ===");
    println!();
    println!(
        "{:<40} {:>10}      {:>12}  {:>10}",
        "测试项", "操作数", "耗时", "吞吐量"
    );
    println!("{}", "-".repeat(80));

    // 1. 插入文件条目
    let cache = MetadataCache::new();
    let result = run_bench("插入文件条目", 10000, |i| {
        let entry = make_file_entry(1000 + i, ROOT_INODE, &format!("file_{}.txt", i));
        cache.insert(entry);
    });
    print_result(&result);

    // 2. 查找文件条目 (by name)
    let result = run_bench("按名称查找条目", 10000, |i| {
        let _ = cache.lookup_in_cache(ROOT_INODE, &format!("file_{}.txt", i));
    });
    print_result(&result);

    // 3. 查找文件条目 (by inode)
    let result = run_bench("按inode查找条目", 10000, |i| {
        let _ = cache.get_inode(1000 + i);
    });
    print_result(&result);

    // 4. 更新文件大小
    let result = run_bench("更新文件大小", 10000, |i| {
        cache.update_size(1000 + i, i * 1024);
    });
    print_result(&result);

    // 5. 更新属性
    let result = run_bench("更新文件属性", 10000, |i| {
        cache.update_attr(
            1000 + i,
            UpdateAttrParams {
                mode: Some(0o100755),
                size: None,
                uid: Some(1001),
                gid: Some(1001),
                atime: Some(9876543210),
                mtime: Some(9876543210),
            },
        );
    });
    print_result(&result);

    // 6. 删除文件条目
    let result = run_bench("删除文件条目", 10000, |i| {
        cache.remove(1000 + i);
    });
    print_result(&result);

    // 7. 目录创建
    let cache = MetadataCache::new();
    let result = run_bench("创建目录条目", 5000, |i| {
        let entry = make_dir_entry(2000 + i, ROOT_INODE, &format!("dir_{}", i));
        cache.insert(entry);
    });
    print_result(&result);

    // 8. 列出目录
    let result = run_bench("列出目录条目", 1000, |_| {
        let _ = cache.list_children(ROOT_INODE);
    });
    print_result(&result);

    // 9. 并发插入
    let cache = Arc::new(MetadataCache::new());
    let thread_count = 4;
    let ops_per_thread = 2500;
    let start = Instant::now();
    let mut handles = Vec::new();
    for t in 0..thread_count {
        let cache_clone = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let inode = 10000 + t * ops_per_thread + i;
                let entry = make_file_entry(inode, ROOT_INODE, &format!("t{}_f{}.txt", t, i));
                cache_clone.insert(entry);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let duration = start.elapsed();
    let result = BenchResult {
        name: format!("并发插入 ({}线程)", thread_count),
        ops: thread_count * ops_per_thread,
        duration,
    };
    print_result(&result);

    // 10. 并发读取
    let cache_clone = Arc::clone(&cache);
    let thread_count = 4;
    let ops_per_thread = 5000;
    let start = Instant::now();
    let mut handles = Vec::new();
    for t in 0..thread_count {
        let cache_clone = Arc::clone(&cache_clone);
        handles.push(thread::spawn(move || {
            for i in 0..ops_per_thread {
                let inode = 10000 + (t * ops_per_thread + i) % (thread_count * 2500);
                let _ = cache_clone.get_inode(inode);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let duration = start.elapsed();
    let result = BenchResult {
        name: format!("并发读取 ({}线程)", thread_count),
        ops: thread_count * ops_per_thread,
        duration,
    };
    print_result(&result);

    println!();
    println!("测试完成！");
}
