#!/bin/bash
set -e

echo "[*] Running local MetadataManager performance test..."

cd /home/portion/powerfs

# 构建release版本
echo "[*] Building release version..."
cargo build --release -p powerfs-fuse > /dev/null 2>&1

# 创建简单的测试程序
cat > /tmp/perf_test.rs << 'EOF'
use powerfs_fuse::metadata_manager::MetadataManager;
use std::time::{Instant, Duration};

fn main() {
    let mgr = MetadataManager::new_local(1);
    
    // Test 1: Single-threaded mkdir/lookup/rmdir
    println!("=== Test 1: Single-threaded mkdir/lookup/rmdir ===");
    let start = Instant::now();
    for i in 0..10000 {
        let _ = mgr.mkdir(1, &format!("dir{}", i), 0o755);
        let _ = mgr.lookup(1, &format!("dir{}", i));
        let _ = mgr.rmdir(1, &format!("dir{}", i));
    }
    let elapsed = start.elapsed();
    println!("Time: {:?}, Throughput: {:.2} ops/s", elapsed, 30000.0 / elapsed.as_secs_f64());
    
    // Test 2: Single-threaded create/unlink
    println!("\n=== Test 2: Single-threaded create/unlink ===");
    let start = Instant::now();
    for i in 0..10000 {
        let _ = mgr.create(1, &format!("file{}", i), 0o644);
        let _ = mgr.lookup(1, &format!("file{}", i));
        let _ = mgr.unlink(1, &format!("file{}", i));
    }
    let elapsed = start.elapsed();
    println!("Time: {:?}, Throughput: {:.2} ops/s", elapsed, 30000.0 / elapsed.as_secs_f64());
    
    // Test 3: List dir with many entries
    println!("\n=== Test 3: List dir with 10000 entries ===");
    for i in 0..10000 {
        let _ = mgr.create(1, &format!("file{}", i), 0o644);
    }
    let start = Instant::now();
    let _ = mgr.list_dir(1);
    let elapsed = start.elapsed();
    println!("Time to list 10000 entries: {:?}", elapsed);
    
    // Cleanup
    for i in 0..10000 {
        let _ = mgr.unlink(1, &format!("file{}", i));
    }
    
    println!("\n[OK] All tests completed!");
}
EOF

# 编译并运行测试程序
echo "[*] Compiling test program..."
rustc --edition=2021 /tmp/perf_test.rs -o /tmp/perf_test \
    --extern powerfs_fuse=/home/portion/powerfs/target/release/deps/libpowerfs_fuse-*.rlib \
    --extern powerfs_orset=/home/portion/powerfs/target/release/deps/libpowerfs_orset-*.rlib \
    --extern powerfs_master=/home/portion/powerfs/target/release/deps/libpowerfs_master-*.rlib \
    --extern powerfs_core=/home/portion/powerfs/target/release/deps/libpowerfs_core-*.rlib \
    --extern fuser=/home/portion/powerfs/target/release/deps/libfuser-*.rlib \
    --extern crossbeam_channel=/home/portion/powerfs/target/release/deps/libcrossbeam_channel-*.rlib \
    --extern log=/home/portion/powerfs/target/release/deps/liblog-*.rlib \
    2>&1 | head -10

if [ -f /tmp/perf_test ]; then
    echo "[*] Running test program..."
    /tmp/perf_test
else
    echo "[!] Failed to compile test program"
    echo "[*] Running cargo test instead..."
    cargo test -p powerfs-fuse --lib -- --test-threads=1
fi
