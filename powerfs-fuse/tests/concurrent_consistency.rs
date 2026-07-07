use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;

mod test_harness;

const FUSE_MOUNT: &str = "/tmp/powerfs-posix-test";

macro_rules! test_path {
    ($name:expr) => {
        Path::new(FUSE_MOUNT).join($name)
    };
}

fn setup() {
    test_harness::ensure_fuse_mounted();
}

#[test]
fn test_concurrent_write_different_offsets() {
    setup();
    let path = test_path!("test_concurrent_offsets.txt");

    let file_size = 1024 * 1024;
    let thread_count = 8;
    let bytes_per_thread = file_size / thread_count;

    let mut f = File::create(&path).unwrap();
    f.write_all(&vec![0u8; file_size]).unwrap();
    drop(f);

    let barrier = Arc::new(Barrier::new(thread_count));
    let mut handles = Vec::new();

    for t in 0..thread_count {
        let _barrier = barrier.clone();
        let path = path.clone();
        let handle = thread::spawn(move || {
            let offset = t * bytes_per_thread;
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(offset as u64)).unwrap();

            let data = vec![(t + 1) as u8; bytes_per_thread];
            f.write_all(&data).unwrap();
            drop(f);
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut f = File::open(&path).unwrap();
    let mut buf = vec![0u8; file_size];
    f.read_exact(&mut buf).unwrap();

    for t in 0..thread_count {
        let offset = t * bytes_per_thread;
        let expected_byte = (t + 1) as u8;
        for i in 0..bytes_per_thread {
            assert_eq!(
                buf[offset + i],
                expected_byte,
                "Thread {} byte at offset {} should be {} but got {}",
                t,
                offset + i,
                expected_byte,
                buf[offset + i]
            );
        }
    }
}

#[test]
fn test_concurrent_write_same_offset() {
    setup();
    let path = test_path!("test_concurrent_same_offset.txt");

    let file_size = 1024;
    let thread_count = 4;

    let mut f = File::create(&path).unwrap();
    f.write_all(&vec![0u8; file_size]).unwrap();
    drop(f);

    let barrier = Arc::new(Barrier::new(thread_count));
    let mut handles = Vec::new();

    for t in 0..thread_count {
        let barrier = barrier.clone();
        let path = path.clone();
        let handle = thread::spawn(move || {
            barrier.wait();
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            let data = vec![(t + 1) as u8; file_size];
            f.write_all(&data).unwrap();
            drop(f);
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut f = File::open(&path).unwrap();
    let mut buf = vec![0u8; file_size];
    f.read_exact(&mut buf).unwrap();

    let first_byte = buf[0];
    for &byte in buf.iter() {
        assert_eq!(
            byte, first_byte,
            "All bytes should be the same value after concurrent writes to same offset"
        );
    }
}

#[test]
fn test_concurrent_append() {
    setup();
    let path = test_path!("test_concurrent_append.txt");

    let thread_count = 8;
    let iterations_per_thread = 100;

    let f = File::create(&path).unwrap();
    drop(f);

    let mut handles = Vec::new();

    for t in 0..thread_count {
        let path = path.clone();
        let handle = thread::spawn(move || {
            for i in 0..iterations_per_thread {
                let mut f = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .unwrap();
                let data = format!("thread{}_iter{}\n", t, i);
                f.write_all(data.as_bytes()).unwrap();
                drop(f);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut f = File::open(&path).unwrap();
    let mut content = String::new();
    f.read_to_string(&mut content).unwrap();

    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), thread_count * iterations_per_thread);

    let mut counts = vec![0; thread_count];
    for line in lines {
        if let Some(start) = line.find("thread") {
            if let Some(end) = line[start + 6..].find('_') {
                let t: usize = line[start + 6..start + 6 + end].parse().unwrap();
                counts[t] += 1;
            }
        }
    }

    for (t, &count) in counts.iter().enumerate().take(thread_count) {
        assert_eq!(
            count, iterations_per_thread,
            "Thread {} should have written {} lines",
            t, iterations_per_thread
        );
    }
}

#[test]
fn test_concurrent_read_write_mixed() {
    setup();
    let path = test_path!("test_concurrent_read_write.txt");

    let file_size = 1024 * 1024;
    let write_threads = 4;
    let read_threads = 4;

    let mut f = File::create(&path).unwrap();
    f.write_all(&vec![1u8; file_size]).unwrap();
    drop(f);

    let barrier = Arc::new(Barrier::new(write_threads + read_threads));
    let mut handles = Vec::new();

    for t in 0..write_threads {
        let barrier = barrier.clone();
        let path = path.clone();
        let handle = thread::spawn(move || {
            barrier.wait();
            for _ in 0..100 {
                let mut f = OpenOptions::new().write(true).open(&path).unwrap();
                let offset = (t * 256 * 1024 + rand::random::<usize>() % 256) as u64;
                f.seek(SeekFrom::Start(offset)).unwrap();
                f.write_all(&[(t + 2) as u8; 64]).unwrap();
                drop(f);
            }
        });
        handles.push(handle);
    }

    for _ in 0..read_threads {
        let barrier = barrier.clone();
        let path = path.clone();
        let handle = thread::spawn(move || {
            barrier.wait();
            for _ in 0..100 {
                let mut f = File::open(&path).unwrap();
                let offset = rand::random::<usize>() % (file_size - 64);
                f.seek(SeekFrom::Start(offset as u64)).unwrap();
                let mut buf = vec![0u8; 64];
                f.read_exact(&mut buf).unwrap();
                drop(f);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut f = File::open(&path).unwrap();
    let mut buf = vec![0u8; file_size];
    f.read_exact(&mut buf).unwrap();

    let valid = buf.iter().all(|&b| b == 1 || (2..=5).contains(&b));
    assert!(valid, "File should only contain bytes 1-5");
}

#[test]
fn test_concurrent_write_flush_consistency() {
    setup();
    let path = test_path!("test_concurrent_flush.txt");

    let file_size = 1024 * 1024;
    let thread_count = 4;

    let mut f = File::create(&path).unwrap();
    f.write_all(&vec![0u8; file_size]).unwrap();
    drop(f);

    let barrier = Arc::new(Barrier::new(thread_count));
    let mut handles = Vec::new();

    for t in 0..thread_count {
        let barrier = barrier.clone();
        let path = path.clone();
        let handle = thread::spawn(move || {
            barrier.wait();
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            let offset = t * 256 * 1024;
            f.seek(SeekFrom::Start(offset as u64)).unwrap();
            let data = vec![(t + 1) as u8; 256 * 1024];
            f.write_all(&data).unwrap();
            f.sync_all().unwrap();
            drop(f);
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut f = File::open(&path).unwrap();
    let mut buf = vec![0u8; file_size];
    f.read_exact(&mut buf).unwrap();

    for t in 0..thread_count {
        let offset = t * 256 * 1024;
        let expected_byte = (t + 1) as u8;
        for &byte in buf[offset..offset + 256 * 1024].iter() {
            assert_eq!(
                byte,
                expected_byte,
                "Thread {} byte should be {} but got {}",
                t,
                expected_byte,
                byte
            );
        }
    }
}
