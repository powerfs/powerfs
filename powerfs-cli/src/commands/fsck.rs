use std::collections::{HashSet, VecDeque};

use clap::Args;
use tonic::transport::Channel;

use powerfs_common::error::{PowerFsError, Result};
use powerfs_filer::powerfs::filer_meta_service_client::FilerMetaServiceClient;
use powerfs_filer::powerfs::{Entry, FileChunk, ListEntriesRequest};

use crate::client::MasterClient;

/// POSIX root inode (matches `powerfs_filer::meta_shard_manager::POSIX_ROOT_INODE`).
const ROOT_INODE: u64 = 1;

/// POSIX stat mode mask / directory bits.
const S_IFMT: u32 = 0o170000;
const S_IFDIR: u32 = 0o040000;

/// Page size for `ListEntries` requests. The filer's gRPC impl does not honor
/// `last_name` pagination, so we request a large page per directory.
const LIST_PAGE_SIZE: u64 = 100_000;

/// Maximum number of example rows printed per anomaly category.
const EXAMPLE_LIMIT: usize = 20;

/// fsck arguments
#[derive(Args)]
pub struct FsckArgs {
    /// Filer gRPC address (e.g., localhost:9334)
    #[arg(long, default_value = "localhost:9334")]
    filer: String,

    /// Check mode: "metadata" (filer only) or "full" (filer + volume, not yet implemented)
    #[arg(long, default_value = "metadata")]
    mode: String,

    /// Repair orphaned references (currently report-only; no mutations are performed).
    #[arg(long)]
    repair: bool,

    /// Maximum inodes to scan (0 = unlimited)
    #[arg(long, default_value = "0")]
    limit: u64,
}

#[derive(Default)]
struct FsckStats {
    total_inodes: u64,
    total_files: u64,
    total_dirs: u64,
    total_chunks: u64,
    /// (inode, chunk_index)
    invalid_needle_id: Vec<(u64, usize)>,
    /// (inode, chunk_index)
    invalid_volume_id: Vec<(u64, usize)>,
    /// (inode, needle_id)
    duplicate_needle_id: Vec<(u64, u64)>,
    /// (inode, volume_id not known to master)
    unknown_volume_id: Vec<(u64, u64)>,
    /// (inode, expected_offset, actual_offset) — may be legitimate sparse holes
    offset_gaps: Vec<(u64, u64, u64)>,
    /// (inode, prev_end, actual_offset) — overlapping chunks, real anomaly
    offset_overlaps: Vec<(u64, u64, u64)>,
    /// (inode, content_size, chunks_total) — chunks claim more bytes than file size
    size_mismatch: Vec<(u64, u64, u64)>,
    /// (parent_ino, error message)
    scan_errors: Vec<(u64, String)>,
}

pub async fn fsck(mut client: MasterClient, args: FsckArgs) -> Result<()> {
    eprintln!(
        "fsck: starting filesystem consistency check (mode={}, repair={})",
        args.mode, args.repair
    );

    if args.mode == "full" {
        eprintln!(
            "fsck: WARNING 'full' mode (volume cross-check) is not yet implemented; \
             falling back to metadata-only"
        );
    } else if args.mode != "metadata" {
        return Err(PowerFsError::InvalidRequest(format!(
            "unknown mode '{}'; use 'metadata' or 'full'",
            args.mode
        )));
    }

    // 1. From master: gather the set of valid volume ids.
    let valid_volume_ids = get_valid_volume_ids(&mut client).await?;
    eprintln!(
        "fsck: {} valid volume id(s) from master",
        valid_volume_ids.len()
    );

    // 2. Connect to filer gRPC.
    let mut filer_client = connect_filer(&args.filer).await?;
    eprintln!("fsck: connected to filer at {}", args.filer);

    // 3. Recursively walk the inode tree from root, checking each file's chunks.
    let mut stats = FsckStats::default();
    walk_tree(
        &mut filer_client,
        ROOT_INODE,
        &valid_volume_ids,
        args.limit,
        &mut stats,
    )
    .await?;

    // 4. Report
    print_report(&stats);

    // 5. Repair (safe-first: report only for now)
    if args.repair {
        eprintln!(
            "fsck: --repair requested but automatic repair is disabled (safe-first). \
             No modifications were made."
        );
        eprintln!(
            "fsck: fix the reported anomalies manually or via a dedicated repair tool \
             (volume 'list_needles' cross-check is deferred to phase 2)."
        );
    }

    Ok(())
}

async fn get_valid_volume_ids(client: &mut MasterClient) -> Result<HashSet<u64>> {
    let mut service = client
        .service()
        .await
        .map_err(|e| PowerFsError::Internal(format!("failed to connect to master: {}", e)))?;
    let response = service
        .volume_list(tonic::Request::new(
            powerfs_master::proto::VolumeListRequest {},
        ))
        .await
        .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?;
    let result = response.into_inner();
    let mut ids = HashSet::new();
    for node in result.data_nodes {
        for vol in node.volumes {
            ids.insert(vol.volume_id);
        }
    }
    Ok(ids)
}

async fn connect_filer(address: &str) -> Result<FilerMetaServiceClient<Channel>> {
    let addr = format!("http://{}", address);
    let channel = Channel::from_shared(addr)
        .map_err(|e| PowerFsError::Internal(format!("invalid filer URI: {}", e)))?
        .connect()
        .await
        .map_err(|e| PowerFsError::Internal(format!("failed to connect to filer: {}", e)))?;
    Ok(FilerMetaServiceClient::new(channel))
}

/// BFS walk from `root`, listing each directory and checking every file's chunks.
/// Cycle-safe via a `visited` set.
async fn walk_tree(
    client: &mut FilerMetaServiceClient<Channel>,
    root: u64,
    valid_volumes: &HashSet<u64>,
    limit: u64,
    stats: &mut FsckStats,
) -> Result<()> {
    let mut visited: HashSet<u64> = HashSet::new();
    let mut queue: VecDeque<u64> = VecDeque::new();
    queue.push_back(root);

    while let Some(parent_ino) = queue.pop_front() {
        if limit > 0 && stats.total_inodes >= limit {
            eprintln!("fsck: reached --limit {} inodes, stopping scan", limit);
            break;
        }

        let entries = match list_dir(client, parent_ino).await {
            Ok(e) => e,
            Err(e) => {
                stats.scan_errors.push((parent_ino, e.to_string()));
                continue;
            }
        };

        for entry in entries {
            stats.total_inodes += 1;

            let attr = match entry.attributes.as_ref() {
                Some(a) => a,
                None => {
                    stats.scan_errors.push((
                        parent_ino,
                        format!("entry '{}' has no attributes", entry.name),
                    ));
                    continue;
                }
            };
            let ino = attr.ino;
            let mode = attr.mode;
            let is_dir = (mode & S_IFMT) == S_IFDIR;

            if is_dir {
                stats.total_dirs += 1;
                if ino != 0 && visited.insert(ino) {
                    queue.push_back(ino);
                }
            } else {
                stats.total_files += 1;
                check_inode_chunks(&entry, ino, valid_volumes, stats);
            }

            if limit > 0 && stats.total_inodes >= limit {
                break;
            }
        }
    }

    Ok(())
}

async fn list_dir(
    client: &mut FilerMetaServiceClient<Channel>,
    parent_ino: u64,
) -> Result<Vec<Entry>> {
    let request = ListEntriesRequest {
        parent_ino,
        limit: LIST_PAGE_SIZE,
        last_name: String::new(),
    };
    let response = client
        .list_entries(tonic::Request::new(request))
        .await
        .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?;
    let inner = response.into_inner();
    if !inner.error.is_empty() {
        return Err(PowerFsError::Internal(format!(
            "filer list_entries error: {}",
            inner.error
        )));
    }
    Ok(inner.entries)
}

fn check_inode_chunks(
    entry: &Entry,
    ino: u64,
    valid_volumes: &HashSet<u64>,
    stats: &mut FsckStats,
) {
    let chunks = &entry.chunks;
    stats.total_chunks += chunks.len() as u64;

    let mut seen_needle_ids: HashSet<u64> = HashSet::new();

    // Per-chunk validity checks (in original order, so chunk_index is meaningful).
    for (idx, chunk) in chunks.iter().enumerate() {
        if chunk.needle_id == 0 {
            stats.invalid_needle_id.push((ino, idx));
        }
        if chunk.volume_id == 0 {
            stats.invalid_volume_id.push((ino, idx));
        }
        if chunk.volume_id > 0 && !valid_volumes.contains(&chunk.volume_id) {
            stats.unknown_volume_id.push((ino, chunk.volume_id));
        }
        if !seen_needle_ids.insert(chunk.needle_id) {
            stats.duplicate_needle_id.push((ino, chunk.needle_id));
        }
    }

    // Offset contiguity on a sorted view (clone refs; do not mutate input).
    let mut sorted: Vec<&FileChunk> = chunks.iter().collect();
    sorted.sort_by_key(|c| c.offset);

    let mut expected_offset: u64 = 0;
    let mut total_chunk_size: u64 = 0;
    for chunk in &sorted {
        if chunk.offset > expected_offset {
            stats.offset_gaps.push((ino, expected_offset, chunk.offset));
        } else if chunk.offset < expected_offset {
            stats
                .offset_overlaps
                .push((ino, expected_offset, chunk.offset));
        }
        expected_offset = expected_offset.saturating_add(chunk.size);
        total_chunk_size = total_chunk_size.saturating_add(chunk.size);
    }

    // Size mismatch: chunks claiming more bytes than content_size is a real anomaly.
    // (chunks_total < content_size is allowed for sparse files.)
    let content_size = entry.content_size;
    if total_chunk_size > content_size {
        stats
            .size_mismatch
            .push((ino, content_size, total_chunk_size));
    }
}

fn print_report(stats: &FsckStats) {
    println!();
    println!("=== fsck report ===");
    println!(
        "Scanned inodes: {} (files={}, dirs={})",
        stats.total_inodes, stats.total_files, stats.total_dirs
    );
    println!("Total chunks checked: {}", stats.total_chunks);
    println!();
    println!("Anomalies:");
    println!(
        "  invalid needle_id (==0):        {}",
        stats.invalid_needle_id.len()
    );
    println!(
        "  invalid volume_id (==0):        {}",
        stats.invalid_volume_id.len()
    );
    println!(
        "  duplicate needle_id:            {}",
        stats.duplicate_needle_id.len()
    );
    println!(
        "  unknown volume_id (not in master): {}",
        stats.unknown_volume_id.len()
    );
    println!(
        "  offset gaps (may be sparse):    {}",
        stats.offset_gaps.len()
    );
    println!(
        "  offset overlaps:                {}",
        stats.offset_overlaps.len()
    );
    println!(
        "  size mismatch (chunks > size):  {}",
        stats.size_mismatch.len()
    );
    println!(
        "  scan errors:                    {}",
        stats.scan_errors.len()
    );

    let show = |label: &str, items: &[String]| {
        if items.is_empty() {
            return;
        }
        let n = items.len().min(EXAMPLE_LIMIT);
        println!();
        println!("--- {} (showing {}) ---", label, n);
        for s in items.iter().take(EXAMPLE_LIMIT) {
            println!("  {}", s);
        }
        if items.len() > EXAMPLE_LIMIT {
            println!("  ... and {} more", items.len() - EXAMPLE_LIMIT);
        }
    };

    show(
        "invalid needle_id",
        &stats
            .invalid_needle_id
            .iter()
            .map(|(i, idx)| format!("inode={}, chunk_index={}", i, idx))
            .collect::<Vec<_>>(),
    );
    show(
        "invalid volume_id",
        &stats
            .invalid_volume_id
            .iter()
            .map(|(i, idx)| format!("inode={}, chunk_index={}", i, idx))
            .collect::<Vec<_>>(),
    );
    show(
        "duplicate needle_id",
        &stats
            .duplicate_needle_id
            .iter()
            .map(|(i, n)| format!("inode={}, needle_id={}", i, n))
            .collect::<Vec<_>>(),
    );
    show(
        "unknown volume_id",
        &stats
            .unknown_volume_id
            .iter()
            .map(|(i, v)| format!("inode={}, volume_id={}", i, v))
            .collect::<Vec<_>>(),
    );
    show(
        "offset gaps",
        &stats
            .offset_gaps
            .iter()
            .map(|(i, exp, act)| {
                format!(
                    "inode={}, expected_offset={}, actual_offset={}",
                    i, exp, act
                )
            })
            .collect::<Vec<_>>(),
    );
    show(
        "offset overlaps",
        &stats
            .offset_overlaps
            .iter()
            .map(|(i, prev_end, act)| {
                format!("inode={}, prev_end={}, actual_offset={}", i, prev_end, act)
            })
            .collect::<Vec<_>>(),
    );
    show(
        "size mismatch",
        &stats
            .size_mismatch
            .iter()
            .map(|(i, cs, ct)| format!("inode={}, content_size={}, chunks_total={}", i, cs, ct))
            .collect::<Vec<_>>(),
    );
    show(
        "scan errors",
        &stats
            .scan_errors
            .iter()
            .map(|(i, msg)| format!("parent_ino={}: {}", i, msg))
            .collect::<Vec<_>>(),
    );

    println!();
    let clean = stats.invalid_needle_id.is_empty()
        && stats.invalid_volume_id.is_empty()
        && stats.duplicate_needle_id.is_empty()
        && stats.unknown_volume_id.is_empty()
        && stats.offset_overlaps.is_empty()
        && stats.size_mismatch.is_empty()
        && stats.scan_errors.is_empty();
    if clean {
        println!(
            "fsck: metadata is consistent (offset gaps, if any, may indicate sparse files \
             which are allowed)."
        );
    } else {
        println!("fsck: anomalies detected; see details above.");
    }
}
