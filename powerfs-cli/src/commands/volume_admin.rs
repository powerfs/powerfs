//! `powerfs-cli volume ...` —— WAL 卷远程管理面（P2 T8）。
//!
//! 所有命令只经 Master 代理（Master 定位属主 volume node 后转发），
//! CLI 不直连 volume server。命令树对齐方案 §19.2：
//!   volume status|stats --volume <id>
//!   volume resize --volume <id> --size <bytes>
//!   volume gc|checkpoint --volume <id> trigger|status

use crate::client::MasterClient;
use clap::{Args, Subcommand};
use powerfs_common::error::{PowerFsError, Result};
use powerfs_master::proto::{
    VolumeAdminCheckpointRequest, VolumeAdminGcRequest, VolumeAdminStatsRequest,
    VolumeResizeRequest,
};

#[derive(Subcommand, Debug)]
pub enum VolumeAdminCommand {
    /// 卷状态概览（容量/Full/属主节点）
    Status(VolumeTargetArgs),
    /// 卷详细空间账与 checkpoint/WAL 进度
    Stats(VolumeTargetArgs),
    /// 调整卷逻辑容量（字节；shrink 不得低于 used+staging+pinned）
    Resize(VolumeResizeArgs),
    /// 段垃圾回收：trigger 立即执行一轮；status 查看相关计数
    Gc(VolumeActionArgs),
    /// 检查点：trigger 立即落一次 checkpoint；status 查看 ckpt 进度
    Checkpoint(VolumeActionArgs),
}

#[derive(Args, Debug)]
pub struct VolumeTargetArgs {
    /// 卷 ID
    #[arg(long, required = true)]
    pub volume: u64,
}

#[derive(Args, Debug)]
pub struct VolumeResizeArgs {
    /// 卷 ID
    #[arg(long, required = true)]
    pub volume: u64,
    /// 新容量（字节）
    #[arg(long, required = true)]
    pub size: u64,
}

#[derive(Args, Debug)]
pub struct VolumeActionArgs {
    /// 卷 ID
    #[arg(long, required = true)]
    pub volume: u64,

    #[command(subcommand)]
    pub action: TriggerStatus,
}

#[derive(Subcommand, Clone, Copy, Debug)]
pub enum TriggerStatus {
    /// 立即触发一次
    Trigger,
    /// 只查看当前状态/计数（等价于 stats 的相关字段）
    Status,
}

pub async fn volume_admin(mut client: MasterClient, command: VolumeAdminCommand) -> Result<()> {
    let mut service = client
        .service()
        .await
        .map_err(|e| PowerFsError::Internal(format!("failed to connect master: {}", e)))?;

    match command {
        VolumeAdminCommand::Status(args) => {
            let resp = service
                .volume_admin_stats(tonic::Request::new(VolumeAdminStatsRequest {
                    volume_id: args.volume,
                }))
                .await
                .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            ensure_success(&resp.success, &resp.error)?;
            print_status(args.volume, &resp);
        }
        VolumeAdminCommand::Stats(args) => {
            let resp = service
                .volume_admin_stats(tonic::Request::new(VolumeAdminStatsRequest {
                    volume_id: args.volume,
                }))
                .await
                .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            ensure_success(&resp.success, &resp.error)?;
            print_stats(args.volume, &resp);
        }
        VolumeAdminCommand::Resize(args) => {
            let resp = service
                .volume_resize(tonic::Request::new(VolumeResizeRequest {
                    volume_id: args.volume,
                    new_size: args.size,
                }))
                .await
                .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            ensure_success(&resp.success, &resp.error)?;
            println!(
                "volume {} resized to {} ({}) via node {}",
                args.volume,
                args.size,
                fmt_bytes(args.size),
                resp.node
            );
        }
        VolumeAdminCommand::Gc(args) => match args.action {
            TriggerStatus::Status => {
                let resp = service
                    .volume_admin_stats(tonic::Request::new(VolumeAdminStatsRequest {
                        volume_id: args.volume,
                    }))
                    .await
                    .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
                    .into_inner();
                ensure_success(&resp.success, &resp.error)?;
                println!(
                    "volume {} GC status: segments={}, staging={} ({}), garbage={} ({}), deleted_needles={}",
                    args.volume,
                    resp.segments,
                    resp.staging_bytes,
                    fmt_bytes(resp.staging_bytes),
                    resp.garbage_bytes,
                    fmt_bytes(resp.garbage_bytes),
                    resp.deleted_count
                );
            }
            TriggerStatus::Trigger => {
                let resp = service
                    .volume_admin_gc(tonic::Request::new(VolumeAdminGcRequest {
                        volume_id: args.volume,
                    }))
                    .await
                    .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
                    .into_inner();
                ensure_success(&resp.success, &resp.error)?;
                println!(
                    "GC complete on volume {} via node {}:\n\
                     \tpurged tombstones: {}\n\
                     \tsegments deleted:  {}\n\
                     \tmigrated needles:  {}\n\
                     \tmigrated bytes:    {} ({})\n\
                     \treclaimed bytes:   {} ({})",
                    args.volume,
                    resp.node,
                    resp.purged,
                    resp.segments_deleted,
                    resp.migrated_needles,
                    resp.migrated_bytes,
                    fmt_bytes(resp.migrated_bytes),
                    resp.reclaimed_bytes,
                    fmt_bytes(resp.reclaimed_bytes)
                );
            }
        },
        VolumeAdminCommand::Checkpoint(args) => match args.action {
            TriggerStatus::Status => {
                let resp = service
                    .volume_admin_stats(tonic::Request::new(VolumeAdminStatsRequest {
                        volume_id: args.volume,
                    }))
                    .await
                    .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
                    .into_inner();
                ensure_success(&resp.success, &resp.error)?;
                println!(
                    "volume {} checkpoint status: last_seq={}, last_applied_lsn={}, durable_lsn={}",
                    args.volume, resp.last_ckpt_seq, resp.last_ckpt_lsn, resp.durable_lsn
                );
            }
            TriggerStatus::Trigger => {
                let resp = service
                    .volume_admin_checkpoint(tonic::Request::new(VolumeAdminCheckpointRequest {
                        volume_id: args.volume,
                    }))
                    .await
                    .map_err(|e| PowerFsError::TonicStatus(Box::new(e)))?
                    .into_inner();
                ensure_success(&resp.success, &resp.error)?;
                println!(
                    "checkpoint complete on volume {} via node {}: seq={}, applied_lsn={}",
                    args.volume, resp.node, resp.ckpt_seq, resp.applied_lsn
                );
            }
        },
    }

    Ok(())
}

fn ensure_success(success: &bool, error: &str) -> Result<()> {
    if *success {
        Ok(())
    } else {
        Err(PowerFsError::Internal(error.to_string()))
    }
}

fn print_status(volume_id: u64, s: &powerfs_master::proto::VolumeAdminStatsResponse) {
    println!("volume {} (node {})", volume_id, s.node);
    println!(
        "  state:       {}",
        if s.is_full { "Full" } else { "Available" }
    );
    println!(
        "  capacity:    {} (volume_size={})",
        if s.volume_size == 0 {
            "unlimited".to_string()
        } else {
            fmt_bytes(s.volume_size)
        },
        s.volume_size
    );
    println!(
        "  free:        {} (free_bytes={})",
        if s.free_bytes == u64::MAX {
            "unlimited".to_string()
        } else {
            fmt_bytes(s.free_bytes)
        },
        s.free_bytes
    );
    println!("  node:        {}", s.node);
}

fn print_stats(volume_id: u64, s: &powerfs_master::proto::VolumeAdminStatsResponse) {
    let capacity = if s.volume_size == 0 {
        "unlimited".to_string()
    } else {
        fmt_bytes(s.volume_size)
    };
    let free = if s.free_bytes == u64::MAX {
        "unlimited".to_string()
    } else {
        fmt_bytes(s.free_bytes)
    };
    println!("volume {} via node {}", volume_id, s.node);
    println!(
        "  state:            {}",
        if s.is_full { "Full" } else { "Available" }
    );
    println!("  capacity:         {} ({})", s.volume_size, capacity);
    println!("  free:             {} ({})", s.free_bytes, free);
    println!(
        "  used:             {} ({})",
        s.used_bytes,
        fmt_bytes(s.used_bytes)
    );
    println!(
        "  staging:          {} ({})",
        s.staging_bytes,
        fmt_bytes(s.staging_bytes)
    );
    println!(
        "  garbage:          {} ({})",
        s.garbage_bytes,
        fmt_bytes(s.garbage_bytes)
    );
    println!(
        "  pinned:           {} ({})",
        s.pinned_bytes,
        fmt_bytes(s.pinned_bytes)
    );
    println!("  active needles:   {}", s.active_count);
    println!("  deleted needles:  {}", s.deleted_count);
    println!("  segments:         {}", s.segments);
    println!("  last ckpt seq:    {}", s.last_ckpt_seq);
    println!("  last ckpt lsn:    {}", s.last_ckpt_lsn);
    println!("  durable lsn:      {}", s.durable_lsn);
}

/// 原始字节 -> 人类可读（1024 进制，保留 2 位小数）。
fn fmt_bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} B", n)
    } else {
        format!("{:.2} {}", value, UNITS[unit])
    }
}
