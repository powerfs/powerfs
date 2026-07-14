use crate::client::MasterClient;
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
pub struct ConflictsArgs {
    #[command(subcommand)]
    command: ConflictsCommands,
}

#[derive(Subcommand)]
pub enum ConflictsCommands {
    List(ConflictsListArgs),
    Resolve(ConflictsResolveArgs),
    SetPolicy(ConflictsSetPolicyArgs),
    AutoResolve(ConflictsAutoResolveArgs),
    BatchDetect(ConflictsBatchDetectArgs),
    BatchResolve(ConflictsBatchResolveArgs),
    Stats(ConflictsStatsArgs),
    BatchIgnore(ConflictsBatchIgnoreArgs),
}

#[derive(Args)]
pub struct ConflictsListArgs {
    #[arg(long)]
    path: Option<String>,

    #[arg(long, default_value = "1")]
    dir_ino: u64,

    #[arg(long, default_value = "true")]
    unresolved_only: bool,

    #[arg(long, default_value = "100")]
    limit: u64,
}

#[derive(Args)]
pub struct ConflictsResolveArgs {
    #[arg(long)]
    conflict_id: String,

    #[arg(long)]
    path: Option<String>,

    #[arg(long, default_value = "1")]
    dir_ino: u64,

    #[arg(long)]
    resolution: String,
}

#[derive(Args)]
pub struct ConflictsSetPolicyArgs {
    #[arg(long)]
    path: Option<String>,

    #[arg(long, default_value = "1")]
    dir_ino: u64,

    #[arg(long)]
    policy: String,
}

#[derive(Args)]
pub struct ConflictsAutoResolveArgs {
    #[arg(long)]
    path: Option<String>,

    #[arg(long, default_value = "1")]
    dir_ino: u64,

    #[arg(long)]
    policy: String,
}

#[derive(Args)]
pub struct ConflictsBatchDetectArgs {
    #[arg(long)]
    path: Option<String>,

    #[arg(long, default_value = "1")]
    dir_ino: u64,

    #[arg(long, default_value = "false")]
    recursive: bool,
}

#[derive(Args)]
pub struct ConflictsBatchResolveArgs {
    #[arg(long)]
    path: Option<String>,

    #[arg(long, default_value = "1")]
    dir_ino: u64,

    #[arg(long)]
    policy: String,

    #[arg(long, default_value = "false")]
    recursive: bool,

    #[arg(long)]
    conflict_type: Option<String>,
}

#[derive(Args)]
pub struct ConflictsStatsArgs {
    #[arg(long)]
    path: Option<String>,

    #[arg(long, default_value = "1")]
    dir_ino: u64,

    #[arg(long, default_value = "false")]
    recursive: bool,
}

#[derive(Args)]
pub struct ConflictsBatchIgnoreArgs {
    #[arg(long)]
    path: Option<String>,

    #[arg(long, default_value = "1")]
    dir_ino: u64,

    #[arg(long)]
    conflict_type: Option<String>,
}

pub async fn conflicts(client: MasterClient, args: ConflictsArgs) -> super::CommandResult {
    match args.command {
        ConflictsCommands::List(args) => list_conflicts(client, args).await,
        ConflictsCommands::Resolve(args) => resolve_conflict(client, args).await,
        ConflictsCommands::SetPolicy(args) => set_merge_policy(client, args).await,
        ConflictsCommands::AutoResolve(args) => auto_resolve_conflicts(client, args).await,
        ConflictsCommands::BatchDetect(args) => batch_detect_conflicts(client, args).await,
        ConflictsCommands::BatchResolve(args) => batch_resolve_conflicts(client, args).await,
        ConflictsCommands::Stats(args) => conflicts_stats(client, args).await,
        ConflictsCommands::BatchIgnore(args) => batch_ignore_conflicts(client, args).await,
    }
}

async fn list_conflicts(mut client: MasterClient, args: ConflictsListArgs) -> super::CommandResult {
    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let dir_path = args.path.unwrap_or_default();
    let dir_ino = if dir_path.is_empty() { args.dir_ino } else { 0 };

    let response = service
        .get_conflicts(tonic::Request::new(
            powerfs_master::proto::powerfs::GetConflictsRequest {
                dir_ino,
                dir_path,
                unresolved_only: args.unresolved_only,
                limit: args.limit,
            },
        ))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let result = response.into_inner();

    if !result.success {
        return Err(powerfs_common::error::PowerFsError::Internal(result.error));
    }

    println!("\n=== Conflicts ===");
    println!("Total: {}\n", result.total_count);

    for (i, conflict) in result.conflicts.iter().enumerate() {
        let conflict_type = match conflict.conflict_type {
            0 => "CreateCreate",
            1 => "WriteWrite",
            2 => "WriteUnlink",
            3 => "DeleteCreate",
            4 => "RenameConflict",
            _ => "Unknown",
        };

        let status = if conflict.resolved {
            "Resolved"
        } else {
            "Unresolved"
        };
        let resolution = if conflict.resolved {
            match conflict.resolution {
                0 => "KeepFirst",
                1 => "KeepLast",
                2 => "KeepAll",
                3 => "Merge",
                _ => "Unknown",
            }
        } else {
            "-"
        };

        println!(
            "[{}] ID: {} | Type: {} | Status: {} | Resolution: {}",
            i + 1,
            conflict.id,
            conflict_type,
            status,
            resolution
        );
        println!("  Base name: {}", conflict.base_name);
        println!("  Created: {}", conflict.create_time);

        for (j, branch) in conflict.branches.iter().enumerate() {
            let file_type = match branch.file_type {
                0 => "File",
                1 => "Dir",
                2 => "Symlink",
                _ => "Unknown",
            };
            println!(
                "  Branch {}: name={}, inode={}, type={}, size={}",
                j + 1,
                branch
                    .id
                    .as_ref()
                    .map(|id| id.name.clone())
                    .unwrap_or_default(),
                branch.inode,
                file_type,
                branch.size
            );
        }
        println!();
    }

    Ok(())
}

async fn resolve_conflict(
    mut client: MasterClient,
    args: ConflictsResolveArgs,
) -> super::CommandResult {
    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let dir_path = args.path.unwrap_or_default();
    let dir_ino = if dir_path.is_empty() { args.dir_ino } else { 0 };

    let resolution = match args.resolution.as_str() {
        "keep-first" => 0,
        "keep-last" => 1,
        "keep-all" => 2,
        "merge" => 3,
        _ => {
            return Err(powerfs_common::error::PowerFsError::Internal(
                "Invalid resolution. Valid values: keep-first, keep-last, keep-all, merge"
                    .to_string(),
            ));
        }
    };

    let response = service
        .resolve_conflict(tonic::Request::new(
            powerfs_master::proto::powerfs::ResolveConflictRequest {
                conflict_id: args.conflict_id,
                resolution,
                dir_ino,
                dir_path,
            },
        ))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let result = response.into_inner();

    if result.success {
        println!("Conflict resolved successfully");
    } else {
        return Err(powerfs_common::error::PowerFsError::Internal(result.error));
    }

    Ok(())
}

async fn set_merge_policy(
    mut client: MasterClient,
    args: ConflictsSetPolicyArgs,
) -> super::CommandResult {
    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let dir_path = args.path.unwrap_or_default();
    let dir_ino = if dir_path.is_empty() { args.dir_ino } else { 0 };

    let policy = match args.policy.as_str() {
        "lww-time" => 0,
        "content-hash" => 1,
        "weight-based" => 2,
        "keep-all" => 3,
        "write-priority" => 4,
        "delete-priority" => 5,
        "aggressive" => 6,
        "conservative" => 7,
        "manual" => 8,
        _ => {
            return Err(powerfs_common::error::PowerFsError::Internal(
                "Invalid policy. Valid values: lww-time, content-hash, weight-based, keep-all, write-priority, delete-priority, aggressive, conservative, manual"
                    .to_string(),
            ));
        }
    };

    let response = service
        .set_merge_policy(tonic::Request::new(
            powerfs_master::proto::powerfs::SetMergePolicyRequest {
                dir_ino,
                dir_path,
                policy,
            },
        ))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let result = response.into_inner();

    if result.success {
        println!("Merge policy set successfully");
    } else {
        return Err(powerfs_common::error::PowerFsError::Internal(result.error));
    }

    Ok(())
}

async fn auto_resolve_conflicts(
    mut client: MasterClient,
    args: ConflictsAutoResolveArgs,
) -> super::CommandResult {
    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let dir_path = args.path.unwrap_or_default();
    let dir_ino = if dir_path.is_empty() { args.dir_ino } else { 0 };

    let policy = match args.policy.as_str() {
        "lww-time" => 0,
        "content-hash" => 1,
        "weight-based" => 2,
        "keep-all" => 3,
        "write-priority" => 4,
        "delete-priority" => 5,
        "aggressive" => 6,
        "conservative" => 7,
        "manual" => 8,
        _ => {
            return Err(powerfs_common::error::PowerFsError::Internal(
                "Invalid policy. Valid values: lww-time, content-hash, weight-based, keep-all, write-priority, delete-priority, aggressive, conservative, manual"
                    .to_string(),
            ));
        }
    };

    let response = service
        .auto_resolve_conflicts(tonic::Request::new(
            powerfs_master::proto::powerfs::AutoResolveConflictsRequest {
                dir_ino,
                dir_path,
                policy,
            },
        ))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let result = response.into_inner();

    if result.success {
        println!(
            "Auto-resolved {} conflicts successfully",
            result.resolved_count
        );
    } else {
        return Err(powerfs_common::error::PowerFsError::Internal(result.error));
    }

    Ok(())
}

async fn batch_detect_conflicts(
    mut client: MasterClient,
    args: ConflictsBatchDetectArgs,
) -> super::CommandResult {
    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let dir_path = args.path.unwrap_or_default();
    let dir_ino = if dir_path.is_empty() { args.dir_ino } else { 0 };

    let response = service
        .batch_detect_conflicts(tonic::Request::new(
            powerfs_master::proto::powerfs::BatchDetectConflictsRequest {
                dir_ino,
                dir_path: dir_path.clone(),
                recursive: args.recursive,
            },
        ))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let result = response.into_inner();

    if !result.success {
        return Err(powerfs_common::error::PowerFsError::Internal(result.error));
    }

    println!("\n=== Batch Conflict Detection ===");
    println!("Path: {}", dir_path);
    println!("Recursive: {}", args.recursive);
    println!("Total conflicts found: {}\n", result.total_count);

    for (conflict_type, count) in [
        ("CreateCreate", result.create_create_count),
        ("WriteWrite", result.write_write_count),
        ("WriteUnlink", result.write_unlink_count),
        ("DeleteCreate", result.delete_create_count),
        ("RenameConflict", result.rename_conflict_count),
    ] {
        if count > 0 {
            println!("  {}: {}", conflict_type, count);
        }
    }

    Ok(())
}

async fn batch_resolve_conflicts(
    mut client: MasterClient,
    args: ConflictsBatchResolveArgs,
) -> super::CommandResult {
    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let dir_path = args.path.unwrap_or_default();
    let dir_ino = if dir_path.is_empty() { args.dir_ino } else { 0 };

    let policy = match args.policy.as_str() {
        "lww-time" => 0,
        "content-hash" => 1,
        "weight-based" => 2,
        "keep-all" => 3,
        "write-priority" => 4,
        "delete-priority" => 5,
        "aggressive" => 6,
        "conservative" => 7,
        "manual" => 8,
        _ => {
            return Err(powerfs_common::error::PowerFsError::Internal(
                "Invalid policy. Valid values: lww-time, content-hash, weight-based, keep-all, write-priority, delete-priority, aggressive, conservative, manual"
                    .to_string(),
            ));
        }
    };

    let conflict_type = match args.conflict_type.as_deref() {
        Some("createcreate") | Some("CreateCreate") => 0,
        Some("writewrite") | Some("WriteWrite") => 1,
        Some("writeunlink") | Some("WriteUnlink") => 2,
        Some("deletecreate") | Some("DeleteCreate") => 3,
        Some("renameconflict") | Some("RenameConflict") => 4,
        None => -1,
        _ => {
            return Err(powerfs_common::error::PowerFsError::Internal(
                "Invalid conflict type. Valid values: CreateCreate, WriteWrite, WriteUnlink, DeleteCreate, RenameConflict"
                    .to_string(),
            ));
        }
    };

    let response = service
        .batch_resolve_conflicts(tonic::Request::new(
            powerfs_master::proto::powerfs::BatchResolveConflictsRequest {
                dir_ino,
                dir_path,
                policy,
                recursive: args.recursive,
                conflict_type,
            },
        ))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let result = response.into_inner();

    if result.success {
        println!(
            "Batch resolved {} conflicts successfully",
            result.resolved_count
        );
    } else {
        return Err(powerfs_common::error::PowerFsError::Internal(result.error));
    }

    Ok(())
}

async fn conflicts_stats(
    mut client: MasterClient,
    args: ConflictsStatsArgs,
) -> super::CommandResult {
    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let dir_path = args.path.unwrap_or_default();
    let dir_ino = if dir_path.is_empty() { args.dir_ino } else { 0 };

    let response = service
        .get_conflict_stats(tonic::Request::new(
            powerfs_master::proto::powerfs::GetConflictStatsRequest {
                dir_ino,
                dir_path: dir_path.clone(),
                recursive: args.recursive,
            },
        ))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let result = response.into_inner();

    if !result.success {
        return Err(powerfs_common::error::PowerFsError::Internal(result.error));
    }

    println!("\n=== Conflict Statistics ===");
    println!("Path: {}", dir_path);
    println!("Recursive: {}", args.recursive);
    println!("\nTotal conflicts: {}", result.total_count);
    println!("  Resolved: {}", result.resolved_count);
    println!("  Unresolved: {}", result.unresolved_count);
    println!("\nBy type:");
    println!("  CreateCreate: {} ({} resolved)", result.create_create_count, result.create_create_resolved);
    println!("  WriteWrite: {} ({} resolved)", result.write_write_count, result.write_write_resolved);
    println!("  WriteUnlink: {} ({} resolved)", result.write_unlink_count, result.write_unlink_resolved);
    println!("  DeleteCreate: {} ({} resolved)", result.delete_create_count, result.delete_create_resolved);
    println!("  RenameConflict: {} ({} resolved)", result.rename_conflict_count, result.rename_conflict_resolved);

    Ok(())
}

async fn batch_ignore_conflicts(
    mut client: MasterClient,
    args: ConflictsBatchIgnoreArgs,
) -> super::CommandResult {
    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    let dir_path = args.path.unwrap_or_default();
    let dir_ino = if dir_path.is_empty() { args.dir_ino } else { 0 };

    let conflict_type = match args.conflict_type.as_deref() {
        Some("createcreate") | Some("CreateCreate") => 0,
        Some("writewrite") | Some("WriteWrite") => 1,
        Some("writeunlink") | Some("WriteUnlink") => 2,
        Some("deletecreate") | Some("DeleteCreate") => 3,
        Some("renameconflict") | Some("RenameConflict") => 4,
        None => -1,
        _ => {
            return Err(powerfs_common::error::PowerFsError::Internal(
                "Invalid conflict type. Valid values: CreateCreate, WriteWrite, WriteUnlink, DeleteCreate, RenameConflict"
                    .to_string(),
            ));
        }
    };

    let response = service
        .batch_ignore_conflicts(tonic::Request::new(
            powerfs_master::proto::powerfs::BatchIgnoreConflictsRequest {
                dir_ino,
                dir_path,
                conflict_type,
            },
        ))
        .await
        .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?;

    let result = response.into_inner();

    if result.success {
        println!(
            "Ignored {} conflicts successfully",
            result.ignored_count
        );
    } else {
        return Err(powerfs_common::error::PowerFsError::Internal(result.error));
    }

    Ok(())
}
