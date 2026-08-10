//! Allocator management API CLI.
//!
//! Wraps the `ManagementApi` gRPC RPCs exposed by the master service so that
//! operators (and the integration test suite) can drive placement-strategy
//! switching, volume pinning, node maintenance, and migration control from a
//! single command.

use crate::client::MasterClient;
use clap::{Args, Subcommand};

/// Allocator management operations.
#[derive(Subcommand, Debug)]
pub enum ManageSubcommand {
    /// Switch the global placement strategy ("round_robin" | "least_loaded" | "anti_affinity")
    PlacementStrategy(PlacementStrategyArgs),
    /// Pin a volume to a specific node (Raft-replicated)
    PinVolume(PinVolumeArgs),
    /// Remove a volume pin
    UnpinVolume(UnpinVolumeArgs),
    /// Enable/disable maintenance mode for a node
    NodeMaintenance(NodeMaintenanceArgs),
    /// Run a rebalance check (dry_run only previews recommended actions)
    RebalanceCheck(RebalanceCheckArgs),
    /// List active migration tasks
    MigrationTasks(MigrationTasksArgs),
    /// Pause all running migrations
    PauseMigrations(PauseMigrationsArgs),
    /// Resume paused migrations
    ResumeMigrations(ResumeMigrationsArgs),
    /// Create a new managed volume
    CreateVolume(CreateVolumeArgs),
    /// Mark a volume as draining (triggers data migration off it)
    DrainVolume(VolumeIdArgs),
    /// Remove a fully-drained volume
    RemoveVolume(VolumeIdArgs),
}

#[derive(Args, Debug)]
pub struct PlacementStrategyArgs {
    pub strategy: String,
}

#[derive(Args, Debug)]
pub struct PinVolumeArgs {
    pub volume_id: u64,
    pub node_id: String,
}

#[derive(Args, Debug)]
pub struct UnpinVolumeArgs {
    pub volume_id: u64,
}

#[derive(Args, Debug)]
pub struct NodeMaintenanceArgs {
    pub node_id: String,
    /// "true" = enter maintenance, "false" = exit maintenance (string parsed to bool)
    pub enabled: String,
}

#[derive(Args, Debug)]
pub struct RebalanceCheckArgs {
    /// Only preview recommended actions; do not execute them
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Args, Debug)]
pub struct MigrationTasksArgs {}

#[derive(Args, Debug)]
pub struct PauseMigrationsArgs {}

#[derive(Args, Debug)]
pub struct ResumeMigrationsArgs {}

#[derive(Args, Debug)]
pub struct CreateVolumeArgs {
    pub zone_id: u32,
    /// Optional node id; empty = auto-select least-loaded node
    pub node_id: Option<String>,
    /// Volume size in bytes (defaults to 1 GiB if omitted)
    pub size: Option<u64>,
}

#[derive(Args, Debug)]
pub struct VolumeIdArgs {
    pub volume_id: u64,
}

/// Top-level args for the `manage` command.
#[derive(Args, Debug)]
pub struct ManageArgs {
    #[command(subcommand)]
    pub command: ManageSubcommand,
}

pub async fn manage(mut client: MasterClient, args: ManageArgs) -> super::CommandResult {
    let mut service = client.service().await.map_err(|e| {
        powerfs_common::error::PowerFsError::Internal(format!("Failed to connect: {}", e))
    })?;

    match args.command {
        ManageSubcommand::PlacementStrategy(a) => {
            let resp = service
                .set_placement_strategy(tonic::Request::new(
                    powerfs_master::proto::SetPlacementStrategyRequest {
                        strategy: a.strategy,
                    },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            print_volume_manage_response("SetPlacementStrategy", resp);
        }
        ManageSubcommand::PinVolume(a) => {
            let resp = service
                .pin_volume(tonic::Request::new(
                    powerfs_master::proto::PinVolumeRequest {
                        volume_id: a.volume_id,
                        node_id: a.node_id,
                    },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            print_volume_manage_response("PinVolume", resp);
        }
        ManageSubcommand::UnpinVolume(a) => {
            let resp = service
                .unpin_volume(tonic::Request::new(
                    powerfs_master::proto::VolumeIdRequest {
                        volume_id: a.volume_id,
                    },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            print_volume_manage_response("UnpinVolume", resp);
        }
        ManageSubcommand::NodeMaintenance(a) => {
            let enabled = match a.enabled.to_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => true,
                "false" | "0" | "no" | "off" => false,
                other => {
                    return Err(powerfs_common::error::PowerFsError::InvalidRequest(
                        format!("invalid boolean value '{}': expected true|false", other),
                    ));
                }
            };
            let resp = service
                .set_node_maintenance(tonic::Request::new(
                    powerfs_master::proto::SetNodeMaintenanceRequest {
                        node_id: a.node_id,
                        enabled,
                    },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            print_migration_control_response("SetNodeMaintenance", resp);
        }
        ManageSubcommand::RebalanceCheck(a) => {
            let resp = service
                .trigger_rebalance_check(tonic::Request::new(
                    powerfs_master::proto::TriggerRebalanceCheckRequest { dry_run: a.dry_run },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            println!("\n=== TriggerRebalanceCheck (dry_run={}) ===", a.dry_run);
            println!("success: {}", resp.success);
            if !resp.error.is_empty() {
                println!("error:   {}", resp.error);
            }
            if resp.actions.is_empty() {
                println!("actions: (none — cluster is balanced)");
            } else {
                println!("actions ({}):", resp.actions.len());
                for (i, action) in resp.actions.iter().enumerate() {
                    println!("  [{}] type={:?} from_vol={} to_vol={} from_node={} to_node={} zone={} size={} needle_count={} volume_count={}",
                        i,
                        action.action_type,
                        action.from_volume,
                        action.to_volume,
                        action.from_node,
                        action.to_node,
                        action.zone_id,
                        action.size,
                        action.needle_ids.len(),
                        action.volume_ids.len(),
                    );
                }
            }
        }
        ManageSubcommand::MigrationTasks(_) => {
            let resp = service
                .get_migration_tasks(tonic::Request::new(
                    powerfs_master::proto::GetMigrationTasksRequest {},
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            println!("\n=== GetMigrationTasks ===");
            if !resp.error.is_empty() {
                println!("error: {}", resp.error);
            }
            if resp.tasks.is_empty() {
                println!("tasks: (none)");
            } else {
                println!("tasks ({}):", resp.tasks.len());
                for t in resp.tasks {
                    println!(
                        "  id={} type={} state={} progress={:.2}% migrated={}/{} bytes{}",
                        t.task_id,
                        t.action_type,
                        t.state,
                        t.progress * 100.0,
                        t.bytes_migrated,
                        t.bytes_total,
                        if t.pause_reason.is_empty() {
                            String::new()
                        } else {
                            format!(" pause_reason={}", t.pause_reason)
                        },
                    );
                }
            }
        }
        ManageSubcommand::PauseMigrations(_) => {
            let resp = service
                .pause_all_migrations(tonic::Request::new(
                    powerfs_master::proto::PauseAllMigrationsRequest {},
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            print_migration_control_response("PauseAllMigrations", resp);
        }
        ManageSubcommand::ResumeMigrations(_) => {
            let resp = service
                .resume_migrations(tonic::Request::new(
                    powerfs_master::proto::ResumeMigrationsRequest {},
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            print_migration_control_response("ResumeMigrations", resp);
        }
        ManageSubcommand::CreateVolume(a) => {
            let size = a.size.unwrap_or(1024 * 1024 * 1024); // default 1 GiB
            let resp = service
                .create_volume_managed(tonic::Request::new(
                    powerfs_master::proto::CreateVolumeManagedRequest {
                        zone_id: a.zone_id,
                        node_id: a.node_id.unwrap_or_default(),
                        size,
                    },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            println!("\n=== CreateVolumeManaged ===");
            println!("success:   {}", resp.success);
            if !resp.error.is_empty() {
                println!("error:     {}", resp.error);
            }
            println!("volume_id: {}", resp.volume_id);
        }
        ManageSubcommand::DrainVolume(a) => {
            let resp = service
                .drain_volume_managed(tonic::Request::new(
                    powerfs_master::proto::VolumeIdRequest {
                        volume_id: a.volume_id,
                    },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            print_volume_manage_response("DrainVolumeManaged", resp);
        }
        ManageSubcommand::RemoveVolume(a) => {
            let resp = service
                .remove_volume_managed(tonic::Request::new(
                    powerfs_master::proto::VolumeIdRequest {
                        volume_id: a.volume_id,
                    },
                ))
                .await
                .map_err(|e| powerfs_common::error::PowerFsError::TonicStatus(Box::new(e)))?
                .into_inner();
            print_volume_manage_response("RemoveVolumeManaged", resp);
        }
    }

    Ok(())
}

fn print_volume_manage_response(name: &str, resp: powerfs_master::proto::VolumeManageResponse) {
    println!("\n=== {} ===", name);
    println!("success: {}", resp.success);
    if !resp.error.is_empty() {
        println!("error:   {}", resp.error);
    }
}

fn print_migration_control_response(
    name: &str,
    resp: powerfs_master::proto::MigrationControlResponse,
) {
    println!("\n=== {} ===", name);
    println!("success: {}", resp.success);
    if !resp.error.is_empty() {
        println!("error:   {}", resp.error);
    }
}
