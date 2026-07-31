//! S3/Filer-facing Master client.
//!
//! Internally backed by [`ResilientMasterClient`] so that leader
//! discovery and failover are handled uniformly across every
//! downstream client (monitor, S3, filer, …) instead of being
//! reimplemented here.

use crate::proto::powerfs::{AssignRequest, LookupVolumeRequest};
use crate::resilient_client::ResilientMasterClient;
use chrono::Utc;
use log::info;
use powerfs_common::{
    error::{PowerFsError, Result},
    types::{
        Collection, DataCenterId, DataNodeInfo, DiskType, Fid, NodeId, NodeState, RackId, Ttl,
        VolumeId, VolumeInfo, VolumeState,
    },
};
use std::sync::Arc;
use tonic::Status;

/// Master client used by the S3 gateway and the Filer.
///
/// Wraps a [`ResilientMasterClient`] shared across clones so that
/// every call benefits from the same cached leader hint and channel
/// pool.
#[derive(Clone)]
pub struct S3MasterClient {
    inner: Arc<ResilientMasterClient>,
}

impl S3MasterClient {
    /// Create a new client from a list of master gRPC endpoints.
    ///
    /// Each endpoint should be in `host:port` form (no scheme); the
    /// `http://` prefix is added internally by `ResilientMasterClient`.
    /// At least one endpoint must be provided.
    pub fn new(endpoints: Vec<String>) -> Result<Self> {
        if endpoints.is_empty() {
            return Err(PowerFsError::Internal(
                "S3MasterClient requires at least one master endpoint".to_string(),
            ));
        }
        let inner = ResilientMasterClient::new(endpoints)
            .map_err(|e| PowerFsError::Internal(format!("Invalid master endpoints: {}", e)))?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    pub async fn assign_volume(
        &self,
        replication: &str,
        collection: &str,
    ) -> Result<(Fid, Vec<DataNodeInfo>)> {
        let request = AssignRequest {
            count: 1,
            replication: replication.to_string(),
            collection: collection.to_string(),
            ttl: String::new(),
            data_center: String::new(),
            rack: String::new(),
            data_node: String::new(),
            disk_type: String::new(),
            stripe_count: 0,
            stripe_size: 0,
        };

        // The closure may be invoked twice (initial call + retry after
        // failover), so it must be `Fn`.  We capture `request` by move
        // and clone it on each invocation.
        let response = self
            .inner
            .call(move |mut client| {
                let req = request.clone();
                async move {
                    let resp = client.assign(tonic::Request::new(req)).await?;
                    let inner = resp.into_inner();
                    // The master may report "not leader" inside the response
                    // body instead of as a gRPC status.  Promote it to a
                    // failed_precondition status so that ResilientMasterClient
                    // can parse the leader hint and fail over.
                    if inner.error.contains("not leader") {
                        return Err(Status::failed_precondition(inner.error));
                    }
                    Ok(inner)
                }
            })
            .await
            .map_err(|e| PowerFsError::Internal(format!("Assign failed: {}", e)))?;

        if !response.error.is_empty() {
            return Err(PowerFsError::Internal(response.error));
        }

        let fid = Fid::from_string(&response.fid)
            .map_err(|e| PowerFsError::Internal(format!("Invalid fid format: {}", e)))?;
        let nodes: Vec<DataNodeInfo> = response
            .replicas
            .into_iter()
            .map(|loc| {
                let mut addr = loc.url.strip_prefix("http://").unwrap_or(&loc.url);
                addr = addr.strip_prefix("https://").unwrap_or(addr);
                addr = addr.split('/').next().unwrap_or(addr);
                let ip: String = if let Some(colon_idx) = addr.rfind(':') {
                    addr[..colon_idx].to_string()
                } else {
                    addr.to_string()
                };
                DataNodeInfo {
                    id: NodeId(loc.url.clone()),
                    address: ip,
                    rack_id: RackId(String::new()),
                    data_center_id: DataCenterId(loc.data_center),
                    total_space: 0,
                    used_space: 0,
                    volume_count: 0,
                    state: NodeState::Healthy,
                    last_heartbeat: Utc::now(),
                    grpc_port: loc.grpc_port,
                    http_port: 8080,
                    public_url: loc.public_url,
                    maintenance_mode: false,
                    soft_error_type: None,
                    degrade_type: None,
                    degrade_severity: 0,
                    state_since: 0,
                }
            })
            .collect();
        Ok((fid, nodes))
    }

    pub async fn get_volume_info(&self, volume_id: &VolumeId) -> Option<VolumeInfo> {
        let vid_str = volume_id.0.to_string();
        let request = LookupVolumeRequest {
            volume_or_file_ids: vec![vid_str],
            collection: String::new(),
        };

        let result = self
            .inner
            .call(move |mut client| {
                let req = request.clone();
                async move { client.lookup_volume(tonic::Request::new(req)).await }
            })
            .await;

        match result {
            Ok(response) => {
                let response = response.into_inner();
                for vol_loc in response.volume_id_locations {
                    if !vol_loc.error.is_empty() {
                        info!(
                            "get_volume_info: lookup returned error for {}: {}",
                            volume_id.0, vol_loc.error
                        );
                        continue;
                    }
                    if let Some(loc) = vol_loc.locations.first() {
                        return Some(VolumeInfo {
                            id: *volume_id,
                            node_id: NodeId(loc.url.clone()),
                            collection: Collection(String::new()),
                            size: 0,
                            used: 0,
                            replica_count: vol_loc.locations.len() as u32,
                            ttl: Ttl::default(),
                            disk_type: DiskType::default(),
                            state: VolumeState::Available,
                            created_at: Utc::now(),
                            modified_at: Utc::now(),
                            next_file_key: 0,
                        });
                    }
                }
                None
            }
            Err(e) => {
                info!("get_volume_info: lookup_volume failed: {}", e);
                None
            }
        }
    }

    /// Pure local helper — does not contact the master.
    pub fn get_node_info(&self, node_id: &str) -> Option<DataNodeInfo> {
        let mut addr = node_id.strip_prefix("http://").unwrap_or(node_id);
        addr = addr.strip_prefix("https://").unwrap_or(addr);
        addr = addr.split('/').next().unwrap_or(addr);
        let ip: String = if let Some(colon_idx) = addr.rfind(':') {
            addr[..colon_idx].to_string()
        } else {
            addr.to_string()
        };
        let grpc_port = if ip.starts_with("172.20.0.2") {
            8080
        } else {
            9333
        };
        Some(DataNodeInfo {
            id: NodeId(node_id.to_string()),
            address: ip,
            rack_id: RackId(String::new()),
            data_center_id: DataCenterId(String::new()),
            total_space: 0,
            used_space: 0,
            volume_count: 0,
            state: NodeState::Healthy,
            last_heartbeat: Utc::now(),
            grpc_port,
            http_port: 8080,
            public_url: String::new(),
            maintenance_mode: false,
            soft_error_type: None,
            degrade_type: None,
            degrade_severity: 0,
            state_since: 0,
        })
    }
}
