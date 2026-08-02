use async_trait::async_trait;
use powerfs_common::{
    error::{PowerFsError, Result},
    traits::{Location, NodeStats, VolumeFilters, VolumeProvider},
    types::{DataNodeInfo, Fid, NodeId, VolumeId, VolumeInfo},
};

use crate::master::MasterNode;

#[async_trait]
impl VolumeProvider for MasterNode {
    async fn assign_volume(
        &self,
        collection: &str,
        replication: &str,
    ) -> Result<(Fid, Vec<Location>)> {
        let (fid, nodes) = self.assign_volume(replication, collection).await?;
        let locations = nodes.into_iter().map(node_to_location).collect();
        Ok((fid, locations))
    }

    async fn lookup_volume(&self, volume_id: VolumeId) -> Result<Vec<Location>> {
        let volume_id_str = volume_id.0.to_string();
        let result = self.lookup_volume(&[volume_id_str]).await;
        if let Some(nodes) = result.get(&volume_id) {
            let locations = nodes.iter().cloned().map(node_to_location).collect();
            Ok(locations)
        } else {
            Err(PowerFsError::VolumeNotFound(volume_id))
        }
    }

    async fn heartbeat(&self, node_id: &NodeId, stats: &NodeStats) -> Result<()> {
        if let Some(mut node) = self.get_node(node_id) {
            node.total_space = stats.total_space;
            node.used_space = stats.used_space;
            node.last_heartbeat = chrono::Utc::now();
            node.volume_count = stats.volume_count;
            Ok(())
        } else {
            Err(PowerFsError::InvalidRequest(format!(
                "node not found: {}",
                node_id
            )))
        }
    }

    async fn list_volumes(&self, filters: &VolumeFilters) -> Result<Vec<VolumeInfo>> {
        let volumes = self.list_volumes().await;
        let mut result: Vec<VolumeInfo> = volumes;

        if let Some(collection) = &filters.collection {
            result.retain(|v| v.collection == *collection);
        }
        if let Some(state) = &filters.state {
            result.retain(|v| {
                let state_str = match v.state {
                    powerfs_common::types::VolumeState::Creating => "creating",
                    powerfs_common::types::VolumeState::Available => "available",
                    powerfs_common::types::VolumeState::Full => "full",
                    powerfs_common::types::VolumeState::ReadOnly => "readonly",
                    powerfs_common::types::VolumeState::Deleting => "deleting",
                };
                state_str == state
            });
        }
        if let Some(node_id) = &filters.node_id {
            result.retain(|v| v.node_id == *node_id);
        }

        Ok(result)
    }
}

fn node_to_location(node: DataNodeInfo) -> Location {
    Location {
        url: node.url(),
        public_url: node.public_url,
        grpc_port: node.grpc_port,
        data_center: node.data_center_id.to_string(),
    }
}
