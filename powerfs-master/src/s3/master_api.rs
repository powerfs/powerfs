use powerfs_common::{
    error::Result,
    types::{DataNodeInfo, Fid, NodeId, VolumeId, VolumeInfo},
};
use std::sync::Arc;

pub enum MasterApi {
    Direct(Arc<crate::master::MasterNode>),
    Remote(Arc<crate::s3::master_client::S3MasterClient>),
}

impl MasterApi {
    pub async fn assign_volume(
        &self,
        replication: &str,
        collection: &str,
    ) -> Result<(Fid, Vec<DataNodeInfo>)> {
        match self {
            MasterApi::Direct(master) => master.assign_volume(replication, collection).await,
            MasterApi::Remote(client) => client.assign_volume(replication, collection).await,
        }
    }

    pub async fn get_volume_info(&self, volume_id: &VolumeId) -> Option<VolumeInfo> {
        match self {
            MasterApi::Direct(master) => master.get_volume_info(volume_id),
            MasterApi::Remote(client) => client.get_volume_info(volume_id).await,
        }
    }

    pub async fn get_node_info(&self, node_id: &NodeId) -> Option<DataNodeInfo> {
        match self {
            MasterApi::Direct(master) => master.get_node_info(node_id),
            MasterApi::Remote(client) => client.get_node_info(&node_id.0),
        }
    }
}
