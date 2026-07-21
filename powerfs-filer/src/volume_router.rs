use std::sync::Arc;

use crate::metadata_store::{MetadataStore, VolumeRoute};

pub struct VolumeRouter {
    metadata_store: Arc<MetadataStore>,
}

impl VolumeRouter {
    pub fn new(metadata_store: Arc<MetadataStore>) -> Self {
        Self { metadata_store }
    }

    pub async fn get_server_addr(&self, volume_id: u32) -> Option<String> {
        self.metadata_store
            .get_volume_route(volume_id)
            .await
            .map(|r| r.server_addr)
    }

    pub async fn get_volume_route(&self, volume_id: u32) -> Option<VolumeRoute> {
        self.metadata_store.get_volume_route(volume_id).await
    }

    pub async fn update_volume_route(&self, route: &VolumeRoute) -> bool {
        self.metadata_store
            .put_volume_route(route.volume_id, route)
            .await
    }

    pub async fn invalidate_volume_cache(&self, volume_id: u32) {
        self.metadata_store.delete_volume_route(volume_id).await;
    }

    pub async fn get_all_volume_routes(&self) -> Vec<VolumeRoute> {
        let mut routes = Vec::new();
        for i in 1..=1000 {
            if let Some(route) = self.metadata_store.get_volume_route(i).await {
                routes.push(route);
            }
        }
        routes
    }
}
