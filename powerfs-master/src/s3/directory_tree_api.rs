use crate::directory_tree::DirectoryTree;
use crate::proto::powerfs::master_service_client::MasterServiceClient;
use crate::proto::{
    CreateEntryRequest, DeleteEntryRequest, Entry, GetEntryRequest, ListEntriesRequest,
};
use powerfs_common::error::{PowerFsError, Result};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Channel;

pub trait DirectoryTreeApi: Sync + Send + 'static {
    fn get_entry(&self, path: &str) -> Option<Entry>;
    fn create_entry(&self, entry: Entry) -> Result<u64>;
    fn create_directory(&self, path: &str) -> Result<u64>;
    fn delete_entry(&self, path: &str) -> Result<bool>;
    fn list_entries(&self, directory: &str, limit: u64, last_name: &str) -> Vec<Entry>;
}

pub enum DirectoryTreeClient {
    Direct(Arc<DirectoryTree>),
    Remote(Arc<RemoteDirectoryTree>),
}

impl DirectoryTreeApi for DirectoryTreeClient {
    fn get_entry(&self, path: &str) -> Option<Entry> {
        match self {
            DirectoryTreeClient::Direct(dt) => dt.get_entry(path),
            DirectoryTreeClient::Remote(rdt) => rdt.get_entry(path),
        }
    }

    fn create_entry(&self, entry: Entry) -> Result<u64> {
        match self {
            DirectoryTreeClient::Direct(dt) => dt
                .create_entry(entry)
                .map_err(|e| PowerFsError::Internal(format!("Failed to create entry: {}", e))),
            DirectoryTreeClient::Remote(rdt) => rdt.create_entry(entry),
        }
    }

    fn create_directory(&self, path: &str) -> Result<u64> {
        match self {
            DirectoryTreeClient::Direct(dt) => dt
                .create_directory(path)
                .map_err(|e| PowerFsError::Internal(format!("Failed to create directory: {}", e))),
            DirectoryTreeClient::Remote(rdt) => rdt.create_directory(path),
        }
    }

    fn delete_entry(&self, path: &str) -> Result<bool> {
        match self {
            DirectoryTreeClient::Direct(dt) => dt
                .delete_entry(path)
                .map_err(|e| PowerFsError::Internal(format!("Failed to delete entry: {}", e))),
            DirectoryTreeClient::Remote(rdt) => rdt.delete_entry(path),
        }
    }

    fn list_entries(&self, directory: &str, limit: u64, last_name: &str) -> Vec<Entry> {
        match self {
            DirectoryTreeClient::Direct(dt) => dt.list_entries(directory, limit, last_name),
            DirectoryTreeClient::Remote(rdt) => rdt.list_entries(directory, limit, last_name),
        }
    }
}

pub struct RemoteDirectoryTree {
    master_address: String,
    channel: Arc<Mutex<Option<Channel>>>,
}

impl RemoteDirectoryTree {
    pub fn new(master_address: &str) -> Self {
        Self {
            master_address: master_address.to_string(),
            channel: Arc::new(Mutex::new(None)),
        }
    }

    async fn get_client(&self) -> Result<MasterServiceClient<Channel>> {
        {
            let channel_guard = self.channel.lock().await;
            if channel_guard.is_some() {
                return Ok(MasterServiceClient::new(
                    channel_guard.as_ref().unwrap().clone(),
                ));
            }
        }

        let addr = format!("http://{}", self.master_address);
        let channel = match Channel::from_shared(addr) {
            Ok(c) => c,
            Err(e) => {
                return Err(PowerFsError::Internal(format!(
                    "Invalid master address: {}",
                    e
                )));
            }
        }
        .connect()
        .await
        .map_err(|e| PowerFsError::Internal(format!("Failed to connect to master: {}", e)))?;

        let mut channel_guard = self.channel.lock().await;
        *channel_guard = Some(channel.clone());
        Ok(MasterServiceClient::new(channel))
    }
}

impl DirectoryTreeApi for RemoteDirectoryTree {
    fn get_entry(&self, path: &str) -> Option<Entry> {
        let master_address = self.master_address.clone();
        let path = path.to_string();

        match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime.block_on(async move {
                let mut client = match Self::new(&master_address).get_client().await {
                    Ok(c) => c,
                    Err(_) => return None,
                };
                let request = GetEntryRequest { path };
                match client.get_entry(tonic::Request::new(request)).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.found {
                            resp.entry
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                }
            }),
            Err(_) => None,
        }
    }

    fn create_entry(&self, entry: Entry) -> Result<u64> {
        let master_address = self.master_address.clone();

        match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime.block_on(async move {
                let mut client = match Self::new(&master_address).get_client().await {
                    Ok(c) => c,
                    Err(e) => return Err(e),
                };
                let request = CreateEntryRequest { entry: Some(entry) };
                match client.create_entry(tonic::Request::new(request)).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.success {
                            Ok(resp.inode)
                        } else {
                            Err(PowerFsError::Internal(resp.error))
                        }
                    }
                    Err(e) => Err(PowerFsError::Internal(format!(
                        "Failed to create entry: {}",
                        e
                    ))),
                }
            }),
            Err(e) => Err(PowerFsError::Internal(format!(
                "Failed to create runtime: {}",
                e
            ))),
        }
    }

    fn create_directory(&self, path: &str) -> Result<u64> {
        let master_address = self.master_address.clone();
        let path_str = path.to_string();

        match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime.block_on(async move {
                let mut client = match Self::new(&master_address).get_client().await {
                    Ok(c) => c,
                    Err(e) => return Err(e),
                };

                let parts: Vec<&str> = path_str.split('/').filter(|p| !p.is_empty()).collect();
                let mut current_path = "/".to_string();

                for part in parts {
                    let parent_path = current_path.clone();
                    current_path = if current_path == "/" {
                        format!("/{}", part)
                    } else {
                        format!("{}/{}", current_path, part)
                    };

                    let get_request = GetEntryRequest {
                        path: current_path.clone(),
                    };
                    let get_response = client.get_entry(tonic::Request::new(get_request)).await;
                    if let Ok(resp) = get_response {
                        if resp.into_inner().found {
                            continue;
                        }
                    }

                    let entry = Entry {
                        name: part.to_string(),
                        directory: parent_path,
                        attributes: Some(crate::proto::FuseAttributes {
                            ino: 0,
                            mode: 0o40755,
                            nlink: 2,
                            uid: 0,
                            gid: 0,
                            rdev: 0,
                            size: 4096,
                            blksize: 4096,
                            blocks: 1,
                            atime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                            mtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                            ctime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                            crtime: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64,
                            perm: 0o755,
                        }),
                        chunks: vec![],
                        hard_link_id: "".to_string(),
                        hard_link_counter: 0,
                        extended: std::collections::HashMap::new(),
                        content_size: 4096,
                        disk_size: 4096,
                        ttl: "".to_string(),
                        symlink_target: "".to_string(),
                    };

                    let create_request = CreateEntryRequest { entry: Some(entry) };
                    match client
                        .create_entry(tonic::Request::new(create_request))
                        .await
                    {
                        Ok(resp) => {
                            let inner = resp.into_inner();
                            if !inner.success {
                                return Err(PowerFsError::Internal(inner.error));
                            }
                        }
                        Err(e) => {
                            return Err(PowerFsError::Internal(format!(
                                "Failed to create directory: {}",
                                e
                            )));
                        }
                    }
                }

                Ok(0)
            }),
            Err(e) => Err(PowerFsError::Internal(format!(
                "Failed to create runtime: {}",
                e
            ))),
        }
    }

    fn delete_entry(&self, path: &str) -> Result<bool> {
        let master_address = self.master_address.clone();
        let path = path.to_string();

        match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime.block_on(async move {
                let mut client = match Self::new(&master_address).get_client().await {
                    Ok(c) => c,
                    Err(e) => return Err(e),
                };
                let request = DeleteEntryRequest {
                    path,
                    is_directory: false,
                };
                match client.delete_entry(tonic::Request::new(request)).await {
                    Ok(response) => {
                        let resp = response.into_inner();
                        if resp.success {
                            Ok(true)
                        } else {
                            Err(PowerFsError::Internal(resp.error))
                        }
                    }
                    Err(e) => Err(PowerFsError::Internal(format!(
                        "Failed to delete entry: {}",
                        e
                    ))),
                }
            }),
            Err(e) => Err(PowerFsError::Internal(format!(
                "Failed to create runtime: {}",
                e
            ))),
        }
    }

    fn list_entries(&self, directory: &str, limit: u64, last_name: &str) -> Vec<Entry> {
        let master_address = self.master_address.clone();
        let directory = directory.to_string();
        let last_name = last_name.to_string();

        match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => {
                let entries = runtime.block_on(async move {
                    let mut client = match Self::new(&master_address).get_client().await {
                        Ok(c) => c,
                        Err(_) => return Vec::new(),
                    };
                    let request = ListEntriesRequest {
                        directory,
                        limit,
                        last_name,
                    };
                    match client.list_entries(tonic::Request::new(request)).await {
                        Ok(response) => response.into_inner().entries,
                        Err(_) => Vec::new(),
                    }
                });
                entries
            }
            Err(_) => Vec::new(),
        }
    }
}
