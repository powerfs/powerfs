use crate::proto::{
    CreateEntryRequest, DeleteEntryRequest, Entry, GetEntryRequest, ListEntriesRequest,
};
use crate::resilient_client::ResilientMasterClient;
use futures::future::BoxFuture;
use powerfs_common::error::{PowerFsError, Result};
use std::sync::Arc;

pub trait DirectoryTreeApi: Sync + Send + 'static {
    fn get_entry(&self, path: &str) -> BoxFuture<'_, Option<Entry>>;
    fn create_entry(&self, entry: Entry) -> BoxFuture<'_, Result<u64>>;
    fn create_directory(&self, path: &str) -> BoxFuture<'_, Result<u64>>;
    fn delete_entry(&self, path: &str) -> BoxFuture<'_, Result<bool>>;
    fn list_entries(
        &self,
        directory: &str,
        limit: u64,
        last_name: &str,
    ) -> BoxFuture<'_, Vec<Entry>>;
}

pub enum DirectoryTreeClient {
    Remote(Arc<RemoteDirectoryTree>),
}

impl DirectoryTreeApi for DirectoryTreeClient {
    fn get_entry(&self, path: &str) -> BoxFuture<'_, Option<Entry>> {
        let path = path.to_string();
        let DirectoryTreeClient::Remote(rdt) = self;
        let rdt = rdt.clone();
        Box::pin(async move { rdt.get_entry(&path).await })
    }

    fn create_entry(&self, entry: Entry) -> BoxFuture<'_, Result<u64>> {
        let DirectoryTreeClient::Remote(rdt) = self;
        let rdt = rdt.clone();
        Box::pin(async move { rdt.create_entry(entry).await })
    }

    fn create_directory(&self, path: &str) -> BoxFuture<'_, Result<u64>> {
        let path = path.to_string();
        let DirectoryTreeClient::Remote(rdt) = self;
        let rdt = rdt.clone();
        Box::pin(async move { rdt.create_directory(&path).await })
    }

    fn delete_entry(&self, path: &str) -> BoxFuture<'_, Result<bool>> {
        let path = path.to_string();
        let DirectoryTreeClient::Remote(rdt) = self;
        let rdt = rdt.clone();
        Box::pin(async move { rdt.delete_entry(&path).await })
    }

    fn list_entries(
        &self,
        directory: &str,
        limit: u64,
        last_name: &str,
    ) -> BoxFuture<'_, Vec<Entry>> {
        let directory = directory.to_string();
        let last_name = last_name.to_string();
        let DirectoryTreeClient::Remote(rdt) = self;
        let rdt = rdt.clone();
        Box::pin(async move { rdt.list_entries(&directory, limit, &last_name).await })
    }
}

/// Remote directory-tree client backed by [`ResilientMasterClient`].
///
/// Every individual gRPC call goes through `ResilientMasterClient::call`
/// so that leader discovery and failover are handled transparently.
pub struct RemoteDirectoryTree {
    inner: Arc<ResilientMasterClient>,
}

impl RemoteDirectoryTree {
    /// Create a new remote directory tree from a list of master gRPC
    /// endpoints (`host:port`, no scheme).
    pub fn new(endpoints: Vec<String>) -> Result<Self> {
        if endpoints.is_empty() {
            return Err(PowerFsError::Internal(
                "RemoteDirectoryTree requires at least one master endpoint".to_string(),
            ));
        }
        let inner = ResilientMasterClient::new(endpoints)
            .map_err(|e| PowerFsError::Internal(format!("Invalid master endpoints: {}", e)))?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }
}

impl DirectoryTreeApi for RemoteDirectoryTree {
    fn get_entry(&self, path: &str) -> BoxFuture<'_, Option<Entry>> {
        let path = path.to_string();
        let this = self.clone();
        Box::pin(async move {
            let request = GetEntryRequest { path: path.clone() };
            let result = this
                .inner
                .call(move |mut client| {
                    let req = request.clone();
                    async move { client.get_entry(tonic::Request::new(req)).await }
                })
                .await;
            match result {
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
        })
    }

    fn create_entry(&self, entry: Entry) -> BoxFuture<'_, Result<u64>> {
        let this = self.clone();
        Box::pin(async move {
            let request = CreateEntryRequest {
                entry: Some(entry),
                client_id: String::new(),
            };
            let response = this
                .inner
                .call(move |mut client| {
                    let req = request.clone();
                    async move { client.create_entry(tonic::Request::new(req)).await }
                })
                .await
                .map_err(|e| PowerFsError::Internal(format!("Failed to create entry: {}", e)))?;
            let resp = response.into_inner();
            if resp.success {
                Ok(resp.inode)
            } else {
                Err(PowerFsError::Internal(resp.error))
            }
        })
    }

    fn create_directory(&self, path: &str) -> BoxFuture<'_, Result<u64>> {
        let path = path.to_string();
        let this = self.clone();
        Box::pin(async move {
            let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
            let mut current_path = "/".to_string();

            for part in parts {
                let parent_path = current_path.clone();
                current_path = if current_path == "/" {
                    format!("/{}", part)
                } else {
                    format!("{}/{}", current_path, part)
                };

                // Check if the directory already exists.
                if this.get_entry(&current_path).await.is_some() {
                    continue;
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
                    hard_link_id: String::new(),
                    hard_link_counter: 0,
                    extended: std::collections::HashMap::new(),
                    content_size: 4096,
                    disk_size: 4096,
                    ttl: String::new(),
                    symlink_target: String::new(),
                    owner: String::new(),
                    generation: 0,
                };

                let request = CreateEntryRequest {
                    entry: Some(entry),
                    client_id: String::new(),
                };
                let response = this
                    .inner
                    .call(move |mut client| {
                        let req = request.clone();
                        async move { client.create_entry(tonic::Request::new(req)).await }
                    })
                    .await
                    .map_err(|e| {
                        PowerFsError::Internal(format!("Failed to create directory: {}", e))
                    })?;
                let inner = response.into_inner();
                if !inner.success {
                    return Err(PowerFsError::Internal(inner.error));
                }
            }

            Ok(0)
        })
    }

    fn delete_entry(&self, path: &str) -> BoxFuture<'_, Result<bool>> {
        let path = path.to_string();
        let this = self.clone();
        Box::pin(async move {
            let entry = this.get_entry(&path).await;
            let ino = match entry {
                Some(e) => e.attributes.map(|a| a.ino).unwrap_or(0),
                None => return Err(PowerFsError::FileNotFound(path)),
            };

            let request = DeleteEntryRequest {
                ino,
                is_directory: false,
                client_id: String::new(),
            };
            let response = this
                .inner
                .call(move |mut client| {
                    let req = request.clone();
                    async move { client.delete_entry(tonic::Request::new(req)).await }
                })
                .await
                .map_err(|e| PowerFsError::Internal(format!("Failed to delete entry: {}", e)))?;
            let resp = response.into_inner();
            if resp.success {
                Ok(true)
            } else {
                Err(PowerFsError::Internal(resp.error))
            }
        })
    }

    fn list_entries(
        &self,
        directory: &str,
        limit: u64,
        last_name: &str,
    ) -> BoxFuture<'_, Vec<Entry>> {
        let directory = directory.to_string();
        let last_name = last_name.to_string();
        let this = self.clone();
        Box::pin(async move {
            let parent_ino = if directory == "/" {
                1
            } else {
                let entry = this.get_entry(&directory).await;
                entry
                    .map(|e| e.attributes.map(|a| a.ino).unwrap_or(0))
                    .unwrap_or(0)
            };

            let request = ListEntriesRequest {
                parent_ino,
                limit,
                last_name: last_name.clone(),
            };
            match this
                .inner
                .call(move |mut client| {
                    let req = request.clone();
                    async move { client.list_entries(tonic::Request::new(req)).await }
                })
                .await
            {
                Ok(response) => response.into_inner().entries,
                Err(_) => Vec::new(),
            }
        })
    }
}

impl Clone for RemoteDirectoryTree {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// LocalDirectoryTree — in-memory implementation for tests
// ---------------------------------------------------------------------------

/// In-memory `DirectoryTreeApi` implementation for unit tests.
///
/// The Master gRPC server returns `UNIMPLEMENTED` for all directory-tree
/// operations (they have moved to the Filer Raft).  Tests that exercise the
/// S3 layer therefore cannot use `RemoteDirectoryTree` — they need this
/// local mock which stores entries in a `HashMap`.
pub struct LocalDirectoryTree {
    /// full_path → (inode, Entry)
    entries: std::sync::RwLock<std::collections::HashMap<String, (u64, Entry)>>,
    next_inode: std::sync::atomic::AtomicU64,
}

impl Default for LocalDirectoryTree {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalDirectoryTree {
    pub fn new() -> Self {
        let mut map = std::collections::HashMap::new();
        // Root directory always exists at "/" with inode 1.
        map.insert(
            "/".to_string(),
            (
                1,
                Entry {
                    name: String::new(),
                    directory: String::new(),
                    attributes: Some(crate::proto::FuseAttributes {
                        ino: 1,
                        mode: 0o40755,
                        nlink: 2,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        size: 4096,
                        blksize: 4096,
                        blocks: 1,
                        atime: 0,
                        mtime: 0,
                        ctime: 0,
                        crtime: 0,
                        perm: 0o755,
                    }),
                    chunks: vec![],
                    hard_link_id: String::new(),
                    hard_link_counter: 0,
                    extended: std::collections::HashMap::new(),
                    content_size: 4096,
                    disk_size: 4096,
                    ttl: String::new(),
                    symlink_target: String::new(),
                    owner: String::new(),
                    generation: 0,
                },
            ),
        );
        Self {
            entries: std::sync::RwLock::new(map),
            next_inode: std::sync::atomic::AtomicU64::new(2),
        }
    }

    fn alloc_inode(&self) -> u64 {
        self.next_inode
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn full_path(directory: &str, name: &str) -> String {
        if directory == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", directory, name)
        }
    }
}

impl DirectoryTreeApi for LocalDirectoryTree {
    fn get_entry(&self, path: &str) -> BoxFuture<'_, Option<Entry>> {
        let path = path.to_string();
        Box::pin(async move {
            self.entries
                .read()
                .unwrap()
                .get(&path)
                .map(|(_, e)| e.clone())
        })
    }

    fn create_entry(&self, entry: Entry) -> BoxFuture<'_, Result<u64>> {
        Box::pin(async move {
            let path = Self::full_path(&entry.directory, &entry.name);
            let ino = self.alloc_inode();
            let mut entry = entry.clone();
            if let Some(ref mut attr) = entry.attributes {
                if attr.ino == 0 {
                    attr.ino = ino;
                }
            }
            self.entries.write().unwrap().insert(path, (ino, entry));
            Ok(ino)
        })
    }

    fn create_directory(&self, path: &str) -> BoxFuture<'_, Result<u64>> {
        let path = path.to_string();
        Box::pin(async move {
            let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
            let mut current_path = "/".to_string();
            let mut last_ino = 1u64;

            for part in parts {
                let parent_path = current_path.clone();
                current_path = if current_path == "/" {
                    format!("/{}", part)
                } else {
                    format!("{}/{}", current_path, part)
                };

                // Skip if already exists.
                if self.entries.read().unwrap().contains_key(&current_path) {
                    last_ino = self
                        .entries
                        .read()
                        .unwrap()
                        .get(&current_path)
                        .map(|(ino, _)| *ino)
                        .unwrap_or(0);
                    continue;
                }

                let ino = self.alloc_inode();
                let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
                let entry = Entry {
                    name: part.to_string(),
                    directory: parent_path,
                    attributes: Some(crate::proto::FuseAttributes {
                        ino,
                        mode: 0o40755,
                        nlink: 2,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        size: 4096,
                        blksize: 4096,
                        blocks: 1,
                        atime: now,
                        mtime: now,
                        ctime: now,
                        crtime: now,
                        perm: 0o755,
                    }),
                    chunks: vec![],
                    hard_link_id: String::new(),
                    hard_link_counter: 0,
                    extended: std::collections::HashMap::new(),
                    content_size: 4096,
                    disk_size: 4096,
                    ttl: String::new(),
                    symlink_target: String::new(),
                    owner: String::new(),
                    generation: 0,
                };

                self.entries
                    .write()
                    .unwrap()
                    .insert(current_path.clone(), (ino, entry));
                last_ino = ino;
            }

            Ok(last_ino)
        })
    }

    fn delete_entry(&self, path: &str) -> BoxFuture<'_, Result<bool>> {
        let path = path.to_string();
        Box::pin(async move {
            let mut entries = self.entries.write().unwrap();
            if entries.remove(&path).is_some() {
                Ok(true)
            } else {
                Err(PowerFsError::FileNotFound(path))
            }
        })
    }

    fn list_entries(
        &self,
        directory: &str,
        limit: u64,
        last_name: &str,
    ) -> BoxFuture<'_, Vec<Entry>> {
        let directory = directory.to_string();
        let last_name = last_name.to_string();
        Box::pin(async move {
            let entries = self.entries.read().unwrap();
            let mut result: Vec<Entry> = entries
                .values()
                .filter_map(|(_, e)| {
                    if e.directory == directory && e.name > last_name {
                        Some(e.clone())
                    } else {
                        None
                    }
                })
                .collect();
            // Sort by name for deterministic pagination.
            result.sort_by(|a, b| a.name.cmp(&b.name));
            result.truncate(limit as usize);
            result
        })
    }
}
