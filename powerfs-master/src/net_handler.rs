//! Master Net Handler - Implements powerfs-net protocol for metadata operations
//!
//! This module provides MasterNetHandler that processes powerfs-net messages
//! and delegates to MasterNode for actual business logic.

use crate::master::MasterNode;
use crate::proto::powerfs::{Entry, FuseAttributes, VolumeShortInfo};
use log::{debug, error, info, warn};
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::{
    FieldId, FrameFlags, MsgType, NetMessage, PowerFsNetHandler, RequestContext,
    ServerRequestHandler, STATUS_ERR_ALREADY_EXISTS, STATUS_ERR_NOT_FOUND, STATUS_ERR_SERVER_ERROR,
    STATUS_OK,
};
use std::sync::Arc;
use tokio::task::spawn_blocking;

/// Master Net Handler implementation
pub struct MasterNetHandler {
    pub master: Arc<MasterNode>,
}

impl MasterNetHandler {
    pub fn new(master: Arc<MasterNode>) -> Self {
        Self { master }
    }

    /// Encode an Assign request
    pub fn encode_assign_req(
        collection: &str,
        replication: &str,
        stripe_count: u32,
        stripe_size: u64,
    ) -> Result<Vec<u8>, powerfs_net::NetError> {
        let mut enc = TlvEncoder::new();
        enc.add_string(FieldId::Name, collection)?;
        enc.add_string(FieldId::Backend, replication)?;
        enc.add_u64(FieldId::Limit, stripe_count as u64);
        enc.add_u64(FieldId::ContentSize, stripe_size);
        Ok(enc.into_bytes())
    }

    /// Decode an Assign response
    pub fn decode_assign_resp(msg: &NetMessage) -> Result<AssignResult, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let fid = dec.next_string(FieldId::Name)?;
        let location_url = dec.next_string(FieldId::Owner)?;
        let locations = if dec.has_field(FieldId::Entries) {
            dec.next_u64(FieldId::Entries)? as usize
        } else {
            0
        };
        Ok(AssignResult {
            fid,
            location_url,
            replica_count: locations,
        })
    }

    /// Encode a LookupVolume request
    pub fn encode_lookup_volume_req(
        volume_ids: &[String],
    ) -> Result<Vec<u8>, powerfs_net::NetError> {
        let mut enc = TlvEncoder::new();
        for (i, vid) in volume_ids.iter().enumerate() {
            enc.add_string(FieldId::Name, vid)?;
            if i < volume_ids.len() - 1 {
                enc.add_u64(FieldId::Limit, 0); // marker for next item
            }
        }
        Ok(enc.into_bytes())
    }

    /// Decode a LookupVolume response
    pub fn decode_lookup_volume_resp(
        msg: &NetMessage,
    ) -> Result<Vec<VolumeLocation>, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let count = dec.next_u64(FieldId::Limit)? as usize;
        let mut locations = Vec::with_capacity(count);

        for _ in 0..count {
            let url = dec.next_string(FieldId::Owner).unwrap_or_default();
            let data_center = dec.next_string(FieldId::Backend).unwrap_or_default();
            locations.push(VolumeLocation { url, data_center });
        }
        Ok(locations)
    }

    /// Encode a Heartbeat request
    pub fn encode_heartbeat_req(
        node_id: &str,
        ip: &str,
        port: u32,
        volumes: &[VolumeShortInfo],
    ) -> Result<Vec<u8>, powerfs_net::NetError> {
        let mut enc = TlvEncoder::new();
        enc.add_string(FieldId::ClientId, node_id)?;
        enc.add_string(FieldId::Owner, ip)?;
        enc.add_u64(FieldId::Blksize, port as u64);
        enc.add_u64(FieldId::Entries, volumes.len() as u64);

        for vol in volumes {
            enc.add_u64(FieldId::Ino, vol.volume_id as u64);
            enc.add_u64(FieldId::Size, vol.size);
            enc.add_u64(FieldId::Mode, vol.read_only as u64);
            enc.add_string(FieldId::Name, &vol.collection)?;
        }
        Ok(enc.into_bytes())
    }

    /// Decode a Heartbeat response
    pub fn decode_heartbeat_resp(
        msg: &NetMessage,
    ) -> Result<HeartbeatResult, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let leader = dec.next_string(FieldId::Owner).unwrap_or_default();
        let volume_size_limit = dec.next_u64(FieldId::Size).unwrap_or(0);
        Ok(HeartbeatResult {
            leader,
            volume_size_limit,
        })
    }

    /// Handle Assign request
    async fn handle_assign(&self, msg: &NetMessage) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let collection = dec
            .next_string(FieldId::Name)
            .unwrap_or_else(|_| "default".to_string());
        let replication = dec
            .next_string(FieldId::Backend)
            .unwrap_or_else(|_| "single".to_string());
        let stripe_count = dec.next_u64(FieldId::Limit).unwrap_or(1) as u32;

        info!(
            "NET_ASSIGN: collection={}, replication={}, stripe_count={}",
            collection, replication, stripe_count
        );

        if !self.master.is_leader().await {
            let leader = self.master.get_leader().await;
            error!(
                "NET_ASSIGN: not leader; current leader is {}, returning error response",
                leader
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                Vec::new(),
                Vec::new(),
            ));
        }

        let result = self.master.assign_volume(&replication, &collection).await;

        match result {
            Ok((fid, nodes)) => {
                let mut enc = TlvEncoder::new();
                enc.add_string(FieldId::Name, &fid.to_string())?;
                if let Some(node) = nodes.first() {
                    enc.add_string(FieldId::Owner, &node.url())?;
                    enc.add_string(FieldId::Backend, &node.data_center_id.to_string())?;
                }
                enc.add_u64(FieldId::Entries, nodes.len() as u64);

                Ok(Self::build_response(
                    msg,
                    STATUS_OK,
                    enc.into_bytes(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                error!("NET_ASSIGN failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle LookupVolume request
    async fn handle_lookup_volume(
        &self,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let volume_id_str = dec.next_string(FieldId::Name).unwrap_or_default();

        info!("NET_LOOKUP_VOLUME: volume_id={}", volume_id_str);

        let vid: u32 = volume_id_str.parse().unwrap_or(0);
        let volume_id = powerfs_common::types::VolumeId(vid);

        if let Some(info) = self.master.get_volume_info(&volume_id) {
            if let Some(node) = self.master.get_node(&info.node_id) {
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Limit, 1); // count
                enc.add_string(FieldId::Owner, &node.url())?;
                enc.add_string(FieldId::Backend, &node.data_center_id.to_string())?;

                return Ok(Self::build_response(
                    msg,
                    STATUS_OK,
                    enc.into_bytes(),
                    Vec::new(),
                ));
            }
        }

        Ok(Self::build_response(
            msg,
            STATUS_ERR_NOT_FOUND,
            Vec::new(),
            Vec::new(),
        ))
    }

    /// Handle Heartbeat request
    async fn handle_heartbeat(
        &self,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let node_id_str = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let ip = dec.next_string(FieldId::Owner).unwrap_or_default();
        let port = dec.next_u64(FieldId::Blksize).unwrap_or(0) as u32;
        let volume_count = dec.next_u64(FieldId::Entries).unwrap_or(0) as usize;

        info!(
            "NET_HEARTBEAT: node={}, ip={}, volumes={}",
            node_id_str, ip, volume_count
        );

        let node_id = powerfs_common::types::NodeId(node_id_str);

        // Parse volumes from request
        let mut volumes = Vec::new();
        for _ in 0..volume_count {
            if let Ok(volume_id) = dec.next_u64(FieldId::Ino) {
                let size = dec.next_u64(FieldId::Size).unwrap_or(0);
                let state = dec.next_u64(FieldId::Mode).unwrap_or(0) as i32;
                let collection = dec.next_string(FieldId::Name).unwrap_or_default();

                volumes.push(VolumeShortInfo {
                    volume_id: volume_id as u32,
                    size,
                    read_only: state == 2, // VolumeState::ReadOnly
                    collection,
                    replica_placement: 1,
                    ttl: 0,
                    disk_type: "ssd".to_string(),
                    used: 0,
                });
            }
        }

        let add_result = self
            .master
            .add_node(crate::master::AddNodeParams {
                node_id: node_id.clone(),
                address: ip.clone(),
                rack: "rack1".to_string(),
                data_center: "dc1".to_string(),
                http_port: port,
                grpc_port: port,
                public_url: format!("http://{}:{}", ip, port),
            })
            .await;

        if let Err(e) = add_result {
            warn!("NET_HEARTBEAT add_node failed: {}", e);
        }

        // Update node volumes
        if !volumes.is_empty() {
            let update_result = self
                .master
                .update_node_volumes(crate::master::UpdateNodeVolumesParams {
                    node_id: node_id.clone(),
                    volumes: volumes.clone(),
                    new_volumes: Vec::new(),
                    deleted_volumes: Vec::new(),
                    ip: ip.clone(),
                    grpc_port: port,
                    http_port: port,
                })
                .await;

            if let Err(e) = update_result {
                warn!("NET_HEARTBEAT update_node_volumes failed: {}", e);
            }
        }

        let leader = self.master.get_leader().await;
        let default_volume_size = powerfs_common::constants::DEFAULT_VOLUME_SIZE;

        let mut enc = TlvEncoder::new();
        enc.add_string(FieldId::Owner, &leader)?;
        enc.add_u64(FieldId::Size, default_volume_size);

        Ok(Self::build_response(
            msg,
            STATUS_OK,
            enc.into_bytes(),
            Vec::new(),
        ))
    }

    /// Handle Lookup request (metadata lookup)
    async fn handle_lookup(&self, msg: &NetMessage) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();

        info!("NET_LOOKUP: parent_ino={}, name={}", parent_ino, name);

        let dt = self.master.directory_tree.clone();
        let lookup_result = spawn_blocking(move || dt.lookup(parent_ino, &name)).await;

        match lookup_result {
            Ok(Some(entry)) => {
                let mut enc = TlvEncoder::new();
                if let Some(attrs) = entry.attributes.as_ref() {
                    enc.add_u64(FieldId::Ino, attrs.ino);
                    enc.add_u64(FieldId::Mode, attrs.mode as u64);
                    enc.add_u64(FieldId::Size, attrs.size);
                    enc.add_u64(FieldId::Uid, attrs.uid as u64);
                    enc.add_u64(FieldId::Gid, attrs.gid as u64);
                    enc.add_u64(FieldId::Mtime, attrs.mtime);
                    enc.add_u64(FieldId::Atime, attrs.atime);
                    enc.add_u64(FieldId::Ctime, attrs.ctime);
                    enc.add_u64(FieldId::Nlink, attrs.nlink as u64);
                }
                Ok(Self::build_response(
                    msg,
                    STATUS_OK,
                    enc.into_bytes(),
                    Vec::new(),
                ))
            }
            Ok(None) => Ok(Self::build_response(
                msg,
                STATUS_ERR_NOT_FOUND,
                Vec::new(),
                Vec::new(),
            )),
            Err(e) => {
                warn!("NET_LOOKUP failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_NOT_FOUND,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Create request
    async fn handle_create(&self, msg: &NetMessage) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();
        let mode = dec.next_u64(FieldId::Mode).unwrap_or(0o644);
        let uid = dec.next_u64(FieldId::Uid).unwrap_or(0);
        let gid = dec.next_u64(FieldId::Gid).unwrap_or(0);

        info!(
            "NET_CREATE: parent_ino={}, name={}, mode={}, uid={}, gid={}",
            parent_ino, name, mode, uid, gid
        );

        let dt = self.master.directory_tree.clone();
        let parent_result = spawn_blocking(move || dt.get_entry_by_inode(parent_ino)).await;

        let parent_path = match parent_result {
            Ok(Some((_, path))) => path,
            _ => {
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_NOT_FOUND,
                    Vec::new(),
                    Vec::new(),
                ));
            }
        };

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let entry = Entry {
            name: name.clone(),
            directory: parent_path,
            attributes: Some(FuseAttributes {
                ino: 0,
                mode: mode as u32,
                nlink: 1,
                uid: uid as u32,
                gid: gid as u32,
                rdev: 0,
                size: 0,
                blksize: 4096,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                perm: (mode as u32) & 0o777,
            }),
            chunks: Vec::new(),
            hard_link_id: String::new(),
            hard_link_counter: 0,
            extended: std::collections::HashMap::new(),
            content_size: 0,
            disk_size: 0,
            ttl: String::new(),
            symlink_target: String::new(),
            owner: String::new(),
            generation: 0,
        };

        let dt = self.master.directory_tree.clone();
        let create_result = spawn_blocking(move || dt.create_entry(entry, "")).await;

        match create_result {
            Ok(Ok(ino)) if ino > 0 => {
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, ino);
                Ok(Self::build_response(
                    msg,
                    STATUS_OK,
                    enc.into_bytes(),
                    Vec::new(),
                ))
            }
            Ok(_) => Ok(Self::build_response(
                msg,
                STATUS_ERR_ALREADY_EXISTS,
                Vec::new(),
                Vec::new(),
            )),
            Err(e) => {
                warn!("NET_CREATE failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_ALREADY_EXISTS,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Mkdir request
    async fn handle_mkdir(&self, msg: &NetMessage) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();
        let mode = dec.next_u64(FieldId::Mode).unwrap_or(0o755);
        let uid = dec.next_u64(FieldId::Uid).unwrap_or(0);
        let gid = dec.next_u64(FieldId::Gid).unwrap_or(0);

        info!(
            "NET_MKDIR: parent_ino={}, name={}, mode={}",
            parent_ino, name, mode
        );

        let dt = self.master.directory_tree.clone();
        let parent_result = spawn_blocking(move || dt.get_entry_by_inode(parent_ino)).await;

        let parent_path = match parent_result {
            Ok(Some((_, path))) => path,
            _ => {
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_NOT_FOUND,
                    Vec::new(),
                    Vec::new(),
                ));
            }
        };

        let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let dir_mode = mode | 0o40000;
        let entry = Entry {
            name: name.clone(),
            directory: parent_path,
            attributes: Some(FuseAttributes {
                ino: 0,
                mode: dir_mode as u32,
                nlink: 2,
                uid: uid as u32,
                gid: gid as u32,
                rdev: 0,
                size: 4096,
                blksize: 4096,
                blocks: 1,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                perm: (dir_mode as u32) & 0o777,
            }),
            chunks: Vec::new(),
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

        let dt = self.master.directory_tree.clone();
        let create_result = spawn_blocking(move || dt.create_entry(entry, "")).await;

        match create_result {
            Ok(Ok(ino)) if ino > 0 => {
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, ino);
                Ok(Self::build_response(
                    msg,
                    STATUS_OK,
                    enc.into_bytes(),
                    Vec::new(),
                ))
            }
            Ok(_) => Ok(Self::build_response(
                msg,
                STATUS_ERR_ALREADY_EXISTS,
                Vec::new(),
                Vec::new(),
            )),
            Err(e) => {
                warn!("NET_MKDIR failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_ALREADY_EXISTS,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Unlink request
    async fn handle_unlink(&self, msg: &NetMessage) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();

        info!("NET_UNLINK: parent_ino={}, name={}", parent_ino, name);

        let dt = self.master.directory_tree.clone();
        let lookup_result = spawn_blocking(move || dt.lookup(parent_ino, &name)).await;

        match lookup_result {
            Ok(Some(entry)) => {
                let ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
                if ino == 0 {
                    return Ok(Self::build_response(
                        msg,
                        STATUS_ERR_NOT_FOUND,
                        Vec::new(),
                        Vec::new(),
                    ));
                }
                let dt = self.master.directory_tree.clone();
                let del_result = spawn_blocking(move || dt.delete_entry(ino, "")).await;
                match del_result {
                    Ok(Ok(true)) => {
                        Ok(Self::build_response(msg, STATUS_OK, Vec::new(), Vec::new()))
                    }
                    Ok(Ok(false)) => Ok(Self::build_response(
                        msg,
                        STATUS_ERR_NOT_FOUND,
                        Vec::new(),
                        Vec::new(),
                    )),
                    Ok(Err(e)) => {
                        warn!("NET_UNLINK delete failed: {}", e);
                        Ok(Self::build_response(
                            msg,
                            STATUS_ERR_SERVER_ERROR,
                            Vec::new(),
                            Vec::new(),
                        ))
                    }
                    Err(e) => {
                        warn!("NET_UNLINK delete failed: {}", e);
                        Ok(Self::build_response(
                            msg,
                            STATUS_ERR_SERVER_ERROR,
                            Vec::new(),
                            Vec::new(),
                        ))
                    }
                }
            }
            Ok(None) => Ok(Self::build_response(
                msg,
                STATUS_ERR_NOT_FOUND,
                Vec::new(),
                Vec::new(),
            )),
            Err(e) => {
                warn!("NET_UNLINK lookup failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_NOT_FOUND,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Rmdir request
    async fn handle_rmdir(&self, msg: &NetMessage) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();

        info!("NET_RMDIR: parent_ino={}, name={}", parent_ino, name);

        let dt = self.master.directory_tree.clone();
        let lookup_result = spawn_blocking(move || dt.lookup(parent_ino, &name)).await;

        match lookup_result {
            Ok(Some(entry)) => {
                let ino = entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0);
                if ino == 0 {
                    return Ok(Self::build_response(
                        msg,
                        STATUS_ERR_NOT_FOUND,
                        Vec::new(),
                        Vec::new(),
                    ));
                }
                let dt = self.master.directory_tree.clone();
                let del_result = spawn_blocking(move || dt.delete_entry(ino, "")).await;
                match del_result {
                    Ok(Ok(true)) => {
                        Ok(Self::build_response(msg, STATUS_OK, Vec::new(), Vec::new()))
                    }
                    Ok(Ok(false)) => Ok(Self::build_response(
                        msg,
                        STATUS_ERR_NOT_FOUND,
                        Vec::new(),
                        Vec::new(),
                    )),
                    Ok(Err(e)) => {
                        warn!("NET_RMDIR delete failed: {}", e);
                        Ok(Self::build_response(
                            msg,
                            STATUS_ERR_SERVER_ERROR,
                            Vec::new(),
                            Vec::new(),
                        ))
                    }
                    Err(e) => {
                        warn!("NET_RMDIR delete failed: {}", e);
                        Ok(Self::build_response(
                            msg,
                            STATUS_ERR_SERVER_ERROR,
                            Vec::new(),
                            Vec::new(),
                        ))
                    }
                }
            }
            Ok(None) => Ok(Self::build_response(
                msg,
                STATUS_ERR_NOT_FOUND,
                Vec::new(),
                Vec::new(),
            )),
            Err(e) => {
                warn!("NET_RMDIR lookup failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_NOT_FOUND,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle ReadDir request
    async fn handle_readdir(&self, msg: &NetMessage) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let limit = dec.next_u64(FieldId::Limit).unwrap_or(0);
        let last_name = dec.next_string(FieldId::LastName).unwrap_or_default();

        info!(
            "NET_READDIR: parent_ino={}, limit={}, last_name={}",
            parent_ino, limit, last_name
        );

        let dt = self.master.directory_tree.clone();
        let list_result =
            spawn_blocking(move || dt.list_entries(parent_ino, limit, &last_name)).await;

        match list_result {
            Ok(entries) => {
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Limit, entries.len() as u64);
                enc.add_u64(FieldId::HasMore, 0);

                for entry in &entries {
                    enc.add_string(FieldId::Name, &entry.name)?;
                    enc.add_u64(
                        FieldId::Ino,
                        entry.attributes.as_ref().map(|a| a.ino).unwrap_or(0),
                    );
                    enc.add_u64(
                        FieldId::Mode,
                        entry.attributes.as_ref().map(|a| a.mode).unwrap_or(0) as u64,
                    );
                }

                Ok(Self::build_response(
                    msg,
                    STATUS_OK,
                    enc.into_bytes(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                warn!("NET_READDIR failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_NOT_FOUND,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle SetAttr request
    async fn handle_setattr(&self, msg: &NetMessage) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let size = dec.next_u64(FieldId::Size).ok();
        let mode = dec.next_u64(FieldId::Mode).ok();
        let uid = dec.next_u64(FieldId::Uid).ok();
        let gid = dec.next_u64(FieldId::Gid).ok();

        info!("NET_SETATTR: ino={}, size={:?}, mode={:?}", ino, size, mode);

        let dt = self.master.directory_tree.clone();
        let lookup_result = spawn_blocking(move || dt.get_entry_by_inode(ino)).await;

        match lookup_result {
            Ok(Some((mut entry, _path))) => {
                if let Some(attrs) = entry.attributes.as_mut() {
                    if let Some(new_size) = size {
                        attrs.size = new_size;
                        entry.content_size = new_size;
                        entry.disk_size = new_size;
                    }
                    if let Some(new_mode) = mode {
                        attrs.mode = new_mode as u32;
                        attrs.perm = (new_mode as u32) & 0o777;
                    }
                    if let Some(new_uid) = uid {
                        attrs.uid = new_uid as u32;
                    }
                    if let Some(new_gid) = gid {
                        attrs.gid = new_gid as u32;
                    }
                }

                let dt = self.master.directory_tree.clone();
                let update_result = spawn_blocking(move || {
                    let old_size = entry.attributes.as_ref().map(|a| a.size).unwrap_or(0);
                    dt.update_entry(entry, "", old_size, false)
                })
                .await;

                match update_result {
                    Ok(Ok(_)) => Ok(Self::build_response(msg, STATUS_OK, Vec::new(), Vec::new())),
                    Ok(Err(e)) => {
                        warn!("NET_SETATTR update failed: {}", e);
                        Ok(Self::build_response(
                            msg,
                            STATUS_ERR_SERVER_ERROR,
                            Vec::new(),
                            Vec::new(),
                        ))
                    }
                    Err(e) => {
                        warn!("NET_SETATTR update failed: {}", e);
                        Ok(Self::build_response(
                            msg,
                            STATUS_ERR_SERVER_ERROR,
                            Vec::new(),
                            Vec::new(),
                        ))
                    }
                }
            }
            Ok(None) => Ok(Self::build_response(
                msg,
                STATUS_ERR_NOT_FOUND,
                Vec::new(),
                Vec::new(),
            )),
            Err(e) => {
                warn!("NET_SETATTR lookup failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_NOT_FOUND,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    /// Helper: build a response message
    fn build_response(msg: &NetMessage, status: u16, body: Vec<u8>, data: Vec<u8>) -> NetMessage {
        let flags = FrameFlags::new(FrameFlags::RESPONSE);
        let header = powerfs_net::FrameHeader::new(
            msg.header.msg_type,
            flags,
            msg.header.seq,
            (body.len() + data.len()) as u32,
        )
        .with_status(status);
        NetMessage::new(header).with_body(body).with_data(data)
    }
}

#[async_trait::async_trait]
impl PowerFsNetHandler for MasterNetHandler {
    async fn handle_request(
        &self,
        client_id: u64,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let msg_type = msg
            .msg_type()
            .ok_or_else(|| powerfs_net::NetError::Protocol("unknown message type".into()))?;

        debug!(
            "NET_MASTER: handling request {:?}, client_id={}, seq={}",
            msg_type, client_id, msg.header.seq
        );

        match msg_type {
            MsgType::Assign => self.handle_assign(msg).await,
            MsgType::LookupVolume => self.handle_lookup_volume(msg).await,
            MsgType::Heartbeat => self.handle_heartbeat(msg).await,
            MsgType::Lookup => self.handle_lookup(msg).await,
            MsgType::Create => self.handle_create(msg).await,
            MsgType::Mkdir => self.handle_mkdir(msg).await,
            MsgType::Unlink => self.handle_unlink(msg).await,
            MsgType::Rmdir => self.handle_rmdir(msg).await,
            MsgType::ReadDir => self.handle_readdir(msg).await,
            MsgType::SetAttr => self.handle_setattr(msg).await,
            MsgType::Ping => {
                let flags = FrameFlags::new(FrameFlags::RESPONSE);
                let header =
                    powerfs_net::FrameHeader::new(msg.header.msg_type, flags, msg.header.seq, 0)
                        .with_status(STATUS_OK);
                Ok(NetMessage::new(header))
            }
            _ => {
                warn!("NET_MASTER: unsupported message type {:?}", msg_type);
                Err(powerfs_net::NetError::UnknownMsgType(msg_type.as_u16()))
            }
        }
    }

    async fn on_connect(&self, client_id: u64, client_type: powerfs_net::ClientType) {
        info!(
            "NET_MASTER: client connected, id={}, type={:?}",
            client_id, client_type
        );
    }

    async fn on_disconnect(&self, client_id: u64) {
        info!("NET_MASTER: client disconnected, id={}", client_id);
    }
}

#[async_trait::async_trait]
impl ServerRequestHandler for MasterNetHandler {
    async fn handle(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> powerfs_net::NetResult<NetMessage> {
        let msg_type = msg
            .msg_type()
            .ok_or_else(|| powerfs_net::NetError::Protocol("unknown message type".into()))?;

        debug!(
            "NET_MASTER: handling request {:?}, trace={}, client_id={}, seq={}",
            msg_type,
            ctx.trace_id(),
            ctx.client.client_id,
            msg.header.seq
        );

        match msg_type {
            MsgType::Assign => self.handle_assign(msg).await,
            MsgType::LookupVolume => self.handle_lookup_volume(msg).await,
            MsgType::Heartbeat => self.handle_heartbeat(msg).await,
            MsgType::Lookup => self.handle_lookup(msg).await,
            MsgType::Create => self.handle_create(msg).await,
            MsgType::Mkdir => self.handle_mkdir(msg).await,
            MsgType::Unlink => self.handle_unlink(msg).await,
            MsgType::Rmdir => self.handle_rmdir(msg).await,
            MsgType::ReadDir => self.handle_readdir(msg).await,
            MsgType::SetAttr => self.handle_setattr(msg).await,
            MsgType::Ping => {
                let flags = FrameFlags::new(FrameFlags::RESPONSE);
                let header =
                    powerfs_net::FrameHeader::new(msg.header.msg_type, flags, msg.header.seq, 0)
                        .with_status(STATUS_OK);
                Ok(NetMessage::new(header))
            }
            _ => {
                warn!("NET_MASTER: unsupported message type {:?}", msg_type);
                Err(powerfs_net::NetError::UnknownMsgType(msg_type.as_u16()))
            }
        }
    }
}

/// Result types for Master net operations
#[derive(Debug, Clone)]
pub struct AssignResult {
    pub fid: String,
    pub location_url: String,
    pub replica_count: usize,
}

#[derive(Debug, Clone)]
pub struct VolumeLocation {
    pub url: String,
    pub data_center: String,
}

#[derive(Debug, Clone)]
pub struct HeartbeatResult {
    pub leader: String,
    pub volume_size_limit: u64,
}
