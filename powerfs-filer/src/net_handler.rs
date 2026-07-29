//! Filer Net Handler - Implements powerfs-net protocol for Filer metadata operations
//!
//! This module provides FilerNetHandler that processes powerfs-net metadata messages
//! using MetaShardManager, which is the authoritative metadata manager with sharded
//! storage, Raft consensus, and CRDT support.

use crate::meta_shard_manager::{MetaShardManager, POSIX_ROOT_INODE};
use crate::raft_group_manager::ShardId;
use crate::shard_store::{FileType, InodeInfo};
use crate::shard_strategy::ShardStrategy;
use log::{debug, info, warn};
use powerfs_net::serialize::{EntryInfo, TlvDecoder, TlvEncoder};
use powerfs_net::{
    ClientType, FieldId, FrameFlags, MsgType, NetMessage, NetResult, PowerFsNetHandler,
    RequestContext, ServerRequestHandler, STATUS_ERR_NOT_FOUND, STATUS_ERR_REDIRECT,
    STATUS_ERR_SERVER_ERROR, STATUS_OK,
};
use std::sync::Arc;

/// Filer Net Handler implementation
pub struct FilerNetHandler {
    pub meta_shard_manager: Arc<MetaShardManager>,
    pub shard_strategy: Arc<ShardStrategy>,
}

impl FilerNetHandler {
    pub fn new(
        meta_shard_manager: Arc<MetaShardManager>,
        shard_strategy: Arc<ShardStrategy>,
    ) -> Self {
        Self {
            meta_shard_manager,
            shard_strategy,
        }
    }

    /// Build a response message
    fn build_response(msg: &NetMessage, status: u16, body: Vec<u8>) -> NetMessage {
        let flags = FrameFlags::new(FrameFlags::RESPONSE);
        let header = powerfs_net::FrameHeader::new(
            msg.header.msg_type,
            flags,
            msg.header.seq,
            body.len() as u32,
        )
        .with_status(status);
        NetMessage::new(header).with_body(body)
    }

    /// Check if current node is the leader for the given shard.
    /// Returns Ok(()) if leader, or Err(redirect_response) if not.
    async fn check_leader(&self, msg: &NetMessage, shard_id: ShardId) -> Result<(), NetMessage> {
        match self
            .meta_shard_manager
            .get_shard_leader_status(shard_id)
            .await
        {
            Some((true, _)) => Ok(()),
            Some((false, leader_addr)) if !leader_addr.is_empty() => {
                // Return redirect response with leader address
                let mut enc = TlvEncoder::new();
                let _ = enc.add_string(FieldId::Owner, &leader_addr);
                Err(Self::build_response(
                    msg,
                    STATUS_ERR_REDIRECT,
                    enc.into_bytes(),
                ))
            }
            _ => {
                // Unknown leader status - cluster not ready, reject write
                warn!(
                    "Leader status unknown for shard {}, rejecting write",
                    shard_id.0
                );
                Err(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Convert InodeInfo to EntryInfo for powerfs-net response
    fn inode_to_entry_info(info: &InodeInfo) -> EntryInfo {
        let is_dir = matches!(info.file_type, FileType::Directory);
        EntryInfo {
            ino: info.inode,
            mode: info.mode,
            uid: info.uid,
            gid: info.gid,
            size: info.size,
            nlink: if is_dir { 2 } else { 1 },
            mtime: info.mtime,
            atime: info.atime,
            ctime: info.ctime,
            name: info.name.clone(),
            is_dir,
            symlink_target: if matches!(info.file_type, FileType::Symlink) {
                Some(info.name.clone())
            } else {
                None
            },
        }
    }

    /// Handle Lookup request
    async fn handle_lookup(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();

        info!(
            "FILER_NET_LOOKUP: seq={}, parent_ino={}, name={}",
            msg.header.seq, parent_ino, name
        );

        // Root lookup - return root entry
        if parent_ino == POSIX_ROOT_INODE && name.is_empty() {
            let mut enc = TlvEncoder::new();
            enc.add_u64(FieldId::Ino, POSIX_ROOT_INODE);
            enc.add_u32(FieldId::Mode, 0o40755);
            enc.add_u32(FieldId::Uid, 0);
            enc.add_u32(FieldId::Gid, 0);
            enc.add_u64(FieldId::Size, 0);
            enc.add_u32(FieldId::Nlink, 2);
            enc.add_string(FieldId::Name, "")?;
            return Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()));
        }

        match self.meta_shard_manager.lookup(parent_ino, name.as_str()) {
            Some(info) => {
                let entry_info = Self::inode_to_entry_info(&info);
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, entry_info.ino);
                enc.add_u32(FieldId::Mode, entry_info.mode);
                enc.add_u32(FieldId::Uid, entry_info.uid);
                enc.add_u32(FieldId::Gid, entry_info.gid);
                enc.add_u64(FieldId::Size, entry_info.size);
                enc.add_u32(FieldId::Nlink, entry_info.nlink);
                enc.add_u64(FieldId::Mtime, entry_info.mtime);
                enc.add_u64(FieldId::Atime, entry_info.atime);
                enc.add_u64(FieldId::Ctime, entry_info.ctime);
                enc.add_string(FieldId::Name, &entry_info.name)?;
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            None => Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new())),
        }
    }

    /// Handle GetAttr request
    async fn handle_getattr(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);

        info!("FILER_NET_GETATTR: ino={}", ino);

        if ino == POSIX_ROOT_INODE {
            let mut enc = TlvEncoder::new();
            enc.add_u64(FieldId::Ino, POSIX_ROOT_INODE);
            enc.add_u32(FieldId::Mode, 0o40755);
            enc.add_u32(FieldId::Uid, 0);
            enc.add_u32(FieldId::Gid, 0);
            enc.add_u64(FieldId::Size, 0);
            enc.add_u32(FieldId::Nlink, 2);
            return Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()));
        }

        match self.meta_shard_manager.get_inode(ino) {
            Some(info) => {
                let entry_info = Self::inode_to_entry_info(&info);
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, entry_info.ino);
                enc.add_u32(FieldId::Mode, entry_info.mode);
                enc.add_u32(FieldId::Uid, entry_info.uid);
                enc.add_u32(FieldId::Gid, entry_info.gid);
                enc.add_u64(FieldId::Size, entry_info.size);
                enc.add_u32(FieldId::Nlink, entry_info.nlink);
                enc.add_u64(FieldId::Mtime, entry_info.mtime);
                enc.add_u64(FieldId::Atime, entry_info.atime);
                enc.add_u64(FieldId::Ctime, entry_info.ctime);
                enc.add_string(FieldId::Name, &entry_info.name)?;
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            None => Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new())),
        }
    }

    /// Handle SetAttr request
    async fn handle_setattr(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let size = dec.next_u64(FieldId::Size).ok();
        let mode = dec.next_u64(FieldId::Mode).ok();
        let uid = dec.next_u64(FieldId::Uid).ok();
        let gid = dec.next_u64(FieldId::Gid).ok();

        info!(
            "FILER_NET_SETATTR: ino={}, size={:?}, mode={:?}, uid={:?}, gid={:?}",
            ino, size, mode, uid, gid
        );

        let shard_id = self.shard_strategy.calculate_shard(ino);
        match self
            .meta_shard_manager
            .setattr(ino, shard_id, size, mode, uid, gid)
            .await
        {
            Ok(_) => Ok(Self::build_response(msg, STATUS_OK, Vec::new())),
            Err(e) => {
                warn!("FILER_NET_SETATTR failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Create request (create file)
    async fn handle_create(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();
        let mode = dec.next_u64(FieldId::Mode).unwrap_or(0o644);
        let uid = dec.next_u64(FieldId::Uid).unwrap_or(0);
        let gid = dec.next_u64(FieldId::Gid).unwrap_or(0);

        info!(
            "FILER_NET_CREATE: parent_ino={}, name={}, mode={:o}, uid={}, gid={}",
            parent_ino, name, mode, uid, gid
        );

        let shard_id = self.shard_strategy.calculate_shard(parent_ino);

        // Check leader - redirect write requests to the correct leader
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_CREATE: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .create_file_with_shard(parent_ino, &name, shard_id)
            .await
        {
            Ok(ino) => {
                // Apply mode/uid/gid via setattr
                let _ = self
                    .meta_shard_manager
                    .setattr(ino, shard_id, None, Some(mode), Some(uid), Some(gid))
                    .await;

                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, ino);
                enc.add_u32(FieldId::Mode, mode as u32);
                enc.add_string(FieldId::Name, &name)?;
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_CREATE failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Mkdir request
    async fn handle_mkdir(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();
        let mode = dec.next_u64(FieldId::Mode).unwrap_or(0o755);
        let uid = dec.next_u64(FieldId::Uid).unwrap_or(0);
        let gid = dec.next_u64(FieldId::Gid).unwrap_or(0);

        info!(
            "FILER_NET_MKDIR: parent_ino={}, name={}, mode={:o}",
            parent_ino, name, mode
        );

        let shard_id = self.shard_strategy.calculate_shard(parent_ino);

        // Check leader - redirect write requests to the correct leader
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_MKDIR: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .create_directory(parent_ino, &name)
            .await
        {
            Ok(info) => {
                let shard_id = self.shard_strategy.calculate_shard(info.inode);
                let _ = self
                    .meta_shard_manager
                    .setattr(info.inode, shard_id, None, Some(mode), Some(uid), Some(gid))
                    .await;

                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, info.inode);
                enc.add_u32(FieldId::Mode, (mode | 0o040000) as u32);
                enc.add_string(FieldId::Name, &name)?;
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_MKDIR failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Unlink request
    async fn handle_unlink(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();

        info!("FILER_NET_UNLINK: parent_ino={}, name={}", parent_ino, name);

        match self.meta_shard_manager.lookup(parent_ino, name.as_str()) {
            Some(info) => {
                let shard_id = self.shard_strategy.calculate_shard(info.inode);

                // Check leader - redirect write requests to the correct leader
                if let Err(redirect) = self.check_leader(msg, shard_id).await {
                    warn!(
                        "FILER_NET_UNLINK: not leader for shard {}, redirecting",
                        shard_id.0
                    );
                    return Ok(redirect);
                }

                match self
                    .meta_shard_manager
                    .delete_file_by_inode(info.inode, shard_id)
                    .await
                {
                    Ok(_) => Ok(Self::build_response(msg, STATUS_OK, Vec::new())),
                    Err(e) => {
                        warn!("FILER_NET_UNLINK failed: {}", e);
                        Ok(Self::build_response(
                            msg,
                            STATUS_ERR_SERVER_ERROR,
                            Vec::new(),
                        ))
                    }
                }
            }
            None => Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new())),
        }
    }

    /// Handle Rmdir request
    async fn handle_rmdir(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();

        info!("FILER_NET_RMDIR: parent_ino={}, name={}", parent_ino, name);

        let shard_id = self.shard_strategy.calculate_shard(parent_ino);

        // Check leader - redirect write requests to the correct leader
        if let Err(redirect) = self.check_leader(msg, shard_id).await {
            warn!(
                "FILER_NET_RMDIR: not leader for shard {}, redirecting",
                shard_id.0
            );
            return Ok(redirect);
        }

        match self
            .meta_shard_manager
            .delete_directory(parent_ino, &name)
            .await
        {
            Ok(_) => Ok(Self::build_response(msg, STATUS_OK, Vec::new())),
            Err(e) => {
                warn!("FILER_NET_RMDIR failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Rename request
    async fn handle_rename(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let old_parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let old_name = dec.next_string(FieldId::Name).unwrap_or_default();
        let new_parent_ino = dec.next_u64(FieldId::NewParentIno).unwrap_or(0);
        let new_name = dec.next_string(FieldId::NewName).unwrap_or_default();

        info!(
            "FILER_NET_RENAME: old_parent={}, old_name={}, new_parent={}, new_name={}",
            old_parent_ino, old_name, new_parent_ino, new_name
        );

        match self
            .meta_shard_manager
            .rename(old_parent_ino, &old_name, new_parent_ino, &new_name)
            .await
        {
            Ok(_) => Ok(Self::build_response(msg, STATUS_OK, Vec::new())),
            Err(e) => {
                warn!("FILER_NET_RENAME failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle ReadDir request
    async fn handle_readdir(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let limit = dec.next_u64(FieldId::Limit).unwrap_or(1000);
        let last_name = dec.next_string(FieldId::LastName).unwrap_or_default();

        info!(
            "FILER_NET_READDIR: parent_ino={}, limit={}, last_name={}",
            parent_ino, limit, last_name
        );

        let entries = self.meta_shard_manager.list_directory(parent_ino);

        // Filter by last_name for pagination
        let filtered: Vec<&InodeInfo> = if last_name.is_empty() {
            entries.iter().collect()
        } else {
            entries
                .iter()
                .filter(|e| e.name.as_str() > last_name.as_str())
                .collect()
        };

        let limited: Vec<&InodeInfo> = filtered.into_iter().take(limit as usize).collect();
        let has_more = (limited.len() as u64) < limit && !entries.is_empty();

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Limit, limited.len() as u64);
        enc.add_u64(FieldId::HasMore, if has_more { 1 } else { 0 });

        for entry in &limited {
            enc.add_string(FieldId::Name, &entry.name)?;
            enc.add_u64(FieldId::Ino, entry.inode);
            enc.add_u32(FieldId::Mode, entry.mode);
        }

        Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
    }

    /// Handle StatFs request
    async fn handle_statfs(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Size, 1024 * 1024 * 1024); // 1TB
        enc.add_u64(FieldId::Blksize, 4096);
        Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
    }

    /// Handle Symlink request
    async fn handle_symlink(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let name = dec.next_string(FieldId::Name).unwrap_or_default();
        let target = dec.next_string(FieldId::SymlinkTarget).unwrap_or_default();

        info!(
            "FILER_NET_SYMLINK: parent_ino={}, name={}, target={}",
            parent_ino, name, target
        );

        match self
            .meta_shard_manager
            .create_symlink(parent_ino, &name, &target)
            .await
        {
            Ok(info) => {
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, info.inode);
                enc.add_string(FieldId::Name, &name)?;
                enc.add_string(FieldId::SymlinkTarget, &target)?;
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_SYMLINK failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }

    /// Handle Readlink request
    async fn handle_readlink(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);

        info!("FILER_NET_READLINK: ino={}", ino);

        match self.meta_shard_manager.get_inode(ino) {
            Some(info) => {
                let target = info.symlink_target.unwrap_or_default();
                let mut enc = TlvEncoder::new();
                enc.add_string(FieldId::SymlinkTarget, &target)?;
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            None => Ok(Self::build_response(msg, STATUS_ERR_NOT_FOUND, Vec::new())),
        }
    }

    /// Handle Link request (hard link)
    async fn handle_link(&self, msg: &NetMessage) -> NetResult<NetMessage> {
        let mut dec = TlvDecoder::new(&msg.body);
        let ino = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let new_parent_ino = dec.next_u64(FieldId::ParentIno).unwrap_or(0);
        let new_name = dec.next_string(FieldId::Name).unwrap_or_default();

        info!(
            "FILER_NET_LINK: ino={}, new_parent={}, new_name={}",
            ino, new_parent_ino, new_name
        );

        match self
            .meta_shard_manager
            .create_hard_link(ino, new_parent_ino, &new_name)
            .await
        {
            Ok(_) => {
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Ino, ino);
                Ok(Self::build_response(msg, STATUS_OK, enc.into_bytes()))
            }
            Err(e) => {
                warn!("FILER_NET_LINK failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                ))
            }
        }
    }
}

#[async_trait::async_trait]
impl PowerFsNetHandler for FilerNetHandler {
    async fn handle_request(&self, client_id: u64, msg: &NetMessage) -> NetResult<NetMessage> {
        let msg_type = msg
            .msg_type()
            .ok_or_else(|| powerfs_net::NetError::Protocol("unknown message type".into()))?;

        debug!(
            "FILER_NET: handling request {:?}, client_id={}, seq={}",
            msg_type, client_id, msg.header.seq
        );

        match msg_type {
            MsgType::Lookup => self.handle_lookup(msg).await,
            MsgType::GetAttr => self.handle_getattr(msg).await,
            MsgType::SetAttr => self.handle_setattr(msg).await,
            MsgType::Create => self.handle_create(msg).await,
            MsgType::Mkdir => self.handle_mkdir(msg).await,
            MsgType::Unlink => self.handle_unlink(msg).await,
            MsgType::Rmdir => self.handle_rmdir(msg).await,
            MsgType::Rename => self.handle_rename(msg).await,
            MsgType::ReadDir => self.handle_readdir(msg).await,
            MsgType::StatFs => self.handle_statfs(msg).await,
            MsgType::Symlink => self.handle_symlink(msg).await,
            MsgType::Readlink => self.handle_readlink(msg).await,
            MsgType::Link => self.handle_link(msg).await,
            // AssignVolumeV2 removed - volume assignment is handled by Master via MsgType::Assign
            MsgType::Ping => {
                let flags = FrameFlags::new(FrameFlags::RESPONSE);
                let header =
                    powerfs_net::FrameHeader::new(msg.header.msg_type, flags, msg.header.seq, 0)
                        .with_status(STATUS_OK);
                Ok(NetMessage::new(header))
            }
            _ => {
                warn!("FILER_NET: unsupported message type {:?}", msg_type);
                Err(powerfs_net::NetError::UnknownMsgType(msg_type.as_u16()))
            }
        }
    }

    async fn on_connect(&self, _client_id: u64, _client_type: ClientType) {
        info!(
            "FILER_NET: client connected, id={}, type={:?}",
            _client_id, _client_type
        );
    }
}

#[async_trait::async_trait]
impl ServerRequestHandler for FilerNetHandler {
    async fn handle(&self, ctx: &mut RequestContext, msg: &NetMessage) -> NetResult<NetMessage> {
        let msg_type = msg
            .msg_type()
            .ok_or_else(|| powerfs_net::NetError::Protocol("unknown message type".into()))?;

        debug!(
            "FILER_NET: handling request {:?}, trace={}, client_id={}, seq={}",
            msg_type,
            ctx.trace_id(),
            ctx.client.client_id,
            msg.header.seq
        );

        match msg_type {
            MsgType::Lookup => self.handle_lookup(msg).await,
            MsgType::GetAttr => self.handle_getattr(msg).await,
            MsgType::SetAttr => self.handle_setattr(msg).await,
            MsgType::Create => self.handle_create(msg).await,
            MsgType::Mkdir => self.handle_mkdir(msg).await,
            MsgType::Unlink => self.handle_unlink(msg).await,
            MsgType::Rmdir => self.handle_rmdir(msg).await,
            MsgType::Rename => self.handle_rename(msg).await,
            MsgType::ReadDir => self.handle_readdir(msg).await,
            MsgType::StatFs => self.handle_statfs(msg).await,
            MsgType::Symlink => self.handle_symlink(msg).await,
            MsgType::Readlink => self.handle_readlink(msg).await,
            MsgType::Link => self.handle_link(msg).await,
            // AssignVolumeV2 removed - volume assignment is handled by Master via MsgType::Assign
            MsgType::Ping => {
                let flags = FrameFlags::new(FrameFlags::RESPONSE);
                let header =
                    powerfs_net::FrameHeader::new(msg.header.msg_type, flags, msg.header.seq, 0)
                        .with_status(STATUS_OK);
                Ok(NetMessage::new(header))
            }
            _ => {
                warn!("FILER_NET: unsupported message type {:?}", msg_type);
                Err(powerfs_net::NetError::UnknownMsgType(msg_type.as_u16()))
            }
        }
    }
}
