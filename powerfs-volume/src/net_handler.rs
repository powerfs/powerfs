use crate::server::VolumeServer;
use log::{debug, error, info, warn};
use powerfs_common::types::{NeedleId, VolumeId};
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::{
    FieldId, FrameFlags, MsgType, NetMessage, PowerFsNetHandler, RequestContext,
    ServerRequestHandler, STATUS_ERR_NOT_FOUND, STATUS_ERR_SERVER_ERROR, STATUS_OK,
};
use std::sync::Arc;

pub struct VolumeNetHandler {
    pub volume_server: Arc<VolumeServer>,
}

impl VolumeNetHandler {
    pub fn new(volume_server: Arc<VolumeServer>) -> Self {
        Self { volume_server }
    }

    async fn handle_write_needle(
        &self,
        msg: &NetMessage,
        session_client_id: u64,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let volume_id = dec.next_u64(FieldId::Ino).unwrap_or(0) as u32;
        let file_key = dec.next_u64(FieldId::Name).unwrap_or(0);
        let data = dec.next_bytes(FieldId::DataLen).unwrap_or_default();
        let lease_token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();
        let holder_client_id = dec
            .next_string(FieldId::ClientId)
            .unwrap_or_else(|_| session_client_id.to_string());

        info!(
            "NET_WRITE_NEEDLE: volume_id={}, file_key={}, size={}, has_lease={}, holder={}",
            volume_id,
            file_key,
            data.len(),
            !lease_token.is_empty(),
            holder_client_id
        );

        if !lease_token.is_empty() {
            let lease_mgr = self.volume_server.range_lease_mgr.clone();
            let validation_result = lease_mgr.validate_token_with_grace_period(
                &lease_token,
                &holder_client_id,
                file_key,
                3000,
            );
            match validation_result {
                Ok(()) => {
                    debug!(
                        "NET_WRITE_NEEDLE: lease validated for file_key={}",
                        file_key
                    );
                }
                Err(e) => {
                    warn!("NET_WRITE_NEEDLE: lease validation failed: {}", e);
                    return Ok(Self::build_response(
                        msg,
                        STATUS_ERR_SERVER_ERROR,
                        Vec::new(),
                        Vec::new(),
                    ));
                }
            }
        }

        let storage_manager = self.volume_server.storage_manager.clone();
        let vid = VolumeId(volume_id);
        let nid = NeedleId(file_key);

        match tokio::task::spawn_blocking(
            move || -> Result<Option<Vec<u8>>, powerfs_common::error::PowerFsError> {
                if let Some(volume) = storage_manager.get_volume(&vid) {
                    match volume.write_needle(nid.0, bytes::Bytes::from(data)) {
                        Ok(info) => {
                            let mut enc = TlvEncoder::new();
                            enc.add_u64(FieldId::Name, info.id.0);
                            Ok(Some(enc.into_bytes()))
                        }
                        Err(e) => {
                            warn!("write_needle failed: {}", e);
                            Ok(None)
                        }
                    }
                } else {
                    warn!("write_needle: volume not found: {}", volume_id);
                    Ok(None)
                }
            },
        )
        .await
        {
            Ok(Ok(Some(body))) => Ok(Self::build_response(msg, STATUS_OK, body, Vec::new())),
            Ok(Ok(None)) => Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                Vec::new(),
                Vec::new(),
            )),
            Ok(Err(e)) => {
                warn!("write_needle inner error: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                error!("write_needle task failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    async fn handle_read_needle(
        &self,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let volume_id = dec.next_u64(FieldId::Ino).unwrap_or(0) as u32;
        let file_key = dec.next_u64(FieldId::Name).unwrap_or(0);

        info!(
            "NET_READ_NEEDLE: volume_id={}, file_key={}",
            volume_id, file_key
        );

        let storage_manager = self.volume_server.storage_manager.clone();
        let vid = VolumeId(volume_id);
        let nid = NeedleId(file_key);

        match tokio::task::spawn_blocking(
            move || -> Result<Option<Vec<u8>>, powerfs_common::error::PowerFsError> {
                if let Some(volume) = storage_manager.get_volume(&vid) {
                    match volume.read_needle(&nid) {
                        Ok(data) => Ok(Some(data.to_vec())),
                        Err(e) => {
                            warn!("read_needle failed: {}", e);
                            Ok(None)
                        }
                    }
                } else {
                    warn!("read_needle: volume not found: {}", volume_id);
                    Ok(None)
                }
            },
        )
        .await
        {
            Ok(Ok(Some(data))) => Ok(Self::build_response(msg, STATUS_OK, Vec::new(), data)),
            Ok(Ok(None)) => Ok(Self::build_response(
                msg,
                STATUS_ERR_NOT_FOUND,
                Vec::new(),
                Vec::new(),
            )),
            Ok(Err(e)) => {
                warn!("read_needle inner error: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                error!("read_needle task failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    async fn handle_delete_needle(
        &self,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let volume_id = dec.next_u64(FieldId::Ino).unwrap_or(0) as u32;
        let file_key = dec.next_u64(FieldId::Name).unwrap_or(0);

        info!(
            "NET_DELETE_NEEDLE: volume_id={}, file_key={}",
            volume_id, file_key
        );

        let storage_manager = self.volume_server.storage_manager.clone();
        let vid = VolumeId(volume_id);
        let nid = NeedleId(file_key);

        match tokio::task::spawn_blocking(
            move || -> Result<bool, powerfs_common::error::PowerFsError> {
                if let Some(volume) = storage_manager.get_volume(&vid) {
                    match volume.delete_needle(&nid) {
                        Ok(_) => Ok(true),
                        Err(e) => {
                            warn!("delete_needle failed: {}", e);
                            Ok(false)
                        }
                    }
                } else {
                    warn!("delete_needle: volume not found: {}", volume_id);
                    Ok(false)
                }
            },
        )
        .await
        {
            Ok(Ok(true)) => Ok(Self::build_response(msg, STATUS_OK, Vec::new(), Vec::new())),
            Ok(Ok(false)) => Ok(Self::build_response(
                msg,
                STATUS_ERR_NOT_FOUND,
                Vec::new(),
                Vec::new(),
            )),
            Ok(Err(e)) => {
                warn!("delete_needle inner error: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                error!("delete_needle task failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    async fn handle_batch_write_needle(
        &self,
        msg: &NetMessage,
        session_client_id: u64,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let volume_id = dec.next_u64(FieldId::Ino).unwrap_or(0) as u32;
        let file_key = dec.next_u64(FieldId::Name).unwrap_or(0);
        let entries = dec.next_u64(FieldId::Entries).unwrap_or(0) as usize;
        let data = dec.next_bytes(FieldId::DataLen).unwrap_or_default();
        let lease_token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();
        let holder_client_id = dec
            .next_string(FieldId::ClientId)
            .unwrap_or_else(|_| session_client_id.to_string());

        info!(
            "NET_BATCH_WRITE_NEEDLE: volume_id={}, file_key={}, entries={}, has_lease={}, holder={}",
            volume_id, file_key, entries, !lease_token.is_empty(), holder_client_id
        );

        if !lease_token.is_empty() {
            let lease_mgr = self.volume_server.range_lease_mgr.clone();
            let validation_result = lease_mgr.validate_token_with_grace_period(
                &lease_token,
                &holder_client_id,
                file_key,
                3000,
            );
            match validation_result {
                Ok(()) => {
                    debug!(
                        "NET_BATCH_WRITE_NEEDLE: lease validated for file_key={}",
                        file_key
                    );
                }
                Err(e) => {
                    warn!("NET_BATCH_WRITE_NEEDLE: lease validation failed: {}", e);
                    return Ok(Self::build_response(
                        msg,
                        STATUS_ERR_SERVER_ERROR,
                        Vec::new(),
                        Vec::new(),
                    ));
                }
            }
        }

        let storage_manager = self.volume_server.storage_manager.clone();
        let vid = VolumeId(volume_id);

        match tokio::task::spawn_blocking(
            move || -> Result<Option<bool>, powerfs_common::error::PowerFsError> {
                if let Some(volume) = storage_manager.get_volume(&vid) {
                    match volume.write_needle(file_key, bytes::Bytes::from(data)) {
                        Ok(_) => Ok(Some(true)),
                        Err(e) => {
                            warn!("batch_write_needle failed: {}", e);
                            Ok(None)
                        }
                    }
                } else {
                    warn!("batch_write_needle: volume not found: {}", volume_id);
                    Ok(None)
                }
            },
        )
        .await
        {
            Ok(Ok(Some(_))) => {
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Entries, entries as u64);
                Ok(Self::build_response(
                    msg,
                    STATUS_OK,
                    enc.into_bytes(),
                    Vec::new(),
                ))
            }
            Ok(Ok(None)) => Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                Vec::new(),
                Vec::new(),
            )),
            Ok(Err(e)) => {
                warn!("batch_write_needle inner error: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                error!("batch_write_needle task failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    async fn handle_read_needle_blob(
        &self,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let volume_id = dec.next_u64(FieldId::Ino).unwrap_or(0) as u32;
        let file_key = dec.next_u64(FieldId::Name).unwrap_or(0);
        let offset = dec.next_u64(FieldId::Offset).unwrap_or(0) as i64;
        let size = dec.next_u64(FieldId::Size).unwrap_or(0);

        info!(
            "NET_READ_NEEDLE_BLOB: volume_id={}, file_key={}, offset={}, size={}",
            volume_id, file_key, offset, size
        );

        let storage_manager = self.volume_server.storage_manager.clone();
        let vid = VolumeId(volume_id);

        match tokio::task::spawn_blocking(
            move || -> Result<Option<Vec<u8>>, powerfs_common::error::PowerFsError> {
                if let Some(volume) = storage_manager.get_volume(&vid) {
                    match volume.read_needle_blob(file_key, offset, size as i32) {
                        Ok(data) => Ok(Some(data.to_vec())),
                        Err(e) => {
                            warn!("read_needle_blob failed: {}", e);
                            Ok(None)
                        }
                    }
                } else {
                    warn!("read_needle_blob: volume not found: {}", volume_id);
                    Ok(None)
                }
            },
        )
        .await
        {
            Ok(Ok(Some(data))) => Ok(Self::build_response(msg, STATUS_OK, Vec::new(), data)),
            Ok(Ok(None)) => Ok(Self::build_response(
                msg,
                STATUS_ERR_NOT_FOUND,
                Vec::new(),
                Vec::new(),
            )),
            Ok(Err(e)) => {
                warn!("read_needle_blob inner error: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                error!("read_needle_blob task failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    fn handle_range_lease(&self, msg: &NetMessage) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let stripe_start = dec.next_u64(FieldId::Offset).unwrap_or(0);
        let stripe_count = dec.next_u64(FieldId::Limit).unwrap_or(1);
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let exclusive = dec.next_u64(FieldId::Mode).unwrap_or(0) != 0;
        let duration_ms = dec.next_u64(FieldId::LeaseDuration).unwrap_or(5000);

        info!(
            "NET_RANGE_LEASE: inode={}, stripe_start={}, stripe_count={}, client={}",
            inode, stripe_start, stripe_count, client_id
        );

        match self.volume_server.range_lease_mgr.acquire(
            inode,
            stripe_start,
            stripe_count,
            &client_id,
            duration_ms,
            exclusive,
            64 * 1024 * 1024,
        ) {
            Ok(lease) => {
                let mut enc = TlvEncoder::new();
                enc.add_string(FieldId::LeaseId, &lease.token)?;
                enc.add_u64(FieldId::LeaseEpoch, lease.epoch);

                Ok(Self::build_response(
                    msg,
                    STATUS_OK,
                    enc.into_bytes(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                warn!("NET_RANGE_LEASE failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

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
impl PowerFsNetHandler for VolumeNetHandler {
    async fn handle_request(
        &self,
        client_id: u64,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let msg_type = msg
            .msg_type()
            .ok_or_else(|| powerfs_net::NetError::Protocol("unknown message type".into()))?;

        debug!(
            "NET_VOLUME: handling request {:?}, client_id={}, seq={}",
            msg_type, client_id, msg.header.seq
        );

        match msg_type {
            MsgType::WriteNeedle => self.handle_write_needle(msg, client_id).await,
            MsgType::ReadNeedle => self.handle_read_needle(msg).await,
            MsgType::DeleteNeedle => self.handle_delete_needle(msg).await,
            MsgType::BatchWriteNeedle => self.handle_batch_write_needle(msg, client_id).await,
            MsgType::ReadNeedleBlob => self.handle_read_needle_blob(msg).await,
            MsgType::RangeLease => self.handle_range_lease(msg),
            MsgType::Ping => {
                let flags = FrameFlags::new(FrameFlags::RESPONSE);
                let header =
                    powerfs_net::FrameHeader::new(msg.header.msg_type, flags, msg.header.seq, 0)
                        .with_status(STATUS_OK);
                Ok(NetMessage::new(header))
            }
            _ => {
                warn!("NET_VOLUME: unsupported message type {:?}", msg_type);
                Err(powerfs_net::NetError::UnknownMsgType(msg_type.as_u16()))
            }
        }
    }

    async fn on_connect(&self, client_id: u64, client_type: powerfs_net::ClientType) {
        info!(
            "NET_VOLUME: client connected, id={}, type={:?}",
            client_id, client_type
        );
    }

    async fn on_disconnect(&self, client_id: u64) {
        info!("NET_VOLUME: client disconnected, id={}", client_id);
    }
}

#[async_trait::async_trait]
impl ServerRequestHandler for VolumeNetHandler {
    async fn handle(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> powerfs_net::NetResult<NetMessage> {
        let msg_type = msg
            .msg_type()
            .ok_or_else(|| powerfs_net::NetError::Protocol("unknown message type".into()))?;

        debug!(
            "NET_VOLUME: handling request {:?}, trace={}, client_id={}, seq={}",
            msg_type,
            ctx.trace_id(),
            ctx.client.client_id,
            msg.header.seq
        );

        match msg_type {
            MsgType::WriteNeedle => self.handle_write_needle(msg, ctx.client.client_id).await,
            MsgType::ReadNeedle => self.handle_read_needle(msg).await,
            MsgType::DeleteNeedle => self.handle_delete_needle(msg).await,
            MsgType::BatchWriteNeedle => {
                self.handle_batch_write_needle(msg, ctx.client.client_id)
                    .await
            }
            MsgType::ReadNeedleBlob => self.handle_read_needle_blob(msg).await,
            MsgType::RangeLease => self.handle_range_lease(msg),
            MsgType::Ping => {
                let flags = FrameFlags::new(FrameFlags::RESPONSE);
                let header =
                    powerfs_net::FrameHeader::new(msg.header.msg_type, flags, msg.header.seq, 0)
                        .with_status(STATUS_OK);
                Ok(NetMessage::new(header))
            }
            _ => {
                warn!("NET_VOLUME: unsupported message type {:?}", msg_type);
                Err(powerfs_net::NetError::UnknownMsgType(msg_type.as_u16()))
            }
        }
    }
}
