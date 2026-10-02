use crate::client_auth::{log_auth_failure, ClientAuthenticator};
use crate::server::VolumeServer;
use log::{debug, error, info, warn};
use powerfs_common::types::{Fid, NeedleId, VolumeId};
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::{
    ClientType, FieldId, MsgType, NetHandler, NetMessage, RequestContext, STATUS_ERR_NOT_FOUND,
    STATUS_ERR_NO_SPACE, STATUS_ERR_PERMISSION_DENIED, STATUS_ERR_SERVER_ERROR, STATUS_OK,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct VolumeNetHandler {
    pub volume_server: Arc<VolumeServer>,
    /// Maps session numeric client_id → UUID-based holder string
    client_id_map: Arc<Mutex<HashMap<u64, String>>>,
    /// Phase D.5: verifies credentials presented by external KV clients.
    authenticator: Arc<ClientAuthenticator>,
}

impl VolumeNetHandler {
    pub fn new(volume_server: Arc<VolumeServer>, authenticator: Arc<ClientAuthenticator>) -> Self {
        Self {
            volume_server,
            client_id_map: Arc::new(Mutex::new(HashMap::new())),
            authenticator,
        }
    }

    /// Register a UUID-based holder for a session client_id.
    /// Called when the client first sends a request with FieldId::ClientId (UUID).
    async fn register_holder(&self, session_client_id: u64, holder: &str) {
        if !holder.is_empty() && !Self::is_session_holder(holder) {
            let mut map = self.client_id_map.lock().await;
            map.insert(session_client_id, holder.to_string());
            debug!(
                "NET_VOLUME: registered holder mapping: session={}, holder={}",
                session_client_id, holder
            );
        }
    }

    fn is_session_holder(holder: &str) -> bool {
        holder.starts_with("session-")
    }

    async fn get_holder_for_session(&self, session_client_id: u64) -> Option<String> {
        let map = self.client_id_map.lock().await;
        map.get(&session_client_id).cloned()
    }

    async fn remove_holder_mapping(&self, session_client_id: u64) {
        let mut map = self.client_id_map.lock().await;
        if let Some(holder) = map.remove(&session_client_id) {
            debug!(
                "NET_VOLUME: removed holder mapping: session={}, holder={}",
                session_client_id, holder
            );
        }
    }

    async fn handle_write_needle(
        &self,
        msg: &NetMessage,
        session_client_id: u64,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let volume_id = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let file_key = dec.next_u64(FieldId::FileKey).unwrap_or(0);
        // inode for lease validation (lease is registered by inode, not file_key)
        let inode = dec.next_u64(FieldId::Inode).unwrap_or(file_key);
        // Data is sent in the frame's data segment (not in TLV body).
        // The kernel client sends data via the data segment; fall back to
        // DataLen TLV field for backward compatibility with older clients.
        let data = if !msg.data.is_empty() {
            msg.data.clone()
        } else {
            dec.next_bytes(FieldId::DataLen).unwrap_or_default()
        };
        let lease_token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();
        let holder_client_id = dec
            .next_string(FieldId::ClientId)
            .unwrap_or_else(|_| session_client_id.to_string());
        // Optional trailing field (D.6): absent on older clients → 0.
        // It MUST stay last because the decoder is strictly sequential.
        let cookie = dec.next_u64(FieldId::Cookie).unwrap_or(0);

        // Auto-register UUID holder mapping when client sends a non-session holder
        self.register_holder(session_client_id, &holder_client_id)
            .await;

        debug!(
            "NET_WRITE_NEEDLE: volume_id={}, file_key={}, inode={}, size={}, has_lease={}, holder={}",
            volume_id,
            file_key,
            inode,
            data.len(),
            !lease_token.is_empty(),
            holder_client_id
        );

        if !lease_token.is_empty() {
            // 方案 A: when lease_enabled=false (e.g., NVMe-oF target backend),
            // Volume Server skips range lease validation. Consistency is
            // enforced by Filer's inode metadata lease (UpdateInodeSizeChunks
            // Raft commit) instead.
            if !self.volume_server.lease_enabled {
                debug!(
                    "NET_WRITE_NEEDLE: lease_enabled=false, skip validation for file_key={} inode={}",
                    file_key, inode
                );
            } else {
                let lease_mgr = self.volume_server.range_lease_mgr.clone();
                let validation_result = lease_mgr.validate_token_with_grace_period(
                    &lease_token,
                    &holder_client_id,
                    inode,
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
        }

        let storage_manager = self.volume_server.storage_manager.clone();
        let vid = VolumeId(volume_id);
        let nid = NeedleId(file_key);

        // Distinguish OutOfSpace from other errors so the client can handle
        // ENOSPC without tripping the circuit breaker (volume full is a
        // permanent condition, not a transient failure).
        enum WriteOutcome {
            Ok(Vec<u8>),
            NoSpace,
            ServerError(String),
        }

        match tokio::task::spawn_blocking(move || -> std::result::Result<WriteOutcome, String> {
            let volume = storage_manager
                .get_volume(&vid)
                .ok_or_else(|| format!("volume not found: {}", volume_id))?;
            let write_res = if cookie == 0 {
                volume.write_needle(nid.0, bytes::Bytes::from(data))
            } else {
                volume.write_needle_with_cookie(nid.0, cookie, bytes::Bytes::from(data))
            };
            match write_res {
                Ok(info) => {
                    let mut enc = TlvEncoder::new();
                    enc.add_u64(FieldId::FileKey, info.id.0);
                    Ok(WriteOutcome::Ok(enc.into_bytes()))
                }
                Err(powerfs_common::error::PowerFsError::OutOfSpace) => Ok(WriteOutcome::NoSpace),
                Err(e) => {
                    warn!("write_needle failed: {}", e);
                    Ok(WriteOutcome::ServerError(e.to_string()))
                }
            }
        })
        .await
        {
            Ok(Ok(WriteOutcome::Ok(body))) => {
                Ok(Self::build_response(msg, STATUS_OK, body, Vec::new()))
            }
            Ok(Ok(WriteOutcome::NoSpace)) => {
                warn!("write_needle: volume {} is full (OutOfSpace)", volume_id);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_NO_SPACE,
                    Vec::new(),
                    Vec::new(),
                ))
            }
            Ok(Ok(WriteOutcome::ServerError(msg_str))) => {
                warn!("write_needle server error: {}", msg_str);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
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

    async fn handle_write_needle_blob(
        &self,
        msg: &NetMessage,
        session_client_id: u64,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let volume_id = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let file_key = dec.next_u64(FieldId::FileKey).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Inode).unwrap_or(file_key);
        let offset = dec.next_u64(FieldId::Offset).unwrap_or(0);
        let data = if !msg.data.is_empty() {
            msg.data.clone()
        } else {
            dec.next_bytes(FieldId::DataLen).unwrap_or_default()
        };
        let size = data.len() as u64;
        let lease_token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();
        let holder_client_id = dec
            .next_string(FieldId::ClientId)
            .unwrap_or_else(|_| session_client_id.to_string());

        self.register_holder(session_client_id, &holder_client_id)
            .await;

        info!(
            "NET_WRITE_NEEDLE_BLOB: volume_id={}, file_key={}, inode={}, offset={}, size={}, has_lease={}, holder={}",
            volume_id, file_key, inode, offset, size,
            !lease_token.is_empty(), holder_client_id
        );

        if !lease_token.is_empty() {
            if !self.volume_server.lease_enabled {
                debug!(
                    "NET_WRITE_NEEDLE_BLOB: lease_enabled=false, skip validation for file_key={} inode={}",
                    file_key, inode
                );
            } else {
                let lease_mgr = self.volume_server.range_lease_mgr.clone();
                let result = lease_mgr.validate_token_with_grace_period(
                    &lease_token,
                    &holder_client_id,
                    inode,
                    3000,
                );
                match result {
                    Ok(()) => {}
                    Err(e) => {
                        warn!("NET_WRITE_NEEDLE_BLOB: lease validation failed: {}", e);
                        return Ok(Self::build_response(
                            msg,
                            STATUS_ERR_SERVER_ERROR,
                            Vec::new(),
                            Vec::new(),
                        ));
                    }
                }
            }
        }

        let storage_manager = self.volume_server.storage_manager.clone();
        let vid = VolumeId(volume_id);
        enum BlobOutcome {
            Ok,
            NoSpace,
            ServerError(String),
        }

        match tokio::task::spawn_blocking(move || -> Result<BlobOutcome, String> {
            let volume = storage_manager
                .get_volume(&vid)
                .ok_or_else(|| format!("volume not found: {}", volume_id))?;
            match volume.write_needle_blob(
                file_key,
                offset as i64,
                size as i32,
                bytes::Bytes::from(data),
                0,
            ) {
                Ok(_) => Ok(BlobOutcome::Ok),
                Err(powerfs_common::error::PowerFsError::OutOfSpace) => Ok(BlobOutcome::NoSpace),
                Err(e) => {
                    warn!("write_needle_blob failed: {}", e);
                    Ok(BlobOutcome::ServerError(e.to_string()))
                }
            }
        })
        .await
        {
            Ok(Ok(BlobOutcome::Ok)) => {
                Ok(Self::build_response(msg, STATUS_OK, Vec::new(), Vec::new()))
            }
            Ok(Ok(BlobOutcome::NoSpace)) => {
                warn!(
                    "NET_WRITE_NEEDLE_BLOB: volume {} full (OutOfSpace)",
                    volume_id
                );
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_NO_SPACE,
                    Vec::new(),
                    Vec::new(),
                ))
            }
            Ok(Ok(BlobOutcome::ServerError(msg_str))) => {
                warn!("NET_WRITE_NEEDLE_BLOB server error: {}", msg_str);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
            Ok(Err(e)) => {
                warn!("NET_WRITE_NEEDLE_BLOB inner error: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                error!("NET_WRITE_NEEDLE_BLOB task failed: {}", e);
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
        let volume_id = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let file_key = dec.next_u64(FieldId::FileKey).unwrap_or(0);

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

    /// Phase D.5: verify a KvClient's credentials on its first message.
    ///
    /// On success the connection (whose state lives in powerfs-net's
    /// ClientConn) is marked authenticated; failure leaves it unmarked and
    /// returns PERMISSION_DENIED. Internal failure details go to logs only,
    /// never to the peer beyond a short reason.
    async fn handle_authenticate(
        &self,
        msg: &NetMessage,
        ctx: &mut RequestContext,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        // Fixed wire order (RegistrationToken then ClientCert); the client
        // SDK and the E2E test client both encode it this way.
        let token = if dec.has_field(FieldId::RegistrationToken) {
            dec.next_string(FieldId::RegistrationToken)
                .unwrap_or_default()
        } else {
            String::new()
        };
        let cert_pem = if dec.has_field(FieldId::ClientCert) {
            dec.next_string(FieldId::ClientCert).unwrap_or_default()
        } else {
            String::new()
        };

        let peer_ip = ctx.client.address.ip();
        let peer = peer_ip.to_string();

        match self.authenticator.authenticate(&cert_pem, &token, peer_ip) {
            Ok(()) => {
                // The server path always supplies the connection handle.
                let conn = match ctx.connection.as_ref() {
                    Some(c) => c,
                    None => {
                        // Fail closed: without a handle we cannot persist the
                        // authenticated state, so the peer must not proceed.
                        error!(
                            "CLIENT_AUTH: no connection handle in context for peer={}",
                            peer
                        );
                        return Ok(Self::build_response(
                            msg,
                            STATUS_ERR_SERVER_ERROR,
                            Vec::new(),
                            Vec::new(),
                        ));
                    }
                };
                conn.mark_authenticated();
                info!("CLIENT_AUTH: accepted KvClient peer={}", peer);
                Ok(Self::build_response(msg, STATUS_OK, Vec::new(), Vec::new()))
            }
            Err(failure) => {
                // Detailed reason is logged locally; the peer only sees a
                // generic message (never local CA/configuration details).
                log_auth_failure(&peer, &failure.reason);
                let public = if failure.status == STATUS_ERR_SERVER_ERROR {
                    "server configuration error"
                } else {
                    "authentication failed"
                };
                Ok(Self::build_response(
                    msg,
                    failure.status,
                    public.as_bytes().to_vec(),
                    Vec::new(),
                ))
            }
        }
    }

    /// Phase D.5/D.6: direct needle read for an authenticated KV client.
    ///
    /// Addressed by a full fid string (`volume_id,cookie,file_key`). Gates,
    /// in order: fid syntax → volume ownership → persisted cookie equality
    /// (constant-time capability check; legacy cookie=0 stays fail-closed)
    /// → needle existence. This path does not touch read_count/hits.
    async fn handle_direct_read_needle(
        &self,
        msg: &NetMessage,
        peer: String,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let fid_str = if dec.has_field(FieldId::Fid) {
            dec.next_string(FieldId::Fid).unwrap_or_default()
        } else {
            String::new()
        };

        let fid = match Fid::from_string(&fid_str) {
            Ok(f) => f,
            Err(e) => {
                warn!("DIRECT_READ_NEEDLE: invalid fid {:?}: {}", fid_str, e);
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_PERMISSION_DENIED,
                    b"invalid fid".to_vec(),
                    Vec::new(),
                ));
            }
        };

        info!(
            "DIRECT_READ_NEEDLE: volume_id={}, file_key={}",
            fid.volume_id.0, fid.file_key
        );

        let storage_manager = self.volume_server.storage_manager.clone();

        // Volume ownership gate: this node must host the addressed volume.
        if storage_manager.get_volume(&fid.volume_id).is_none() {
            warn!(
                "DIRECT_READ_NEEDLE: volume {} not hosted on this node",
                fid.volume_id.0
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_PERMISSION_DENIED,
                b"volume not hosted on this node".to_vec(),
                Vec::new(),
            ));
        }

        let sm = storage_manager.clone();
        let vid = fid.volume_id;
        let nid = NeedleId(fid.file_key);

        enum DirectReadOutcome {
            Data(Vec<u8>),
            NotFound,
            CookieMismatch,
        }

        match tokio::task::spawn_blocking(
            move || -> Result<DirectReadOutcome, powerfs_common::error::PowerFsError> {
                let volume = match sm.get_volume(&vid) {
                    Some(v) => v,
                    // Raced a volume removal: the block is unreachable here.
                    None => return Ok(DirectReadOutcome::NotFound),
                };
                // The persisted cookie is the capability check (D.6); it is
                // read before the data so a tampered/guessed fid never
                // reaches the payload. Deleted needles count as not found.
                let persisted_cookie = match volume.read_needle_meta(fid.file_key) {
                    Some(meta) if meta.deleted_at.is_none() => meta.cookie,
                    _ => return Ok(DirectReadOutcome::NotFound),
                };
                if !crate::client_auth::constant_time_eq(
                    &fid.cookie.to_be_bytes(),
                    &persisted_cookie.to_be_bytes(),
                ) {
                    return Ok(DirectReadOutcome::CookieMismatch);
                }
                match volume.read_needle(&nid) {
                    Ok(data) => Ok(DirectReadOutcome::Data(data.to_vec())),
                    Err(powerfs_common::error::PowerFsError::NeedleNotFound(_)) => {
                        Ok(DirectReadOutcome::NotFound)
                    }
                    Err(e) => Err(e),
                }
            },
        )
        .await
        {
            Ok(Ok(DirectReadOutcome::Data(data))) => {
                Ok(Self::build_response(msg, STATUS_OK, Vec::new(), data))
            }
            Ok(Ok(DirectReadOutcome::NotFound)) => Ok(Self::build_response(
                msg,
                STATUS_ERR_NOT_FOUND,
                Vec::new(),
                Vec::new(),
            )),
            Ok(Ok(DirectReadOutcome::CookieMismatch)) => {
                log_auth_failure(&peer, "cookie mismatch");
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_PERMISSION_DENIED,
                    b"invalid fid".to_vec(),
                    Vec::new(),
                ))
            }
            Ok(Err(e)) => {
                warn!("DIRECT_READ_NEEDLE: read error: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                error!("DIRECT_READ_NEEDLE: task failed: {}", e);
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
        let volume_id = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let file_key = dec.next_u64(FieldId::FileKey).unwrap_or(0);

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
                        Err(powerfs_common::error::PowerFsError::NeedleNotFound(_)) => {
                            // Idempotent delete: needle already gone = desired state.
                            // Returning success prevents the circuit breaker from
                            // tripping on expected conditions (e.g., file created and
                            // deleted quickly before data flush, or retry double-delete).
                            debug!("delete_needle: needle not found (idempotent): {}", nid.0);
                            Ok(true)
                        }
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
        let volume_id = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let file_key = dec.next_u64(FieldId::FileKey).unwrap_or(0);
        let inode = dec.next_u64(FieldId::Inode).unwrap_or(file_key);
        let entries = dec.next_u64(FieldId::Entries).unwrap_or(0) as usize;
        let data = dec.next_bytes(FieldId::DataLen).unwrap_or_default();
        let lease_token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();
        let holder_client_id = dec
            .next_string(FieldId::ClientId)
            .unwrap_or_else(|_| session_client_id.to_string());
        // Optional trailing cookie (D.6); strictly sequential decoder.
        let cookie = dec.next_u64(FieldId::Cookie).unwrap_or(0);

        info!(
            "NET_BATCH_WRITE_NEEDLE: volume_id={}, file_key={}, inode={}, entries={}, has_lease={}, holder={}",
            volume_id, file_key, inode, entries, !lease_token.is_empty(), holder_client_id
        );

        if !lease_token.is_empty() {
            // 方案 A: when lease_enabled=false, skip range lease validation.
            if !self.volume_server.lease_enabled {
                debug!(
                    "NET_BATCH_WRITE_NEEDLE: lease_enabled=false, skip validation for file_key={} inode={}",
                    file_key, inode
                );
            } else {
                let lease_mgr = self.volume_server.range_lease_mgr.clone();
                let validation_result = lease_mgr.validate_token_with_grace_period(
                    &lease_token,
                    &holder_client_id,
                    inode,
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
        }

        let storage_manager = self.volume_server.storage_manager.clone();
        let vid = VolumeId(volume_id);

        match tokio::task::spawn_blocking(
            move || -> Result<Option<bool>, powerfs_common::error::PowerFsError> {
                if let Some(volume) = storage_manager.get_volume(&vid) {
                    let write_res = if cookie == 0 {
                        volume.write_needle(file_key, bytes::Bytes::from(data))
                    } else {
                        volume.write_needle_with_cookie(file_key, cookie, bytes::Bytes::from(data))
                    };
                    match write_res {
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
        let volume_id = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let file_key = dec.next_u64(FieldId::FileKey).unwrap_or(0);
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

    /// 轻量级 flush barrier：kernel 在 fsync/release 时调用。
    /// 不再 fsync WAL（由后台统一维护线程负责），
    /// 只触发后台 flush 将指定 needle 从 coalescer 物化到后端数据文件。
    async fn handle_flush_needles(
        &self,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let volume_id = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let count = dec.next_u64(FieldId::Limit).unwrap_or(0);
        let mut file_keys: Vec<u64> = Vec::with_capacity(count as usize);
        for _ in 0..count {
            match dec.next_u64(FieldId::FileKey) {
                Ok(k) => file_keys.push(k),
                Err(_) => break,
            }
        }

        debug!(
            "NET_FLUSH_NEEDLES: volume_id={}, count={} keys",
            volume_id,
            file_keys.len()
        );

        let storage_manager = self.volume_server.storage_manager.clone();
        let vid = VolumeId(volume_id);

        match tokio::task::spawn_blocking(move || -> Result<usize, String> {
            let volume = storage_manager
                .get_volume(&vid)
                .ok_or_else(|| format!("volume not found: {}", volume_id))?;
            let ids: Vec<NeedleId> = file_keys.into_iter().map(NeedleId).collect();
            // flush_specific 只物化数据到后端文件，不 fsync WAL
            volume
                .flush_specific_needles(&ids)
                .map_err(|e| format!("{}", e))
        })
        .await
        {
            Ok(Ok(n)) => {
                debug!(
                    "NET_FLUSH_NEEDLES: volume_id={} materialised {} needles",
                    volume_id, n
                );
                Ok(Self::build_response(msg, STATUS_OK, Vec::new(), Vec::new()))
            }
            Ok(Err(e)) => {
                warn!("NET_FLUSH_NEEDLES server error: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                error!("flush_needles task failed: {}", e);
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

    fn handle_acquire_lease(&self, msg: &NetMessage) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let stripe_start = dec.next_u64(FieldId::Offset).unwrap_or(0);
        let stripe_count = dec.next_u64(FieldId::Limit).unwrap_or(1);
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let exclusive = dec.next_u64(FieldId::Mode).unwrap_or(0) != 0;
        let duration_ms = dec.next_u64(FieldId::LeaseDuration).unwrap_or(30000);

        info!(
            "NET_ACQUIRE_LEASE: inode={}, stripe_start={}, stripe_count={}, client={}, exclusive={}",
            inode, stripe_start, stripe_count, client_id, exclusive
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
                enc.add_u64(FieldId::LeaseDuration, duration_ms);

                Ok(Self::build_response(
                    msg,
                    STATUS_OK,
                    enc.into_bytes(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                warn!("NET_ACQUIRE_LEASE failed: {}", e);
                let mut enc = TlvEncoder::new();
                enc.add_string(FieldId::Owner, &e)?;
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    enc.into_bytes(),
                    Vec::new(),
                ))
            }
        }
    }

    fn handle_acquire_lease_batch(
        &self,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let inode = dec.next_u64(FieldId::Ino).unwrap_or(0);
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let exclusive = dec.next_u64(FieldId::Mode).unwrap_or(0) != 0;
        let duration_ms = dec.next_u64(FieldId::LeaseDuration).unwrap_or(30000);
        let specs_blob = dec.next_bytes(FieldId::LeaseBatchSpecs).unwrap_or_default();

        // Decode specs: each spec is 16 bytes (stripe_start: u64 LE + stripe_count: u64 LE)
        if !specs_blob.len().is_multiple_of(16) {
            warn!(
                "NET_ACQUIRE_LEASE_BATCH: malformed specs blob len={} (not multiple of 16)",
                specs_blob.len()
            );
            let mut enc = TlvEncoder::new();
            enc.add_string(FieldId::Owner, "malformed specs blob")?;
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                enc.into_bytes(),
                Vec::new(),
            ));
        }

        let mut stripe_specs: Vec<(u64, u64)> = Vec::with_capacity(specs_blob.len() / 16);
        for chunk in specs_blob.as_chunks::<16>().0 {
            let start = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
            let count = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
            stripe_specs.push((start, count));
        }

        info!(
            "NET_ACQUIRE_LEASE_BATCH: inode={}, specs={}, client={}, exclusive={}",
            inode,
            stripe_specs.len(),
            client_id,
            exclusive
        );

        match self.volume_server.range_lease_mgr.acquire_batch(
            inode,
            &stripe_specs,
            &client_id,
            duration_ms,
            exclusive,
            64 * 1024 * 1024,
        ) {
            Ok(leases) => {
                // Encode response: Count + flat blob of (token_len: u32 LE + token_bytes + epoch: u64 LE)
                let mut enc = TlvEncoder::new();
                enc.add_u32(FieldId::Count, leases.len() as u32);

                let mut blob = Vec::new();
                for lease in &leases {
                    let token_bytes = lease.token.as_bytes();
                    blob.extend_from_slice(&(token_bytes.len() as u32).to_le_bytes());
                    blob.extend_from_slice(token_bytes);
                    blob.extend_from_slice(&lease.epoch.to_le_bytes());
                }
                enc.add_bytes(FieldId::LeaseBatchSpecs, &blob)?;

                Ok(Self::build_response(
                    msg,
                    STATUS_OK,
                    enc.into_bytes(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                warn!("NET_ACQUIRE_LEASE_BATCH failed: {}", e);
                let mut enc = TlvEncoder::new();
                enc.add_string(FieldId::Owner, &e)?;
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    enc.into_bytes(),
                    Vec::new(),
                ))
            }
        }
    }

    fn handle_release_lease(&self, msg: &NetMessage) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();

        info!("NET_RELEASE_LEASE: token={}, client={}", token, client_id);

        if token.is_empty() {
            return Ok(Self::build_response(msg, STATUS_OK, Vec::new(), Vec::new()));
        }

        match self
            .volume_server
            .range_lease_mgr
            .release(&token, &client_id)
        {
            Ok(()) => Ok(Self::build_response(msg, STATUS_OK, Vec::new(), Vec::new())),
            Err(e) => {
                warn!("NET_RELEASE_LEASE failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    fn handle_renew_lease(&self, msg: &NetMessage) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let token = dec.next_string(FieldId::LeaseToken).unwrap_or_default();
        let client_id = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let duration_ms = dec.next_u64(FieldId::LeaseDuration).unwrap_or(30000);

        info!(
            "NET_RENEW_LEASE: token={}, client={}, duration_ms={}",
            token, client_id, duration_ms
        );

        match self
            .volume_server
            .range_lease_mgr
            .renew(&token, &client_id, duration_ms)
        {
            Ok(()) => {
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::LeaseDuration, duration_ms);
                Ok(Self::build_response(
                    msg,
                    STATUS_OK,
                    enc.into_bytes(),
                    Vec::new(),
                ))
            }
            Err(e) => {
                warn!("NET_RENEW_LEASE failed: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ))
            }
        }
    }

    fn handle_lookup_volume(&self, msg: &NetMessage) -> NetMessage {
        info!("NET_LOOKUP_VOLUME: handling lookup volume request");

        let response_json = r#"{"success":true,"data":{"locations":[]}}"#;
        let response_bytes = response_json.as_bytes().to_vec();

        Self::build_response(msg, STATUS_OK, response_bytes, Vec::new())
    }

    /// 处理 StatFs 请求：返回本 Volume 的容量统计
    fn handle_statfs(&self, msg: &NetMessage) -> NetMessage {
        info!("NET_STATFS: handling statfs request");

        let storage = &self.volume_server.storage_manager;
        let total_space = storage.total_space();
        let used_space = storage.used_space();
        let free_space = storage.free_space();

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Size, total_space);
        enc.add_u64(FieldId::Blocks, used_space);
        enc.add_u64(FieldId::Blksize, free_space);
        enc.add_u64(FieldId::Count, storage.volume_count() as u64);

        let body = enc.into_bytes();

        info!(
            "NET_STATFS: total={}, used={}, free={}, volumes={}",
            total_space,
            used_space,
            free_space,
            storage.volume_count()
        );

        Self::build_response(msg, STATUS_OK, body, Vec::new())
    }

    fn build_response(msg: &NetMessage, status: u16, body: Vec<u8>, data: Vec<u8>) -> NetMessage {
        NetMessage::response(msg, status, body, data)
    }
}

#[async_trait::async_trait]
impl NetHandler for VolumeNetHandler {
    async fn handle(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> powerfs_net::NetResult<NetMessage> {
        let msg_type = msg
            .msg_type()
            .ok_or_else(|| powerfs_net::NetError::Protocol("unknown message type".into()))?;

        let is_kv_client = ctx.client.client_type == ClientType::KvClient;

        // Phase D.5: connection-level gate for external KV clients.
        if is_kv_client {
            match msg_type {
                // Health probe is always permitted and does not authenticate.
                MsgType::Ping => {
                    return Ok(NetMessage::ok_response(msg, Vec::new(), Vec::new()));
                }
                MsgType::Authenticate => {
                    return self.handle_authenticate(msg, ctx).await;
                }
                _ => {
                    let authenticated = ctx
                        .connection
                        .as_ref()
                        .map(|c| c.is_authenticated())
                        .unwrap_or(false);
                    if !authenticated {
                        warn!(
                            "NET_VOLUME: unauthenticated KvClient attempted {:?}",
                            msg_type
                        );
                        return Ok(Self::build_response(
                            msg,
                            STATUS_ERR_PERMISSION_DENIED,
                            b"authentication required".to_vec(),
                            Vec::new(),
                        ));
                    }
                    // Authenticated connection: least privilege — the only
                    // business message a KV client may use is DirectReadNeedle.
                    // Write/delete/lease/StatFs and everything else is refused.
                    if msg_type != MsgType::DirectReadNeedle {
                        warn!(
                            "NET_VOLUME: authenticated KvClient attempted forbidden {:?}",
                            msg_type
                        );
                        return Ok(Self::build_response(
                            msg,
                            STATUS_ERR_PERMISSION_DENIED,
                            b"only DirectReadNeedle permitted for KV clients".to_vec(),
                            Vec::new(),
                        ));
                    }
                    return self
                        .handle_direct_read_needle(msg, ctx.client.address.ip().to_string())
                        .await;
                }
            }
        }

        debug!(
            "NET_VOLUME: handling request {:?}, trace={}, client_id={}, seq={}",
            msg_type,
            ctx.trace_id(),
            ctx.client.client_id,
            msg.header.seq
        );

        match msg_type {
            MsgType::WriteNeedle => self.handle_write_needle(msg, ctx.client.client_id).await,
            MsgType::WriteNeedleBlob => {
                self.handle_write_needle_blob(msg, ctx.client.client_id)
                    .await
            }
            MsgType::ReadNeedle => self.handle_read_needle(msg).await,
            MsgType::DeleteNeedle => self.handle_delete_needle(msg).await,
            MsgType::BatchWriteNeedle => {
                self.handle_batch_write_needle(msg, ctx.client.client_id)
                    .await
            }
            MsgType::ReadNeedleBlob => self.handle_read_needle_blob(msg).await,
            MsgType::FlushNeedles => self.handle_flush_needles(msg).await,
            MsgType::RangeLease => self.handle_range_lease(msg),
            MsgType::AcquireLease => self.handle_acquire_lease(msg),
            MsgType::AcquireLeaseBatch => self.handle_acquire_lease_batch(msg),
            MsgType::ReleaseLease => self.handle_release_lease(msg),
            MsgType::RenewLease => self.handle_renew_lease(msg),
            MsgType::LookupVolume => Ok(self.handle_lookup_volume(msg)),
            MsgType::StatFs => Ok(self.handle_statfs(msg)),
            MsgType::Ping => Ok(NetMessage::ok_response(msg, Vec::new(), Vec::new())),
            MsgType::DirectReadNeedle => {
                // KvClient requests are served (and only reachable) inside
                // the gate above; anything reaching here is a non-Kv client.
                warn!(
                    "NET_VOLUME: DirectReadNeedle from non-Kv client type {:?}",
                    ctx.client.client_type
                );
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_PERMISSION_DENIED,
                    b"direct read not allowed for this client type".to_vec(),
                    Vec::new(),
                ))
            }
            MsgType::Authenticate => {
                // Non-Kv peers never pass the gate into the real handler.
                warn!(
                    "NET_VOLUME: Authenticate from non-Kv client type {:?}",
                    ctx.client.client_type
                );
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_PERMISSION_DENIED,
                    b"authenticate not allowed for this client type".to_vec(),
                    Vec::new(),
                ))
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
        let lease_mgr = self.volume_server.range_lease_mgr.clone();

        // Release session-scoped leases
        let session_holder = format!("session-{}", client_id);
        let mut total_removed = lease_mgr.disconnect_holder(&session_holder);

        // Also release UUID-based holder leases registered by this client
        if let Some(uuid_holder) = self.get_holder_for_session(client_id).await {
            let removed = lease_mgr.disconnect_holder(&uuid_holder);
            if removed > 0 {
                warn!(
                    "NET_VOLUME: released {} UUID-held leases for disconnected client={}",
                    removed, client_id
                );
                total_removed += removed;
            }
            // Clean up the mapping
            self.remove_holder_mapping(client_id).await;
        }

        if total_removed > 0 {
            warn!(
                "NET_VOLUME: released {} total leases for disconnected client={}",
                total_removed, client_id
            );
        }
    }
}
