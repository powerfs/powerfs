//! Master Net Handler - Implements powerfs-net protocol for metadata operations
//!
//! This module provides MasterNetHandler that processes powerfs-net messages
//! and delegates to MasterNode for actual business logic.

use crate::master::MasterNode;
use crate::proto::powerfs::VolumeShortInfo;
use log::{debug, error, info, warn};
use powerfs_allocator::{ShardMap, ShardState};
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::{
    FieldId, MsgType, NetHandler, NetMessage, NetResult, RequestContext, STATUS_ERR_BAD_REQUEST,
    STATUS_ERR_NOT_FOUND, STATUS_ERR_PERMISSION_DENIED, STATUS_ERR_REDIRECT,
    STATUS_ERR_SERVER_ERROR, STATUS_OK,
};
use std::sync::Arc;

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
        net_port: u32,
        volumes: &[VolumeShortInfo],
    ) -> Result<Vec<u8>, powerfs_net::NetError> {
        let mut enc = TlvEncoder::new();
        enc.add_string(FieldId::ClientId, node_id)?;
        enc.add_string(FieldId::Owner, ip)?;
        enc.add_u64(FieldId::Blksize, port as u64);
        enc.add_u64(FieldId::NetPort, net_port as u64);
        enc.add_u64(FieldId::Entries, volumes.len() as u64);

        for vol in volumes {
            enc.add_u64(FieldId::Ino, vol.volume_id);
            enc.add_u64(FieldId::Size, vol.size);
            enc.add_u64(FieldId::Mode, vol.read_only as u64);
            enc.add_string(FieldId::Name, &vol.collection)?;
            enc.add_u64(FieldId::UsedSpace, vol.used);
            enc.add_u64(FieldId::FileCount, vol.file_count);
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
    async fn handle_assign(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let _ = ctx;
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

        if let Err(resp) = self.check_raft_available(msg, "NET_ASSIGN").await {
            return resp;
        }

        let result = self.master.assign_volume(&replication, &collection).await;

        match result {
            Ok((fid, nodes)) => {
                let mut enc = TlvEncoder::new();
                // Return structured fields so the client can directly use them
                let _ = enc.add_u64(FieldId::VolumeId, fid.volume_id.0);
                let _ = enc.add_u64(FieldId::Cookie, fid.cookie);
                let _ = enc.add_u64(FieldId::FileKey, fid.file_key);
                // Use volume route addr (net_port) instead of node.url() (http_port)
                // The FUSE client connects via powerfs-net protocol, not HTTP
                let route_addr = self
                    .master
                    .get_volume_route(fid.volume_id.0)
                    .map(|r| r.addr)
                    .unwrap_or_else(|| nodes.first().map(|n| n.url()).unwrap_or_default());
                let _ = enc.add_string(FieldId::Owner, &route_addr);
                let _ = enc.add_u64(FieldId::Entries, nodes.len() as u64);

                info!(
                    "NET_ASSIGN: assigned volume_id={}, cookie={}, file_key={}, route_addr={}, nodes={}",
                    fid.volume_id.0,
                    fid.cookie,
                    fid.file_key,
                    route_addr,
                    nodes.len()
                );

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
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let _ = ctx;
        let mut dec = TlvDecoder::new(&msg.body);
        let volume_id_str = dec.next_string(FieldId::Name).unwrap_or_default();

        info!("NET_LOOKUP_VOLUME: volume_id={}", volume_id_str);

        // Volume lookup reads topology state. Redirect non-leader requests
        // to ensure the client gets up-to-date volume routing from the leader.
        if let Err(resp) = self.check_raft_available(msg, "NET_LOOKUP_VOLUME").await {
            return resp;
        }

        let original_id: u64 = volume_id_str.parse().unwrap_or(0);

        // Look up volume info. Modern volume servers use UUID-based IDs
        // (e.g. 6941703278889880408) which are stored verbatim in
        // `self.volumes`, so try an exact match first. The legacy
        // `get_volume_info_by_original_id` (composite_id % 1000) path is
        // kept as a fallback for old deployments that still use the
        // node_seq * 1000 + original_id encoding.
        let info = self
            .master
            .get_volume_info(&powerfs_common::types::VolumeId(original_id))
            .or_else(|| self.master.get_volume_info_by_original_id(original_id));

        if let Some(info) = info {
            info!(
                "NET_LOOKUP_VOLUME: found volume info for id={}, volume_id={}, node_id={}",
                original_id, info.id.0, info.node_id.0
            );

            // Prefer the volume route address (ip:net_port) since FUSE
            // clients connect via powerfs-net, not HTTP. Fall back to the
            // node's HTTP url only if no route is registered.
            let route_addr = self
                .master
                .get_volume_route(info.id.0)
                .map(|r| r.addr)
                .or_else(|| self.master.get_node(&info.node_id).map(|n| n.url()));

            if let Some(addr) = route_addr {
                info!(
                    "NET_LOOKUP_VOLUME: returning route addr={} for volume_id={}",
                    addr, info.id.0
                );
                let mut enc = TlvEncoder::new();
                enc.add_u64(FieldId::Limit, 1); // count
                enc.add_string(FieldId::Owner, &addr)?;
                enc.add_string(FieldId::Backend, &info.node_id.0.to_string())?;

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
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let node_id_str = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let ip = dec.next_string(FieldId::Owner).unwrap_or_default();
        let port = dec.next_u64(FieldId::Blksize).unwrap_or(0) as u32;
        let net_port = dec.next_u64(FieldId::NetPort).unwrap_or(0) as u32;
        let volume_count = dec.next_u64(FieldId::Entries).unwrap_or(0) as usize;

        // P5: Parse node load metrics (basis points 0-10000 → ratio 0.0-1.0).
        // Absent for pre-P5 volume servers; defaults to 0.0.
        //
        // Use has_field() to peek at the next field before reading. Calling
        // next_u64() on a mismatched field corrupts the decoder position
        // (the header is consumed but the value is not), which cascades into
        // garbled volume parsing downstream. has_field() checks without
        // consuming, so absent optional fields leave the decoder aligned at
        // the next present field.
        let cpu_bps = if dec.has_field(FieldId::CpuUsage) {
            dec.next_u64(FieldId::CpuUsage).unwrap_or(0)
        } else {
            0
        };
        let mem_bps = if dec.has_field(FieldId::MemoryUsage) {
            dec.next_u64(FieldId::MemoryUsage).unwrap_or(0)
        } else {
            0
        };
        let cpu_usage = (cpu_bps as f32) / 10000.0;
        let memory_usage = (mem_bps as f32) / 10000.0;
        // Registration token for node authentication. Absent on old clients
        // (dev mode allows empty token when master has no token configured).
        let reg_token = if dec.has_field(FieldId::RegistrationToken) {
            dec.next_string(FieldId::RegistrationToken)
                .unwrap_or_default()
        } else {
            String::new()
        };
        // Client certificate (PEM) for production node authentication.
        let client_cert_pem = if dec.has_field(FieldId::ClientCert) {
            dec.next_string(FieldId::ClientCert).unwrap_or_default()
        } else {
            String::new()
        };

        info!(
            "NET_HEARTBEAT: node={}, ip={}, volumes={}, cpu={:.1}%, mem={:.1}%",
            node_id_str,
            ip,
            volume_count,
            cpu_usage * 100.0,
            memory_usage * 100.0
        );

        // Authenticate the node before processing the heartbeat. This runs
        // before the leader check so unauthorized nodes are rejected even by
        // followers (no redirect to leader for bad tokens).
        if !self.master.verify_registration_token(&reg_token) {
            warn!(
                "NET_HEARTBEAT: rejected node={} — invalid registration token",
                node_id_str
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_PERMISSION_DENIED,
                b"invalid registration token".to_vec(),
                Vec::new(),
            ));
        }

        // ---- Certificate enforcement (production) ----
        //
        // When the master has a live CA manager, all Heartbeat (volume
        // server) requests MUST carry a ClientCert(0xD4) TLV validated
        // through the same 4-level chain as RegisterClient.  The cert's
        // client_name MUST match the volume server's node_id.
        let peer_ip = ctx.client.address.ip().to_string();
        if self.master.cert_enforcement_enabled() {
            if client_cert_pem.is_empty() {
                warn!(
                    "NET_HEARTBEAT: rejected node={} peer={} — missing ClientCert(0xD4) TLV (cert enforcement ON)",
                    node_id_str, peer_ip
                );
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_PERMISSION_DENIED,
                    b"volume server certificate required (0xD4 PEM missing)".to_vec(),
                    Vec::new(),
                ));
            }
            if let Err(e) = self.master.validate_server_node_cert(
                &client_cert_pem,
                Some(&peer_ip),
                &node_id_str,
            ) {
                warn!(
                    "NET_HEARTBEAT: rejected node={} peer={} — {}",
                    node_id_str, peer_ip, e
                );
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_PERMISSION_DENIED,
                    format!("volume server certificate rejected: {}", e).into_bytes(),
                    Vec::new(),
                ));
            }
        } else if client_cert_pem.is_empty() {
            // Dev mode: warn once if cert absent, still allow.
            warn!(
                "NET_HEARTBEAT: cert not provided by node={} peer={} — ALLOWED (dev mode, no CA configured)",
                node_id_str, peer_ip
            );
        }

        // Heartbeat mutates Master topology state (add_node, volume registration).
        // Only the Raft leader should process it; followers return REDIRECT.
        if let Err(resp) = self.check_raft_available(msg, "NET_HEARTBEAT").await {
            return resp;
        }

        let node_id = powerfs_common::types::NodeId(node_id_str);

        // Parse volumes from request
        let mut volumes = Vec::new();
        for _ in 0..volume_count {
            if let Ok(volume_id) = dec.next_u64(FieldId::Ino) {
                let size = dec.next_u64(FieldId::Size).unwrap_or(0);
                let state = dec.next_u64(FieldId::Mode).unwrap_or(0) as i32;
                let collection = dec.next_string(FieldId::Name).unwrap_or_default();
                let used = dec.next_u64(FieldId::UsedSpace).unwrap_or(0);
                let file_count = dec.next_u64(FieldId::FileCount).unwrap_or(0);

                volumes.push(VolumeShortInfo {
                    volume_id,
                    size,
                    read_only: state == 2, // VolumeState::ReadOnly
                    collection,
                    replica_placement: 1,
                    ttl: 0,
                    disk_type: "ssd".to_string(),
                    used,
                    file_count,
                    compact_status: 0,
                    append_offset: 0,
                });
            }
        }

        // DataNodeInfo.grpc_port is used by callers (S3 PutObject/Get/Delete,
        // apply_assign_volume, kv_cache_service) to build the needle-write
        // address "ip:grpc_port". It must hold the powerfs-net DATA port
        // (net_port, e.g. 8901), NOT the HTTP metrics port (http_port, e.g.
        // 8093). Falling back to http_port only when the volume is a legacy
        // node that doesn't report net_port.
        let data_port = if net_port > 0 { net_port } else { port };

        let add_result = self
            .master
            .add_node(crate::master::AddNodeParams {
                node_id: node_id.clone(),
                address: ip.clone(),
                rack: "rack1".to_string(),
                data_center: "dc1".to_string(),
                http_port: port,
                grpc_port: data_port,
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
                    grpc_port: data_port,
                    http_port: port,
                    net_port,
                })
                .await;

            if let Err(e) = update_result {
                warn!("NET_HEARTBEAT update_node_volumes failed: {}", e);
            }
        }

        // P5: Update node load metrics (local, non-Raft — ephemeral monitoring
        // data that doesn't need strong consistency). Stored on DataNodeInfo
        // in the leader's in-memory topology.
        self.master
            .update_node_load_metrics(&node_id, cpu_usage, memory_usage);

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

    /// Handle KeepConnected request from a TLV FUSE/kernel client.
    ///
    /// This is the TLV equivalent of the gRPC `keep_connected` bidi
    /// stream's inbound `KeepConnectedRequest`.  The client periodically
    /// sends this message to (a) register itself with the Master and
    /// (b) refresh its heartbeat/stats.  Topology updates are pushed
    /// back asynchronously via `TopologyChanged` NOTIFY frames, so this
    /// method only needs to return the current leader.
    async fn handle_keep_connected(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let _ = ctx;
        let mut dec = TlvDecoder::new(&msg.body);
        let client_id = dec.next_string(FieldId::ClientUuid).unwrap_or_default();
        let client_type = dec
            .next_string(FieldId::Backend)
            .unwrap_or_else(|_| "fuse".to_string());
        let mount_point = dec.next_string(FieldId::Name).unwrap_or_default();
        let collection = dec.next_string(FieldId::Collection).unwrap_or_default();
        let replication = dec.next_string(FieldId::Replication).unwrap_or_default();
        let host = dec.next_string(FieldId::Owner).unwrap_or_default();
        let pid = dec.next_u64(FieldId::Limit).unwrap_or(0);

        // KeepConnected registers/refreshes FUSE client info on the Master.
        // Only the leader should process this to maintain a consistent client
        // registry; followers return REDIRECT.
        if let Err(resp) = self.check_raft_available(msg, "NET_KEEP_CONNECTED").await {
            return resp;
        }

        if client_id.is_empty() {
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                Vec::new(),
                Vec::new(),
            ));
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let fuse_info = crate::master::FuseClientInfo {
            client_id: client_id.clone(),
            assigned_client_id: 0,
            client_type,
            mount_point,
            collection,
            replication,
            host,
            pid,
            connected_at: now,
            last_heartbeat: now,
            dirty_chunks: 0,
            dirty_bytes: 0,
            stats: None,
        };
        self.master.register_fuse_client(fuse_info);

        debug!(
            "NET_KEEP_CONNECTED: registered/refreshed fuse client {}",
            client_id
        );

        let leader = self.master.get_leader().await;
        let mut enc = TlvEncoder::new();
        enc.add_string(FieldId::Owner, &leader)?;

        Ok(Self::build_response(
            msg,
            STATUS_OK,
            enc.into_bytes(),
            Vec::new(),
        ))
    }

    async fn handle_register_client(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let client_uuid = dec.next_string(FieldId::ClientUuid).unwrap_or_default();
        let client_type = dec
            .next_string(FieldId::Backend)
            .unwrap_or_else(|_| "fuse".to_string());
        let mount_point = dec.next_string(FieldId::Name).unwrap_or_default();
        let collection = dec.next_string(FieldId::Collection).unwrap_or_default();
        let replication = dec.next_string(FieldId::Replication).unwrap_or_default();
        let host = dec.next_string(FieldId::Owner).unwrap_or_default();
        let pid = dec.next_u64(FieldId::Limit).unwrap_or(0);
        let client_cert_pem = dec.next_string(FieldId::ClientCert).unwrap_or_default();

        // ---- Certificate enforcement (production) ----
        //
        // When the master has a live CA manager, all RegisterClient
        // requests MUST carry a ClientCert(0xD4) TLV that:
        //   * was issued by this master (fingerprint present in the
        //     persistent client registry),
        //   * has the caller's source IP listed in san_ips (prevents the
        //     cert being copied to a different node),
        //   * has the mount-point Name recorded in mount_dirs (prevents
        //     reuse on a different directory).
        // When enforcement is disabled (ca_dir not configured → dev mode)
        // we skip the check and emit a single warning per connection.
        let peer_ip = ctx.client.address.ip().to_string();
        let cert_client_name = if self.master.cert_enforcement_enabled() {
            if client_cert_pem.is_empty() {
                warn!(
                    "NET_REGISTER_CLIENT: rejected uuid={} peer={} — missing ClientCert(0xD4) TLV (cert enforcement ON)",
                    client_uuid, peer_ip
                );
                let mut enc = TlvEncoder::new();
                enc.add_u8(FieldId::MountAllowed, 0);
                enc.add_string(
                    FieldId::Message,
                    "client certificate required (0xD4 PEM missing)",
                )?;
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_PERMISSION_DENIED,
                    enc.into_bytes(),
                    Vec::new(),
                ));
            }
            match self
                .master
                .validate_client_cert(&client_cert_pem, Some(&peer_ip), &mount_point)
            {
                Ok(name) => Some(name),
                Err(e) => {
                    warn!(
                        "NET_REGISTER_CLIENT: rejected uuid={} peer={} mount='{}' — {}",
                        client_uuid, peer_ip, mount_point, e
                    );
                    let mut enc = TlvEncoder::new();
                    enc.add_u8(FieldId::MountAllowed, 0);
                    enc.add_string(
                        FieldId::Message,
                        &format!("client certificate rejected: {}", e),
                    )?;
                    return Ok(Self::build_response(
                        msg,
                        STATUS_ERR_PERMISSION_DENIED,
                        enc.into_bytes(),
                        Vec::new(),
                    ));
                }
            }
        } else {
            // Dev mode: warn once per uuid if cert absent, still allow.
            if client_cert_pem.is_empty() {
                warn!(
                    "NET_REGISTER_CLIENT: cert not provided by uuid={} peer={} — ALLOWED (dev mode, no CA configured)",
                    client_uuid, peer_ip
                );
            }
            None
        };

        if let Err(resp) = self.check_raft_available(msg, "NET_REGISTER_CLIENT").await {
            return resp;
        }

        if client_uuid.is_empty() {
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                Vec::new(),
                Vec::new(),
            ));
        }

        if self.master.is_client_blacklisted(&client_uuid) {
            let leader = self.master.get_leader().await;
            let mut enc = TlvEncoder::new();
            enc.add_u8(FieldId::MountAllowed, 0);
            enc.add_string(FieldId::Message, "client blacklisted by admin")?;
            enc.add_string(FieldId::Owner, &leader)?;
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_PERMISSION_DENIED,
                enc.into_bytes(),
                Vec::new(),
            ));
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let fuse_info = crate::master::FuseClientInfo {
            client_id: client_uuid.clone(),
            assigned_client_id: 0,
            client_type,
            mount_point: mount_point.clone(),
            collection,
            replication,
            host,
            pid,
            connected_at: now,
            last_heartbeat: now,
            dirty_chunks: 0,
            dirty_bytes: 0,
            stats: None,
        };

        let assigned_id = self
            .master
            .register_client_by_uuid(client_uuid.clone(), fuse_info);

        let leader_addr = self.master.get_leader().await;
        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::ClientId, assigned_id);
        enc.add_string(FieldId::Owner, &leader_addr)?;
        enc.add_u8(FieldId::MountAllowed, 1);

        info!(
            "NET_REGISTER_CLIENT: uuid={}, assigned_id={}, leader={}, cert_name={:?}, peer={}",
            client_uuid, assigned_id, leader_addr, cert_client_name, peer_ip
        );

        Ok(Self::build_response(
            msg,
            STATUS_OK,
            enc.into_bytes(),
            Vec::new(),
        ))
    }

    async fn handle_deregister_client(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let client_uuid = dec.next_string(FieldId::ClientUuid).unwrap_or_default();
        let _assigned = dec.next_u64(FieldId::ClientId).unwrap_or(0);
        let client_cert_pem = dec.next_string(FieldId::ClientCert).unwrap_or_default();
        let peer_ip = ctx.client.address.ip().to_string();

        // Mirror register_client's enforcement: in production mode the
        // same cert binding must hold for a deregister to be accepted
        // (prevents an unrelated host from removing a healthy client's
        // lease by UUID spoofing). Dev mode simply warns.
        if self.master.cert_enforcement_enabled() {
            if client_cert_pem.is_empty() {
                warn!(
                    "NET_DEREGISTER_CLIENT: rejected uuid={} peer={} — missing ClientCert(0xD4)",
                    client_uuid, peer_ip
                );
                let mut enc = TlvEncoder::new();
                enc.add_string(
                    FieldId::Message,
                    "client certificate required for deregister (0xD4 PEM missing)",
                )?;
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_PERMISSION_DENIED,
                    enc.into_bytes(),
                    Vec::new(),
                ));
            }
            // Deregister doesn't carry a mount point, so we match the
            // cert against the mount point the master already recorded
            // for this UUID (or any valid mount dir in the registry when
            // the UUID is unknown — latter case is a no-op).
            let recorded_mount = self
                .master
                .client_manager
                .read()
                .unwrap()
                .fuse_clients
                .get(&client_uuid)
                .map(|c| c.mount_point.clone())
                .unwrap_or_default();
            let probe = if recorded_mount.is_empty() {
                // UUID gone — skip point check, just require IP binding.
                "".to_string()
            } else {
                recorded_mount.clone()
            };
            if let Err(e) =
                self.master
                    .validate_client_cert(&client_cert_pem, Some(&peer_ip), &probe)
            {
                // If we fell into the "UUID gone" branch and hit a
                // mount-point mismatch the error is benign; allow it.
                if !recorded_mount.is_empty() {
                    warn!(
                        "NET_DEREGISTER_CLIENT: rejected uuid={} peer={} — {}",
                        client_uuid, peer_ip, e
                    );
                    let mut enc = TlvEncoder::new();
                    enc.add_string(
                        FieldId::Message,
                        &format!("client certificate rejected: {}", e),
                    )?;
                    return Ok(Self::build_response(
                        msg,
                        STATUS_ERR_PERMISSION_DENIED,
                        enc.into_bytes(),
                        Vec::new(),
                    ));
                }
            }
        } else if client_cert_pem.is_empty() {
            warn!(
                "NET_DEREGISTER_CLIENT: cert not provided for uuid={} peer={} — ALLOWED (dev mode)",
                client_uuid, peer_ip
            );
        }

        if let Err(resp) = self
            .check_raft_available(msg, "NET_DEREGISTER_CLIENT")
            .await
        {
            return resp;
        }

        self.master.deregister_client_by_uuid(&client_uuid);
        let leader = self.master.get_leader().await;

        info!(
            "NET_DEREGISTER_CLIENT: uuid={}, leader={}, peer={}",
            client_uuid, leader, peer_ip
        );

        let mut enc = TlvEncoder::new();
        enc.add_string(FieldId::Owner, &leader)?;

        Ok(Self::build_response(
            msg,
            STATUS_OK,
            enc.into_bytes(),
            Vec::new(),
        ))
    }

    /// Handle GetTopology request - returns leader address AND volume routes
    /// If this node is not the Raft leader, returns STATUS_ERR_REDIRECT with leader address
    async fn handle_get_topology(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let _ = ctx;
        // If not leader, redirect client to the actual leader
        if let Err(resp) = self.check_raft_available(msg, "NET_GET_TOPOLOGY").await {
            return resp;
        }

        // Leader path: fetch own leader address (self) for the response body
        let leader = self.master.get_leader().await;
        if leader.is_empty() {
            // 自身是 leader 但 get_leader() 返回空：raft 状态异常，防御性报错
            warn!(
                "NET_GET_TOPOLOGY: is_leader=true but get_leader() returned empty; \
                 raft state inconsistent, returning SERVER_ERROR"
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                Vec::new(),
                Vec::new(),
            ));
        }

        info!("NET_GET_TOPOLOGY: returning topology info with volume routes");

        // Build volume routes from the route table
        let routes = self.master.list_volume_routes();
        let volume_count = routes.len() as u64;

        let mut enc = TlvEncoder::new();
        enc.add_string(FieldId::Owner, &leader)?;
        enc.add_u64(FieldId::Entries, volume_count);

        // Encode each volume route: volume_id + addr + size + used + file_count
        for route in routes.iter() {
            info!(
                "NET_GET_TOPOLOGY: volume_id={}, addr={}, size={}, used={}, file_count={}",
                route.volume_id, route.addr, route.size, route.used, route.file_count
            );
            enc.add_u64(FieldId::VolumeId, route.volume_id);
            let _ = enc.add_string(FieldId::Owner, &route.addr);
            enc.add_u64(FieldId::Size, route.size);
            enc.add_u64(FieldId::UsedSpace, route.used);
            enc.add_u64(FieldId::FileCount, route.file_count);
        }

        info!(
            "NET_GET_TOPOLOGY: leader={}, volumes={}",
            leader, volume_count
        );

        // ---- Topology extension: filer list + global total_shards ----
        //
        // Fuse clients must compute `calculate_shard(inode)` using the same
        // shard_count the filer cluster uses. Before this extension the
        // client hardcoded 256, which mismatched the filer's configured
        // value (e.g. 3) and routed migrate_inline_alloc / setattr to the
        // wrong shard. We now ship:
        //   * FilerListEntries = N (followed by N filer records)
        //   * Per filer: FilerAddress + NetPort + IsDir(healthy) + ShardIdList
        //   * TotalShards = global shard_count (the value every filer agrees on)
        //
        // Old clients that don't know these fields simply ignore them; the
        // volume section above is unchanged so the response is backward
        // compatible.
        let filers = self.master.list_filers();
        let filer_count = filers.len() as u64;
        enc.add_u64(FieldId::FilerListEntries, filer_count);
        for f in &filers {
            let _ = enc.add_string(FieldId::FilerAddress, &f.address);
            enc.add_u64(FieldId::NetPort, f.net_port as u64);
            enc.add_u8(FieldId::IsDir, if f.is_healthy { 1 } else { 0 });
            // Pack shard_ids as little-endian u64 array.
            let mut blob = Vec::with_capacity(f.shard_ids.len() * 8);
            for sid in &f.shard_ids {
                blob.extend_from_slice(&sid.to_le_bytes());
            }
            let _ = enc.add_bytes(FieldId::ShardIdList, &blob);
            info!(
                "NET_GET_TOPOLOGY: filer addr={}, net_port={}, healthy={}, shards={:?}",
                f.address, f.net_port, f.is_healthy, f.shard_ids
            );
        }

        // Determine the cluster-wide total_shards: every healthy filer must
        // report the same value; if they disagree we log a warning and fall
        // back to the max so clients over-approximate (a too-large modulus
        // routes to a non-existent shard → redirect → recovery, vs. a
        // too-small modulus that collapses two shards into one and silently
        // corrupts). The cleaner fix (master-owned shard_count in Raft
        // state) is deferred to a later PR.
        let mut total_shards: u64 = 0;
        let mut disagreement = false;
        for f in &filers {
            if !f.is_healthy || f.total_shards == 0 {
                continue;
            }
            if total_shards == 0 {
                total_shards = f.total_shards;
            } else if f.total_shards != total_shards {
                disagreement = true;
                if f.total_shards > total_shards {
                    total_shards = f.total_shards;
                }
            }
        }
        if disagreement {
            warn!(
                "NET_GET_TOPOLOGY: filers disagree on total_shards; using max={}. \
                 Filer shard_counts: {:?}",
                total_shards,
                filers
                    .iter()
                    .map(|f| (f.address.clone(), f.total_shards))
                    .collect::<Vec<_>>()
            );
        }
        enc.add_u64(FieldId::TotalShards, total_shards);
        info!(
            "NET_GET_TOPOLOGY: filers={}, total_shards={}",
            filer_count, total_shards
        );

        // ---- ShardMap entries snapshot (S3) ----
        //
        // Encode the full ShardMap entries so clients can reconstruct the
        // exact same range-based routing table the Filer uses, including
        // post-split ranges. Currently constructed from `total_shards`
        // (equivalent to `ShardMap::from_shard_count`); once Filer reports
        // actual entries via heartbeat, the Master will forward those
        // directly.
        //
        // Format: packed blob, each entry = 25 bytes:
        //   range_start:u64 LE | range_end:u64 LE | shard_id:u64 LE | state:u8
        //
        // Absent (empty blob) → client falls back to from_shard_count.
        //
        // IMPORTANT: This MUST be encoded BEFORE ShardLeaderEntries to match
        // the client's strict-sequential TLV decoder reading order
        // (TotalShards → ShardMapEntries → ShardLeaderEntries).
        if total_shards > 0 {
            let shard_map = ShardMap::from_shard_count(total_shards);
            let entries = shard_map.entries_snapshot();
            let mut blob = Vec::with_capacity(entries.len() * 25);
            for (range_start, range_end, sid, state) in &entries {
                blob.extend_from_slice(&range_start.to_le_bytes());
                blob.extend_from_slice(&range_end.to_le_bytes());
                blob.extend_from_slice(&sid.0.to_le_bytes());
                blob.push(match state {
                    ShardState::Active => 0u8,
                    ShardState::Draining => 1u8,
                });
            }
            let _ = enc.add_bytes(FieldId::ShardMapEntries, &blob);
            info!(
                "NET_GET_TOPOLOGY: ShardMap entries={} ({} bytes)",
                entries.len(),
                blob.len()
            );
        }

        // ---- Per-shard leader addresses ----
        //
        // Populated from ShardLeaderUpdate notifications sent by filers
        // when they gain/lose leadership of a shard Raft group. FUSE
        // clients use this to route cap RPCs directly to the shard leader
        // on the very first request (zero-redirect fast path).
        //
        // Format: ShardLeaderEntries(u64 count) + N × (ShardId + FilerAddress)
        // Old clients that don't know this field simply ignore it.
        let leaders = self.master.list_shard_leaders();
        let leader_count = leaders.len() as u64;
        enc.add_u64(FieldId::ShardLeaderEntries, leader_count);
        for (sid, addr) in &leaders {
            enc.add_u64(FieldId::ShardId, *sid);
            let _ = enc.add_string(FieldId::FilerAddress, addr);
            info!("NET_GET_TOPOLOGY: shard_leader sid={} addr={}", sid, addr);
        }

        Ok(Self::build_response(
            msg,
            STATUS_OK,
            enc.into_bytes(),
            Vec::new(),
        ))
    }

    /// Handle RegisterFiler request: register filer node + 分配 Zone + 选物理 volume
    ///
    /// Request TLV:
    ///   Owner       = filer_id (string, e.g. "filer-1")
    ///   FilerAddress = filer advertise address (string, "ip:net_port") — for ListFilers discovery
    ///   Blksize     = net_port (u64) — for ListFilers discovery
    ///   Limit       = shard_count (u64)
    ///   ShardIdList = packed u64 LE array of shard ids (bytes)
    ///   Force       = force flag (u8, 0/1) — bypass shard_count consistency check
    ///
    /// shard_count consistency:
    ///   The first filer to register establishes the cluster-wide shard_count.
    ///   Subsequent filers MUST report the same value. A mismatch means the
    ///   new filer was started with a different config and including it in
    ///   the cluster would cause inconsistent routing (some filers route
    ///   `inode / 1M % N`, others `inode / 1M % M`, N != M).
    ///
    ///   Without `Force=1`, the master rejects the registration with
    ///   STATUS_ERR_BAD_REQUEST and a descriptive error string; the filer
    ///   should exit (or fix its config and retry).
    ///
    ///   With `Force=1`, the master logs a warning and registers the filer
    ///   with the reported value anyway. This is for emergency repair only.
    ///
    /// Response TLV (多 Zone):
    ///   Entries(zone_count) + [ZoneId + Limit(vol_count) + [VolumeId + Owner(addr) + Size + UsedSpace] × N] × M
    async fn handle_register_filer(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let filer_id = dec.next_string(FieldId::Owner).unwrap_or_default();
        // Filer node discovery fields (optional for backward compat — old clients
        // that only send Owner will skip node registration, but zone allocation still works)
        let filer_addr = dec.next_string(FieldId::FilerAddress).unwrap_or_default();
        let net_port = dec.next_u64(FieldId::Blksize).unwrap_or(0) as u32;
        // Filer HTTP (S3) port — used by the Master for proxying /admin/shards.
        // Absent on old clients → default to 0 (meaning "unknown").
        let http_port = dec.next_u64(FieldId::FilerHttpPort).unwrap_or(0) as u32;
        // Filer metrics HTTP port — used by the Master for proxying
        // /admin/meta-cache-stats and /admin/lease-stats.
        // Absent on old clients → default to 0 (meaning "unknown").
        let metrics_port = dec.next_u64(FieldId::FilerMetricsPort).unwrap_or(0) as u32;
        let shard_count = dec.next_u64(FieldId::Limit).unwrap_or(0);
        let shard_ids_blob = dec.next_bytes(FieldId::ShardIdList).unwrap_or_default();
        let force = dec.next_u8(FieldId::Force).unwrap_or(0) != 0;
        // Registration token for node authentication. Absent on old clients
        // (dev mode allows empty token when master has no token configured).
        let reg_token = dec
            .next_string(FieldId::RegistrationToken)
            .unwrap_or_default();
        // Client certificate (PEM) for production node authentication.
        // When cert enforcement is ON (master has CA), the filer MUST present
        // a valid cert bound to its filer_id + source IP.
        let client_cert_pem = dec.next_string(FieldId::ClientCert).unwrap_or_default();

        // Authenticate the filer before processing the registration. This
        // runs before the leader check so unauthorized filers are rejected
        // even by followers (no redirect to leader for bad tokens).
        if !self.master.verify_registration_token(&reg_token) {
            warn!(
                "NET_REGISTER_FILER: rejected filer={} — invalid registration token",
                filer_id
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_PERMISSION_DENIED,
                b"invalid registration token".to_vec(),
                Vec::new(),
            ));
        }

        // ---- Certificate enforcement (production) ----
        //
        // When the master has a live CA manager, all RegisterFiler
        // requests MUST carry a ClientCert(0xD4) TLV validated through
        // the same 4-level chain as RegisterClient (fingerprint, not
        // revoked, valid expiry, IP match).  Additionally the cert's
        // client_name MUST match the filer_id reported by the caller.
        let peer_ip = ctx.client.address.ip().to_string();
        if self.master.cert_enforcement_enabled() {
            if client_cert_pem.is_empty() {
                warn!(
                    "NET_REGISTER_FILER: rejected filer={} peer={} — missing ClientCert(0xD4) TLV (cert enforcement ON)",
                    filer_id, peer_ip
                );
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_PERMISSION_DENIED,
                    b"filer certificate required (0xD4 PEM missing)".to_vec(),
                    Vec::new(),
                ));
            }
            if let Err(e) =
                self.master
                    .validate_server_node_cert(&client_cert_pem, Some(&peer_ip), &filer_id)
            {
                warn!(
                    "NET_REGISTER_FILER: rejected filer={} peer={} — {}",
                    filer_id, peer_ip, e
                );
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_PERMISSION_DENIED,
                    format!("filer certificate rejected: {}", e).into_bytes(),
                    Vec::new(),
                ));
            }
        } else if client_cert_pem.is_empty() {
            // Dev mode: warn once if cert absent, still allow.
            warn!(
                "NET_REGISTER_FILER: cert not provided by filer={} peer={} — ALLOWED (dev mode, no CA configured)",
                filer_id, peer_ip
            );
        }

        if let Err(resp) = self.check_raft_available(msg, "NET_REGISTER_FILER").await {
            return resp;
        }

        // Register filer node for ListFilers discovery (replaces gRPC RegisterFiler).
        // Only register if the filer provided discovery info (addr + shard_ids).
        if !filer_addr.is_empty() && !filer_id.is_empty() {
            let shard_ids: Vec<u64> = shard_ids_blob
                .as_chunks::<8>()
                .0
                .iter()
                .map(|c| u64::from_le_bytes(*c))
                .collect();

            // ---- shard_count consistency check ----
            //
            // Look at every already-registered healthy filer. If any has a
            // different shard_count, refuse the new registration (unless the
            // caller passes Force=1). This protects the cluster against
            // accidental config drift: a filer started with the wrong
            // shard_count would compute `calculate_shard(inode)` differently
            // from its peers and silently corrupt routing.
            //
            // The very first filer to register (or the first after a clean
            // cluster restart) seeds the value — there is nothing to
            // compare against yet, so it always passes.
            let existing = self.master.list_filers();
            let mut conflict_addr: Option<String> = None;
            let mut existing_count: u64 = 0;
            for f in &existing {
                if !f.is_healthy || f.total_shards == 0 {
                    continue;
                }
                if f.total_shards != shard_count {
                    conflict_addr = Some(f.address.clone());
                    existing_count = f.total_shards;
                    break;
                }
            }
            if let Some(addr) = conflict_addr {
                let err_msg = format!(
                    "shard_count mismatch: new filer {} reports shard_count={}, but \
                     registered filer {} already uses shard_count={}. \
                     Refusing registration to prevent routing corruption. \
                     Fix the config and restart, or pass --force to override.",
                    filer_addr, shard_count, addr, existing_count
                );
                if force {
                    warn!("NET_REGISTER_FILER: {}", err_msg);
                } else {
                    error!("NET_REGISTER_FILER: {}", err_msg);
                    return Ok(Self::build_response(
                        msg,
                        STATUS_ERR_BAD_REQUEST,
                        err_msg.into_bytes(),
                        Vec::new(),
                    ));
                }
            }

            let filer_info = crate::master::FilerNodeInfo {
                node_id: filer_id.clone(),
                address: filer_addr.clone(),
                grpc_port: 0, // gRPC being removed; not used by TLV clients
                http_port,
                net_port,
                metrics_port,
                is_healthy: true,
                leader_count: 0,
                total_shards: shard_count,
                shard_ids,
            };
            self.master.register_filer(filer_info);
            info!(
                "NET_REGISTER_FILER: registered filer node id={}, addr={}, net_port={}, shard_count={}",
                filer_id, filer_addr, net_port, shard_count
            );
        }

        info!(
            "NET_REGISTER_FILER: filer_id={}, assigning zone(s)",
            filer_id
        );

        let zones = self.master.register_filer_zone(&filer_id).await;

        let mut enc = TlvEncoder::new();
        // 多 Zone 编码: Entries=zone_count, 每个 Zone 含 ZoneId + Limit=vol_count + vol 条目
        // 每个 vol 条目: VolumeId + Owner(addr) + Size + UsedSpace + Backend(node_id)
        enc.add_u64(FieldId::Entries, zones.len() as u64);
        for zone in &zones {
            enc.add_u32(FieldId::ZoneId, zone.zone_id);
            enc.add_u64(FieldId::Limit, zone.physical_volumes.len() as u64);
            for vol in &zone.physical_volumes {
                enc.add_u64(FieldId::VolumeId, vol.volume_id);
                enc.add_string(FieldId::Owner, &vol.addr)?;
                enc.add_u64(FieldId::Size, vol.size);
                enc.add_u64(FieldId::UsedSpace, vol.used);
                enc.add_string(FieldId::Backend, &vol.node_id)?;
            }
        }

        // 统计节点多样性用于日志
        let node_count: std::collections::HashSet<&str> = zones
            .iter()
            .flat_map(|z| z.physical_volumes.iter().map(|v| v.node_id.as_str()))
            .collect();
        info!(
            "NET_REGISTER_FILER: filer_id={}, zones={}, total_volumes={}, unique_nodes={}",
            filer_id,
            zones.len(),
            zones
                .iter()
                .map(|z| z.physical_volumes.len())
                .sum::<usize>(),
            node_count.len()
        );

        Ok(Self::build_response(
            msg,
            STATUS_OK,
            enc.into_bytes(),
            Vec::new(),
        ))
    }

    /// Filer → Master: notify that this filer gained/lost leadership of a
    /// shard Raft group. Updates the `shard_leaders` table and, if changed,
    /// broadcasts TopologyChanged to all connected TLV clients so they
    /// re-fetch topology and pick up the new per-shard leader address.
    ///
    /// This enables the zero-redirect fast path: fuse clients fetch
    /// per-shard leaders from GetTopology and route cap RPCs directly to
    /// the leader on the very first request.
    async fn handle_shard_leader_update(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let _ = ctx;
        let mut dec = TlvDecoder::new(&msg.body);
        // ShardId 0 is a valid shard (shards are 0-indexed). Distinguish
        // "field present with value 0" from "field missing" by checking the
        // Result rather than using unwrap_or(0) as a sentinel.
        let shard_id = match dec.next_u64(FieldId::ShardId) {
            Ok(sid) => sid,
            Err(_) => {
                warn!("SHARD_LEADER_UPDATE: missing ShardId field");
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_BAD_REQUEST,
                    Vec::new(),
                    Vec::new(),
                ));
            }
        };
        let is_leader = dec.next_u8(FieldId::Force).unwrap_or(0) != 0;
        let filer_id = dec.next_string(FieldId::Owner).unwrap_or_default();
        let leader_addr = dec.next_string(FieldId::FilerAddress).unwrap_or_default();

        if filer_id.is_empty() {
            warn!("SHARD_LEADER_UPDATE: missing filer_id (shard={})", shard_id);
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_BAD_REQUEST,
                Vec::new(),
                Vec::new(),
            ));
        }

        // Not leader? Redirect to the master leader.
        if let Err(resp) = self.check_raft_available(msg, "SHARD_LEADER_UPDATE").await {
            return resp;
        }

        // Gained leadership → update entry with leader_addr.
        // Lost leadership → clear entry (pass empty string).
        let addr = if is_leader { leader_addr.as_str() } else { "" };
        let changed = self.master.update_shard_leader(shard_id, addr);

        info!(
            "SHARD_LEADER_UPDATE: shard={} is_leader={} filer_id={} addr={} changed={}",
            shard_id,
            is_leader,
            filer_id,
            if is_leader { &leader_addr } else { "(cleared)" },
            changed
        );

        // Broadcast TopologyChanged so fuse clients re-fetch topology and
        // pick up the new per-shard leader (zero-redirect cap routing).
        if changed {
            let n = self.master.broadcast_topology_changed();
            if n > 0 {
                debug!(
                    "SHARD_LEADER_UPDATE: broadcast TopologyChanged to {} TLV clients (shard={} is_leader={})",
                    n, shard_id, is_leader
                );
            }
        }

        Ok(Self::build_response(msg, STATUS_OK, Vec::new(), Vec::new()))
    }

    /// Handle StatFs request from kernel client.
    ///
    /// Aggregates all volumes' size/used/file_count from the volume router
    /// to return real filesystem statistics. The kernel caches this with
    /// a 30s TTL, so real-time precision is not required.
    ///
    /// Response TLV fields:
    ///   Size       (u64) — total bytes across all volumes
    ///   Free       (u64) — free bytes (total - used)
    ///   Nlink      (u64) — total file count (reused as total_inodes)
    ///   FreeInodes (u64) — free inodes (estimated, no hard limit)
    ///   Blksize    (u32) — block size (4096)
    async fn handle_statfs(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> powerfs_net::NetResult<NetMessage> {
        let _ = ctx;
        let volumes = self.master.list_volume_routes();

        let total_size: u64 = volumes.iter().map(|v| v.size).sum();
        let total_used: u64 = volumes.iter().map(|v| v.used).sum();
        let total_files: u64 = volumes.iter().map(|v| v.file_count).sum();
        let free = total_size.saturating_sub(total_used);

        // For free inodes: PowerFS has no hard inode limit (metadata stored in
        // Raft). Return a large number so df doesn't show "0% free inodes".
        let free_inodes = 1_000_000_000u64;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Size, total_size);
        enc.add_u64(FieldId::Free, free);
        enc.add_u64(FieldId::Nlink, total_files);
        enc.add_u64(FieldId::FreeInodes, free_inodes);
        enc.add_u32(FieldId::Blksize, 4096);

        debug!(
            "MASTER_STATFS: volumes={}, total_size={}, used={}, free={}, files={}",
            volumes.len(),
            total_size,
            total_used,
            free,
            total_files
        );

        Ok(Self::build_response(
            msg,
            STATUS_OK,
            enc.into_bytes(),
            Vec::new(),
        ))
    }

    /// Handle GetDebugConfig request from fuse/filer/volume nodes.
    ///
    /// Nodes poll this every 2s to fetch their effective debug config
    /// (merged "all" defaults + node-specific overrides). The response
    /// carries log_level, target_filter, and subsystem flags.
    ///
    /// TLV request: NodeId(string)
    /// TLV response: see `encode_get_debug_config_resp`
    async fn handle_get_debug_config(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> powerfs_net::NetResult<NetMessage> {
        let _ = ctx;
        let node_id = match powerfs_net::serialize::decode_get_debug_config_req(&msg.body) {
            Ok(id) => id,
            Err(e) => {
                warn!("MASTER_GET_DEBUG_CONFIG: decode failed: {}", e);
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_BAD_REQUEST,
                    Vec::new(),
                    Vec::new(),
                ));
            }
        };

        let config = self.master.debug_config().effective_config(&node_id);
        debug!(
            "MASTER_GET_DEBUG_CONFIG: node='{}' level={:?} filter={:?} flags={}",
            node_id,
            config.log_level,
            config.target_filter,
            config.flags.len()
        );

        let body =
            powerfs_net::serialize::encode_get_debug_config_resp(&config).unwrap_or_default();
        Ok(Self::build_response(msg, STATUS_OK, body, Vec::new()))
    }

    /// Handle ListFilers request from kernel client.
    /// Returns all registered filer nodes with their powerfs-net addresses,
    /// allowing the kernel to populate its connection pool without manual
    /// configuration of each filer address.
    ///
    /// TLV response:
    ///   Entries = filer_count
    ///   Per filer: Owner=addr, Blksize=net_port, IsDir=is_healthy
    async fn handle_list_filers(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let _ = ctx;
        if let Err(resp) = self.check_raft_available(msg, "NET_LIST_FILERS").await {
            return resp;
        }

        let filers = self.master.list_filers();
        let count = filers.len() as u64;

        let mut enc = TlvEncoder::new();
        enc.add_u64(FieldId::Entries, count);

        for f in &filers {
            enc.add_string(FieldId::Owner, &f.address)?;
            enc.add_u64(FieldId::Blksize, f.net_port as u64);
            enc.add_u8(FieldId::IsDir, if f.is_healthy { 1 } else { 0 });
            info!(
                "NET_LIST_FILERS: addr={}, net_port={}, healthy={}, shards={:?}",
                f.address, f.net_port, f.is_healthy, f.shard_ids
            );
        }

        info!("NET_LIST_FILERS: returning {} filers", count);

        Ok(Self::build_response(
            msg,
            STATUS_OK,
            enc.into_bytes(),
            Vec::new(),
        ))
    }

    /// Handle Raft inter-node message (MsgType::RaftMessage).
    ///
    /// Replaces the gRPC RaftService::send_raft_message RPC.
    /// Decodes the TLV body (ShardId + RaftPayload), deserializes the
    /// protobuf eraftpb::Message, and steps it into the local Raft node.
    ///
    /// The Master runs a single (non-sharded) Raft group, so `ShardId` is
    /// accepted for protocol parity with the Filer but ignored (default 0).
    ///
    /// Request TLV:
    ///   ShardId     = shard id (u64, ignored by Master, default 0)
    ///   RaftPayload = serialized eraftpb::Message (bytes)
    /// Response: STATUS_OK (empty body) or STATUS_ERR_SERVER_ERROR (error string).
    async fn handle_raft_message(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let _ = ctx;
        // openraft manages its own gRPC transport (RaftService started inside
        // RaftNodeV2::new). The legacy TLV-based RaftMessage path is deprecated;
        // return an error so any stale caller fails fast.
        warn!(
            "MASTER_RAFT: TLV RaftMessage transport is deprecated; \
             openraft uses its own RaftService gRPC transport"
        );
        Ok(Self::build_response(
            msg,
            STATUS_ERR_SERVER_ERROR,
            "TLV raft transport deprecated; openraft uses gRPC RaftService"
                .to_string()
                .into_bytes(),
            Vec::new(),
        ))
    }

    /// Helper: build a response message
    fn build_response(msg: &NetMessage, status: u16, body: Vec<u8>, data: Vec<u8>) -> NetMessage {
        NetMessage::response(msg, status, body, data)
    }

    /// 集中构造非 leader 节点的重定向响应。
    ///
    /// **防御性**：follower 必须能给出有效 leader 地址才能 REDIRECT；若 `get_leader()`
    /// 返回空（选举未完成 / raft 无 leader / membership 不一致），立即返回
    /// `STATUS_ERR_SERVER_ERROR` 让客户端重试，**绝不返回空地址的 REDIRECT**——
    /// 否则客户端拿到空 leader 会无限重连或失败，形成路由黑洞。
    ///
    /// `ctx`：调用上下文名（如 "NET_ASSIGN"），用于告警定位。
    async fn build_redirect_response(&self, msg: &NetMessage, ctx: &str) -> NetResult<NetMessage> {
        let leader = self.master.get_leader().await;
        if leader.is_empty() {
            warn!(
                "{}: not leader AND get_leader() returned empty — no leader elected yet or \
                 raft membership inconsistent; returning SERVER_ERROR to force client retry \
                 (returning empty REDIRECT would cause client routing black hole)",
                ctx
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                Vec::new(),
                Vec::new(),
            ));
        }
        debug!("{}: not leader, redirecting to leader at {}", ctx, leader);
        let mut enc = TlvEncoder::new();
        let _ = enc.add_string(FieldId::Owner, &leader);
        Ok(Self::build_response(
            msg,
            STATUS_ERR_REDIRECT,
            enc.into_bytes(),
            Vec::new(),
        ))
    }

    /// Build a SERVER_ERROR response when Raft is unavailable (fake Leader).
    ///
    /// Returns SERVER_ERROR (not REDIRECT) intentionally: a fake Leader has no
    /// valid leader to redirect to (`current_leader()` returns None because
    /// Quorum does not acknowledge it). Redirecting to None would cause the
    /// client-side `forward to: None, None` infinite loop. SERVER_ERROR lets
    /// the client retry with backoff and pick up a new leader via topology
    /// refresh once one is re-elected.
    async fn build_unavailable_response(
        &self,
        msg: &NetMessage,
        ctx: &str,
    ) -> NetResult<NetMessage> {
        warn!(
            "{}: raft unavailable (fake Leader — lease expired); returning SERVER_ERROR to \
             break client retry loop. is_leader=true but is_raft_available=false",
            ctx
        );
        Ok(Self::build_response(
            msg,
            STATUS_ERR_SERVER_ERROR,
            Vec::new(),
            Vec::new(),
        ))
    }

    /// Unified gate for Raft-write requests: must be leader AND raft must be
    /// available (not in fake-Leader state). Returns `Ok(())` when the request
    /// can proceed, otherwise builds the appropriate error response.
    ///
    /// - Not leader → REDIRECT to actual leader (or SERVER_ERROR if no leader).
    /// - Leader but `raft_unavailable` → SERVER_ERROR (breaks forward-None loop).
    async fn check_raft_available(
        &self,
        msg: &NetMessage,
        ctx: &str,
    ) -> Result<(), NetResult<NetMessage>> {
        if !self.master.is_leader().await {
            return Err(self.build_redirect_response(msg, ctx).await);
        }
        if !self.master.is_raft_available() {
            return Err(self.build_unavailable_response(msg, ctx).await);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl NetHandler for MasterNetHandler {
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
            MsgType::Assign => self.handle_assign(ctx, msg).await,
            MsgType::LookupVolume => self.handle_lookup_volume(ctx, msg).await,
            MsgType::Heartbeat => self.handle_heartbeat(ctx, msg).await,
            MsgType::KeepConnected => self.handle_keep_connected(ctx, msg).await,
            MsgType::RegisterClient => self.handle_register_client(ctx, msg).await,
            MsgType::DeregisterClient => self.handle_deregister_client(ctx, msg).await,
            MsgType::GetTopology => self.handle_get_topology(ctx, msg).await,
            MsgType::RegisterFiler => self.handle_register_filer(ctx, msg).await,
            MsgType::ShardLeaderUpdate => self.handle_shard_leader_update(ctx, msg).await,
            MsgType::ListFilers => self.handle_list_filers(ctx, msg).await,
            MsgType::StatFs => self.handle_statfs(ctx, msg).await,
            MsgType::GetDebugConfig => self.handle_get_debug_config(ctx, msg).await,
            MsgType::RaftMessage => self.handle_raft_message(ctx, msg).await,
            MsgType::Ping => Ok(NetMessage::ok_response(msg, Vec::new(), Vec::new())),
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
