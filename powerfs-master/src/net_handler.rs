//! Master Net Handler - Implements powerfs-net protocol for metadata operations
//!
//! This module provides MasterNetHandler that processes powerfs-net messages
//! and delegates to MasterNode for actual business logic.

use crate::master::MasterNode;
use crate::proto::powerfs::VolumeShortInfo;
use log::{debug, error, info, warn};
use powerfs_net::serialize::{TlvDecoder, TlvEncoder};
use powerfs_net::{
    FieldId, MsgType, NetHandler, NetMessage, RequestContext, STATUS_ERR_NOT_FOUND,
    STATUS_ERR_REDIRECT, STATUS_ERR_SERVER_ERROR, STATUS_OK,
};
use protobuf::Message as ProtoMessage;
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
            warn!(
                "NET_ASSIGN: not leader; current leader is {}, returning redirect response",
                leader
            );
            // Return redirect response with leader address
            let mut enc = TlvEncoder::new();
            let _ = enc.add_string(FieldId::Owner, &leader);
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_REDIRECT,
                enc.into_bytes(),
                Vec::new(),
            ));
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
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let volume_id_str = dec.next_string(FieldId::Name).unwrap_or_default();

        info!("NET_LOOKUP_VOLUME: volume_id={}", volume_id_str);

        // Volume lookup reads topology state. Redirect non-leader requests
        // to ensure the client gets up-to-date volume routing from the leader.
        if !self.master.is_leader().await {
            let leader = self.master.get_leader().await;
            warn!(
                "NET_LOOKUP_VOLUME: not leader; current leader is {}, returning redirect response",
                leader
            );
            let mut enc = TlvEncoder::new();
            let _ = enc.add_string(FieldId::Owner, &leader);
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_REDIRECT,
                enc.into_bytes(),
                Vec::new(),
            ));
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
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let node_id_str = dec.next_string(FieldId::ClientId).unwrap_or_default();
        let ip = dec.next_string(FieldId::Owner).unwrap_or_default();
        let port = dec.next_u64(FieldId::Blksize).unwrap_or(0) as u32;
        let net_port = dec.next_u64(FieldId::NetPort).unwrap_or(0) as u32;
        let volume_count = dec.next_u64(FieldId::Entries).unwrap_or(0) as usize;

        info!(
            "NET_HEARTBEAT: node={}, ip={}, volumes={}",
            node_id_str, ip, volume_count
        );

        // Heartbeat mutates Master topology state (add_node, volume registration).
        // Only the Raft leader should process it; followers return REDIRECT.
        if !self.master.is_leader().await {
            let leader = self.master.get_leader().await;
            warn!(
                "NET_HEARTBEAT: not leader; current leader is {}, returning redirect response",
                leader
            );
            let mut enc = TlvEncoder::new();
            let _ = enc.add_string(FieldId::Owner, &leader);
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_REDIRECT,
                enc.into_bytes(),
                Vec::new(),
            ));
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
                    net_port,
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
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
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
        if !self.master.is_leader().await {
            let leader = self.master.get_leader().await;
            warn!(
                "NET_KEEP_CONNECTED: not leader; current leader is {}, returning redirect response",
                leader
            );
            let mut enc = TlvEncoder::new();
            let _ = enc.add_string(FieldId::Owner, &leader);
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_REDIRECT,
                enc.into_bytes(),
                Vec::new(),
            ));
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

    /// Handle GetTopology request - returns leader address AND volume routes
    /// If this node is not the Raft leader, returns STATUS_ERR_REDIRECT with leader address
    async fn handle_get_topology(
        &self,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let leader = self.master.get_leader().await;

        // If not leader, redirect client to the actual leader
        if !self.master.is_leader().await {
            info!(
                "NET_GET_TOPOLOGY: not leader, redirecting to leader at {}",
                leader
            );
            let mut enc = TlvEncoder::new();
            enc.add_string(FieldId::Owner, &leader)?;
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_REDIRECT,
                enc.into_bytes(),
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
    ///
    /// Response TLV (多 Zone):
    ///   Entries(zone_count) + [ZoneId + Limit(vol_count) + [VolumeId + Owner(addr) + Size + UsedSpace] × N] × M
    async fn handle_register_filer(
        &self,
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let filer_id = dec.next_string(FieldId::Owner).unwrap_or_default();
        // Filer node discovery fields (optional for backward compat — old clients
        // that only send Owner will skip node registration, but zone allocation still works)
        let filer_addr = dec.next_string(FieldId::FilerAddress).unwrap_or_default();
        let net_port = dec.next_u64(FieldId::Blksize).unwrap_or(0) as u32;
        let shard_count = dec.next_u64(FieldId::Limit).unwrap_or(0);
        let shard_ids_blob = dec.next_bytes(FieldId::ShardIdList).unwrap_or_default();

        if !self.master.is_leader().await {
            let leader = self.master.get_leader().await;
            if leader.is_empty() {
                // 无 leader (选举未完成): 返回 SERVER_ERROR 让 Filer 重试,
                // 而非返回空地址的 REDIRECT 导致 Filer 端连接失败.
                warn!(
                    "NET_REGISTER_FILER: not leader and no leader elected yet, filer_id={}",
                    filer_id
                );
                return Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    Vec::new(),
                    Vec::new(),
                ));
            }
            let mut enc = TlvEncoder::new();
            enc.add_string(FieldId::Owner, &leader)?;
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_REDIRECT,
                enc.into_bytes(),
                Vec::new(),
            ));
        }

        // Register filer node for ListFilers discovery (replaces gRPC RegisterFiler).
        // Only register if the filer provided discovery info (addr + shard_ids).
        if !filer_addr.is_empty() && !filer_id.is_empty() {
            let shard_ids: Vec<u64> = shard_ids_blob
                .chunks_exact(8)
                .map(|c| u64::from_le_bytes(c.try_into().unwrap_or([0u8; 8])))
                .collect();
            let filer_info = crate::master::FilerNodeInfo {
                node_id: filer_id.clone(),
                address: filer_addr.clone(),
                grpc_port: 0, // gRPC being removed; not used by TLV clients
                http_port: 0,
                net_port,
                is_healthy: true,
                leader_count: 0,
                total_shards: shard_count,
                shard_ids,
            };
            self.master.register_filer(filer_info);
            info!(
                "NET_REGISTER_FILER: registered filer node id={}, addr={}, net_port={}",
                filer_id, filer_addr, net_port
            );
        }

        info!(
            "NET_REGISTER_FILER: filer_id={}, assigning zone(s)",
            filer_id
        );

        let zones = self.master.register_filer_zone(&filer_id).await;

        let mut enc = TlvEncoder::new();
        // 多 Zone 编码: Entries=zone_count, 每个 Zone 含 ZoneId + Limit=vol_count + vol 条目
        enc.add_u64(FieldId::Entries, zones.len() as u64);
        for zone in &zones {
            enc.add_u32(FieldId::ZoneId, zone.zone_id);
            enc.add_u64(FieldId::Limit, zone.physical_volumes.len() as u64);
            for vol in &zone.physical_volumes {
                enc.add_u64(FieldId::VolumeId, vol.volume_id);
                enc.add_string(FieldId::Owner, &vol.addr)?;
                enc.add_u64(FieldId::Size, vol.size);
                enc.add_u64(FieldId::UsedSpace, vol.used);
            }
        }

        info!(
            "NET_REGISTER_FILER: filer_id={}, zones={}, total_volumes={}",
            filer_id,
            zones.len(),
            zones
                .iter()
                .map(|z| z.physical_volumes.len())
                .sum::<usize>()
        );

        Ok(Self::build_response(
            msg,
            STATUS_OK,
            enc.into_bytes(),
            Vec::new(),
        ))
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
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        if !self.master.is_leader().await {
            let leader = self.master.get_leader().await;
            let mut enc = TlvEncoder::new();
            enc.add_string(FieldId::Owner, &leader)?;
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_REDIRECT,
                enc.into_bytes(),
                Vec::new(),
            ));
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
        msg: &NetMessage,
    ) -> Result<NetMessage, powerfs_net::NetError> {
        let mut dec = TlvDecoder::new(&msg.body);
        let shard_id_val = dec.next_u64(FieldId::ShardId).unwrap_or(0);
        let payload = dec.next_bytes(FieldId::RaftPayload).unwrap_or_default();

        if payload.is_empty() {
            warn!(
                "MASTER_RAFT: received empty RaftPayload for shard {}",
                shard_id_val
            );
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                b"empty raft payload".to_vec(),
                Vec::new(),
            ));
        }

        let mut raft_msg = raft::eraftpb::Message::new();
        if let Err(e) = ProtoMessage::merge_from_bytes(&mut raft_msg, &payload) {
            warn!("MASTER_RAFT: failed to deserialize Raft message: {}", e);
            return Ok(Self::build_response(
                msg,
                STATUS_ERR_SERVER_ERROR,
                format!("failed to deserialize: {}", e).into_bytes(),
                Vec::new(),
            ));
        }

        debug!(
            "MASTER_RAFT: received raft message (type={:?}, to={}, from={})",
            raft_msg.msg_type, raft_msg.to, raft_msg.from
        );

        let step_tx = self.master.raft_step_tx();
        match step_tx.send(raft_msg).await {
            Ok(_) => {
                debug!("MASTER_RAFT: stepped raft message");
                Ok(Self::build_response(msg, STATUS_OK, Vec::new(), Vec::new()))
            }
            Err(e) => {
                warn!("MASTER_RAFT: failed to step raft message: {}", e);
                Ok(Self::build_response(
                    msg,
                    STATUS_ERR_SERVER_ERROR,
                    "failed to send message to step channel"
                        .to_string()
                        .into_bytes(),
                    Vec::new(),
                ))
            }
        }
    }

    /// Helper: build a response message
    fn build_response(msg: &NetMessage, status: u16, body: Vec<u8>, data: Vec<u8>) -> NetMessage {
        NetMessage::response(msg, status, body, data)
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
            MsgType::Assign => self.handle_assign(msg).await,
            MsgType::LookupVolume => self.handle_lookup_volume(msg).await,
            MsgType::Heartbeat => self.handle_heartbeat(msg).await,
            MsgType::KeepConnected => self.handle_keep_connected(msg).await,
            MsgType::GetTopology => self.handle_get_topology(msg).await,
            MsgType::RegisterFiler => self.handle_register_filer(msg).await,
            MsgType::ListFilers => self.handle_list_filers(msg).await,
            MsgType::RaftMessage => self.handle_raft_message(msg).await,
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
