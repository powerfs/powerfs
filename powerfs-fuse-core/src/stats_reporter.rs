//! Reports FUSE client identity/heartbeat to the Master via the TLV
//! `KeepConnected` message and receives topology-change notifications.
//!
//! This is the TLV replacement for the former gRPC `keep_connected` bidi
//! stream.  The reporter owns a single background task that:
//! 1. Installs a `NotificationHandler` on the shared [`MasterClient`] so
//!    that server-pushed `TopologyChanged` NOTIFY frames mark the local
//!    topology as stale.
//! 2. Every `report_interval`, sends a `KeepConnected` request carrying
//!    the client identity (re-registering + refreshing the heartbeat).
//! 3. When the topology is marked stale, re-fetches the full topology
//!    via `MasterClient::fetch_topology` and updates the shared
//!    [`ClusterTopologyManager`].
//!
//! All communication goes through the TLV protocol (`powerfs-net`),
//! keeping FUSE and the future kernel filesystem on the same wire
//! format as the rest of the Master interaction.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{info, warn};

use powerfs_net::{MsgType, NetMessage, NotificationHandler};

use crate::topology::{ClusterTopologyManager, MasterClient};

/// Identity and connection parameters for the stats reporter.
#[derive(Clone)]
pub struct StatsReporterConfig {
    /// Stable client identifier reported to Master, e.g. `fuse_<uuid>`.
    pub client_id: String,
    /// FUSE client type label, e.g. `"fuse"`.
    pub client_type: String,
    /// Mount point path.
    pub mount_point: String,
    /// Collection name.
    pub collection: String,
    /// Replication placement string.
    pub replication: String,
    /// Hostname of the FUSE process host.
    pub host: String,
    /// PID of the FUSE process.
    pub pid: u64,
    /// Reporting interval. Should be <= master's heartbeat timeout (5s).
    pub report_interval: Duration,
}

impl Default for StatsReporterConfig {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            client_type: "fuse".to_string(),
            mount_point: String::new(),
            collection: String::new(),
            replication: String::new(),
            host: String::new(),
            pid: 0,
            report_interval: Duration::from_secs(5),
        }
    }
}

/// `NotificationHandler` that marks the local topology stale whenever
/// the Master pushes a `TopologyChanged` NOTIFY.
struct TopologyChangedHandler {
    dirty: Arc<AtomicBool>,
}

impl NotificationHandler for TopologyChangedHandler {
    fn handle_notification(&self, msg: &NetMessage) {
        if msg.msg_type() == Some(MsgType::TopologyChanged) {
            self.dirty.store(true, Ordering::Relaxed);
            log::debug!("TopologyChanged NOTIFY received, topology marked stale");
        }
    }
}

/// Background reporter that pushes client identity to the Master and
/// refreshes the local topology on change notifications.
pub struct MasterStatsReporter {
    config: StatsReporterConfig,
    master_client: Arc<MasterClient>,
    topology_manager: Arc<ClusterTopologyManager>,
    topology_dirty: Arc<AtomicBool>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl MasterStatsReporter {
    pub fn new(
        config: StatsReporterConfig,
        master_client: Arc<MasterClient>,
        topology_manager: Arc<ClusterTopologyManager>,
    ) -> Self {
        Self {
            config,
            master_client,
            topology_manager,
            topology_dirty: Arc::new(AtomicBool::new(false)),
            shutdown_tx: None,
            join_handle: None,
        }
    }

    /// Spawn the reporter background task. Idempotent.
    pub fn start(&mut self) {
        if self.join_handle.is_some() {
            return;
        }
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        self.shutdown_tx = Some(shutdown_tx);

        // Install the notification handler so server-pushed
        // `TopologyChanged` frames mark the topology stale.
        let handler: Arc<dyn NotificationHandler + Send + Sync> =
            Arc::new(TopologyChangedHandler {
                dirty: self.topology_dirty.clone(),
            });
        self.master_client.set_notification_handler(handler);

        let config = self.config.clone();
        let master_client = self.master_client.clone();
        let topology_manager = self.topology_manager.clone();
        let topology_dirty = self.topology_dirty.clone();

        self.join_handle = Some(tokio::spawn(async move {
            run_reporter_loop(
                config,
                master_client,
                topology_manager,
                topology_dirty,
                shutdown_rx,
            )
            .await;
        }));
    }

    /// Stop the reporter. Waits for the background task to exit.
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.await;
        }
    }
}

async fn run_reporter_loop(
    config: StatsReporterConfig,
    master_client: Arc<MasterClient>,
    topology_manager: Arc<ClusterTopologyManager>,
    topology_dirty: Arc<AtomicBool>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    if config.client_id.is_empty() {
        info!("MasterStatsReporter: no client_id configured, not starting");
        return;
    }
    info!(
        "MasterStatsReporter: starting (client_id={}, mount={}, interval={}s)",
        config.client_id,
        config.mount_point,
        config.report_interval.as_secs()
    );

    let mut interval = tokio::time::interval(config.report_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                info!("MasterStatsReporter: shutdown received, stopping");
                return;
            }
            _ = interval.tick() => {
                // 1. Refresh topology if the Master pushed a change.
                if topology_dirty.swap(false, Ordering::Relaxed) {
                    match master_client.fetch_topology().await {
                        Ok(topo) => {
                            topology_manager.update_topology(topo);
                            log::debug!("MasterStatsReporter: topology refreshed after NOTIFY");
                        }
                        Err(e) => {
                            warn!("MasterStatsReporter: fetch_topology after NOTIFY failed: {}", e);
                            // Re-mark dirty so we retry next tick.
                            topology_dirty.store(true, Ordering::Relaxed);
                        }
                    }
                }

                // 2. Send KeepConnected heartbeat (re-register + refresh).
                if let Err(e) = master_client
                    .send_keep_connected(
                        &config.client_id,
                        &config.client_type,
                        &config.mount_point,
                        &config.collection,
                        &config.replication,
                        &config.host,
                        config.pid,
                    )
                    .await
                {
                    warn!("MasterStatsReporter: keep_connected failed: {}", e);
                }
            }
        }
    }
}
