//! Reports FUSE client runtime statistics to the Master via the
//! `KeepConnected` gRPC bidi stream.
//!
//! The reporter owns a single background task that:
//! 1. Opens a `KeepConnected` stream with the master.
//! 2. Sends an initial `KeepConnectedRequest` carrying client identity.
//! 3. Every `report_interval`, snapshots `ClientStats` from the
//!    `VolumeClient` (and optionally other providers) and sends a new
//!    request so the master always has fresh stats.
//! 4. Reconnects automatically on stream errors.
//!
//! Stats that are not owned by `VolumeClient` (latency p50/p99, connection
//! pool counters, coalescer counters) are left at their default `0` for now
//! and can be overlaid by future providers.

use std::sync::Arc;
use std::time::Duration;

use log::{info, warn};
use tokio::sync::mpsc;
use tonic::transport::Channel;
use tonic::Status;

use powerfs_master::proto::powerfs::master_service_client::MasterServiceClient;
use powerfs_master::proto::powerfs::KeepConnectedRequest;

use crate::volume_client::VolumeClient;

/// Identity and connection parameters for the stats reporter.
#[derive(Clone)]
pub struct StatsReporterConfig {
    /// gRPC endpoint of the master, e.g. `http://127.0.0.1:9333`.
    pub master_endpoint: String,
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
            master_endpoint: String::new(),
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

/// Background reporter that pushes `ClientStats` to the master.
pub struct MasterStatsReporter {
    config: StatsReporterConfig,
    volume_client: Arc<VolumeClient>,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl MasterStatsReporter {
    pub fn new(config: StatsReporterConfig, volume_client: Arc<VolumeClient>) -> Self {
        Self {
            config,
            volume_client,
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

        let config = self.config.clone();
        let volume_client = self.volume_client.clone();

        self.join_handle = Some(tokio::spawn(async move {
            run_reporter_loop(config, volume_client, shutdown_rx).await;
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
    volume_client: Arc<VolumeClient>,
    mut shutdown_rx: tokio::sync::oneshot::Receiver<()>,
) {
    if config.master_endpoint.is_empty() {
        info!("MasterStatsReporter: no master_endpoint configured, not starting");
        return;
    }
    info!(
        "MasterStatsReporter: starting (master={}, mount={}, interval={}s)",
        config.master_endpoint,
        config.mount_point,
        config.report_interval.as_secs()
    );

    loop {
        tokio::select! {
            _ = &mut shutdown_rx => {
                info!("MasterStatsReporter: shutdown received, stopping");
                return;
            }
            res = run_one_session(&config, &volume_client) => {
                match res {
                    Ok(()) => {
                        // Stream ended cleanly (master closed); reconnect after a short delay.
                        warn!("MasterStatsReporter: stream closed by master, reconnecting in 1s");
                    }
                    Err(e) => {
                        warn!("MasterStatsReporter: session error: {}, reconnecting in 1s", e);
                    }
                }
            }
        }

        // Backoff before reconnecting, but remain responsive to shutdown.
        tokio::select! {
            _ = &mut shutdown_rx => {
                info!("MasterStatsReporter: shutdown during backoff, stopping");
                return;
            }
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

/// Run a single KeepConnected session until the stream ends or errors.
async fn run_one_session(
    config: &StatsReporterConfig,
    volume_client: &VolumeClient,
) -> Result<(), Status> {
    let channel = Channel::from_shared(config.master_endpoint.clone())
        .map_err(|e| Status::invalid_argument(format!("bad endpoint: {}", e)))?
        .connect_timeout(Duration::from_secs(5))
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .connect()
        .await
        .map_err(|e| Status::unavailable(format!("connect failed: {}", e)))?;

    let mut client = MasterServiceClient::new(channel);

    // Bidirectional stream: master reads our requests and sends VolumeLocation updates.
    let (tx, rx) = mpsc::channel::<KeepConnectedRequest>(16);
    let response_stream = client
        .keep_connected(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await?;

    let mut resp_stream = response_stream.into_inner();

    // Send the initial registration request with identity + current stats.
    let initial = build_request(config, volume_client);
    if tx.send(initial).await.is_err() {
        return Ok(());
    }

    // Now alternate between: master responses (drain), timer (send stats), shutdown.
    let mut interval = tokio::time::interval(config.report_interval);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // First tick fires immediately; skip it since we already sent the initial request.
    interval.tick().await;

    loop {
        tokio::select! {
            msg = resp_stream.message() => {
                match msg {
                    Ok(Some(_resp)) => {
                        // VolumeLocation updates from master; we don't act on them here
                        // (topology is handled by MasterClient). Just drain.
                    }
                    Ok(None) => {
                        // Stream closed by server.
                        return Ok(());
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            _ = interval.tick() => {
                let req = build_request(config, volume_client);
                if tx.send(req).await.is_err() {
                    // Receiver dropped (stream closed); exit.
                    return Ok(());
                }
            }
        }
    }
}

fn build_request(
    config: &StatsReporterConfig,
    volume_client: &VolumeClient,
) -> KeepConnectedRequest {
    let stats = Some(volume_client.client_stats());
    KeepConnectedRequest {
        client_type: config.client_type.clone(),
        client_address: String::new(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        filer_group: String::new(),
        data_center: String::new(),
        rack: String::new(),
        mount_point: config.mount_point.clone(),
        collection: config.collection.clone(),
        replication: config.replication.clone(),
        pid: config.pid,
        host: config.host.clone(),
        dirty_chunks: 0,
        dirty_bytes: 0,
        stats,
    }
}
