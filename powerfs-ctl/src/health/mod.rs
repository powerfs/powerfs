//! Raft health gate — blocks `powerfs-ctl up` until the master quorum has
//! elected exactly one leader AND that leader can actually commit
//! (commit_index greater than zero, last_applied equals commit_index, term
//! stable). This catches zombie leaders created by asymmetric split-brain
//! (heartbeats round-trip but AppendEntries responses dropped) that scheme C
//! inside master cannot detect.

pub mod reqwest_probe;
pub use reqwest_probe::ReqwestProbe;

use async_trait::async_trait;
use std::time::{Duration, Instant};

/// Snapshot of one master's raft state as observed via /metrics + /healthz.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterMetrics {
    pub is_leader: bool,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub healthz_ok: bool,
}

#[async_trait]
pub trait MetricsProbe: Send + Sync {
    async fn fetch(&self, ip: &str, port: u16) -> Result<MasterMetrics, String>;
}

/// Outcome of a health-gate run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaderInfo {
    pub ip: String,
    pub term: u64,
    pub commit_index: u64,
}

pub struct HealthGate {
    pub port: u16,
    pub poll_interval: Duration,
    pub timeout: Duration,
}

impl HealthGate {
    pub fn new(port: u16) -> Self {
        Self {
            port,
            poll_interval: Duration::from_secs(2),
            timeout: Duration::from_secs(120),
        }
    }

    /// Block until exactly one leader emerges and proves it can commit.
    ///
    /// Algorithm:
    /// 1. Poll every master; collect those reporting is_leader.
    /// 2. If !=1 leader: keep polling (timeout → "no leader or split-brain").
    /// 3. Once exactly 1 leader: take a second sample after poll_interval.
    /// 4. Pass iff: commit_index > 0 AND last_applied == commit_index AND
    ///    term unchanged across the two samples AND healthz_ok.
    ///    — commit_index==0 → zombie (can't even commit initial membership).
    ///    — last_applied < commit_index → applier stalled.
    ///    — term changed → election storm.
    pub async fn run(
        &self,
        probe: &dyn MetricsProbe,
        master_ips: &[String],
    ) -> Result<LeaderInfo, String> {
        if master_ips.is_empty() {
            return Err("health gate: no master IPs configured".into());
        }
        let deadline = Instant::now() + self.timeout;
        loop {
            if Instant::now() >= deadline {
                return Err(format!(
                    "health gate: timed out after {:?} waiting for a healthy leader",
                    self.timeout
                ));
            }
            let samples = sample_all(probe, self.port, master_ips).await;
            let mut leaders: Vec<(String, MasterMetrics)> = Vec::new();
            for (ip, res) in samples {
                match res {
                    Ok(m) if m.is_leader => leaders.push((ip, m)),
                    Ok(_) => {}
                    Err(e) => {
                        // transient probe failure — log and continue; the
                        // timeout will catch persistent unreachable nodes.
                        eprintln!("  health: probe {} failed: {}", ip, e);
                    }
                }
            }
            if leaders.len() > 1 {
                // Two+ nodes in openraft Leader state simultaneously is a
                // definitive split-brain (not transient — Candidates don't
                // set is_leader; only elected Leaders do). Fail fast.
                let ips: Vec<&str> = leaders.iter().map(|(ip, _)| ip.as_str()).collect();
                return Err(format!(
                    "health gate: split-brain — {} masters claim leadership ({})",
                    leaders.len(),
                    ips.join(", ")
                ));
            }
            if leaders.is_empty() {
                if Instant::now() + self.poll_interval >= deadline {
                    return Err(format!(
                        "health gate: no leader elected after {:?} — quorum may be down",
                        self.timeout
                    ));
                }
                eprintln!("  health: waiting for leader election...");
                tokio::time::sleep(self.poll_interval).await;
                continue;
            }
            // exactly one leader — take a second sample to verify stability.
            let (lip, m1) = leaders.pop().unwrap();
            if !m1.healthz_ok {
                eprintln!("  health: leader {} up but /healthz not OK (scheme C flagged fake-leader); waiting...", lip);
                tokio::time::sleep(self.poll_interval).await;
                continue;
            }
            tokio::time::sleep(self.poll_interval).await;
            if Instant::now() >= deadline {
                return Err(format!(
                    "health gate: timed out during second sample of leader {}",
                    lip
                ));
            }
            let m2 = probe.fetch(&lip, self.port).await?;
            if m2.term != m1.term {
                return Err(format!(
                    "health gate: leader {} term changed {}→{} — election storm (asymmetric network?)",
                    lip, m1.term, m2.term
                ));
            }
            if m2.commit_index == 0 {
                return Err(format!(
                    "health gate: leader {} commit_index stalled at 0 — zombie leader \
                     (heartbeats round-trip but AppendEntries can't commit; \
                     check MTU/firewall for dropped data packets vs control packets)",
                    lip
                ));
            }
            if m2.commit_index < m1.commit_index {
                return Err(format!(
                    "health gate: leader {} commit_index went {}→{} (regression impossible)",
                    lip, m1.commit_index, m2.commit_index
                ));
            }
            if m2.last_applied < m2.commit_index {
                return Err(format!(
                    "health gate: leader {} applier lagging — last_applied={} < commit_index={}; \
                     state machine replay is stalled",
                    lip, m2.last_applied, m2.commit_index
                ));
            }
            return Ok(LeaderInfo {
                ip: lip,
                term: m2.term,
                commit_index: m2.commit_index,
            });
        }
    }
}

/// One-shot findings (vs HealthGate, which polls until healthy or timeout).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftFinding {
    Healthy {
        leader: String,
        term: u64,
        commit_index: u64,
    },
    /// Nobody claims leadership (cluster stopped or mid-election).
    NoLeader,
    /// Every probe failed — usually a network/daemon problem rather than raft.
    ProbeFailure { errors: Vec<(String, String)> },
    /// Two+ masters claim leadership simultaneously.
    SplitBrain { leaders: Vec<String> },
    /// A leader exists but cannot serve: commit_index==0 (asymmetric
    /// split-brain zombie) or /healthz still rejects it (scheme C fake-leader).
    Zombie { leader: String },
    /// Raft commits, but the state-machine applier is behind.
    ApplierLag {
        leader: String,
        last_applied: u64,
        commit_index: u64,
    },
    /// Leader term moved between the two samples — election storm.
    ElectionStorm {
        leader: String,
        term_from: u64,
        term_to: u64,
    },
}

/// Probe every master once. Failed probes are kept as Err so callers can
/// distinguish "reachable but follower" from "unreachable".
pub(crate) async fn sample_all(
    probe: &dyn MetricsProbe,
    port: u16,
    master_ips: &[String],
) -> Vec<(String, Result<MasterMetrics, String>)> {
    let mut out = Vec::with_capacity(master_ips.len());
    for ip in master_ips {
        let res = probe.fetch(ip, port).await;
        out.push((ip.clone(), res));
    }
    out
}

/// Single-round raft diagnosis: sample every master, and when exactly one
/// leader shows up, take a second sample after `settle` to verify term
/// stability and commit progress. Never polls or waits beyond `settle`.
pub async fn inspect(
    probe: &dyn MetricsProbe,
    port: u16,
    master_ips: &[String],
    settle: Duration,
) -> RaftFinding {
    let samples = sample_all(probe, port, master_ips).await;
    let mut leaders: Vec<(String, MasterMetrics)> = Vec::new();
    let mut errors: Vec<(String, String)> = Vec::new();
    for (ip, res) in samples {
        match res {
            Ok(m) if m.is_leader => leaders.push((ip, m)),
            Ok(_) => {}
            Err(e) => errors.push((ip, e)),
        }
    }
    if leaders.len() > 1 {
        return RaftFinding::SplitBrain {
            leaders: leaders.into_iter().map(|(ip, _)| ip).collect(),
        };
    }
    if leaders.is_empty() {
        return if !errors.is_empty() && errors.len() == master_ips.len() {
            RaftFinding::ProbeFailure { errors }
        } else {
            RaftFinding::NoLeader
        };
    }

    let (lip, m1) = leaders.pop().unwrap();
    if !m1.healthz_ok {
        // scheme C flagged this leader as fake — it cannot serve writes.
        return RaftFinding::Zombie { leader: lip };
    }
    tokio::time::sleep(settle).await;
    let m2 = match probe.fetch(&lip, port).await {
        Ok(m) => m,
        Err(e) => {
            return RaftFinding::ProbeFailure {
                errors: vec![(lip, e)],
            }
        }
    };
    if m2.term != m1.term {
        return RaftFinding::ElectionStorm {
            leader: lip,
            term_from: m1.term,
            term_to: m2.term,
        };
    }
    if m2.commit_index == 0 {
        return RaftFinding::Zombie { leader: lip };
    }
    if m2.last_applied < m2.commit_index {
        return RaftFinding::ApplierLag {
            leader: lip,
            last_applied: m2.last_applied,
            commit_index: m2.commit_index,
        };
    }
    RaftFinding::Healthy {
        leader: lip,
        term: m2.term,
        commit_index: m2.commit_index,
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Mock probe that returns canned metrics per-IP, cycling through a
    /// sequence of states to simulate leader election / stalls.
    pub struct MockProbe {
        pub states: Mutex<Vec<MasterMetrics>>,
    }
    impl MockProbe {
        pub fn fixed(m: MasterMetrics) -> Self {
            Self {
                states: Mutex::new(vec![m]),
            }
        }
    }
    #[async_trait]
    impl MetricsProbe for MockProbe {
        async fn fetch(&self, _ip: &str, _port: u16) -> Result<MasterMetrics, String> {
            let mut s = self.states.lock().unwrap();
            if s.len() == 1 {
                Ok(s[0].clone())
            } else {
                Ok(s.remove(0))
            }
        }
    }

    fn leader(commit: u64, applied: u64, term: u64) -> MasterMetrics {
        MasterMetrics {
            is_leader: true,
            term,
            commit_index: commit,
            last_applied: applied,
            healthz_ok: true,
        }
    }
    fn follower() -> MasterMetrics {
        MasterMetrics {
            is_leader: false,
            term: 1,
            commit_index: 0,
            last_applied: 0,
            healthz_ok: true,
        }
    }

    fn gate() -> HealthGate {
        HealthGate {
            port: 9300,
            poll_interval: Duration::from_millis(1),
            timeout: Duration::from_millis(500),
        }
    }

    #[tokio::test]
    async fn gate_detects_zombie_leader() {
        // leader but commit_index==0 → zombie
        let probe = MockProbe::fixed(leader(0, 0, 1));
        let g = gate();
        let err = g.run(&probe, &["m1".into()]).await.unwrap_err();
        assert!(err.contains("zombie"), "got: {err}");
    }

    #[tokio::test]
    async fn gate_passes_when_committed_and_applied() {
        // two samples: first leader(5,5,2), second leader(5,5,2) — stable
        let probe = MockProbe {
            states: Mutex::new(vec![leader(5, 5, 2), leader(5, 5, 2)]),
        };
        let g = gate();
        let info = g.run(&probe, &["m1".into()]).await.unwrap();
        assert_eq!(info.ip, "m1");
        assert_eq!(info.commit_index, 5);
    }

    #[tokio::test]
    async fn gate_rejects_lagging_applier() {
        let probe = MockProbe {
            states: Mutex::new(vec![leader(5, 3, 2), leader(5, 3, 2)]),
        };
        let g = gate();
        let err = g.run(&probe, &["m1".into()]).await.unwrap_err();
        assert!(err.contains("applier lagging"), "got: {err}");
    }

    #[tokio::test]
    async fn gate_rejects_term_change() {
        let probe = MockProbe {
            states: Mutex::new(vec![leader(5, 5, 2), leader(5, 5, 3)]),
        };
        let g = gate();
        let err = g.run(&probe, &["m1".into()]).await.unwrap_err();
        assert!(err.contains("election storm"), "got: {err}");
    }

    #[tokio::test]
    async fn gate_rejects_multiple_leaders() {
        // two masters both claim leadership — but MockProbe returns the same
        // state for any IP, so both become leaders → split-brain timeout.
        let probe = MockProbe::fixed(leader(5, 5, 2));
        let g = gate();
        let err = g
            .run(&probe, &["m1".into(), "m2".into()])
            .await
            .unwrap_err();
        assert!(
            err.contains("split-brain") || err.contains("expected 1 leader"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn gate_rejects_unhealthy_leader() {
        // leader but healthz 503 (scheme C flagged fake-leader)
        let mut m = leader(5, 5, 2);
        m.healthz_ok = false;
        let probe = MockProbe::fixed(m);
        let g = gate();
        let err = g.run(&probe, &["m1".into()]).await.unwrap_err();
        // should timeout because leader is never healthy
        assert!(
            err.contains("timed out") || err.contains("not OK"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn gate_waits_for_follower_to_become_leader() {
        // first sample follower, second sample leader — but MockProbe with
        // 2 states cycles: first call returns follower, then leader.
        let probe = MockProbe {
            states: Mutex::new(vec![follower(), leader(3, 3, 2), leader(3, 3, 2)]),
        };
        let g = gate();
        let info = g.run(&probe, &["m1".into()]).await.unwrap();
        assert_eq!(info.commit_index, 3);
    }

    /// Probe whose every fetch fails (docker daemon down / network cut).
    struct FailingProbe;
    #[async_trait]
    impl MetricsProbe for FailingProbe {
        async fn fetch(&self, _ip: &str, _port: u16) -> Result<MasterMetrics, String> {
            Err("connection refused".into())
        }
    }

    #[tokio::test]
    async fn inspect_reports_healthy_leader() {
        // round 1 across 3 IPs: leader, follower, follower; round 2: leader.
        let probe = MockProbe {
            states: Mutex::new(vec![
                leader(5, 5, 2),
                follower(),
                follower(),
                leader(6, 6, 2),
            ]),
        };
        let f = inspect(
            &probe,
            9300,
            &["m1".into(), "m2".into(), "m3".into()],
            Duration::from_millis(0),
        )
        .await;
        assert_eq!(
            f,
            RaftFinding::Healthy {
                leader: "m1".into(),
                term: 2,
                commit_index: 6,
            }
        );
    }

    #[tokio::test]
    async fn inspect_reports_zombie() {
        let probe = MockProbe::fixed(leader(0, 0, 1));
        let f = inspect(&probe, 9300, &["m1".into()], Duration::from_millis(0)).await;
        assert_eq!(
            f,
            RaftFinding::Zombie {
                leader: "m1".into()
            }
        );
    }

    #[tokio::test]
    async fn inspect_reports_split_brain() {
        // every IP reports the same leader state → two leaders in round 1.
        let probe = MockProbe::fixed(leader(5, 5, 2));
        let f = inspect(
            &probe,
            9300,
            &["m1".into(), "m2".into()],
            Duration::from_millis(0),
        )
        .await;
        assert_eq!(
            f,
            RaftFinding::SplitBrain {
                leaders: vec!["m1".into(), "m2".into()]
            }
        );
    }

    #[tokio::test]
    async fn inspect_reports_applier_lag() {
        let probe = MockProbe::fixed(leader(5, 3, 2));
        let f = inspect(&probe, 9300, &["m1".into()], Duration::from_millis(0)).await;
        assert_eq!(
            f,
            RaftFinding::ApplierLag {
                leader: "m1".into(),
                last_applied: 3,
                commit_index: 5,
            }
        );
    }

    #[tokio::test]
    async fn inspect_reports_probe_failure_when_all_unreachable() {
        let f = inspect(
            &FailingProbe,
            9300,
            &["m1".into(), "m2".into()],
            Duration::from_millis(0),
        )
        .await;
        match f {
            RaftFinding::ProbeFailure { errors } => {
                assert_eq!(errors.len(), 2);
                assert!(errors[0].1.contains("connection refused"));
            }
            other => panic!("expected ProbeFailure, got {other:?}"),
        }
    }
}
