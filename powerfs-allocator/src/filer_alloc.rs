//! Filer-side allocation decision logic (P4).
//!
//! Extracted from `FilerNetHandler::alloc_for_new_file` /
//! `alloc_for_stripe_file` in `powerfs-filer/src/net_handler.rs`.
//!
//! ## Decision vs Execution
//!
//! This module is **pure decision**: given a read-only view of zones +
//! volumes, it decides *which* volumes to use and in *what* order. It has
//! no locks, no atomic counters, no side effects — making it trivially
//! testable and replaceable.
//!
//! The filer keeps the **execution** (allocating `needle_id`s from per-zone
//! atomic counters) because counter ownership is a service-level concern.
//! The flow is:
//!
//! ```text
//! Filer builds ZoneView snapshot from ZoneState
//!   → FilerAllocator::pick_for_new_file(&zones) → VolumePick
//!   → Filer allocates needle_id from pick.zone_id's counter
//!   → returns (volume_id, needle_id)
//! ```
//!
//! This is an intermediate step (P4). Once P5 enriches heartbeats with
//! load metrics, the filer can switch to the `Allocator` trait
//! (ClusterSnapshot-based) without changing the execution layer.

use std::sync::atomic::{AtomicU32, Ordering};

use powerfs_common::types::ZoneVolume;

/// Read-only view of a zone + its volumes.
///
/// Built by the filer from its `ZoneState` list (without the atomic counter,
/// which stays in the filer for needle_id allocation).
#[derive(Clone, Debug)]
pub struct ZoneView {
    pub zone_id: u32,
    pub volumes: Vec<ZoneVolume>,
}

/// A volume selected by the allocator for a file chunk.
///
/// The filer uses `zone_id` to locate the per-zone counter and allocate a
/// `needle_id`, then pairs it with `volume_id`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VolumePick {
    pub volume_id: u64,
    pub zone_id: u32,
    pub node_id: String,
}

/// Filer-side allocator: pure decision logic over zone/volume lists.
///
/// Holds a round-robin counter for zone selection. Clone-safe (each clone
/// shares the RR counter via the AtomicU32 — the filer holds one instance).
#[derive(Debug)]
pub struct FilerAllocator {
    zone_rr: AtomicU32,
}

impl Default for FilerAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl FilerAllocator {
    /// Create a new allocator with the zone round-robin counter at zero.
    pub fn new() -> Self {
        Self {
            zone_rr: AtomicU32::new(0),
        }
    }

    /// Decide which volume to use for a new single file.
    ///
    /// Strategy (identical to the original `alloc_for_new_file`):
    /// 1. Round-robin select a zone (`zone_rr.fetch_add % zones.len()`).
    /// 2. From the selected zone, pick the volume with the most free space.
    ///
    /// Returns `None` if no zones or no volumes are available.
    pub fn pick_for_new_file(&self, zones: &[ZoneView]) -> Option<VolumePick> {
        if zones.is_empty() {
            return None;
        }
        let rr = self.zone_rr.fetch_add(1, Ordering::SeqCst);
        let idx = (rr as usize) % zones.len();
        let zone = &zones[idx];

        let vol = select_volume_by_free_space(&zone.volumes)?;
        Some(VolumePick {
            volume_id: vol.volume_id,
            zone_id: zone.zone_id,
            node_id: vol.node_id.clone(),
        })
    }

    /// Decide which volumes to use for a stripe file (`count` chunks).
    ///
    /// Strategy (identical to the original `alloc_for_stripe_file`):
    /// 1. Collect all volumes across all zones, recording node_id + zone_idx.
    /// 2. Group volumes by node_id (anti-affinity).
    /// 3. Round-robin across nodes, picking one volume per node per pass,
    ///    until `count` picks are gathered or volumes are exhausted.
    ///
    /// Returns `None` if no volumes are available.
    pub fn pick_for_stripe_file(
        &self,
        zones: &[ZoneView],
        count: usize,
    ) -> Option<Vec<VolumePick>> {
        if count == 0 {
            return Some(Vec::new());
        }

        // Collect all volumes with their zone index.
        #[derive(Clone)]
        struct VolEntry {
            volume_id: u64,
            node_id: String,
            zone_id: u32,
        }
        let mut all_volumes: Vec<VolEntry> = Vec::new();
        for zone in zones {
            for vol in &zone.volumes {
                all_volumes.push(VolEntry {
                    volume_id: vol.volume_id,
                    node_id: vol.node_id.clone(),
                    zone_id: zone.zone_id,
                });
            }
        }
        if all_volumes.is_empty() {
            return None;
        }

        // Group by node_id, preserving zone-traversal order within each group.
        use std::collections::HashMap;
        let mut node_groups: Vec<(String, Vec<VolEntry>)> = Vec::new();
        let mut node_map: HashMap<String, usize> = HashMap::new();
        for vol in &all_volumes {
            if let std::collections::hash_map::Entry::Vacant(e) =
                node_map.entry(vol.node_id.clone())
            {
                e.insert(node_groups.len());
                node_groups.push((vol.node_id.clone(), Vec::new()));
            }
            let idx = node_map[&vol.node_id];
            node_groups[idx].1.push(vol.clone());
        }

        // Round-robin across nodes, one volume per node per pass.
        let mut result = Vec::with_capacity(count);
        loop {
            let mut picked = false;
            for (_, group) in node_groups.iter_mut() {
                if result.len() >= count {
                    break;
                }
                if let Some(vol) = group.first().cloned() {
                    result.push(VolumePick {
                        volume_id: vol.volume_id,
                        zone_id: vol.zone_id,
                        node_id: vol.node_id.clone(),
                    });
                    group.remove(0);
                    picked = true;
                }
            }
            if !picked || result.len() >= count {
                break;
            }
        }

        Some(result)
    }
}

/// Select the volume with the most free space (free ratio).
///
/// Extracted from `zone_client::select_volume` so the allocator crate is
/// self-contained. Identical logic: `1.0 - used/size`, max wins.
pub(crate) fn select_volume_by_free_space(volumes: &[ZoneVolume]) -> Option<&ZoneVolume> {
    volumes.iter().max_by(|a, b| {
        let free_a = free_ratio(a);
        let free_b = free_ratio(b);
        free_a.partial_cmp(&free_b).unwrap_or(std::cmp::Ordering::Equal)
    })
}

/// Free-space ratio of a volume (0.0 = full, 1.0 = empty).
fn free_ratio(v: &ZoneVolume) -> f64 {
    if v.size == 0 {
        0.0
    } else {
        1.0 - (v.used as f64 / v.size as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zv(volume_id: u64, node_id: &str, size: u64, used: u64) -> ZoneVolume {
        ZoneVolume {
            volume_id,
            addr: format!("10.0.0.{}", volume_id),
            size,
            used,
            node_id: node_id.to_string(),
        }
    }

    fn zone(zone_id: u32, volumes: Vec<ZoneVolume>) -> ZoneView {
        ZoneView { zone_id, volumes }
    }

    #[test]
    fn test_pick_for_new_file_round_robin_zone() {
        let alloc = FilerAllocator::new();
        let zones = vec![
            zone(1, vec![zv(10, "n1", 100, 10)]),
            zone(2, vec![zv(20, "n2", 100, 10)]),
        ];

        // First call → zone 1 (rr=0), second → zone 2 (rr=1), third → zone 1
        let p1 = alloc.pick_for_new_file(&zones).unwrap();
        assert_eq!(p1.zone_id, 1);
        assert_eq!(p1.volume_id, 10);

        let p2 = alloc.pick_for_new_file(&zones).unwrap();
        assert_eq!(p2.zone_id, 2);
        assert_eq!(p2.volume_id, 20);

        let p3 = alloc.pick_for_new_file(&zones).unwrap();
        assert_eq!(p3.zone_id, 1);
    }

    #[test]
    fn test_pick_for_new_file_selects_most_free() {
        let alloc = FilerAllocator::new();
        let zones = vec![zone(1, vec![
            zv(1, "n1", 100, 80), // 20% free
            zv(2, "n2", 100, 20), // 80% free
            zv(3, "n3", 100, 50), // 50% free
        ])];

        let pick = alloc.pick_for_new_file(&zones).unwrap();
        assert_eq!(pick.volume_id, 2); // most free space
    }

    #[test]
    fn test_pick_for_new_file_empty_zones() {
        let alloc = FilerAllocator::new();
        assert!(alloc.pick_for_new_file(&[]).is_none());
    }

    #[test]
    fn test_pick_for_new_file_empty_volumes() {
        let alloc = FilerAllocator::new();
        let zones = vec![zone(1, vec![])];
        assert!(alloc.pick_for_new_file(&zones).is_none());
    }

    #[test]
    fn test_pick_for_stripe_file_anti_affinity() {
        let alloc = FilerAllocator::new();
        // 3 nodes, 2 volumes each → 6 volumes
        let zones = vec![zone(1, vec![
            zv(1, "n1", 100, 0),
            zv(2, "n1", 100, 0),
            zv(3, "n2", 100, 0),
            zv(4, "n2", 100, 0),
            zv(5, "n3", 100, 0),
            zv(6, "n3", 100, 0),
        ])];

        let picks = alloc.pick_for_stripe_file(&zones, 3).unwrap();
        assert_eq!(picks.len(), 3);

        // Anti-affinity: first 3 picks should be on 3 different nodes
        let nodes: std::collections::HashSet<&str> =
            picks.iter().map(|p| p.node_id.as_str()).collect();
        assert_eq!(nodes.len(), 3, "first 3 picks should span 3 nodes");
    }

    #[test]
    fn test_pick_for_stripe_file_count_zero() {
        let alloc = FilerAllocator::new();
        let zones = vec![zone(1, vec![zv(1, "n1", 100, 0)])];
        let picks = alloc.pick_for_stripe_file(&zones, 0).unwrap();
        assert!(picks.is_empty());
    }

    #[test]
    fn test_pick_for_stripe_file_no_volumes() {
        let alloc = FilerAllocator::new();
        let zones = vec![zone(1, vec![])];
        assert!(alloc.pick_for_stripe_file(&zones, 3).is_none());
    }

    #[test]
    fn test_pick_for_stripe_file_fewer_nodes_than_count() {
        let alloc = FilerAllocator::new();
        // 2 nodes, 3 volumes each → 6 volumes, request 5
        let zones = vec![zone(1, vec![
            zv(1, "n1", 100, 0),
            zv(2, "n1", 100, 0),
            zv(3, "n1", 100, 0),
            zv(4, "n2", 100, 0),
            zv(5, "n2", 100, 0),
            zv(6, "n2", 100, 0),
        ])];

        let picks = alloc.pick_for_stripe_file(&zones, 5).unwrap();
        assert_eq!(picks.len(), 5);
        // First 2 picks on different nodes, then round 2 fills remaining
        let n1_count = picks.iter().filter(|p| p.node_id == "n1").count();
        let n2_count = picks.iter().filter(|p| p.node_id == "n2").count();
        // 5 picks across 2 nodes: 3 + 2 (round-robin)
        assert_eq!(n1_count + n2_count, 5);
        assert!((n1_count as i32 - n2_count as i32).abs() <= 1);
    }

    #[test]
    fn test_pick_for_stripe_file_cross_zone() {
        let alloc = FilerAllocator::new();
        let zones = vec![
            zone(1, vec![zv(1, "n1", 100, 0), zv(2, "n2", 100, 0)]),
            zone(2, vec![zv(3, "n3", 100, 0), zv(4, "n4", 100, 0)]),
        ];

        let picks = alloc.pick_for_stripe_file(&zones, 4).unwrap();
        assert_eq!(picks.len(), 4);
        // Each pick carries its originating zone_id
        let zone_ids: std::collections::HashSet<u32> =
            picks.iter().map(|p| p.zone_id).collect();
        assert!(zone_ids.contains(&1));
        assert!(zone_ids.contains(&2));
    }

    #[test]
    fn test_free_ratio() {
        assert!((free_ratio(&zv(1, "n", 100, 0)) - 1.0).abs() < 1e-9);
        assert!((free_ratio(&zv(1, "n", 100, 100)) - 0.0).abs() < 1e-9);
        assert!((free_ratio(&zv(1, "n", 100, 30)) - 0.7).abs() < 1e-9);
        assert_eq!(free_ratio(&zv(1, "n", 0, 0)), 0.0); // size=0 guard
    }
}
