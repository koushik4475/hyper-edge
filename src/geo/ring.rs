use crate::error::{GeoError, Result};
use crate::geo::distance::haversine_distance_km;
use crate::geo::node::EdgeNode;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Deterministic 64-bit FNV-1a hash function for consistent ring distribution
pub fn hash_key(key: &str) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    let mut hash = FNV_OFFSET_BASIS;
    for byte in key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

pub struct GeoHashRing {
    vnodes_per_node: usize,
    nodes: RwLock<HashMap<String, Arc<EdgeNode>>>,
    // Sorted list of (vnode_hash, node_id)
    ring: RwLock<Vec<(u64, String)>>,
}

impl GeoHashRing {
    pub fn new(vnodes_per_node: usize) -> Self {
        Self {
            vnodes_per_node: vnodes_per_node.max(16),
            nodes: RwLock::new(HashMap::new()),
            ring: RwLock::new(Vec::new()),
        }
    }

    pub fn add_node(&self, node: Arc<EdgeNode>) {
        let mut nodes_lock = self.nodes.write();
        let mut ring_lock = self.ring.write();

        nodes_lock.insert(node.id.clone(), node.clone());

        // Rebuild virtual nodes for this node based on weight
        let effective_vnodes = (self.vnodes_per_node * (node.weight as usize)) / 100;
        let vnodes_count = effective_vnodes.max(16);

        // Remove any existing vnodes for this node_id
        ring_lock.retain(|(_, id)| id != &node.id);

        for i in 0..vnodes_count {
            let vnode_token = format!("{}:#vn-{}", node.id, i);
            let h = hash_key(&vnode_token);
            ring_lock.push((h, node.id.clone()));
        }

        // Keep ring sorted by hash token
        ring_lock.sort_by_key(|&(h, _)| h);
    }

    pub fn remove_node(&self, node_id: &str) -> bool {
        let mut nodes_lock = self.nodes.write();
        let mut ring_lock = self.ring.write();

        let removed = nodes_lock.remove(node_id).is_some();
        if removed {
            ring_lock.retain(|(_, id)| id != node_id);
        }
        removed
    }

    pub fn get_node_by_key(&self, key: &str) -> Result<Arc<EdgeNode>> {
        let ring_lock = self.ring.read();
        if ring_lock.is_empty() {
            return Err(GeoError::EmptyRing.into());
        }

        let key_hash = hash_key(key);
        // Binary search for first vnode with hash >= key_hash
        let idx = ring_lock.partition_point(|&(h, _)| h < key_hash);
        let node_idx = if idx == ring_lock.len() { 0 } else { idx };
        let node_id = &ring_lock[node_idx].1;

        let nodes_lock = self.nodes.read();
        nodes_lock
            .get(node_id)
            .cloned()
            .ok_or_else(|| GeoError::NodeNotFound(node_id.clone()).into())
    }

    /// Retrieve N distinct replica nodes for a given cache key
    pub fn get_replicas(&self, key: &str, replica_count: usize) -> Result<Vec<Arc<EdgeNode>>> {
        if replica_count == 0 {
            return Ok(Vec::new());
        }

        let ring_lock = self.ring.read();
        let nodes_lock = self.nodes.read();

        if ring_lock.is_empty() {
            return Err(GeoError::EmptyRing.into());
        }

        let total_nodes = nodes_lock.len();
        let count = replica_count.min(total_nodes);
        let key_hash = hash_key(key);

        let mut start_idx = ring_lock.partition_point(|&(h, _)| h < key_hash);
        if start_idx == ring_lock.len() {
            start_idx = 0;
        }

        let mut selected = Vec::with_capacity(count);
        let mut seen = std::collections::HashSet::new();

        let ring_len = ring_lock.len();
        for i in 0..ring_len {
            let idx = (start_idx + i) % ring_len;
            let node_id = &ring_lock[idx].1;

            if !seen.contains(node_id) {
                seen.insert(node_id.clone());
                if let Some(node) = nodes_lock.get(node_id) {
                    if node.is_healthy() {
                        selected.push(node.clone());
                    }
                }
                if selected.len() >= count {
                    break;
                }
            }
        }

        Ok(selected)
    }

    /// Find the geographically closest healthy PoP node to the client coordinates
    pub fn find_nearest_pop(&self, client_lat: f64, client_lon: f64) -> Result<Arc<EdgeNode>> {
        let nodes_lock = self.nodes.read();
        if nodes_lock.is_empty() {
            return Err(GeoError::EmptyRing.into());
        }

        let mut nearest_node: Option<Arc<EdgeNode>> = None;
        let mut min_distance = f64::MAX;

        for node in nodes_lock.values() {
            if !node.is_healthy() {
                continue;
            }
            let dist = haversine_distance_km(client_lat, client_lon, node.latitude, node.longitude);
            if dist < min_distance {
                min_distance = dist;
                nearest_node = Some(node.clone());
            }
        }

        nearest_node.ok_or_else(|| GeoError::EmptyRing.into())
    }

    /// Hybrid geo-consistent routing: looks up key replicas and ranks them by geographic proximity
    pub fn route_geo_consistent(
        &self,
        key: &str,
        client_lat: f64,
        client_lon: f64,
        replica_count: usize,
    ) -> Result<Arc<EdgeNode>> {
        let replicas = self.get_replicas(key, replica_count)?;
        if replicas.is_empty() {
            return Err(GeoError::EmptyRing.into());
        }

        // Rank replicas by distance to client
        let mut ranked: Vec<(f64, Arc<EdgeNode>)> = replicas
            .into_iter()
            .map(|node| {
                let d = haversine_distance_km(client_lat, client_lon, node.latitude, node.longitude);
                (d, node)
            })
            .collect();

        ranked.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        Ok(ranked[0].1.clone())
    }

    pub fn node_count(&self) -> usize {
        self.nodes.read().len()
    }

    pub fn vnode_count(&self) -> usize {
        self.ring.read().len()
    }
}
