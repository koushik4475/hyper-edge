pub mod entry;
pub mod eviction;
pub mod policy;

pub use entry::CacheEntry;
pub use eviction::EvictionEngine;
pub use policy::EvictionPolicy;

use crate::config::CacheConfig;
use crate::metrics::EdgeMetrics;
use bytes::Bytes;
use dashmap::DashMap;
use serde::Serialize;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

pub struct EdgeCache {
    map: DashMap<String, Arc<CacheEntry>>,
    metrics: Arc<EdgeMetrics>,
    max_capacity: usize,
    max_memory_bytes: usize,
    current_memory: AtomicUsize,
    default_ttl: Duration,
    policy: EvictionPolicy,
    eviction_engine: EvictionEngine,
}

#[derive(Debug, Clone, Serialize)]
pub struct CacheStats {
    pub count: usize,
    pub max_capacity: usize,
    pub memory_bytes: usize,
    pub max_memory_bytes: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub hit_ratio: f64,
}

impl EdgeCache {
    pub fn new(config: &CacheConfig, metrics: Arc<EdgeMetrics>) -> Arc<Self> {
        let policy = EvictionPolicy::from_str(&config.eviction_policy).unwrap_or(EvictionPolicy::Lru);
        let eviction_engine = EvictionEngine::new(policy, 16);

        Arc::new(Self {
            map: DashMap::new(),
            metrics,
            max_capacity: config.max_capacity,
            max_memory_bytes: config.max_memory_bytes,
            current_memory: AtomicUsize::new(0),
            default_ttl: Duration::from_secs(config.default_ttl_secs),
            policy,
            eviction_engine,
        })
    }

    pub fn get(&self, key: &str) -> Option<Arc<CacheEntry>> {
        self.metrics.inc_requests();

        if let Some(item) = self.map.get(key) {
            let entry = item.value().clone();
            if entry.is_expired() {
                drop(item);
                self.remove(key);
                self.metrics.inc_cache_miss();
                None
            } else {
                entry.touch();
                self.metrics.inc_cache_hit();
                Some(entry)
            }
        } else {
            self.metrics.inc_cache_miss();
            None
        }
    }

    pub fn insert(
        &self,
        key: String,
        value: Bytes,
        ttl: Option<Duration>,
        content_type: Option<String>,
        status_code: u16,
    ) -> Arc<CacheEntry> {
        let effective_ttl = ttl.or(Some(self.default_ttl));
        let entry = Arc::new(CacheEntry::new(
            key.clone(),
            value,
            effective_ttl,
            content_type,
            status_code,
        ));
        let item_size = entry.size_bytes;

        // An entry larger than the entire cache budget, or a disabled cache,
        // must not trigger an eviction loop or displace useful entries.
        if self.max_capacity == 0 || item_size > self.max_memory_bytes {
            return entry;
        }

        // Replacing a key should not evict an unrelated entry just because the
        // map is currently at its count limit.
        self.remove(&key);

        // Ensure capacity and memory limits before inserting. Under contention,
        // an eviction may fail; in that case leave the cache unchanged.
        if !self.enforce_capacity(item_size) {
            return entry;
        }

        self.map.insert(key, entry.clone());
        self.current_memory.fetch_add(item_size, Ordering::Relaxed);

        entry
    }

    pub fn remove(&self, key: &str) -> bool {
        if let Some((_, old)) = self.map.remove(key) {
            self.current_memory.fetch_sub(old.size_bytes, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    fn enforce_capacity(&self, incoming_bytes: usize) -> bool {
        // Check if eviction needed for count capacity.
        while self.map.len() >= self.max_capacity {
            if !self.evict_one() {
                return false;
            }
        }

        // Check if eviction needed for memory limit. Saturating arithmetic
        // avoids overflow if a caller supplies an extreme configured budget.
        while self.current_memory.load(Ordering::Relaxed).saturating_add(incoming_bytes)
            > self.max_memory_bytes
            && !self.map.is_empty()
        {
            if !self.evict_one() {
                return false;
            }
        }

        self.map.len() < self.max_capacity
            && self.current_memory.load(Ordering::Relaxed).saturating_add(incoming_bytes)
                <= self.max_memory_bytes
    }

    fn evict_one(&self) -> bool {
        let candidates = self.eviction_engine.select_eviction_candidates(&self.map, 1);
        if let Some(target) = candidates.first() {
            if self.remove(target) {
                self.metrics.inc_cache_eviction();
                debug!(target = target.as_str(), "Evicted cache entry");
                return true;
            }
        }
        false
    }

    pub fn purge_expired(&self) -> usize {
        let mut purged = 0;
        let mut expired_keys = Vec::new();

        for item in self.map.iter() {
            if item.value().is_expired() {
                expired_keys.push(item.key().clone());
            }
        }

        for key in expired_keys {
            if self.remove(&key) {
                purged += 1;
            }
        }

        if purged > 0 {
            debug!(count = purged, "Purged expired cache entries");
        }
        purged
    }

    pub fn clear(&self) {
        self.map.clear();
        self.current_memory.store(0, Ordering::Relaxed);
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn memory_usage(&self) -> usize {
        self.current_memory.load(Ordering::Relaxed)
    }

    pub fn policy(&self) -> EvictionPolicy {
        self.policy
    }

    pub fn stats(&self) -> CacheStats {
        let snap = self.metrics.snapshot();
        CacheStats {
            count: self.map.len(),
            max_capacity: self.max_capacity,
            memory_bytes: self.current_memory.load(Ordering::Relaxed),
            max_memory_bytes: self.max_memory_bytes,
            hits: snap.cache_hits_total,
            misses: snap.cache_misses_total,
            evictions: snap.cache_evictions_total,
            hit_ratio: snap.hit_ratio,
        }
    }

    pub fn start_janitor(cache: Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let reclaimed = cache.purge_expired();
                if reclaimed > 0 {
                    info!(reclaimed, "Cache janitor sweep completed");
                }
            }
        })
    }
}
