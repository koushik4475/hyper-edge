use bytes::Bytes;
use hyper_edge::cache::{EdgeCache, EvictionPolicy};
use hyper_edge::config::CacheConfig;
use hyper_edge::metrics::EdgeMetrics;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_cache_hit_and_miss_tracking() {
    let metrics = EdgeMetrics::new();
    let config = CacheConfig {
        max_capacity: 50,
        max_memory_bytes: 1024 * 1024,
        default_ttl_secs: 60,
        eviction_policy: "lru".to_string(),
        cleanup_interval_secs: 5,
    };

    let cache = EdgeCache::new(&config, metrics.clone());

    // 1. Initial lookup -> MISS
    let miss = cache.get("user:1001");
    assert!(miss.is_none());
    assert_eq!(metrics.cache_misses_total.load(std::sync::atomic::Ordering::Relaxed), 1);
    assert_eq!(metrics.cache_hits_total.load(std::sync::atomic::Ordering::Relaxed), 0);

    // 2. Insert item
    cache.insert(
        "user:1001".to_string(),
        Bytes::from("Alice Cooper"),
        Some(Duration::from_secs(10)),
        Some("text/plain".to_string()),
        200,
    );

    // 3. Subsequent lookup -> HIT
    let hit = cache.get("user:1001");
    assert!(hit.is_some());
    let entry = hit.unwrap();
    assert_eq!(entry.value.as_ref(), b"Alice Cooper");
    assert_eq!(metrics.cache_hits_total.load(std::sync::atomic::Ordering::Relaxed), 1);

    // 4. Repeated hits
    for _ in 0..9 {
        let h = cache.get("user:1001");
        assert!(h.is_some());
    }

    // 10 hits, 1 miss -> 10/11 ~ 90.9% hit ratio
    let ratio = metrics.hit_ratio();
    assert!(ratio > 0.90, "Hit ratio was {}", ratio);
}

#[tokio::test]
async fn test_cache_ttl_expiration() {
    let metrics = EdgeMetrics::new();
    let config = CacheConfig {
        max_capacity: 10,
        max_memory_bytes: 1024 * 1024,
        default_ttl_secs: 1,
        eviction_policy: "lru".to_string(),
        cleanup_interval_secs: 1,
    };

    let cache = EdgeCache::new(&config, metrics.clone());

    // Insert with short TTL of 50ms
    cache.insert(
        "temp:token".to_string(),
        Bytes::from("secret-payload"),
        Some(Duration::from_millis(50)),
        None,
        200,
    );

    assert!(cache.get("temp:token").is_some());

    // Sleep past expiration
    tokio::time::sleep(Duration::from_millis(80)).await;

    // Lookup must detect expiration and return None
    assert!(cache.get("temp:token").is_none());
}

#[tokio::test]
async fn test_concurrent_zipfian_hit_ratio() {
    let metrics = EdgeMetrics::new();
    let config = CacheConfig {
        max_capacity: 30, // Can hold 30 items
        max_memory_bytes: 1024 * 1024,
        default_ttl_secs: 300,
        eviction_policy: "adaptive".to_string(),
        cleanup_interval_secs: 10,
    };

    let cache = EdgeCache::new(&config, metrics.clone());

    // Populate initial items
    for i in 0..100 {
        let key = format!("item:{}", i);
        cache.insert(
            key,
            Bytes::from(format!("value-data-{}", i)),
            Some(Duration::from_secs(300)),
            None,
            200,
        );
    }

    // Simulate 16 concurrent workers accessing keys with skewed Zipf-like distribution
    // 80% of accesses target the top 20 keys (item:0 to item:19)
    let mut handles = Vec::new();
    for worker_id in 0..8 {
        let cache_clone = cache.clone();
        handles.push(tokio::spawn(async move {
            for step in 0..500 {
                let key_id = if (step + worker_id) % 10 < 8 {
                    // Hot keys
                    (step * 7) % 20
                } else {
                    // Cold keys
                    20 + ((step * 13) % 80)
                };

                let key = format!("item:{}", key_id);
                if cache_clone.get(&key).is_none() {
                    // Cache miss replenishment
                    cache_clone.insert(
                        key,
                        Bytes::from(format!("value-data-{}", key_id)),
                        Some(Duration::from_secs(300)),
                        None,
                        200,
                    );
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let stats = cache.stats();
    println!("Workload complete. Stats: {:?}", stats);

    // Under skewed access, hit ratio should be high even with small capacity
    assert!(stats.hit_ratio > 0.40, "Expected hit ratio > 40%, got {}", stats.hit_ratio);
    assert!(stats.count <= 30, "Cache exceeded capacity: {}", stats.count);
}


#[tokio::test]
async fn test_zero_capacity_cache_does_not_loop_or_store_entries() {
    let metrics = EdgeMetrics::new();
    let config = CacheConfig {
        max_capacity: 0,
        max_memory_bytes: 1024,
        default_ttl_secs: 60,
        eviction_policy: "lru".to_string(),
        cleanup_interval_secs: 5,
    };
    let cache = EdgeCache::new(&config, metrics);

    cache.insert(
        "disabled".to_string(),
        Bytes::from("value"),
        None,
        None,
        200,
    );

    assert!(cache.is_empty());
    assert_eq!(cache.memory_usage(), 0);
}

#[tokio::test]
async fn test_entry_larger_than_memory_budget_is_not_cached() {
    let metrics = EdgeMetrics::new();
    let config = CacheConfig {
        max_capacity: 10,
        max_memory_bytes: 8,
        default_ttl_secs: 60,
        eviction_policy: "lru".to_string(),
        cleanup_interval_secs: 5,
    };
    let cache = EdgeCache::new(&config, metrics);

    cache.insert(
        "oversized".to_string(),
        Bytes::from("payload that cannot fit"),
        None,
        None,
        200,
    );

    assert!(cache.is_empty());
    assert_eq!(cache.memory_usage(), 0);
}

#[tokio::test]
async fn test_replacing_entry_at_capacity_preserves_unrelated_entries() {
    let metrics = EdgeMetrics::new();
    let config = CacheConfig {
        max_capacity: 2,
        max_memory_bytes: 1024,
        default_ttl_secs: 60,
        eviction_policy: "lru".to_string(),
        cleanup_interval_secs: 5,
    };
    let cache = EdgeCache::new(&config, metrics);

    cache.insert("first".to_string(), Bytes::from("one"), None, None, 200);
    cache.insert("second".to_string(), Bytes::from("two"), None, None, 200);
    cache.insert("first".to_string(), Bytes::from("updated"), None, None, 200);

    assert_eq!(cache.len(), 2);
    assert_eq!(cache.get("first").unwrap().value.as_ref(), b"updated");
    assert!(cache.get("second").is_some());
}
