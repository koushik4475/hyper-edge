use hyper_edge::geo::distance::{haversine_distance_km, estimate_fiber_rtt_ms};
use hyper_edge::geo::node::{EdgeNode, EdgeNodeConfig};
use hyper_edge::geo::ring::GeoHashRing;
use std::sync::Arc;

#[test]
fn test_geo_consistent_ring_distribution() {
    let ring = GeoHashRing::new(128);

    let pops = vec![
        ("pop-iad", "IAD", 38.9531, -77.4565),
        ("pop-sfo", "SFO", 37.6213, -122.3790),
        ("pop-lhr", "LHR", 51.4700, -0.4543),
        ("pop-nrt", "NRT", 35.7720, 140.3929),
        ("pop-fra", "FRA", 50.0379, 8.5622),
    ];

    for (id, code, lat, lon) in pops {
        let node = EdgeNode::new(EdgeNodeConfig {
            id: id.to_string(),
            pop_code: code.to_string(),
            addr: format!("{}.edge.internal:8080", id),
            latitude: lat,
            longitude: lon,
            weight: 100,
        });
        ring.add_node(node);
    }

    assert_eq!(ring.node_count(), 5);
    assert_eq!(ring.vnode_count(), 5 * 128);

    // Test key routing determinism
    let node1 = ring.get_node_by_key("asset:image-hero.png").unwrap();
    let node2 = ring.get_node_by_key("asset:image-hero.png").unwrap();
    assert_eq!(node1.id, node2.id, "Consistent hash must be deterministic");

    // Test multi-replica distribution
    let replicas = ring.get_replicas("asset:bundle.js", 3).unwrap();
    assert_eq!(replicas.len(), 3);
    assert_ne!(replicas[0].id, replicas[1].id);
    assert_ne!(replicas[1].id, replicas[2].id);
}

#[test]
fn test_geo_nearest_pop_selection() {
    let ring = GeoHashRing::new(64);

    let iad = EdgeNode::new(EdgeNodeConfig {
        id: "pop-iad".to_string(),
        pop_code: "IAD".to_string(),
        addr: "127.0.0.1:8081".to_string(),
        latitude: 38.9531,
        longitude: -77.4565,
        weight: 100,
    });

    let sfo = EdgeNode::new(EdgeNodeConfig {
        id: "pop-sfo".to_string(),
        pop_code: "SFO".to_string(),
        addr: "127.0.0.1:8082".to_string(),
        latitude: 37.6213,
        longitude: -122.3790,
        weight: 100,
    });

    ring.add_node(iad);
    ring.add_node(sfo);

    // Client in New York (lat 40.7128, lon -74.0060) is closer to IAD than SFO
    let nearest_ny = ring.find_nearest_pop(40.7128, -74.0060).unwrap();
    assert_eq!(nearest_ny.pop_code, "IAD");

    // Client in San Jose (lat 37.3382, lon -121.8863) is closer to SFO than IAD
    let nearest_sj = ring.find_nearest_pop(37.3382, -121.8863).unwrap();
    assert_eq!(nearest_sj.pop_code, "SFO");
}

#[test]
fn test_minimal_key_redistribution_on_node_leave() {
    let ring = GeoHashRing::new(128);

    for code in &["IAD", "SFO", "LHR", "NRT"] {
        ring.add_node(EdgeNode::new(EdgeNodeConfig {
            id: format!("pop-{}", code.to_lowercase()),
            pop_code: code.to_string(),
            addr: "127.0.0.1".to_string(),
            latitude: 0.0,
            longitude: 0.0,
            weight: 100,
        }));
    }

    let sample_keys: Vec<String> = (0..1000).map(|i| format!("key:uuid-{}", i)).collect();
    let initial_mapping: Vec<String> = sample_keys
        .iter()
        .map(|k| ring.get_node_by_key(k).unwrap().id.clone())
        .collect();

    // Remove one node
    ring.remove_node("pop-nrt");

    let post_removal_mapping: Vec<String> = sample_keys
        .iter()
        .map(|k| ring.get_node_by_key(k).unwrap().id.clone())
        .collect();

    let mut shifted = 0;
    for (old, new) in initial_mapping.iter().zip(post_removal_mapping.iter()) {
        if old != new {
            shifted += 1;
        }
    }

    // In a 4-node ring, removing 1 node should only shift approximately ~25% of keys
    let shift_percentage = (shifted as f64) / 1000.0;
    println!("Shifted keys percentage: {:.2}%", shift_percentage * 100.0);
    assert!(
        shift_percentage < 0.35,
        "Consistent hashing churn too high: {}%",
        shift_percentage * 100.0
    );
}


#[test]
fn test_zero_replica_request_returns_empty_list() {
    let ring = GeoHashRing::new(64);
    ring.add_node(EdgeNode::new(EdgeNodeConfig {
        id: "pop-iad".to_string(),
        pop_code: "IAD".to_string(),
        addr: "127.0.0.1:8081".to_string(),
        latitude: 38.9531,
        longitude: -77.4565,
        weight: 100,
    }));

    assert!(ring.get_replicas("asset:zero-replicas", 0).unwrap().is_empty());
}
