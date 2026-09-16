use simulation_021::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_basic_caching_and_ttl_expiration() {
    let cache = make_ttl_cache::<String, i32>(10);
    let compute_count = Arc::new(AtomicUsize::new(0));

    // First call: computes and stores with 50ms TTL
    let c1 = Arc::clone(&compute_count);
    let v1 = cache.get_or_compute("key_1".to_string(), Duration::from_millis(50), move || {
        c1.fetch_add(1, Ordering::SeqCst);
        100
    });
    assert_eq!(v1, 100);
    assert_eq!(compute_count.load(Ordering::SeqCst), 1);
    assert_eq!(cache.len(), 1);

    // Immediate second call: must hit cache
    let c2 = Arc::clone(&compute_count);
    let v2 = cache.get_or_compute("key_1".to_string(), Duration::from_millis(50), move || {
        c2.fetch_add(1, Ordering::SeqCst);
        999
    });
    assert_eq!(v2, 100);
    assert_eq!(compute_count.load(Ordering::SeqCst), 1);

    // Sleep until TTL expires
    thread::sleep(Duration::from_millis(80));

    // Third call: expired -> recomputes
    let c3 = Arc::clone(&compute_count);
    let v3 = cache.get_or_compute("key_1".to_string(), Duration::from_millis(50), move || {
        c3.fetch_add(1, Ordering::SeqCst);
        200
    });
    assert_eq!(v3, 200);
    assert_eq!(compute_count.load(Ordering::SeqCst), 2);
}

#[test]
fn test_single_flight_coalescing_concurrent_callers() {
    let cache = make_ttl_cache::<String, String>(10);
    let compute_count = Arc::new(AtomicUsize::new(0));
    const CALLERS: usize = 10;

    let mut handles = Vec::new();
    for _ in 0..CALLERS {
        let c = cache.clone();
        let cnt = Arc::clone(&compute_count);
        handles.push(thread::spawn(move || {
            c.get_or_compute("dns_query".to_string(), Duration::from_millis(200), || {
                cnt.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(80));
                "1.1.1.1".to_string()
            })
        }));
    }

    for h in handles {
        let res = h.join().unwrap();
        assert_eq!(res, "1.1.1.1");
    }

    assert_eq!(
        compute_count.load(Ordering::SeqCst),
        1,
        "Compute must be executed exactly ONCE across all concurrent callers"
    );
}

#[test]
fn test_capacity_eviction_prefers_expired_entry_over_lru() {
    let cache = make_ttl_cache::<&'static str, &'static str>(2);

    // 1. Insert "expired_soon" with 30ms TTL
    cache.get_or_compute("expired_soon", Duration::from_millis(30), || "val_1");

    // 2. Insert "active_old" with 500ms TTL
    cache.get_or_compute("active_old", Duration::from_millis(500), || "val_2");

    assert_eq!(cache.len(), 2);

    // Wait for "expired_soon" to expire
    thread::sleep(Duration::from_millis(60));

    // Access "active_old" so it has recent access time
    cache.get_or_compute("active_old", Duration::from_millis(500), || panic!("Should hit cache"));

    // 3. Insert "new_key" -> capacity (2) reached -> must evict "expired_soon" because it's expired!
    cache.get_or_compute("new_key", Duration::from_millis(500), || "val_3");
    assert_eq!(cache.len(), 2);

    // "active_old" must still be in cache
    let recomputed_active = Arc::new(AtomicUsize::new(0));
    let ra = Arc::clone(&recomputed_active);
    let val_active = cache.get_or_compute("active_old", Duration::from_millis(500), move || {
        ra.fetch_add(1, Ordering::SeqCst);
        "recomputed"
    });
    assert_eq!(val_active, "val_2");
    assert_eq!(recomputed_active.load(Ordering::SeqCst), 0);
}

#[test]
fn test_capacity_eviction_pure_lru_when_no_expired() {
    let cache = make_ttl_cache::<&'static str, i32>(2);

    // Insert key A and key B with long TTL
    cache.get_or_compute("A", Duration::from_secs(10), || 1);
    cache.get_or_compute("B", Duration::from_secs(10), || 2);

    // Access A to make B the least recently used
    cache.get_or_compute("A", Duration::from_secs(10), || panic!("Hit"));

    // Insert key C -> B must be evicted (LRU)
    cache.get_or_compute("C", Duration::from_secs(10), || 3);
    assert_eq!(cache.len(), 2);

    // A must still be present
    assert_eq!(cache.get_or_compute("A", Duration::from_secs(10), || panic!("Hit")), 1);

    // B was evicted -> computing B invokes closure
    let b_computed = Arc::new(AtomicUsize::new(0));
    let bc = Arc::clone(&b_computed);
    let b_val = cache.get_or_compute("B", Duration::from_secs(10), move || {
        bc.fetch_add(1, Ordering::SeqCst);
        222
    });
    assert_eq!(b_val, 222);
    assert_eq!(b_computed.load(Ordering::SeqCst), 1);
}

#[test]
fn test_send_sync_bounds() {
    let cache = make_ttl_cache::<String, i32>(10);
    assert_send(&cache);
    assert_sync(&cache);
}
