use simulation_045::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

fn assert_send_sync<T: Send + Sync>(_val: &T) {}

#[test]
fn test_trait_bounds() {
    let cache = make_sharded_cache::<String, String>(4);
    assert_send_sync(&cache);
}

#[test]
fn test_basic_put_and_get() {
    let cache = make_sharded_cache::<u32, String>(4);
    assert_eq!(cache.shard_count(), 4);

    cache.put(1, "one".to_string());
    cache.put(2, "two".to_string());

    assert_eq!(cache.get(&1), Some("one".to_string()));
    assert_eq!(cache.get(&2), Some("two".to_string()));
    assert_eq!(cache.get(&99), None);
}

#[test]
fn test_independent_shards_do_not_contend() {
    let cache = Arc::new(make_sharded_cache::<usize, usize>(8));

    // Identify two keys that hash to different shards
    let mut key1 = 0;
    let mut key2 = 1;
    cache.put(key1, 100);
    cache.put(key2, 200);

    let c1 = Arc::clone(&cache);
    let barrier = Arc::new(Barrier::new(2));
    let b1 = Arc::clone(&barrier);

    let h = thread::spawn(move || {
        b1.wait();
        for _ in 0..10_000 {
            c1.put(key1, 101);
            let _ = c1.get(&key1);
        }
    });

    barrier.wait();
    for _ in 0..10_000 {
        cache.put(key2, 201);
        let _ = cache.get(&key2);
    }

    h.join().unwrap();
}

#[test]
fn test_rehash_preserves_all_entries() {
    let cache = make_sharded_cache::<u32, u32>(2);

    for i in 0..100 {
        cache.put(i, i * 10);
    }

    assert_eq!(cache.shard_count(), 2);

    // Expand shards from 2 to 8
    cache.rehash(8);
    assert_eq!(cache.shard_count(), 8);

    for i in 0..100 {
        assert_eq!(cache.get(&i), Some(i * 10));
    }

    // Shrink shards from 8 to 1
    cache.rehash(1);
    assert_eq!(cache.shard_count(), 1);

    for i in 0..100 {
        assert_eq!(cache.get(&i), Some(i * 10));
    }
}

#[test]
fn test_rehash_blocks_concurrent_readers_and_writers() {
    let cache = Arc::new(make_sharded_cache::<u32, u32>(2));

    for i in 0..10 {
        cache.put(i, i * 10);
    }

    let c_clone = Arc::clone(&cache);
    let reader_finished = Arc::new(AtomicBool::new(false));
    let rf_clone = Arc::clone(&reader_finished);

    // Rehash to 4 shards
    cache.rehash(4);

    let h = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        let val = c_clone.get(&5);
        rf_clone.store(true, Ordering::SeqCst);
        assert_eq!(val, Some(50));
    });

    h.join().unwrap();
    assert!(reader_finished.load(Ordering::SeqCst));
}

#[test]
fn test_high_concurrency_stress_with_rehash() {
    let cache = Arc::new(make_sharded_cache::<usize, usize>(4));
    let mut handles = Vec::new();

    // 4 worker threads performing put and get
    for t in 0..4 {
        let c = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for i in 0..200 {
                let key = t * 1000 + i;
                c.put(key, key * 2);
                let _ = c.get(&key);
            }
        }));
    }

    // 1 thread periodically rehashing the cache
    let c_rehash = Arc::clone(&cache);
    let rehash_handle = thread::spawn(move || {
        let shard_counts = [8, 3, 6, 2, 4];
        for &shards in &shard_counts {
            thread::sleep(Duration::from_millis(15));
            c_rehash.rehash(shards);
        }
    });

    for h in handles {
        h.join().unwrap();
    }
    rehash_handle.join().unwrap();
}