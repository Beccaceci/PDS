use simulation_044::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

fn assert_send_sync<T: Send + Sync>(_val: &T) {}

#[test]
fn test_trait_bounds() {
    let pool = make_intern_pool::<String, String>();
    assert_send_sync(&pool);
}

#[test]
fn test_single_key_intern_and_len() {
    let pool = make_intern_pool::<i32, String>();
    assert_eq!(pool.len(), 0);

    let handle1 = pool.intern(1, || "hello".to_string());
    assert_eq!(handle1.get(), "hello");
    assert_eq!(pool.len(), 1);

    let handle2 = pool.intern(1, || panic!("Should not call create for existing key"));
    assert_eq!(handle2.get(), "hello");
    assert_eq!(pool.len(), 1);
}

#[test]
fn test_eviction_on_last_drop() {
    let pool = make_intern_pool::<i32, Vec<u8>>();
    assert_eq!(pool.len(), 0);

    let h1 = pool.intern(42, || vec![1, 2, 3]);
    let h2 = pool.intern(42, || vec![9, 9, 9]);
    assert_eq!(pool.len(), 1);

    drop(h1);
    // Still 1 handle alive (h2)
    assert_eq!(pool.len(), 1);

    drop(h2);
    // Last handle dropped -> key must be purged
    assert_eq!(pool.len(), 0);

    // Re-interning must rebuild the value
    let h3 = pool.intern(42, || vec![4, 5, 6]);
    assert_eq!(h3.get(), &vec![4, 5, 6]);
    assert_eq!(pool.len(), 1);
}

#[test]
fn test_create_not_held_under_global_lock() {
    let pool = make_intern_pool::<i32, i32>();
    let pool_clone = pool.clone();

    // Thread 1 initiates a slow construction for key 1
    let handle = thread::spawn(move || {
        let _h = pool_clone.intern(1, || {
            thread::sleep(Duration::from_millis(60));
            100
        });
    });

    thread::sleep(Duration::from_millis(15));

    // Thread 2 interns unrelated key 2: must complete immediately without waiting for key 1
    let start = std::time::Instant::now();
    let h2 = pool.intern(2, || 200);
    let elapsed = start.elapsed();

    assert_eq!(*h2.get(), 200);
    assert!(elapsed < Duration::from_millis(30), "intern on key 2 blocked on slow create of key 1");

    handle.join().unwrap();
    assert_eq!(pool.len(), 2);
}

#[test]
fn test_concurrent_resurrection_and_deduplication() {
    let pool = Arc::new(make_intern_pool::<u32, Arc<AtomicUsize>>());
    let create_count = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();

    for _ in 0..4 {
        let p = Arc::clone(&pool);
        let b = Arc::clone(&barrier);
        let cc = Arc::clone(&create_count);

        handles.push(thread::spawn(move || {
            b.wait();
            p.intern(999, || {
                cc.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(20));
                Arc::new(AtomicUsize::new(42))
            })
        }));
    }

    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

    // All threads must observe identical values and only one initialization
    assert_eq!(create_count.load(Ordering::SeqCst), 1);
    assert_eq!(pool.len(), 1);

    drop(results);
    assert_eq!(pool.len(), 0);
}

#[test]
fn test_high_concurrency_stress() {
    let pool = Arc::new(make_intern_pool::<usize, usize>());
    let mut handles = Vec::new();

    for thread_id in 0..8 {
        let p = Arc::clone(&pool);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let key = (thread_id % 2) * 10 + (i % 5);
                let handle = p.intern(key, move || key * 100);
                assert_eq!(*handle.get(), key * 100);
                if i % 3 == 0 {
                    drop(handle);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}