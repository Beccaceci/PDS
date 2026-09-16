use simulation_016::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_capacity() {
    let items = vec![1, 2, 3, 4, 5];
    let pool = make_batch_pool(items);
    assert_eq!(pool.capacity(), 5);
}

#[test]
fn test_basic_acquire_and_items_slice() {
    let items = vec!["A".to_string(), "B".to_string(), "C".to_string()];
    let pool = make_batch_pool(items);

    let batch = pool.acquire_batch(2);
    assert_eq!(batch.items().len(), 2);
}

#[test]
fn test_raii_return_on_drop() {
    let items = vec![10, 20, 30, 40];
    let pool = make_batch_pool(items);

    // Acquire all 4 items
    let batch1 = pool.acquire_batch(4);
    assert_eq!(batch1.items().len(), 4);

    // Drop batch1 -> resources returned
    drop(batch1);

    // Should now be able to acquire all 4 items again
    let batch2 = pool.acquire_batch(4);
    assert_eq!(batch2.items().len(), 4);
}

#[test]
fn test_partial_batch_allocations() {
    let items = vec![1, 2, 3, 4, 5, 6];
    let pool = make_batch_pool(items);

    let b1 = pool.acquire_batch(3);
    assert_eq!(b1.items().len(), 3);

    let b2 = pool.acquire_batch(2);
    assert_eq!(b2.items().len(), 2);

    let b3 = pool.acquire_batch(1);
    assert_eq!(b3.items().len(), 1);
}

#[test]
fn test_acquire_batch_blocks_until_resources_available() {
    let pool = Arc::new(make_batch_pool(vec![1, 2, 3, 4]));

    // Acquire 3 items out of 4 (1 left)
    let batch_main = pool.acquire_batch(3);

    let p_clone = Arc::clone(&pool);
    let acquired = Arc::new(AtomicBool::new(false));
    let a_clone = Arc::clone(&acquired);

    // Worker asks for 2 items (only 1 available -> must block)
    let worker = thread::spawn(move || {
        let batch = p_clone.acquire_batch(2);
        a_clone.store(true, Ordering::SeqCst);
        assert_eq!(batch.items().len(), 2);
    });

    thread::sleep(Duration::from_millis(50));
    assert!(!acquired.load(Ordering::SeqCst), "Worker should be blocked");

    // Release main batch -> unblocks worker
    drop(batch_main);

    worker.join().unwrap();
    assert!(acquired.load(Ordering::SeqCst));
}

#[test]
fn test_acquire_batch_timeout_expiration() {
    let pool = make_batch_pool(vec![10, 20]);

    // Acquire both items
    let _b = pool.acquire_batch(2);

    let start = Instant::now();
    // Try to acquire 1 item with 50ms timeout -> must expire and return None
    let res = pool.acquire_batch_timeout(1, Duration::from_millis(50));
    let elapsed = start.elapsed();

    assert!(res.is_none(), "Timeout should expire and return None");
    assert!(
        elapsed >= Duration::from_millis(40),
        "Timeout must have waited approximately 50ms, elapsed: {:?}",
        elapsed
    );
}

#[test]
fn test_acquire_batch_timeout_success_before_expiry() {
    let pool = Arc::new(make_batch_pool(vec![1, 2, 3]));

    let batch_held = pool.acquire_batch(3);

    let p_clone = Arc::clone(&pool);
    let worker = thread::spawn(move || {
        let res = p_clone.acquire_batch_timeout(2, Duration::from_millis(300));
        res.map(|b| b.items().len())
    });

    thread::sleep(Duration::from_millis(50));
    // Release resources in time before 300ms timeout
    drop(batch_held);

    let res = worker.join().unwrap();
    assert_eq!(res, Some(2), "Should have acquired 2 items before timeout expired");
}

#[test]
fn test_zero_count_acquire() {
    let pool = make_batch_pool(vec![1, 2, 3]);

    let b = pool.acquire_batch(0);
    assert_eq!(b.items().len(), 0);

    let b_timeout = pool.acquire_batch_timeout(0, Duration::from_millis(10));
    assert!(b_timeout.is_some());
    assert_eq!(b_timeout.unwrap().items().len(), 0);
}

#[test]
fn test_concurrent_multi_thread_stress() {
    const TOTAL_ITEMS: usize = 12;
    let items: Vec<usize> = (0..TOTAL_ITEMS).collect();
    let pool = Arc::new(make_batch_pool(items));
    const WORKERS: usize = 8;
    const ITERATIONS: usize = 40;

    let mut handles = Vec::new();
    for w in 0..WORKERS {
        let p = Arc::clone(&pool);
        handles.push(thread::spawn(move || {
            for i in 0..ITERATIONS {
                let count = ((w + i) % 4) + 1; // requests 1 to 4 items
                let batch = p.acquire_batch(count);
                assert_eq!(batch.items().len(), count);
                thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    // At the end, all resources must be back in pool -> can acquire full capacity
    let final_batch = pool.acquire_batch(TOTAL_ITEMS);
    assert_eq!(final_batch.items().len(), TOTAL_ITEMS);
}

#[test]
fn test_send_sync_bounds() {
    let pool = make_batch_pool(vec![1, 2, 3]);
    assert_send(&pool);
    assert_sync(&pool);
}
