use simulation_036::*;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_send_sync_bounds() {
    let reactor = make_reactor();
    assert_send(&reactor);
    assert_sync(&reactor);
}

#[test]
fn test_single_register_mark_ready_poll() {
    let reactor = make_reactor();
    let _reg = reactor.register(1, false);

    reactor.mark_ready(1);
    let ready = reactor.poll();
    assert_eq!(ready, vec![1]);

    // Subsequent poll_timeout must return empty because 1 was consumed
    let second_ready = reactor.poll_timeout(Duration::from_millis(30));
    assert!(second_ready.is_empty());
}

#[test]
fn test_urgent_priority_over_normal() {
    let reactor = make_reactor();
    let _r_normal1 = reactor.register(1, false);
    let _r_normal2 = reactor.register(2, false);
    let _r_urgent1 = reactor.register(10, true);
    let _r_urgent2 = reactor.register(20, true);

    // Mark ready both normal and urgent items
    reactor.mark_ready(1);
    reactor.mark_ready(2);
    reactor.mark_ready(10);
    reactor.mark_ready(20);

    // First poll must return ONLY the urgent IDs (10, 20)
    let first = reactor.poll();
    let first_set: HashSet<u64> = first.into_iter().collect();
    let expected_urgent: HashSet<u64> = vec![10, 20].into_iter().collect();
    assert_eq!(first_set, expected_urgent, "First poll must yield only urgent ready items");

    // Second poll must return the remaining normal IDs (1, 2)
    let second = reactor.poll();
    let second_set: HashSet<u64> = second.into_iter().collect();
    let expected_normal: HashSet<u64> = vec![1, 2].into_iter().collect();
    assert_eq!(second_set, expected_normal, "Second poll must yield normal ready items");

    // Third poll timeout must be empty
    assert!(reactor.poll_timeout(Duration::from_millis(30)).is_empty());
}

#[test]
fn test_deregister_cancels_pending_and_ready() {
    let reactor = make_reactor();
    let r1 = reactor.register(100, false);
    let _r2 = reactor.register(200, false);

    // Mark 100 ready, but deregister it before poll
    reactor.mark_ready(100);
    r1.deregister();

    // Mark 200 ready
    reactor.mark_ready(200);

    // Poll must return ONLY 200 (100 was discarded upon deregister)
    let ready = reactor.poll();
    assert_eq!(ready, vec![200]);
}

#[test]
fn test_mark_ready_unregistered_is_noop() {
    let reactor = make_reactor();
    // Mark ready on an id that was never registered
    reactor.mark_ready(999);

    let ready = reactor.poll_timeout(Duration::from_millis(30));
    assert!(ready.is_empty(), "Marking unregistered ID must be no-op");
}

#[test]
fn test_poll_timeout_expires_empty() {
    let reactor = make_reactor();
    let _r = reactor.register(1, false);

    let start = Instant::now();
    let ready = reactor.poll_timeout(Duration::from_millis(40));
    let elapsed = start.elapsed();

    assert!(ready.is_empty());
    assert!(elapsed >= Duration::from_millis(35));
}

#[test]
fn test_poll_blocks_until_dynamic_register_and_mark_ready() {
    let reactor = make_reactor();
    let unblocked = Arc::new(AtomicBool::new(false));

    let r_clone = reactor.clone();
    let ub = Arc::clone(&unblocked);
    let handle = thread::spawn(move || {
        let ready = r_clone.poll();
        ub.store(true, Ordering::SeqCst);
        ready
    });

    thread::sleep(Duration::from_millis(30));
    assert!(!unblocked.load(Ordering::SeqCst), "poll() must block while no items are ready");

    // Dynamically register and mark ready from main thread
    let _reg = reactor.register(42, false);
    reactor.mark_ready(42);

    let ready = handle.join().unwrap();
    assert!(unblocked.load(Ordering::SeqCst));
    assert_eq!(ready, vec![42]);
}

#[test]
fn test_concurrent_multi_thread_pollers_no_duplicate_delivery() {
    let reactor = make_reactor();
    let total_items = 60u64;

    let mut _regs = Vec::new();
    for id in 0..total_items {
        _regs.push(reactor.register(id, id % 3 == 0));
    }

    let collected = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    // Spawn 6 poller threads
    for _ in 0..6 {
        let r_clone = reactor.clone();
        let col = Arc::clone(&collected);
        handles.push(thread::spawn(move || {
            let mut my_items = Vec::new();
            loop {
                let items = r_clone.poll_timeout(Duration::from_millis(50));
                if items.is_empty() {
                    break;
                }
                my_items.extend(items);
            }
            col.lock().unwrap().extend(my_items);
        }));
    }

    // Mark items ready in batches with small delays
    for chunk in (0..total_items).collect::<Vec<_>>().chunks(10) {
        thread::sleep(Duration::from_millis(10));
        for &id in chunk {
            reactor.mark_ready(id);
        }
    }

    for h in handles {
        h.join().unwrap();
    }

    let all_collected = collected.lock().unwrap().clone();
    assert_eq!(
        all_collected.len(),
        total_items as usize,
        "Total items received must match total items marked ready"
    );

    let unique: HashSet<u64> = all_collected.into_iter().collect();
    assert_eq!(
        unique.len(),
        total_items as usize,
        "No item must ever be delivered to more than one poll() call"
    );
}

#[test]
fn test_urgent_arriving_during_normal_ready() {
    let reactor = make_reactor();
    let _r_norm = reactor.register(1, false);
    let _r_urg = reactor.register(2, true);

    // 1 becomes ready first
    reactor.mark_ready(1);
    // 2 becomes ready immediately after
    reactor.mark_ready(2);

    // First poll must return ONLY 2 (urgent)
    let first = reactor.poll();
    assert_eq!(first, vec![2]);

    // Second poll returns 1 (normal)
    let second = reactor.poll();
    assert_eq!(second, vec![1]);
}
