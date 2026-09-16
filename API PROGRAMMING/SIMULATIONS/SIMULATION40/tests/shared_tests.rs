use simulation_040::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

fn assert_send_sync<T: Send + Sync>(_val: &T) {}

#[test]
fn test_trait_bounds() {
    let vm = make_virtual_memory::<usize, String>(4, |_k, _v| {});
    assert_send_sync(&vm);
}

#[test]
fn test_single_page_fault_and_hit() {
    let load_count = Arc::new(AtomicUsize::new(0));
    let lc = Arc::clone(&load_count);

    let vm = make_virtual_memory::<u32, String>(2, |_k, _v| {});

    // Fault in page 1
    {
        let guard = vm.fault_in(1, || {
            lc.fetch_add(1, Ordering::SeqCst);
            "Page 1 Content".to_string()
        });
        assert_eq!(guard.get(), "Page 1 Content");
    }

    // Hit page 1: load must not be executed again
    {
        let guard = vm.fault_in(1, || {
            lc.fetch_add(1, Ordering::SeqCst);
            panic!("Should not reload already resident page");
        });
        assert_eq!(guard.get(), "Page 1 Content");
    }

    assert_eq!(load_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_single_flight_concurrent_fault_in_same_key() {
    let load_count = Arc::new(AtomicUsize::new(0));
    let lc = Arc::clone(&load_count);

    let vm = Arc::new(make_virtual_memory::<u32, u32>(2, |_k, _v| {}));
    let start_barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();

    for _ in 0..4 {
        let vm_clone = Arc::clone(&vm);
        let b_clone = Arc::clone(&start_barrier);
        let lc_clone = Arc::clone(&lc);

        handles.push(thread::spawn(move || {
            b_clone.wait();
            let guard = vm_clone.fault_in(42, || {
                lc_clone.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(50));
                12345
            });
            assert_eq!(*guard.get(), 12345);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(load_count.load(Ordering::SeqCst), 1, "Single-flight must load key exactly once");
}

#[test]
fn test_lru_eviction_without_dirty_writeback() {
    let write_backs = Arc::new(Mutex::new(Vec::new()));
    let wb_clone = Arc::clone(&write_backs);

    // Frame capacity 2
    let vm = make_virtual_memory::<u32, String>(2, move |k, v| {
        wb_clone.lock().unwrap().push((*k, v.clone()));
    });

    // 1. Load key 1
    let g1 = vm.fault_in(1, || "V1".to_string());
    drop(g1); // unpin 1

    // 2. Load key 2
    let g2 = vm.fault_in(2, || "V2".to_string());
    drop(g2); // unpin 2

    // 3. Touch key 1 so key 2 becomes the LRU victim
    let g1_again = vm.fault_in(1, || panic!("Key 1 must be resident"));
    drop(g1_again);

    // 4. Load key 3 -> capacity reached (2 frames). Key 2 must be evicted.
    let g3 = vm.fault_in(3, || "V3".to_string());
    assert_eq!(g3.get(), "V3");

    // No get_mut was invoked, so no writeback must have happened
    assert!(write_backs.lock().unwrap().is_empty());
}

#[test]
fn test_write_back_on_dirty_page_eviction() {
    let write_backs = Arc::new(Mutex::new(HashMap::new()));
    let wb_clone = Arc::clone(&write_backs);

    let vm = make_virtual_memory::<u32, String>(2, move |k, v| {
        wb_clone.lock().unwrap().insert(*k, v.clone());
    });

    // Load key 1 and modify it
    let mut g1 = vm.fault_in(1, || "Clean".to_string());
    *g1.get_mut() = "Dirty V1".to_string();
    drop(g1); // unpin

    // Load key 2 (clean)
    let g2 = vm.fault_in(2, || "Clean V2".to_string());
    drop(g2); // unpin

    // Evict key 1 by loading key 3
    let g3 = vm.fault_in(3, || "V3".to_string());
    drop(g3);

    let written = write_backs.lock().unwrap();
    assert_eq!(written.get(&1), Some(&"Dirty V1".to_string()));
    assert!(!written.contains_key(&2), "Clean key 2 should not be written back");
}

#[test]
fn test_pinning_prevents_eviction_and_blocks_until_unpin() {
    let vm = Arc::new(make_virtual_memory::<u32, u32>(1, |_k, _v| {}));

    // Pin the only available frame
    let guard_k1 = vm.fault_in(1, || 100);

    let k2_loaded = Arc::new(AtomicBool::new(false));
    let k2_clone = Arc::clone(&k2_loaded);
    let vm_clone = Arc::clone(&vm);

    let handle = thread::spawn(move || {
        let guard_k2 = vm_clone.fault_in(2, || {
            k2_clone.store(true, Ordering::SeqCst);
            200
        });
        assert_eq!(*guard_k2.get(), 200);
    });

    thread::sleep(Duration::from_millis(40));
    // Thread must be blocked waiting for key 1 to unpin
    assert!(!k2_loaded.load(Ordering::SeqCst));

    // Release key 1
    drop(guard_k1);

    handle.join().unwrap();
    assert!(k2_loaded.load(Ordering::SeqCst));
}

#[test]
fn test_lock_not_held_during_slow_load_and_writeback() {
    let vm = Arc::new(make_virtual_memory::<u32, u32>(2, |_k, _v| {
        // Slow writeback
        thread::sleep(Duration::from_millis(50));
    }));

    // Thread A initiates a slow load for key 1
    let vm_a = Arc::clone(&vm);
    let handle_a = thread::spawn(move || {
        let _g1 = vm_a.fault_in(1, || {
            thread::sleep(Duration::from_millis(60));
            111
        });
    });

    // Let thread A enter the load closure
    thread::sleep(Duration::from_millis(15));

    // Thread B faults in an unrelated key 2. It must NOT be blocked by Thread A's slow load!
    let start = std::time::Instant::now();
    let g2 = vm.fault_in(2, || 222);
    let elapsed = start.elapsed();

    assert_eq!(*g2.get(), 222);
    assert!(elapsed < Duration::from_millis(40), "fault_in on key 2 blocked on unrelated key 1 load");

    handle_a.join().unwrap();
}

#[test]
fn test_multiple_readers_pinning_same_page() {
    let vm = Arc::new(make_virtual_memory::<u32, u32>(1, |_k, _v| {}));

    let g1 = vm.fault_in(42, || 999);
    let g2 = vm.fault_in(42, || panic!("Should hit cache"));

    let evicted = Arc::new(AtomicBool::new(false));
    let ev_clone = Arc::clone(&evicted);
    let vm_clone = Arc::clone(&vm);

    let h = thread::spawn(move || {
        let g3 = vm_clone.fault_in(100, || {
            ev_clone.store(true, Ordering::SeqCst);
            1000
        });
        assert_eq!(*g3.get(), 1000);
    });

    thread::sleep(Duration::from_millis(30));
    drop(g1); // Still pinned by g2!
    thread::sleep(Duration::from_millis(30));
    assert!(!evicted.load(Ordering::SeqCst), "Page must stay pinned as long as at least 1 guard is alive");

    drop(g2); // Now fully unpinned
    h.join().unwrap();
    assert!(evicted.load(Ordering::SeqCst));
}