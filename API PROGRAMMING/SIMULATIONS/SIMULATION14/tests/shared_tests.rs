use simulation_014::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

#[test]
fn test_initial_state_version_zero() {
    let cell = make_versioned_cell(42);
    let snap = cell.read();

    assert_eq!(snap.version(), 0, "Initial version must be 0");
    assert_eq!(*snap.value(), 42, "Initial value must match constructor argument");
}

#[test]
fn test_successful_compare_and_update_increments_version() {
    let cell = make_versioned_cell("initial".to_string());

    let snap0 = cell.read();
    assert_eq!(snap0.version(), 0);
    assert_eq!(snap0.value(), "initial");

    // Successful update from version 0 -> 1
    let success1 = cell.compare_and_update(0, "update_1".to_string());
    assert!(success1, "compare_and_update(0, ...) must succeed on version 0");

    let snap1 = cell.read();
    assert_eq!(snap1.version(), 1, "Version must increment by 1 after update");
    assert_eq!(snap1.value(), "update_1", "Value must reflect the update");

    // Successful update from version 1 -> 2
    let success2 = cell.compare_and_update(1, "update_2".to_string());
    assert!(success2, "compare_and_update(1, ...) must succeed on version 1");

    let snap2 = cell.read();
    assert_eq!(snap2.version(), 2, "Version must be 2 after second update");
    assert_eq!(snap2.value(), "update_2");
}

#[test]
fn test_failed_compare_and_update_on_stale_or_future_version() {
    let cell = make_versioned_cell(100);

    // Attempt with future version (version is 0, expected is 1)
    let failed_future = cell.compare_and_update(1, 999);
    assert!(!failed_future, "compare_and_update with future expected_version must return false");

    let snap = cell.read();
    assert_eq!(snap.version(), 0, "Version must remain 0 after failed update");
    assert_eq!(*snap.value(), 100, "Value must remain unchanged after failed update");

    // Perform valid update 0 -> 1
    assert!(cell.compare_and_update(0, 200));
    assert_eq!(cell.read().version(), 1);

    // Attempt with stale version 0
    let failed_stale = cell.compare_and_update(0, 300);
    assert!(!failed_stale, "compare_and_update with stale expected_version (0 vs 1) must return false");

    let snap_after_stale = cell.read();
    assert_eq!(snap_after_stale.version(), 1, "Version must remain 1 after stale update attempt");
    assert_eq!(*snap_after_stale.value(), 200, "Value must remain 200 after stale update attempt");
}

#[test]
fn test_snapshot_immutability_across_subsequent_updates() {
    let cell = make_versioned_cell(10);

    let snap0 = cell.read();
    assert_eq!(snap0.version(), 0);
    assert_eq!(*snap0.value(), 10);

    // Advance cell version to 1
    assert!(cell.compare_and_update(0, 20));
    let snap1 = cell.read();

    // Advance cell version to 2
    assert!(cell.compare_and_update(1, 30));
    let snap2 = cell.read();

    // Verify all snapshots retained their respective historical values and versions
    assert_eq!(snap0.version(), 0);
    assert_eq!(*snap0.value(), 10);

    assert_eq!(snap1.version(), 1);
    assert_eq!(*snap1.value(), 20);

    assert_eq!(snap2.version(), 2);
    assert_eq!(*snap2.value(), 30);
}

#[test]
fn test_cloned_handles_share_same_state() {
    let cell1 = make_versioned_cell("shared".to_string());
    let cell2 = cell1.clone();

    // Update through cell1
    assert!(cell1.compare_and_update(0, "modified_via_cell1".to_string()));

    // Read through cell2
    let snap2 = cell2.read();
    assert_eq!(snap2.version(), 1);
    assert_eq!(snap2.value(), "modified_via_cell1");

    // Update through cell2
    assert!(cell2.compare_and_update(1, "modified_via_cell2".to_string()));

    // Read through cell1
    let snap1 = cell1.read();
    assert_eq!(snap1.version(), 2);
    assert_eq!(snap1.value(), "modified_via_cell2");
}

#[test]
fn test_concurrent_compare_and_update_exactly_one_winner() {
    const NUM_THREADS: usize = 12;
    let cell = Arc::new(make_versioned_cell(0));
    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let winners_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for thread_id in 0..NUM_THREADS {
        let cell_clone = Arc::clone(&cell);
        let barrier_clone = Arc::clone(&barrier);
        let winners_clone = Arc::clone(&winners_count);

        handles.push(thread::spawn(move || {
            // Synchronize all threads so they compete at the exact same moment
            barrier_clone.wait();
            let won = cell_clone.compare_and_update(0, (thread_id + 1) * 100);
            if won {
                winners_clone.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        winners_count.load(Ordering::SeqCst),
        1,
        "Exactly ONE thread must succeed when multiple threads race on expected_version = 0"
    );

    let final_snap = cell.read();
    assert_eq!(final_snap.version(), 1, "Cell version must be exactly 1");
    assert!(
        *final_snap.value() >= 100,
        "Final value must be the value set by the single winning thread"
    );
}

#[test]
fn test_concurrent_optimistic_writers_stress() {
    const NUM_WRITERS: usize = 8;
    const UPDATES_PER_WRITER: usize = 50;
    const TOTAL_UPDATES: usize = NUM_WRITERS * UPDATES_PER_WRITER;

    let cell = Arc::new(make_versioned_cell(0usize));
    let barrier = Arc::new(Barrier::new(NUM_WRITERS));

    let mut handles = Vec::new();
    for _ in 0..NUM_WRITERS {
        let cell_clone = Arc::clone(&cell);
        let barrier_clone = Arc::clone(&barrier);

        handles.push(thread::spawn(move || {
            barrier_clone.wait();
            for _ in 0..UPDATES_PER_WRITER {
                // Optimistic retry loop
                loop {
                    let snap = cell_clone.read();
                    let expected_v = snap.version();
                    let current_val = *snap.value();
                    if cell_clone.compare_and_update(expected_v, current_val + 1) {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let final_snap = cell.read();
    assert_eq!(
        final_snap.version(),
        TOTAL_UPDATES as u64,
        "Final version must equal the total number of successful updates ({})",
        TOTAL_UPDATES
    );
    assert_eq!(
        *final_snap.value(),
        TOTAL_UPDATES,
        "Final value must equal the total increments ({})",
        TOTAL_UPDATES
    );
}

#[test]
fn test_non_blocking_readers_concurrent_with_writers() {
    const NUM_WRITERS: usize = 4;
    const UPDATES_PER_WRITER: usize = 40;
    const NUM_READERS: usize = 4;

    let cell = Arc::new(make_versioned_cell((0u64, 0u64))); // (version, counter)
    let running = Arc::new(AtomicBool::new(true));

    // Spawn readers that continuously read without blocking
    let mut reader_handles = Vec::new();
    for _ in 0..NUM_READERS {
        let cell_clone = Arc::clone(&cell);
        let running_clone = Arc::clone(&running);

        reader_handles.push(thread::spawn(move || {
            let mut last_observed_version = 0u64;
            let mut read_count = 0usize;

            while running_clone.load(Ordering::Relaxed) {
                let snap = cell_clone.read();
                let v = snap.version();
                let (val_v, _) = *snap.value();

                // Consistency check: value version must match snapshot version
                assert_eq!(v, val_v, "Snapshot version and internal value version must be consistent");
                assert!(
                    v >= last_observed_version,
                    "Observed versions must be monotonically non-decreasing (saw {} after {})",
                    v, last_observed_version
                );

                last_observed_version = v;
                read_count += 1;
            }

            read_count
        }));
    }

    // Spawn writers that perform optimistic updates
    let mut writer_handles = Vec::new();
    for _ in 0..NUM_WRITERS {
        let cell_clone = Arc::clone(&cell);

        writer_handles.push(thread::spawn(move || {
            for _ in 0..UPDATES_PER_WRITER {
                loop {
                    let snap = cell_clone.read();
                    let current_version = snap.version();
                    let (_, count) = *snap.value();
                    let new_value = (current_version + 1, count + 1);

                    if cell_clone.compare_and_update(current_version, new_value) {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }));
    }

    for h in writer_handles {
        h.join().unwrap();
    }

    // Stop readers
    running.store(false, Ordering::Relaxed);

    for h in reader_handles {
        let count = h.join().unwrap();
        assert!(count > 0, "Reader must have completed at least one read");
    }

    let final_snap = cell.read();
    let expected_final_version = (NUM_WRITERS * UPDATES_PER_WRITER) as u64;
    assert_eq!(final_snap.version(), expected_final_version);
    assert_eq!(final_snap.value().0, expected_final_version);
    assert_eq!(final_snap.value().1, expected_final_version);
}

#[test]
fn test_complex_cloneable_data_types() {
    #[derive(Clone, Debug, PartialEq)]
    struct ServerConfig {
        name: String,
        port: u16,
        tags: Vec<String>,
    }

    let initial_config = ServerConfig {
        name: "Gateway".to_string(),
        port: 8080,
        tags: vec!["api".to_string(), "v1".to_string()],
    };

    let cell = make_versioned_cell(initial_config.clone());
    let snap0 = cell.read();
    assert_eq!(snap0.version(), 0);
    assert_eq!(*snap0.value(), initial_config);

    let new_config = ServerConfig {
        name: "Gateway".to_string(),
        port: 8443,
        tags: vec!["api".to_string(), "v2".to_string(), "tls".to_string()],
    };

    assert!(cell.compare_and_update(0, new_config.clone()));

    let snap1 = cell.read();
    assert_eq!(snap1.version(), 1);
    assert_eq!(*snap1.value(), new_config);
}
