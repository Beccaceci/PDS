use simulation_034::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_basic_write_and_snapshot() {
    let store = make_mvcc_store::<String, String>();

    store.write("k1".to_string(), "v1".to_string());
    let s1 = store.snapshot();

    store.write("k1".to_string(), "v2".to_string());
    let s2 = store.snapshot();

    assert_eq!(s1.get(&"k1".to_string()), Some("v1".to_string()));
    assert_eq!(s2.get(&"k1".to_string()), Some("v2".to_string()));
}

#[test]
fn test_snapshot_sees_none_for_key_written_after_snapshot() {
    let store = make_mvcc_store::<String, i32>();

    let s0 = store.snapshot();
    store.write("a".to_string(), 100);

    assert_eq!(s0.get(&"a".to_string()), None);

    let s1 = store.snapshot();
    assert_eq!(s1.get(&"a".to_string()), Some(100));
}

#[test]
fn test_multi_key_point_in_time_consistency() {
    let store = make_mvcc_store::<String, i32>();

    store.write("x".to_string(), 10);
    store.write("y".to_string(), 20);

    let s1 = store.snapshot();

    store.write("x".to_string(), 99);
    store.write("y".to_string(), 88);
    store.write("z".to_string(), 77);

    assert_eq!(s1.get(&"x".to_string()), Some(10));
    assert_eq!(s1.get(&"y".to_string()), Some(20));
    assert_eq!(s1.get(&"z".to_string()), None);
}

#[test]
fn test_gc_prunes_outdated_versions_on_snapshot_drop() {
    let store = make_mvcc_store::<String, String>();

    store.write("k".to_string(), "v1".to_string());
    let s1 = store.snapshot();

    store.write("k".to_string(), "v2".to_string());
    let s2 = store.snapshot();

    store.write("k".to_string(), "v3".to_string());

    // In memory: v1 (for s1), v2 (for s2), v3 (current)
    assert_eq!(store.version_count(&"k".to_string()), 3);

    // Dropping s1 makes v1 obsolete (s2 needs v2, current is v3)
    drop(s1);
    assert_eq!(
        store.version_count(&"k".to_string()),
        2,
        "Dropping oldest snapshot must prune obsolete version v1"
    );

    // Dropping s2 makes v2 obsolete (only v3 current remains)
    drop(s2);
    assert_eq!(
        store.version_count(&"k".to_string()),
        1,
        "Dropping all snapshots must prune down to single current version"
    );
}

#[test]
fn test_retaining_old_version_if_oldest_snapshot_alive() {
    let store = make_mvcc_store::<String, i32>();

    store.write("a".to_string(), 1);
    let s1 = store.snapshot(); // needs version 1

    store.write("a".to_string(), 2);
    let s2 = store.snapshot(); // needs version 2

    store.write("a".to_string(), 3);
    let s3 = store.snapshot(); // needs version 3

    store.write("a".to_string(), 4);

    assert_eq!(store.version_count(&"a".to_string()), 4);

    // Dropping intermediate snapshot s2
    drop(s2);
    // Versions 1, 3, 4 are still needed (1 by s1, 3 by s3, 4 is latest)
    // Version 2 can be pruned since no snapshot needs it (s1 needs 1, s3 needs 3)
    assert_eq!(s1.get(&"a".to_string()), Some(1));
    assert_eq!(s3.get(&"a".to_string()), Some(3));
}

#[test]
fn test_high_concurrency_multi_thread_writers_and_snapshots() {
    let store = make_mvcc_store::<usize, usize>();
    let running = Arc::new(AtomicBool::new(true));

    let mut handles = Vec::new();

    // 4 Writer threads
    for writer_id in 0..4 {
        let s = store.clone();
        let r = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            let mut val = 0;
            while r.load(Ordering::Relaxed) {
                s.write(writer_id, val);
                val += 1;
                thread::sleep(Duration::from_micros(100));
            }
        }));
    }

    // 4 Snapshot Reader threads
    let total_queries = Arc::new(AtomicUsize::new(0));
    for _ in 0..4 {
        let s = store.clone();
        let r = Arc::clone(&running);
        let tq = Arc::clone(&total_queries);
        handles.push(thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                let snap = s.snapshot();
                let v0 = snap.get(&0);
                let v1 = snap.get(&1);
                thread::sleep(Duration::from_micros(150));
                // Point-in-time sanity check (values don't change within snapshot)
                assert_eq!(snap.get(&0), v0);
                assert_eq!(snap.get(&1), v1);
                tq.fetch_add(1, Ordering::Relaxed);
                drop(snap);
            }
        }));
    }

    thread::sleep(Duration::from_millis(80));
    running.store(false, Ordering::SeqCst);

    for h in handles {
        h.join().unwrap();
    }

    assert!(total_queries.load(Ordering::SeqCst) > 0);
}

#[test]
fn test_send_sync_bounds() {
    let store = make_mvcc_store::<String, String>();
    let snap = store.snapshot();

    assert_send(&store);
    assert_sync(&store);
    assert_send(&snap);
    assert_sync(&snap);
}
