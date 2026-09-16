use simulation_050::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_send_sync_bounds() {
    let wal = make_segmented_wal(10, 5);
    assert_send(&wal);
    assert_sync(&wal);

    let lease = wal.open_lease(1);
    if let Ok(l) = lease {
        assert_send(&l);
    }
}

#[test]
fn test_single_segment_append_and_read() {
    let wal = make_segmented_wal(5, 3);
    assert_eq!(wal.active_segment_id(), 0);
    assert_eq!(wal.sealed_segment_count(), 0);

    let lsn1 = wal.append(b"payload_1").unwrap();
    let lsn2 = wal.append(b"payload_2").unwrap();
    let lsn3 = wal.append(b"payload_3").unwrap();

    assert_eq!(lsn1, 1);
    assert_eq!(lsn2, 2);
    assert_eq!(lsn3, 3);

    let lease = wal.open_lease(1).unwrap();
    assert_eq!(lease.current_lsn(), 1);
    assert_eq!(lease.pinned_segment_id(), 0);

    let r1 = lease.read_next().unwrap().unwrap();
    assert_eq!(r1.lsn, 1);
    assert_eq!(r1.payload, b"payload_1");

    let r2 = lease.read_next().unwrap().unwrap();
    assert_eq!(r2.lsn, 2);
    assert_eq!(r2.payload, b"payload_2");

    let r3 = lease.read_next().unwrap().unwrap();
    assert_eq!(r3.lsn, 3);
    assert_eq!(r3.payload, b"payload_3");

    // Reading past end of log returns None without blocking
    assert_eq!(lease.read_next().unwrap(), None);
}

#[test]
fn test_segment_rotation_on_capacity() {
    let capacity = 2;
    let max_sealed = 5;
    let wal = make_segmented_wal(capacity, max_sealed);

    assert_eq!(wal.active_segment_id(), 0);
    assert_eq!(wal.sealed_segment_count(), 0);

    // Fill segment 0
    assert_eq!(wal.append(b"r1").unwrap(), 1);
    assert_eq!(wal.append(b"r2").unwrap(), 2);
    assert_eq!(wal.active_segment_id(), 0);
    assert_eq!(wal.sealed_segment_count(), 0);

    // Appending record 3 triggers rotation: segment 0 is sealed, segment 1 becomes active
    assert_eq!(wal.append(b"r3").unwrap(), 3);
    assert_eq!(wal.active_segment_id(), 1);
    assert_eq!(wal.sealed_segment_count(), 1);

    // Fill segment 1
    assert_eq!(wal.append(b"r4").unwrap(), 4);
    assert_eq!(wal.active_segment_id(), 1);

    // Appending record 5 triggers rotation: segment 1 is sealed, segment 2 becomes active
    assert_eq!(wal.append(b"r5").unwrap(), 5);
    assert_eq!(wal.active_segment_id(), 2);
    assert_eq!(wal.sealed_segment_count(), 2);
}

#[test]
fn test_reader_lease_pins_segment_preventing_compaction() {
    let wal = make_segmented_wal(2, 10);

    // Create 3 segments (0 and 1 sealed, 2 active)
    wal.append(b"a").unwrap(); // 1
    wal.append(b"b").unwrap(); // 2
    wal.append(b"c").unwrap(); // 3
    wal.append(b"d").unwrap(); // 4
    wal.append(b"e").unwrap(); // 5

    assert_eq!(wal.sealed_segment_count(), 2);

    // Open a lease that pins segment 0 (from LSN 1)
    let lease = wal.open_lease(1).unwrap();
    assert_eq!(lease.pinned_segment_id(), 0);

    // Compaction must NOT delete segment 0 because it is pinned and low watermark is 1
    let reclaimed = wal.compact();
    assert_eq!(reclaimed, 0, "Pinned segment must not be reclaimed");
    assert_eq!(wal.sealed_segment_count(), 2);

    // Low watermark is anchored to the lease LSN
    assert_eq!(wal.low_watermark(), 1);
}

#[test]
fn test_reader_lease_drop_unpins_and_allows_compaction() {
    let wal = make_segmented_wal(2, 10);

    wal.append(b"1").unwrap(); // 1
    wal.append(b"2").unwrap(); // 2
    wal.append(b"3").unwrap(); // 3
    wal.append(b"4").unwrap(); // 4
    wal.append(b"5").unwrap(); // 5

    assert_eq!(wal.sealed_segment_count(), 2);

    {
        let _lease = wal.open_lease(1).unwrap();
        assert_eq!(wal.compact(), 0);
        // Lease exits scope here -> Drop unpins segment 0!
    }

    // Now low watermark advances past segment 0 and 1, and pin count is 0
    let reclaimed = wal.compact();
    assert_eq!(reclaimed, 2, "Both sealed segments should now be reclaimed");
    assert_eq!(wal.sealed_segment_count(), 0);
}

#[test]
fn test_write_stall_blocks_when_max_sealed_segments_reached() {
    let capacity = 2;
    let max_sealed = 2;
    let wal = make_segmented_wal(capacity, max_sealed);

    // Fill segments 0 and 1
    wal.append(b"a").unwrap(); // 1
    wal.append(b"b").unwrap(); // 2 (segment 0 full)
    wal.append(b"c").unwrap(); // 3 (segment 0 sealed, segment 1 active)
    wal.append(b"d").unwrap(); // 4 (segment 1 full)
    wal.append(b"e").unwrap(); // 5 (segment 1 sealed, segment 2 active)
    wal.append(b"f").unwrap(); // 6 (segment 2 full)

    // Currently: sealed segments count == 2 (max_sealed reached).
    assert_eq!(wal.sealed_segment_count(), 2);

    // Appending record 7 would require sealing segment 2 -> exceeding max_sealed!
    // This MUST trigger Write-Stall!
    let append_finished = Arc::new(AtomicBool::new(false));
    let af_clone = Arc::clone(&append_finished);
    let wal_clone = wal.clone();

    let h = thread::spawn(move || {
        let lsn = wal_clone.append(b"g").unwrap();
        af_clone.store(true, Ordering::SeqCst);
        lsn
    });

    thread::sleep(Duration::from_millis(40));
    assert!(
        !append_finished.load(Ordering::SeqCst),
        "Append must be blocked by Write-Stall when max_sealed_segments is reached"
    );

    // Run compaction to reclaim sealed segments and relieve write stall
    let reclaimed = wal.compact();
    assert!(reclaimed >= 1, "Compaction should reclaim sealed segments");

    h.join().unwrap();
    assert!(
        append_finished.load(Ordering::SeqCst),
        "Write-Stall should be unblocked after compaction frees space"
    );
}

#[test]
fn test_multiple_concurrent_reader_leases_low_watermark() {
    let wal = make_segmented_wal(2, 10);

    for i in 1..=8 {
        wal.append(format!("rec_{i}").as_bytes()).unwrap();
    }

    let lease_a = wal.open_lease(2).unwrap();
    let lease_b = wal.open_lease(6).unwrap();

    // Low watermark is min(2, 6) == 2
    assert_eq!(wal.low_watermark(), 2);

    // Drop lease A: low watermark advances to 6
    drop(lease_a);
    assert_eq!(wal.low_watermark(), 6);

    // Drop lease B: low watermark advances to active segment start
    drop(lease_b);
    assert!(wal.low_watermark() >= 7);
}

#[test]
fn test_open_lease_on_compacted_lsn_fails() {
    let wal = make_segmented_wal(2, 10);

    wal.append(b"1").unwrap();
    wal.append(b"2").unwrap();
    wal.append(b"3").unwrap();
    wal.append(b"4").unwrap();
    wal.append(b"5").unwrap();

    // Reclaim segments 0 and 1
    assert_eq!(wal.compact(), 2);

    // Opening lease at LSN 1 (which was in reclaimed segment 0) must fail with LsnCompacted
    let res = wal.open_lease(1);
    assert_eq!(res.map(|_| ()), Err(WalError::LsnCompacted));

    // Opening lease at LSN 5 (which is in active segment 2) must succeed
    assert!(wal.open_lease(5).is_ok());
}

#[test]
fn test_wal_close_unblocks_waiting_writers() {
    let wal = make_segmented_wal(2, 1);

    wal.append(b"a").unwrap();
    wal.append(b"b").unwrap();
    wal.append(b"c").unwrap();
    wal.append(b"d").unwrap();

    let wal_clone = wal.clone();
    let h = thread::spawn(move || {
        // Will block in Write-Stall because sealed count == 1 (which is max_sealed)
        wal_clone.append(b"e")
    });

    thread::sleep(Duration::from_millis(30));
    wal.close();

    let res = h.join().unwrap();
    assert_eq!(
        res,
        Err(WalError::WalClosed),
        "Blocked writer must unblock with WalClosed on close"
    );

    // Further appends should immediately fail with WalClosed
    assert_eq!(wal.append(b"x"), Err(WalError::WalClosed));
}

#[test]
fn test_stress_concurrent_append_read_compact() {
    let wal = make_segmented_wal(4, 5);
    let writer_count = 6;
    let appends_per_writer = 30;
    let mut writer_handles = Vec::new();

    let total_appended = Arc::new(AtomicUsize::new(0));

    // Spawn writers
    for w in 0..writer_count {
        let wal_w = wal.clone();
        let ta = Arc::clone(&total_appended);
        writer_handles.push(thread::spawn(move || {
            for i in 0..appends_per_writer {
                let payload = format!("w_{w}_rec_{i}");
                if wal_w.append(payload.as_bytes()).is_ok() {
                    ta.fetch_add(1, Ordering::SeqCst);
                }
                thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    // Spawn compactor thread
    let wal_c = wal.clone();
    let compactor_stop = Arc::new(AtomicBool::new(false));
    let cs_clone = Arc::clone(&compactor_stop);
    let compactor_handle = thread::spawn(move || {
        while !cs_clone.load(Ordering::SeqCst) {
            wal_c.compact();
            thread::sleep(Duration::from_millis(5));
        }
        wal_c.compact();
    });

    for h in writer_handles {
        h.join().unwrap();
    }

    compactor_stop.store(true, Ordering::SeqCst);
    compactor_handle.join().unwrap();

    assert_eq!(
        total_appended.load(Ordering::SeqCst),
        writer_count * appends_per_writer
    );
}
