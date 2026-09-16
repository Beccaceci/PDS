use simulation_029::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_single_epoch_append_and_checkpoint() {
    let log = make_append_log();

    let g1 = log.begin_append();
    let g2 = log.begin_append();
    let g3 = log.begin_append();

    drop(g1);
    drop(g2);
    drop(g3);

    let count = log.checkpoint();
    assert_eq!(count, 3, "Checkpoint must return total appends that belonged to the epoch");
}

#[test]
fn test_checkpoint_blocks_until_guards_dropped() {
    let log = make_append_log();

    let g1 = log.begin_append();
    let checkpoint_finished = Arc::new(AtomicBool::new(false));

    let log_clone = log.clone();
    let cf = Arc::clone(&checkpoint_finished);
    let handle = thread::spawn(move || {
        let count = log_clone.checkpoint();
        assert_eq!(count, 1);
        cf.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(50));
    assert!(!checkpoint_finished.load(Ordering::SeqCst), "Checkpoint must wait for active guard");

    drop(g1);

    handle.join().unwrap();
    assert!(checkpoint_finished.load(Ordering::SeqCst), "Checkpoint must unblock after guard drop");
}

#[test]
fn test_appends_during_checkpoint_belong_to_next_epoch() {
    let log = make_append_log();

    // Guard in epoch 0
    let g0 = log.begin_append();

    let checkpoint_0_result = Arc::new(AtomicUsize::new(999));
    let log_clone = log.clone();
    let c0_res = Arc::clone(&checkpoint_0_result);

    let handle_cp0 = thread::spawn(move || {
        let count = log_clone.checkpoint();
        c0_res.store(count, Ordering::SeqCst);
    });

    // Wait until checkpoint 0 starts waiting and opens epoch 1
    thread::sleep(Duration::from_millis(30));

    // These appends start AFTER checkpoint 0 was called -> belong to Epoch 1!
    let g1_a = log.begin_append();
    let g1_b = log.begin_append();

    // Dropping g0 must finish Checkpoint 0 with count 1, ignoring g1_a and g1_b!
    drop(g0);
    handle_cp0.join().unwrap();

    assert_eq!(
        checkpoint_0_result.load(Ordering::SeqCst),
        1,
        "Checkpoint 0 must count ONLY appends from Epoch 0"
    );

    // Now finish g1_a, but keep g1_b alive
    drop(g1_a);

    let checkpoint_1_finished = Arc::new(AtomicBool::new(false));
    let log_clone2 = log.clone();
    let c1_fin = Arc::clone(&checkpoint_1_finished);

    let handle_cp1 = thread::spawn(move || {
        let count = log_clone2.checkpoint();
        assert_eq!(count, 2, "Checkpoint 1 must count g1_a and g1_b");
        c1_fin.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(40));
    assert!(!checkpoint_1_finished.load(Ordering::SeqCst));

    drop(g1_b);
    handle_cp1.join().unwrap();
    assert!(checkpoint_1_finished.load(Ordering::SeqCst));
}

#[test]
fn test_multiple_checkpoints_independent() {
    let log = make_append_log();

    // Epoch 0
    let _g = log.begin_append();
    drop(_g);
    assert_eq!(log.checkpoint(), 1);

    // Epoch 1
    let g1 = log.begin_append();
    let g2 = log.begin_append();
    drop(g1);
    drop(g2);
    assert_eq!(log.checkpoint(), 2);

    // Epoch 2 (Empty)
    assert_eq!(log.checkpoint(), 0);
}

#[test]
fn test_empty_epoch_checkpoint() {
    let log = make_append_log();
    assert_eq!(log.checkpoint(), 0);
}

#[test]
fn test_high_concurrency_stress() {
    let log = make_append_log();
    let running = Arc::new(AtomicBool::new(true));
    let total_appends = Arc::new(AtomicUsize::new(0));

    let mut writer_handles = Vec::new();
    for _ in 0..8 {
        let l = log.clone();
        let r = Arc::clone(&running);
        let ta = Arc::clone(&total_appends);

        writer_handles.push(thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                let guard = l.begin_append();
                ta.fetch_add(1, Ordering::Relaxed);
                thread::sleep(Duration::from_micros(200));
                drop(guard);
            }
        }));
    }

    let mut checkpoint_total = 0;
    for _ in 0..5 {
        thread::sleep(Duration::from_millis(15));
        let count = log.checkpoint();
        checkpoint_total += count;
    }

    running.store(false, Ordering::SeqCst);
    for h in writer_handles {
        h.join().unwrap();
    }

    // Final drain checkpoint
    checkpoint_total += log.checkpoint();

    assert_eq!(
        checkpoint_total,
        total_appends.load(Ordering::SeqCst),
        "Sum of all checkpoint counts must equal total appends started"
    );
}

#[test]
fn test_send_sync_bounds() {
    let log = make_append_log();
    assert_send(&log);
    assert_sync(&log);
}
