use simulation_025::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_successful_transfer_and_verification() {
    let pipeline = make_transfer_pipeline(2);

    let handle = pipeline.submit(
        3,
        || {
            thread::sleep(Duration::from_millis(30));
            true
        },
        || {
            thread::sleep(Duration::from_millis(20));
            true
        },
    );

    let final_status = handle.join();
    assert_eq!(final_status, TransferStatus::Complete);
    assert_eq!(handle.status(), TransferStatus::Complete);
}

#[test]
fn test_retry_mechanism_eventual_success() {
    let pipeline = make_transfer_pipeline(2);
    let attempts = Arc::new(AtomicUsize::new(0));

    let att = Arc::clone(&attempts);
    let handle = pipeline.submit(
        3,
        move || {
            let n = att.fetch_add(1, Ordering::SeqCst) + 1;
            if n < 3 {
                false // Fail first 2 attempts
            } else {
                true // Succeed on 3rd attempt
            }
        },
        || true,
    );

    let final_status = handle.join();
    assert_eq!(final_status, TransferStatus::Complete);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[test]
fn test_retry_exhausted_leads_to_failed() {
    let pipeline = make_transfer_pipeline(2);
    let attempts = Arc::new(AtomicUsize::new(0));

    let att = Arc::clone(&attempts);
    let handle = pipeline.submit(
        3,
        move || {
            att.fetch_add(1, Ordering::SeqCst);
            false // Always fail
        },
        || true,
    );

    let final_status = handle.join();
    assert_eq!(final_status, TransferStatus::Failed);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[test]
fn test_failed_verification_leads_to_failed() {
    let pipeline = make_transfer_pipeline(2);

    let handle = pipeline.submit(
        3,
        || true,  // Transfer succeeds
        || false, // Verification fails
    );

    let final_status = handle.join();
    assert_eq!(final_status, TransferStatus::Failed);
}

#[test]
fn test_cancel_queued_transfer() {
    let pipeline = make_transfer_pipeline(1);

    // 1. Submit long blocking transfer to fill worker
    let _h1 = pipeline.submit(
        1,
        || {
            thread::sleep(Duration::from_millis(100));
            true
        },
        || true,
    );

    // 2. Submit second transfer that stays Queued
    let h2 = pipeline.submit(1, || true, || true);

    // Cancel while queued
    assert!(h2.cancel());
    assert_eq!(h2.join(), TransferStatus::Cancelled);
}

#[test]
fn test_cancel_transferring_transfer() {
    let pipeline = make_transfer_pipeline(2);
    let verify_called = Arc::new(AtomicBool::new(false));

    let vc = Arc::clone(&verify_called);
    let handle = pipeline.submit(
        3,
        || {
            thread::sleep(Duration::from_millis(60));
            true
        },
        move || {
            vc.store(true, Ordering::SeqCst);
            true
        },
    );

    // Wait until it enters Transferring
    thread::sleep(Duration::from_millis(20));
    assert!(handle.cancel());

    let final_status = handle.join();
    assert_eq!(final_status, TransferStatus::Cancelled);
    assert!(
        !verify_called.load(Ordering::SeqCst),
        "do_verify must NOT be called after successful cancellation in Transferring"
    );
}

#[test]
fn test_cancel_verifying_transfer_fails() {
    let pipeline = make_transfer_pipeline(2);
    let in_verify = Arc::new(AtomicBool::new(false));

    let iv = Arc::clone(&in_verify);
    let handle = pipeline.submit(
        1,
        || true,
        move || {
            iv.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(60));
            true
        },
    );

    // Wait until it enters Verifying
    while !in_verify.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(5));
    }

    // Cancel must fail once in Verifying
    assert!(!handle.cancel());

    let final_status = handle.join();
    assert_eq!(final_status, TransferStatus::Complete);
}

#[test]
fn test_multi_worker_concurrency_stress() {
    let pipeline = make_transfer_pipeline(4);
    let success_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for i in 0..20 {
        handles.push(pipeline.submit(
            2,
            move || {
                thread::sleep(Duration::from_millis(5));
                i % 3 != 0
            },
            move || {
                thread::sleep(Duration::from_millis(5));
                true
            },
        ));
    }

    for h in handles {
        if h.join() == TransferStatus::Complete {
            success_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    assert!(success_count.load(Ordering::SeqCst) > 0);
}

#[test]
fn test_send_sync_bounds() {
    let pipeline = make_transfer_pipeline(2);
    assert_send(&pipeline);
    assert_sync(&pipeline);
}
