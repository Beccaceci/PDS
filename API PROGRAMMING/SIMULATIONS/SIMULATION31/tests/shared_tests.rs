use simulation_031::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_basic_send_recv_with_initial_credit() {
    let (sender, receiver) = make_credited_channel::<i32>(3);

    sender.send(10);
    sender.send(20);
    sender.send(30);

    assert_eq!(receiver.recv(), Some(10));
    assert_eq!(receiver.recv(), Some(20));
    assert_eq!(receiver.recv(), Some(30));
}

#[test]
fn test_send_blocks_when_credit_exhausted() {
    let (sender, receiver) = make_credited_channel::<&'static str>(1);

    // First send consumes the 1 initial credit
    sender.send("msg1");

    let second_sent = Arc::new(AtomicBool::new(false));
    let sender_clone = sender.clone();
    let ss = Arc::clone(&second_sent);

    let handle = thread::spawn(move || {
        sender_clone.send("msg2"); // Must block!
        ss.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(40));
    assert!(!second_sent.load(Ordering::SeqCst), "Sender must block when credit is 0");

    // Grant 1 credit -> unblocks sender
    receiver.grant_credit(1);

    handle.join().unwrap();
    assert!(second_sent.load(Ordering::SeqCst));

    assert_eq!(receiver.recv(), Some("msg1"));
    assert_eq!(receiver.recv(), Some("msg2"));
}

#[test]
fn test_grant_credit_exact_amount_unblocks() {
    let (sender, receiver) = make_credited_channel::<usize>(0); // 0 initial credit

    let completed_sends = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for i in 0..5 {
        let s = sender.clone();
        let cs = Arc::clone(&completed_sends);
        handles.push(thread::spawn(move || {
            s.send(i);
            cs.fetch_add(1, Ordering::SeqCst);
        }));
    }

    thread::sleep(Duration::from_millis(40));
    assert_eq!(completed_sends.load(Ordering::SeqCst), 0);

    // Grant credit for exactly 2 sends
    receiver.grant_credit(2);

    thread::sleep(Duration::from_millis(50));
    assert_eq!(
        completed_sends.load(Ordering::SeqCst),
        2,
        "Exactly 2 senders must proceed when 2 credits granted"
    );

    // Grant remaining 3 credits
    receiver.grant_credit(3);

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(completed_sends.load(Ordering::SeqCst), 5);
}

#[test]
fn test_recv_blocks_when_empty() {
    let (sender, receiver) = make_credited_channel::<i32>(2);

    let received_val = Arc::new(AtomicUsize::new(0));
    let rv = Arc::clone(&received_val);

    let handle = thread::spawn(move || {
        if let Some(val) = receiver.recv() {
            rv.store(val as usize, Ordering::SeqCst);
        }
    });

    thread::sleep(Duration::from_millis(40));
    assert_eq!(received_val.load(Ordering::SeqCst), 0);

    sender.send(42);

    handle.join().unwrap();
    assert_eq!(received_val.load(Ordering::SeqCst), 42);
}

#[test]
fn test_close_and_drain() {
    let (sender, receiver) = make_credited_channel::<i32>(5);

    sender.send(1);
    sender.send(2);

    receiver.close();

    // Drains remaining items
    assert_eq!(receiver.recv(), Some(1));
    assert_eq!(receiver.recv(), Some(2));
    // Then returns None
    assert_eq!(receiver.recv(), None);
    assert_eq!(receiver.recv(), None);
}

#[test]
fn test_high_concurrency_multi_sender_stress() {
    let (sender, receiver) = make_credited_channel::<usize>(10);
    let total_received = Arc::new(AtomicUsize::new(0));

    let mut senders = Vec::new();
    for thread_id in 0..8 {
        let s = sender.clone();
        senders.push(thread::spawn(move || {
            for i in 0..25 {
                s.send(thread_id * 1000 + i);
            }
        }));
    }

    let tr = Arc::clone(&total_received);
    let receiver_handle = thread::spawn(move || {
        let mut count = 0;
        while count < 200 {
            receiver.grant_credit(10);
            while let Some(_) = receiver.recv() {
                count += 1;
                tr.fetch_add(1, Ordering::SeqCst);
                if count % 10 == 0 {
                    break;
                }
            }
        }
    });

    for h in senders {
        h.join().unwrap();
    }
    receiver_handle.join().unwrap();

    assert_eq!(total_received.load(Ordering::SeqCst), 200);
}

#[test]
fn test_recv_does_not_grant_credit() {
    let (sender, receiver) = make_credited_channel::<i32>(1);

    sender.send(100);
    assert_eq!(receiver.recv(), Some(100)); // Receives item, credit is still 0!

    let blocked = Arc::new(AtomicBool::new(true));
    let s_clone = sender.clone();
    let b = Arc::clone(&blocked);

    let handle = thread::spawn(move || {
        s_clone.send(200); // MUST BLOCK!
        b.store(false, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(40));
    assert!(blocked.load(Ordering::SeqCst), "recv() must NOT grant credit to senders!");

    receiver.grant_credit(1);
    handle.join().unwrap();
    assert!(!blocked.load(Ordering::SeqCst));
    assert_eq!(receiver.recv(), Some(200));
}

#[test]
fn test_send_sync_bounds() {
    let (sender, receiver) = make_credited_channel::<i32>(2);
    assert_send(&sender);
    assert_sync(&sender);
    assert_send(&receiver);
}
