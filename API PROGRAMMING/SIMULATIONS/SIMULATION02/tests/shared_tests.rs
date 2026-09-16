use simulation_002::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_priority_ordering() {
    let (tx, rx) = create_channel::<&str>();

    tx.submit("prio_10", 10);
    tx.submit("prio_50", 50);
    tx.submit("prio_5", 5);
    tx.submit("prio_100", 100);
    tx.submit("prio_20", 20);

    assert_eq!(rx.pop_next(), Some("prio_100"));
    assert_eq!(rx.pop_next(), Some("prio_50"));
    assert_eq!(rx.pop_next(), Some("prio_20"));
    assert_eq!(rx.pop_next(), Some("prio_10"));
    assert_eq!(rx.pop_next(), Some("prio_5"));

    // Check FIFO ordering for equal priorities
    tx.submit("first_equal", 40);
    tx.submit("second_equal", 40);
    tx.submit("third_equal", 40);

    assert_eq!(rx.pop_next(), Some("first_equal"));
    assert_eq!(rx.pop_next(), Some("second_equal"));
    assert_eq!(rx.pop_next(), Some("third_equal"));
}

#[test]
fn test_basic_task_cancellation() {
    let (tx, rx) = create_channel::<i32>();

    let _h1 = tx.submit(10, 10).expect("Submit failed");
    let h2 = tx.submit(20, 20).expect("Submit failed");
    let _h3 = tx.submit(30, 30).expect("Submit failed");

    // Cancel item with priority 20
    assert!(h2.cancel(), "Cancel should return true for queued task");

    // pop_next should skip item 20 and return 30 then 10
    assert_eq!(rx.pop_next(), Some(30));
    assert_eq!(rx.pop_next(), Some(10));

    drop(tx);
    assert_eq!(rx.pop_next(), None);
}

#[test]
fn test_cancel_after_popped() {
    let (tx, rx) = create_channel::<i32>();

    let h = tx.submit(42, 10).expect("Submit failed");

    assert_eq!(rx.pop_next(), Some(42));

    // Cancel after task is already popped must return false
    assert!(!h.cancel(), "Cancel after pop must return false");
    // Repeating cancel must also return false
    assert!(!h.cancel(), "Second cancel after pop must return false");
}

#[test]
fn test_double_cancellation() {
    let (tx, rx) = create_channel::<i32>();

    let h = tx.submit(100, 10).expect("Submit failed");

    // First cancel succeeds
    assert!(h.cancel(), "First cancel should return true");
    // Second cancel fails
    assert!(!h.cancel(), "Second cancel should return false");

    drop(tx);
    assert_eq!(rx.pop_next(), None);
}

#[test]
fn test_is_done_status() {
    let (tx, rx) = create_channel::<i32>();

    let h1 = tx.submit(100, 10).expect("Submit failed");
    let h2 = tx.submit(200, 20).expect("Submit failed");

    assert!(!h1.is_done(), "is_done should be false before pop");
    assert!(!h2.is_done(), "is_done should be false before pop");

    // Pop item 200
    assert_eq!(rx.pop_next(), Some(200));
    assert!(h2.is_done(), "is_done should be true after pop");
    assert!(!h1.is_done(), "h1 is still in queue");

    // Cancel h1
    assert!(h1.cancel());
    assert!(!h1.is_done(), "Canceled task should not be marked as done");
}

#[test]
fn test_multi_producer_cloning_and_multithreaded_submission() {
    let (tx, rx) = create_channel::<(u8, usize)>();
    let num_producers = 10;
    let items_per_producer = 50;

    let mut handles = Vec::new();

    for p in 0..num_producers {
        let tx_clone = tx.clone();
        handles.push(thread::spawn(move || {
            for i in 0..items_per_producer {
                let prio = ((p * 13 + i * 7) % 256) as u8;
                tx_clone.submit((prio, p * 1000 + i), prio);
            }
        }));
    }

    drop(tx); // Drop initial producer

    for h in handles {
        h.join().unwrap();
    }

    // Spawn consumer thread to collect items
    let rx_handle = thread::spawn(move || {
        let mut popped = Vec::new();
        while let Some(item) = rx.pop_next() {
            popped.push(item);
        }
        popped
    });

    let popped = rx_handle.join().unwrap();
    assert_eq!(popped.len(), num_producers * items_per_producer);

    // Verify priority ordering
    for i in 0..popped.len() - 1 {
        assert!(
            popped[i].0 >= popped[i + 1].0,
            "Priority violation at index {}: {} < {}",
            i,
            popped[i].0,
            popped[i + 1].0
        );
    }
}

#[test]
fn test_graceful_consumer_shutdown() {
    let (tx1, rx) = create_channel::<i32>();
    let tx2 = tx1.clone();

    tx1.submit(1, 10);
    drop(tx1);

    assert_eq!(rx.pop_next(), Some(1));

    // Spawn thread to submit on tx2 after short delay
    let tx2_clone = tx2.clone();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        tx2_clone.submit(2, 20);
        drop(tx2_clone);
    });

    // Second pop should block until tx2 submits item 2
    assert_eq!(rx.pop_next(), Some(2));

    drop(tx2);
    // Now all producers are dropped, pop_next returns None
    assert_eq!(rx.pop_next(), None);
}

#[test]
fn test_multiple_waiting_consumers_wake_on_producer_drop() {
    let (tx, rx) = create_channel::<i32>();
    let num_consumers = 4;
    let mut handles = Vec::new();

    for _ in 0..num_consumers {
        let rx_clone = rx.clone();
        handles.push(thread::spawn(move || {
            rx_clone.pop_next()
        }));
    }

    // Give consumers time to enter blocked state inside pop_next()
    thread::sleep(Duration::from_millis(50));

    // Drop all producers
    drop(tx);

    // All waiting consumers must wake up and return None
    for h in handles {
        let res = h.join().unwrap();
        assert_eq!(res, None, "Blocked consumer must wake and return None on producer drop");
    }
}

#[test]
fn test_consumer_drop_before_producer_submit() {
    let (tx, rx) = create_channel::<i32>();

    drop(rx);

    let res = tx.submit(100, 10);
    assert!(res.is_none(), "Submit after consumer drop must return None");
}

#[test]
fn test_high_concurrency_stress() {
    let (tx, rx) = create_channel::<(u8, usize)>();
    let num_producers = 30;
    let items_per_producer = 100;
    let total_submitted = num_producers * items_per_producer;

    let canceled_count = Arc::new(AtomicUsize::new(0));
    let popped_count = Arc::new(AtomicUsize::new(0));

    let mut producer_handles = Vec::new();

    for p in 0..num_producers {
        let tx_clone = tx.clone();
        let canceled_counter = Arc::clone(&canceled_count);
        producer_handles.push(thread::spawn(move || {
            for i in 0..items_per_producer {
                let prio = ((p + i) % 256) as u8;
                let handle = tx_clone.submit((prio, p * 10000 + i), prio).unwrap();
                // Randomly cancel ~20% of items
                if (p + i) % 5 == 0 {
                    if handle.cancel() {
                        canceled_counter.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }));
    }

    drop(tx); // Drop original producer handle

    let num_consumers = 5;
    let mut consumer_handles = Vec::new();

    for _ in 0..num_consumers {
        let rx_clone = rx.clone();
        let popped_counter = Arc::clone(&popped_count);
        consumer_handles.push(thread::spawn(move || {
            let mut count = 0;
            while let Some(_item) = rx_clone.pop_next() {
                count += 1;
            }
            popped_counter.fetch_add(count, Ordering::SeqCst);
        }));
    }

    for h in producer_handles {
        h.join().unwrap();
    }
    for h in consumer_handles {
        h.join().unwrap();
    }

    let total_popped = popped_count.load(Ordering::SeqCst);
    let total_canceled = canceled_count.load(Ordering::SeqCst);

    assert_eq!(
        total_popped + total_canceled,
        total_submitted,
        "Sum of popped ({}) + canceled ({}) must equal total submitted ({})",
        total_popped,
        total_canceled,
        total_submitted
    );
}

#[test]
fn test_immediate_cancellation_all_queued_tasks() {
    let (tx, rx) = create_channel::<i32>();
    let mut handles = Vec::new();

    for i in 0..50 {
        if let Some(h) = tx.submit(i, (i % 256) as u8) {
            handles.push(h);
        }
    }

    // Immediately cancel all queued tasks
    for h in handles {
        assert!(h.cancel());
    }

    drop(tx);

    // Consumer should silently discard all canceled tasks and return None immediately/gracefully
    assert_eq!(rx.pop_next(), None);
}

#[test]
fn test_heavy_load_priority_sorting() {
    let (tx, rx) = create_channel::<(u8, usize)>();
    let total_items = 1000;

    for i in 0..total_items {
        let prio = ((i * 1664525 + 1013904223) % 256) as u8;
        tx.submit((prio, i), prio);
    }

    drop(tx);

    let mut prev_priority: Option<u8> = None;
    let mut popped_count = 0;

    while let Some((prio, _id)) = rx.pop_next() {
        popped_count += 1;
        if let Some(prev) = prev_priority {
            assert!(
                prev >= prio,
                "Priority ordering broken: previous {} < current {}",
                prev,
                prio
            );
        }
        prev_priority = Some(prio);
    }

    assert_eq!(popped_count, total_items);
}
