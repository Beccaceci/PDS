use simulation_005::*;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_basic_fifo_operations() {
    let queue = make_bounded_queue::<i32>(5);

    assert!(queue.push(10));
    assert!(queue.push(20));
    assert!(queue.push(30));

    assert_eq!(queue.pop(), Some(10));
    assert_eq!(queue.pop(), Some(20));

    assert!(queue.push(40));

    assert_eq!(queue.pop(), Some(30));
    assert_eq!(queue.pop(), Some(40));
}

#[test]
fn test_producer_blocking_on_full_queue() {
    let queue = make_bounded_queue::<i32>(2);

    assert!(queue.push(1));
    assert!(queue.push(2));

    let pushed_flag = Arc::new(AtomicBool::new(false));
    let pushed_flag_clone = Arc::clone(&pushed_flag);
    let q_clone = queue.clone();

    let handle = thread::spawn(move || {
        let res = q_clone.push(3);
        pushed_flag_clone.store(true, Ordering::SeqCst);
        res
    });

    // Short sleep to ensure background thread tries to push and gets blocked
    thread::sleep(Duration::from_millis(50));
    assert!(!pushed_flag.load(Ordering::SeqCst), "Producer should be blocked when queue is full");

    // Main thread pops one item, freeing space
    assert_eq!(queue.pop(), Some(1));

    // The producer thread should now unblock
    let push_result = handle.join().expect("Thread should not panic");
    assert!(push_result, "Push should return true after freeing space");
    assert!(pushed_flag.load(Ordering::SeqCst));

    assert_eq!(queue.pop(), Some(2));
    assert_eq!(queue.pop(), Some(3));
}

#[test]
fn test_consumer_blocking_on_empty_queue() {
    let queue = make_bounded_queue::<i32>(5);

    let received = Arc::new(AtomicBool::new(false));
    let received_clone = Arc::clone(&received);
    let q_clone = queue.clone();

    let handle = thread::spawn(move || {
        let val = q_clone.pop();
        received_clone.store(true, Ordering::SeqCst);
        val
    });

    // Short sleep to ensure background thread tries to pop and gets blocked
    thread::sleep(Duration::from_millis(50));
    assert!(!received.load(Ordering::SeqCst), "Consumer should be blocked when queue is empty");

    // Push item from main thread
    assert!(queue.push(42));

    // Consumer should unblock and receive value
    let popped_val = handle.join().expect("Thread should not panic");
    assert_eq!(popped_val, Some(42));
    assert!(received.load(Ordering::SeqCst));
}

#[test]
fn test_close_empty_queue() {
    let queue = make_bounded_queue::<i32>(5);
    queue.close();

    assert!(!queue.push(1), "Push on closed queue should return false");
    assert_eq!(queue.pop(), None, "Pop on closed empty queue should return None");
}

#[test]
fn test_close_drains_remaining_items() {
    let queue = make_bounded_queue::<String>(5);

    assert!(queue.push("first".to_string()));
    assert!(queue.push("second".to_string()));

    queue.close();

    assert!(!queue.push("third".to_string()), "Push after close should return false");

    assert_eq!(queue.pop(), Some("first".to_string()));
    assert_eq!(queue.pop(), Some("second".to_string()));
    assert_eq!(queue.pop(), None, "Pop after draining closed queue should return None");
}

#[test]
fn test_close_unblocks_blocked_producers() {
    let queue = make_bounded_queue::<i32>(1);
    assert!(queue.push(100)); // Queue is now full

    let mut handles = Vec::new();
    for i in 0..3 {
        let q_clone = queue.clone();
        handles.push(thread::spawn(move || q_clone.push(200 + i)));
    }

    // Wait to ensure all producers are blocked
    thread::sleep(Duration::from_millis(50));

    // Close the queue
    queue.close();

    // All producers must wake up and return false
    for handle in handles {
        let res = handle.join().expect("Producer thread panicked");
        assert!(!res, "Blocked producer should return false upon close");
    }

    // Existing item can still be popped
    assert_eq!(queue.pop(), Some(100));
    assert_eq!(queue.pop(), None);
}

#[test]
fn test_close_unblocks_blocked_consumers() {
    let queue = make_bounded_queue::<i32>(5);

    let mut handles = Vec::new();
    for _ in 0..3 {
        let q_clone = queue.clone();
        handles.push(thread::spawn(move || q_clone.pop()));
    }

    // Wait to ensure all consumers are blocked
    thread::sleep(Duration::from_millis(50));

    // Close the queue
    queue.close();

    // All consumers must wake up and return None
    for handle in handles {
        let res = handle.join().expect("Consumer thread panicked");
        assert_eq!(res, None, "Blocked consumer should receive None upon close");
    }
}

#[test]
fn test_capacity_one_edge_case() {
    let queue = make_bounded_queue::<usize>(1);

    assert!(queue.push(1));

    let q_clone = queue.clone();
    let producer = thread::spawn(move || {
        assert!(q_clone.push(2));
    });

    thread::sleep(Duration::from_millis(30));
    assert_eq!(queue.pop(), Some(1));
    producer.join().expect("Producer failed");

    assert_eq!(queue.pop(), Some(2));
}

#[test]
fn test_mpmc_concurrent_stress() {
    const NUM_PRODUCERS: usize = 4;
    const NUM_CONSUMERS: usize = 4;
    const ITEMS_PER_PRODUCER: usize = 250;
    const CAPACITY: usize = 10;

    let queue = make_bounded_queue::<usize>(CAPACITY);
    let mut producer_handles = Vec::new();

    for p in 0..NUM_PRODUCERS {
        let q_clone = queue.clone();
        producer_handles.push(thread::spawn(move || {
            for i in 0..ITEMS_PER_PRODUCER {
                let item = p * ITEMS_PER_PRODUCER + i;
                assert!(q_clone.push(item), "Push failed unexpectedly");
            }
        }));
    }

    let mut consumer_handles = Vec::new();
    let total_consumed = Arc::new(AtomicUsize::new(0));

    for _ in 0..NUM_CONSUMERS {
        let q_clone = queue.clone();
        let total_counter = Arc::clone(&total_consumed);
        consumer_handles.push(thread::spawn(move || {
            let mut items = Vec::new();
            while let Some(val) = q_clone.pop() {
                items.push(val);
                total_counter.fetch_add(1, Ordering::SeqCst);
            }
            items
        }));
    }

    // Wait for all producers to finish pushing
    for handle in producer_handles {
        handle.join().expect("Producer panicked");
    }

    // Close the queue so consumers break their pop loop on empty
    queue.close();

    let mut all_received = Vec::new();
    for handle in consumer_handles {
        let items = handle.join().expect("Consumer panicked");
        all_received.extend(items);
    }

    let expected_total = NUM_PRODUCERS * ITEMS_PER_PRODUCER;
    assert_eq!(all_received.len(), expected_total);
    assert_eq!(total_consumed.load(Ordering::SeqCst), expected_total);

    let set: HashSet<usize> = all_received.into_iter().collect();
    assert_eq!(set.len(), expected_total);
    for i in 0..expected_total {
        assert!(set.contains(&i), "Missing item {}", i);
    }
}
