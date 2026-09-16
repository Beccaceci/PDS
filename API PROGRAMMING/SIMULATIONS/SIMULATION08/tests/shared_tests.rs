use simulation_008::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn test_basic_delay_queue_push_and_pop() {
    let queue = make_delay_queue::<String>();

    let start = Instant::now();
    queue.push_after("Hello Delayed".to_string(), Duration::from_millis(100));

    let val = queue.pop();
    let elapsed = start.elapsed();

    assert_eq!(val, "Hello Delayed");
    assert!(
        elapsed >= Duration::from_millis(90),
        "pop() must block until delay expires (elapsed: {:?})",
        elapsed
    );
}

#[test]
fn test_earliest_deadline_popped_first() {
    let queue = make_delay_queue::<i32>();

    // Push item with longer delay first (200ms)
    queue.push_after(200, Duration::from_millis(200));

    // Push item with shorter delay second (50ms)
    queue.push_after(50, Duration::from_millis(50));

    let start = Instant::now();
    let first = queue.pop();
    let first_elapsed = start.elapsed();

    assert_eq!(first, 50, "Item with shorter delay must be popped first");
    assert!(first_elapsed < Duration::from_millis(150));

    let second = queue.pop();
    assert_eq!(second, 200);
}

#[test]
fn test_cancel_scheduled_item_success() {
    let queue = make_delay_queue::<i32>();

    let item1 = queue.push_after(10, Duration::from_millis(100));
    let _item2 = queue.push_after(20, Duration::from_millis(200));

    // Cancel item1 before it expires
    let canceled = item1.cancel();
    assert!(canceled, "cancel() on pending item must return true");

    let val = queue.pop();
    // Since item1 was canceled, pop() should return item2
    assert_eq!(val, 20);
}

#[test]
fn test_cancel_after_popped_returns_false() {
    let queue = make_delay_queue::<i32>();

    let item = queue.push_after(42, Duration::from_millis(20));

    thread::sleep(Duration::from_millis(50));
    let val = queue.pop();
    assert_eq!(val, 42);

    let canceled = item.cancel();
    assert!(
        !canceled,
        "cancel() after item was already popped must return false"
    );
}

#[test]
fn test_dynamic_deadline_adjustment() {
    let queue = Arc::new(make_delay_queue::<i32>());

    // Producer 1 inserts item with long delay (300ms)
    queue.push_after(300, Duration::from_millis(300));

    let q_clone = Arc::clone(&queue);
    let start = Instant::now();

    let consumer_handle = thread::spawn(move || {
        q_clone.pop()
    });

    // Wait a bit to ensure consumer is blocked in pop() waiting for the 300ms item
    thread::sleep(Duration::from_millis(30));

    // Producer 2 inserts item with shorter delay (60ms)
    queue.push_after(60, Duration::from_millis(60));

    let result = consumer_handle.join().unwrap();
    let elapsed = start.elapsed();

    assert_eq!(
        result, 60,
        "Consumer must receive newly inserted earlier deadline (60ms)"
    );
    assert!(
        elapsed < Duration::from_millis(200),
        "Consumer must dynamically adjust wait time and not wait the full 300ms (elapsed: {:?})",
        elapsed
    );
}

#[test]
fn test_multi_producer_multi_consumer_stress() {
    let queue = Arc::new(make_delay_queue::<usize>());
    const PRODUCERS: usize = 8;
    const CONSUMERS: usize = 4;
    const ITEMS_PER_PRODUCER: usize = 20;

    let total_popped = Arc::new(AtomicUsize::new(0));

    let mut producer_handles = vec![];
    for p in 0..PRODUCERS {
        let q = Arc::clone(&queue);
        producer_handles.push(thread::spawn(move || {
            for i in 0..ITEMS_PER_PRODUCER {
                let delay = Duration::from_millis((i % 5 * 10) as u64 + 5);
                let item = q.push_after(p * 100 + i, delay);
                if i % 4 == 0 {
                    // Cancel ~25% of items
                    let _ = item.cancel();
                }
            }
        }));
    }

    let mut consumer_handles = vec![];
    for _ in 0..CONSUMERS {
        let q = Arc::clone(&queue);
        let tp = Arc::clone(&total_popped);
        consumer_handles.push(thread::spawn(move || {
            for _ in 0..(PRODUCERS * ITEMS_PER_PRODUCER * 3 / 4 / CONSUMERS) {
                let _val = q.pop();
                tp.fetch_add(1, Ordering::SeqCst);
            }
        }));
    }

    for h in producer_handles {
        h.join().unwrap();
    }

    for h in consumer_handles {
        h.join().unwrap();
    }

    assert!(total_popped.load(Ordering::SeqCst) > 0);
}
