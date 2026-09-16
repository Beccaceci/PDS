use simulation_013::*;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_basic_push_and_pop() {
    let scheduler = make_aging_scheduler::<String>(0.0);

    let _ticket = scheduler.push("document_1".to_string(), 10).expect("Push should succeed");
    let popped = scheduler.pop();

    assert_eq!(popped, Some("document_1".to_string()));
}

#[test]
fn test_base_priority_ordering_without_aging() {
    let scheduler = make_aging_scheduler::<i32>(0.0);

    let _t1 = scheduler.push(10, 10).unwrap();
    let _t2 = scheduler.push(50, 50).unwrap();
    let _t3 = scheduler.push(30, 30).unwrap();

    // Popping without aging should extract items strictly by descending base priority
    assert_eq!(scheduler.pop(), Some(50), "Highest base priority (50) must be popped first");
    assert_eq!(scheduler.pop(), Some(30), "Second highest base priority (30) must be popped second");
    assert_eq!(scheduler.pop(), Some(10), "Lowest base priority (10) must be popped last");
}

#[test]
fn test_equal_base_priority_retrieval() {
    let scheduler = make_aging_scheduler::<&'static str>(0.0);

    let _t1 = scheduler.push("job_A", 100).unwrap();
    let _t2 = scheduler.push("job_B", 100).unwrap();
    let _t3 = scheduler.push("job_C", 100).unwrap();

    let mut results = vec![
        scheduler.pop().unwrap(),
        scheduler.pop().unwrap(),
        scheduler.pop().unwrap(),
    ];
    results.sort();
    assert_eq!(results, vec!["job_A", "job_B", "job_C"]);
}

#[test]
fn test_aging_promotes_older_low_priority_item() {
    // Aging rate: 20 units of priority per second (0.02 units per ms)
    let scheduler = make_aging_scheduler::<&'static str>(20.0);

    // Push item A with low base priority = 10 at t = 0
    let _t_a = scheduler.push("low_priority_old", 10).unwrap();

    // Wait 150ms -> aging adds ~3.0 priority units -> effective priority of A becomes ~13.0
    thread::sleep(Duration::from_millis(150));

    // Push item B with base priority = 11 at t = 150ms -> effective priority of B is 11.0
    let _t_b = scheduler.push("medium_priority_fresh", 11).unwrap();

    // Item A (effective priority ~13.0) must beat Item B (effective priority 11.0)
    let first = scheduler.pop();
    assert_eq!(
        first,
        Some("low_priority_old"),
        "Aging should promote older item with base 10 over fresh item with base 11"
    );

    let second = scheduler.pop();
    assert_eq!(second, Some("medium_priority_fresh"));
}

#[test]
fn test_aging_rate_insufficient_to_overtake_high_priority() {
    // Aging rate: 10 units of priority per second
    let scheduler = make_aging_scheduler::<&'static str>(10.0);

    // Push item A with base priority = 10 at t = 0
    let _t_a = scheduler.push("low_priority", 10).unwrap();

    // Wait 100ms -> aging adds ~1.0 priority unit -> effective priority is ~11.0
    thread::sleep(Duration::from_millis(100));

    // Push item B with much higher base priority = 50 -> effective priority is 50.0
    let _t_b = scheduler.push("high_priority_fresh", 50).unwrap();

    // Item B (50.0) should easily beat Item A (11.0)
    assert_eq!(scheduler.pop(), Some("high_priority_fresh"));
    assert_eq!(scheduler.pop(), Some("low_priority"));
}

#[test]
fn test_boost_immediate_promotion() {
    let scheduler = make_aging_scheduler::<&'static str>(0.0);

    let ticket_a = scheduler.push("doc_A", 10).unwrap();
    let _ticket_b = scheduler.push("doc_B", 20).unwrap();

    // Without boost, doc_B (20) > doc_A (10).
    // Boost doc_A by 25 -> doc_A base priority becomes 10 + 25 = 35 > doc_B (20).
    ticket_a.boost(25);

    assert_eq!(
        scheduler.pop(),
        Some("doc_A"),
        "Boosted item (base 10 + boost 25 = 35) must overtake base 20"
    );
    assert_eq!(scheduler.pop(), Some("doc_B"));
}

#[test]
fn test_boost_cumulative_with_aging() {
    // Aging rate: 5 units per second
    let scheduler = make_aging_scheduler::<&'static str>(5.0);

    let ticket_a = scheduler.push("doc_A", 5).unwrap();
    let _ticket_b = scheduler.push("doc_B", 50).unwrap();

    // Boost doc_A by 100
    ticket_a.boost(100);

    // doc_A effective priority is (5 + 100) + aging > doc_B (50 + aging)
    assert_eq!(scheduler.pop(), Some("doc_A"));
    assert_eq!(scheduler.pop(), Some("doc_B"));
}

#[test]
fn test_cancel_silent_discarding() {
    let scheduler = make_aging_scheduler::<&'static str>(0.0);

    let ticket_a = scheduler.push("doc_A_urgent", 100).unwrap();
    let _ticket_b = scheduler.push("doc_B_normal", 20).unwrap();

    // Cancel the high-priority item
    let canceled = ticket_a.cancel();
    assert!(canceled, "cancel() on pending item must return true");

    // doc_A must be silently discarded and doc_B returned
    assert_eq!(scheduler.pop(), Some("doc_B_normal"));
}

#[test]
fn test_cancel_already_popped_returns_false() {
    let scheduler = make_aging_scheduler::<&'static str>(0.0);

    let ticket_a = scheduler.push("doc_A", 50).unwrap();
    let popped = scheduler.pop();
    assert_eq!(popped, Some("doc_A"));

    let canceled = ticket_a.cancel();
    assert!(
        !canceled,
        "cancel() after item was already popped must return false"
    );
}

#[test]
fn test_cancel_idempotence() {
    let scheduler = make_aging_scheduler::<i32>(0.0);

    let ticket = scheduler.push(42, 10).unwrap();
    assert!(ticket.cancel(), "First cancel() must return true");
    assert!(!ticket.cancel(), "Second cancel() must return false");
}

#[test]
fn test_boost_on_canceled_or_popped_item_is_noop() {
    let scheduler = make_aging_scheduler::<&'static str>(0.0);

    let ticket_a = scheduler.push("doc_A", 10).unwrap();
    assert!(ticket_a.cancel());

    // Boost on canceled item should have no effect and not resurrect it
    ticket_a.boost(1000);

    let _ticket_b = scheduler.push("doc_B", 5).unwrap();
    assert_eq!(
        scheduler.pop(),
        Some("doc_B"),
        "Canceled item must never be returned even after boost"
    );
}

#[test]
fn test_blocking_pop_on_empty_queue() {
    let scheduler = make_aging_scheduler::<i32>(0.0);
    let s_clone = scheduler.clone();

    let start = Instant::now();
    let consumer = thread::spawn(move || {
        s_clone.pop()
    });

    // Ensure consumer has started and is waiting in pop()
    thread::sleep(Duration::from_millis(50));

    // Producer pushes item after delay
    scheduler.push(777, 1).unwrap();

    let result = consumer.join().expect("Consumer thread must join cleanly");
    let elapsed = start.elapsed();

    assert_eq!(result, Some(777));
    assert!(
        elapsed >= Duration::from_millis(45),
        "pop() must have blocked until producer pushed the item"
    );
}

#[test]
fn test_blocking_pop_when_all_items_canceled() {
    let scheduler = make_aging_scheduler::<i32>(0.0);

    let ticket = scheduler.push(100, 50).unwrap();
    assert!(ticket.cancel());

    let s_clone = scheduler.clone();
    let consumer = thread::spawn(move || {
        s_clone.pop()
    });

    // Consumer should be blocked because the only item in queue was canceled
    thread::sleep(Duration::from_millis(50));

    // Push a new valid item
    scheduler.push(200, 10).unwrap();

    let result = consumer.join().expect("Consumer thread must join cleanly");
    assert_eq!(result, Some(200));
}

#[test]
fn test_close_behavior_and_drain() {
    let scheduler = make_aging_scheduler::<i32>(0.0);

    scheduler.push(10, 10).unwrap();
    scheduler.push(20, 20).unwrap();

    // Close the scheduler
    scheduler.close();

    // Subsequent push must fail
    assert!(scheduler.push(30, 30).is_none(), "Push after close must return None");

    // Pop should drain existing elements
    assert_eq!(scheduler.pop(), Some(20));
    assert_eq!(scheduler.pop(), Some(10));

    // After draining, pop returns None
    assert_eq!(scheduler.pop(), None, "Pop on drained closed queue must return None");
}

#[test]
fn test_close_wakes_blocked_consumers() {
    let scheduler = make_aging_scheduler::<i32>(0.0);
    let s_clone = scheduler.clone();

    let consumer = thread::spawn(move || {
        s_clone.pop()
    });

    thread::sleep(Duration::from_millis(50));
    scheduler.close();

    let result = consumer.join().unwrap();
    assert_eq!(result, None, "Blocked consumer must wake up and return None when closed");
}

#[test]
fn test_multithreaded_producer_consumer() {
    let scheduler = make_aging_scheduler::<usize>(10.0);
    let num_producers = 4;
    let items_per_producer = 50;
    let total_items = num_producers * items_per_producer;

    let mut producer_handles = Vec::new();
    for p_id in 0..num_producers {
        let s = scheduler.clone();
        let handle = thread::spawn(move || {
            for i in 0..items_per_producer {
                let val = p_id * items_per_producer + i;
                let priority = (val % 10) as u32;
                s.push(val, priority).unwrap();
                if i % 10 == 0 {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        });
        producer_handles.push(handle);
    }

    let num_consumers = 4;
    let items_per_consumer = total_items / num_consumers;
    let consumed_items = Arc::new(Mutex::new(Vec::new()));

    let mut consumer_handles = Vec::new();
    for _ in 0..num_consumers {
        let s = scheduler.clone();
        let c_items = Arc::clone(&consumed_items);
        let handle = thread::spawn(move || {
            for _ in 0..items_per_consumer {
                let val = s.pop().unwrap();
                c_items.lock().unwrap().push(val);
            }
        });
        consumer_handles.push(handle);
    }

    for h in producer_handles {
        h.join().unwrap();
    }
    for h in consumer_handles {
        h.join().unwrap();
    }

    let items = consumed_items.lock().unwrap();
    assert_eq!(items.len(), total_items);

    let unique: HashSet<_> = items.iter().cloned().collect();
    assert_eq!(unique.len(), total_items, "No items lost or duplicated");
}

#[test]
fn test_multithreaded_concurrent_cancellations_and_boosts() {
    let scheduler = Arc::new(make_aging_scheduler::<usize>(5.0));
    let total_items = 100;
    let mut tickets = Vec::new();

    for i in 0..total_items {
        let ticket = scheduler.push(i, (i % 20) as u32).unwrap();
        tickets.push((i, Arc::new(ticket)));
    }

    let canceled_set = Arc::new(Mutex::new(HashSet::new()));

    // Worker that cancels all even elements
    let tickets_cancel = tickets.clone();
    let c_set = Arc::clone(&canceled_set);
    let cancel_handle = thread::spawn(move || {
        for (val, t) in tickets_cancel {
            if val % 2 == 0 && t.cancel() {
                c_set.lock().unwrap().insert(val);
            }
        }
    });

    // Worker that boosts odd elements
    let tickets_boost = tickets.clone();
    let boost_handle = thread::spawn(move || {
        for (val, t) in tickets_boost {
            if val % 2 != 0 {
                t.boost(50);
            }
        }
    });

    cancel_handle.join().unwrap();
    boost_handle.join().unwrap();

    let canceled = canceled_set.lock().unwrap().clone();
    let expected_popped = total_items - canceled.len();

    let mut popped_items = Vec::new();
    for _ in 0..expected_popped {
        popped_items.push(scheduler.pop().unwrap());
    }

    for val in &popped_items {
        assert!(
            !canceled.contains(val),
            "Popped item {} was canceled and should not have been returned",
            val
        );
    }
    assert_eq!(popped_items.len(), expected_popped);
}

#[test]
fn test_scheduler_and_ticket_send_sync() {
    let scheduler = make_aging_scheduler::<i32>(1.0);
    assert_send(&scheduler);
    assert_sync(&scheduler);

    let ticket = scheduler.push(1, 10).unwrap();
    assert_send(&ticket);
    assert_sync(&ticket);

    let t_handle = thread::spawn(move || {
        ticket.boost(5);
        assert!(ticket.cancel());
    });
    t_handle.join().unwrap();
}
