use simulation_001::*;
use std::thread;
use std::time::Duration;

// =================================================================================
// INDIVIDUAL TEST FUNCTIONS FOR CONDVAR & MPSC IMPLEMENTATIONS
// =================================================================================

macro_rules! generate_event_bus_tests {
    ($mod_name:ident, $factory:expr) => {
        mod $mod_name {
            use super::*;

            fn get_bus<E: Send + Clone + 'static>() -> impl EventBus<E> {
                ($factory)()
            }

            #[test]
            fn test_single_subscriber_single_event() {
                let bus = get_bus::<String>();
                assert_eq!(bus.subscriber_count(), 0);
                let sub = bus.subscribe();
                assert_eq!(bus.subscriber_count(), 1);

                bus.publish("Event1".to_string());
                assert_eq!(sub.next_event(), Some("Event1".to_string()));
            }

            #[test]
            fn test_subscriber_count_raii_invariants() {
                let bus = get_bus::<String>();
                assert_eq!(bus.subscriber_count(), 0);
                let sub1 = bus.subscribe();
                let sub2 = bus.subscribe();
                assert_eq!(bus.subscriber_count(), 2);
                drop(sub1);
                assert_eq!(bus.subscriber_count(), 1);
                drop(sub2);
                assert_eq!(bus.subscriber_count(), 0);
            }

            #[test]
            fn test_events_received_only_after_subscribe() {
                let bus = get_bus::<String>();
                bus.publish("OldEvent".to_string());
                let sub = bus.subscribe();
                bus.publish("NewEvent".to_string());

                assert_eq!(sub.next_event(), Some("NewEvent".to_string()));
            }

            #[test]
            fn test_multiple_subscribers_receive_all_events() {
                let bus = get_bus::<String>();
                let sub1 = bus.subscribe();
                let sub2 = bus.subscribe();

                bus.publish("SharedEvent".to_string());
                assert_eq!(sub1.next_event(), Some("SharedEvent".to_string()));
                assert_eq!(sub2.next_event(), Some("SharedEvent".to_string()));
            }

            #[test]
            fn test_fifo_order_delivery() {
                let bus = get_bus::<String>();
                let sub = bus.subscribe();

                for i in 0..5 {
                    bus.publish(format!("Msg{}", i));
                }

                for i in 0..5 {
                    assert_eq!(sub.next_event(), Some(format!("Msg{}", i)));
                }
            }

            #[test]
            fn test_closing_bus_drains_and_returns_none() {
                let bus = get_bus::<String>();
                let sub = bus.subscribe();
                bus.publish("Msg1".to_string());
                bus.close();

                assert_eq!(sub.next_event(), Some("Msg1".to_string()));
                assert_eq!(sub.next_event(), None);
            }

            #[test]
            fn test_close_unblocks_waiting_subscriber() {
                let bus = get_bus::<String>();
                let sub = bus.subscribe();

                let handle = thread::spawn(move || {
                    sub.next_event()
                });

                thread::sleep(Duration::from_millis(50));
                bus.close();

                assert_eq!(handle.join().unwrap(), None);
            }

            #[test]
            fn test_cloned_bus_shares_state() {
                let bus1 = get_bus::<String>();
                let bus2 = bus1.clone();

                let sub = bus2.subscribe();
                assert_eq!(bus1.subscriber_count(), 1);

                bus1.publish("ClonedBusMsg".to_string());
                assert_eq!(sub.next_event(), Some("ClonedBusMsg".to_string()));
            }

            #[test]
            fn test_early_drop_of_subscription_does_not_affect_others() {
                let bus = get_bus::<String>();
                let sub1 = bus.subscribe();
                let sub2 = bus.subscribe();

                drop(sub1);
                bus.publish("MsgForSub2".to_string());
                assert_eq!(sub2.next_event(), Some("MsgForSub2".to_string()));
            }

            #[test]
            fn test_subscribe_after_close() {
                let bus = get_bus::<String>();
                bus.close();
                let sub = bus.subscribe();
                assert_eq!(sub.next_event(), None);
            }

            #[test]
            fn test_concurrent_stress_25_threads() {
                let bus = get_bus::<usize>();
                let mut handles = vec![];

                for _ in 0..20 {
                    let sub = bus.subscribe();
                    handles.push(thread::spawn(move || {
                        let mut count = 0;
                        while let Some(_) = sub.next_event() {
                            count += 1;
                        }
                        count
                    }));
                }

                let mut pub_handles = vec![];
                for p in 0..5 {
                    let bus_clone = bus.clone();
                    pub_handles.push(thread::spawn(move || {
                        for i in 0..10 {
                            bus_clone.publish(p * 100 + i);
                        }
                    }));
                }

                for h in pub_handles {
                    h.join().unwrap();
                }

                thread::sleep(Duration::from_millis(50));
                bus.close();

                for h in handles {
                    let count = h.join().unwrap();
                    assert_eq!(count, 50);
                }
            }
        }
    };
}

// Generate 11 individual tests for Condvar implementation
generate_event_bus_tests!(condvar_tests, make_condvar_event_bus);

// Generate 11 individual tests for MPSC Channel implementation
generate_event_bus_tests!(mpsc_tests, make_mpsc_event_bus);
