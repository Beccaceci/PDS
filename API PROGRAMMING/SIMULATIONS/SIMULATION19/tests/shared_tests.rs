use simulation_019::*;
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

macro_rules! test_broadcast_suite {
    ($mod_name:ident, $factory:path) => {
        mod $mod_name {
            use super::*;

            #[test]
            fn test_single_subscriber_iterator_next_and_close() {
                let b = $factory();
                let mut sub = b.subscribe();

                b.publish("msg_1".to_string());
                b.publish("msg_2".to_string());
                b.close();

                assert_eq!(sub.next(), Some("msg_1".to_string()));
                assert_eq!(sub.next(), Some("msg_2".to_string()));
                assert_eq!(sub.next(), None);
            }

            #[test]
            fn test_multiple_subscribers_each_receive_full_stream() {
                let b = $factory();
                let mut sub1 = b.subscribe();
                let mut sub2 = b.subscribe();

                b.publish(10);
                b.publish(20);
                b.close();

                assert_eq!(sub1.next(), Some(10));
                assert_eq!(sub1.next(), Some(20));
                assert_eq!(sub1.next(), None);

                assert_eq!(sub2.next(), Some(10));
                assert_eq!(sub2.next(), Some(20));
                assert_eq!(sub2.next(), None);
            }

            #[test]
            fn test_late_subscriber_receives_only_post_subscription_events() {
                let b = $factory();

                b.publish("old_event".to_string());

                let mut late_sub = b.subscribe();

                b.publish("new_event_1".to_string());
                b.publish("new_event_2".to_string());
                b.close();

                assert_eq!(late_sub.next(), Some("new_event_1".to_string()));
                assert_eq!(late_sub.next(), Some("new_event_2".to_string()));
                assert_eq!(late_sub.next(), None);
            }

            #[test]
            fn test_subscribe_after_close_returns_none_immediately() {
                let b = $factory();

                b.publish("msg_before_close".to_string());
                b.close();

                // Sottoscrizione successiva a close(): deve terminare immediatamente con None
                let mut future_sub = b.subscribe();
                assert_eq!(
                    future_sub.next(),
                    None,
                    "Subscriber created after close() must return None immediately without blocking!"
                );

                // Eventuali publish successive devono essere ignorate
                b.publish("msg_after_close".to_string());
                assert_eq!(future_sub.next(), None);
            }

            #[test]
            fn test_for_loop_ergonomics() {
                let b = $factory();
                let sub = b.subscribe();

                let producer = thread::spawn({
                    let b = b.clone();
                    move || {
                        for i in 1..=5 {
                            thread::sleep(Duration::from_millis(5));
                            b.publish(i);
                        }
                        b.close();
                    }
                });

                let mut collected = Vec::new();
                for val in sub {
                    collected.push(val);
                }

                producer.join().unwrap();
                assert_eq!(collected, vec![1, 2, 3, 4, 5]);
            }

            #[test]
            fn test_concurrent_producers_and_subscribers() {
                let b = $factory();
                const NUM_SUBSCRIBERS: usize = 4;
                const NUM_MESSAGES: usize = 30;

                let mut sub_handles = Vec::new();
                for _ in 0..NUM_SUBSCRIBERS {
                    let sub = b.subscribe();
                    sub_handles.push(thread::spawn(move || {
                        let mut count = 0;
                        for _ in sub {
                            count += 1;
                        }
                        count
                    }));
                }

                for i in 0..NUM_MESSAGES {
                    b.publish(i);
                }
                b.close();

                for h in sub_handles {
                    let count = h.join().unwrap();
                    assert_eq!(count, NUM_MESSAGES);
                }
            }

            #[test]
            fn test_send_sync_bounds() {
                let b = $factory();
                b.publish(42i32);
                assert_send(&b);
                assert_sync(&b);
                let sub = b.subscribe();
                assert_send(&sub);
            }
        }
    };
}

// 1. Suite di test per l'implementazione basata su MPSC
test_broadcast_suite!(mpsc_suite, make_mpsc_broadcast);

// 2. Suite di test per l'implementazione basata su Log Condiviso + Cursore
test_broadcast_suite!(shared_log_suite, make_shared_log_broadcast);

// 3. Suite di test per l'implementazione basata su Due Livelli di Mutex
test_broadcast_suite!(two_level_mutex_suite, make_two_level_mutex_broadcast);
