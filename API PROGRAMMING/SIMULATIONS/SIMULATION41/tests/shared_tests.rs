use simulation_041::*;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::Duration;

fn assert_send_sync<T: Send + Sync>(_val: &T) {}

#[test]
fn test_trait_bounds() {
    let client = make_rpc_client::<String, String>();
    assert_send_sync(&client);
}

#[test]
fn test_single_request_response() {
    let client = make_rpc_client::<String, String>();

    let req = client.send("ping".to_string(), {
        let client_clone = client.clone();
        move |id, val| {
            thread::spawn(move || {
                client_clone.deliver_response(id, format!("{}_pong", val));
            });
        }
    });

    let resp = req.wait_response();
    assert_eq!(resp, Some("ping_pong".to_string()));
}

#[test]
fn test_multiple_out_of_order_responses() {
    let client = make_rpc_client::<u32, u32>();
    let (tx, rx) = mpsc::channel::<(u64, u32)>();

    let req1 = client.send(10, {
        let tx = tx.clone();
        move |id, val| {
            tx.send((id, val)).unwrap();
        }
    });

    let req2 = client.send(20, {
        let tx = tx.clone();
        move |id, val| {
            tx.send((id, val)).unwrap();
        }
    });

    let (id1, val1) = rx.recv().unwrap();
    let (id2, val2) = rx.recv().unwrap();

    // Deliver responses in reverse order
    client.deliver_response(id2, val2 * 2);
    client.deliver_response(id1, val1 * 2);

    assert_eq!(req1.wait_response(), Some(20));
    assert_eq!(req2.wait_response(), Some(40));
}

#[test]
fn test_send_transport_executed_outside_of_lock() {
    let client = make_rpc_client::<u32, u32>();
    let in_transport = Arc::new(AtomicBool::new(false));
    let in_transport_clone = Arc::clone(&in_transport);

    let client_clone = client.clone();
    let handle = thread::spawn(move || {
        client_clone.send(1, move |_id, _val| {
            in_transport_clone.store(true, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(60));
            in_transport_clone.store(false, Ordering::SeqCst);
        });
    });

    thread::sleep(Duration::from_millis(15));
    assert!(in_transport.load(Ordering::SeqCst));

    // A concurrent deliver_response or send should NOT be blocked by the slow transport closure
    let start = std::time::Instant::now();
    client.deliver_response(9999, 0); // Non-existent ID must return instantly
    let elapsed = start.elapsed();

    assert!(elapsed < Duration::from_millis(30), "deliver_response blocked on transport lock");
    handle.join().unwrap();
}

#[test]
fn test_abandoned_pending_request_cleans_up_on_drop() {
    let client = make_rpc_client::<String, String>();
    let generated_id = Arc::new(AtomicU64::new(0));
    let gen_id_clone = Arc::clone(&generated_id);

    {
        let _req = client.send("hello".to_string(), move |id, _| {
            gen_id_clone.store(id, Ordering::SeqCst);
        });
        // _req goes out of scope here without wait_response being called
    }

    let id = generated_id.load(Ordering::SeqCst);
    assert!(id > 0);

    // Delivering a response for a dropped handle must not panic or cause deadlocks
    client.deliver_response(id, "response_to_nowhere".to_string());
}

#[test]
fn test_close_unblocks_all_pending_requests_with_none() {
    let client = make_rpc_client::<usize, usize>();
    let mut handles = Vec::new();
    let barrier = Arc::new(Barrier::new(4));

    for i in 0..3 {
        let client_clone = client.clone();
        let b = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let req = client_clone.send(i, |_id, _req| {});
            b.wait();
            req.wait_response()
        }));
    }

    barrier.wait();
    thread::sleep(Duration::from_millis(30));

    // Close the client while 3 threads are waiting for responses
    client.close();

    for h in handles {
        let resp = h.join().unwrap();
        assert_eq!(resp, None, "Expected None after client close");
    }

    // New sends after close should immediately return None when awaited
    let req_after = client.send(100, |_id, _req| {});
    assert_eq!(req_after.wait_response(), None);
}

#[test]
fn test_high_concurrency_stress_interleaved_deliveries_and_drops() {
    let client = make_rpc_client::<usize, usize>();
    let counter = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for i in 0..8 {
        let client_clone = client.clone();
        let counter_clone = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            for j in 0..50 {
                let val = i * 100 + j;
                let c_inner = client_clone.clone();
                let req = client_clone.send(val, move |id, req_val| {
                    if req_val % 2 == 0 {
                        // Deliver response from another thread
                        let c_delivery = c_inner.clone();
                        thread::spawn(move || {
                            c_delivery.deliver_response(id, req_val * 10);
                        });
                    }
                });

                if val % 2 == 0 {
                    if let Some(r) = req.wait_response() {
                        assert_eq!(r, val * 10);
                        counter_clone.fetch_add(1, Ordering::SeqCst);
                    }
                } else {
                    // Silently drop odd requests to exercise RAII cancellation races
                    drop(req);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 8 * 25);
}