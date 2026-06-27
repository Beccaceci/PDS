mod shared_tests;
mod dispatcher_mpsc;
mod dispatcher_crossbeam;
mod dispatcher_manual;

use std::time::Instant;

fn main() {
    let msg_count = 1_000_000;
    
    println!("=== PUBSUB BENCHMARK: MPSC vs CROSSBEAM vs MANUAL ===");
    println!("Test: 1 Dispatcher invia {} messaggi a 5 Subscription contemporaneamente.", msg_count);
    
    // ==========================================
    // BENCHMARK 1: Standard Library (mpsc)
    // ==========================================
    {
        println!("\nAvvio test MPSC...");
        let start = Instant::now();
        
        let dispatcher = dispatcher_mpsc::Dispatcher::new();
        let sub1 = dispatcher.subscribe();
        let sub2 = dispatcher.subscribe();
        let sub3 = dispatcher.subscribe();
        let sub4 = dispatcher.subscribe();
        let sub5 = dispatcher.subscribe();
        
        for i in 0..msg_count {
            dispatcher.dispatch(i);
        }
        
        // Svuotiamo le code per essere giusti
        for _ in 0..msg_count {
            sub1.read(); sub2.read(); sub3.read(); sub4.read(); sub5.read();
        }
        
        println!("> Risultato MPSC:      {:?}", start.elapsed());
    }

    // ==========================================
    // BENCHMARK 2: Lock-Free (crossbeam)
    // ==========================================
    {
        println!("\nAvvio test CROSSBEAM...");
        let start = Instant::now();
        
        let dispatcher = dispatcher_crossbeam::Dispatcher::new();
        let sub1 = dispatcher.subscribe();
        let sub2 = dispatcher.subscribe();
        let sub3 = dispatcher.subscribe();
        let sub4 = dispatcher.subscribe();
        let sub5 = dispatcher.subscribe();
        
        for i in 0..msg_count { dispatcher.dispatch(i); }
        for _ in 0..msg_count { sub1.read(); sub2.read(); sub3.read(); sub4.read(); sub5.read(); }
        
        println!("> Risultato CROSSBEAM: {:?}", start.elapsed());
    }

    // ==========================================
    // BENCHMARK 3: Manual (VecDeque + Mutex + Condvar)
    // ==========================================
    {
        println!("\nAvvio test MANUAL...");
        let start = Instant::now();
        
        let dispatcher = dispatcher_manual::Dispatcher::new();
        let sub1 = dispatcher.subscribe();
        let sub2 = dispatcher.subscribe();
        let sub3 = dispatcher.subscribe();
        let sub4 = dispatcher.subscribe();
        let sub5 = dispatcher.subscribe();
        
        for i in 0..msg_count { dispatcher.dispatch(i); }
        for _ in 0..msg_count { sub1.read(); sub2.read(); sub3.read(); sub4.read(); sub5.read(); }
        
        println!("> Risultato MANUAL:    {:?}", start.elapsed());
    }
}
