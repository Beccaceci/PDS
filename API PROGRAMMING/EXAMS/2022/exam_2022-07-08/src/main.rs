mod shared_tests;
mod limiter_manual;
mod limiter_crossbeam;

use std::time::Instant;
use std::sync::Arc;
use std::thread;

fn main() {
    let limit = 4;
    let executions_per_thread = 50_000;
    let num_threads = 16;
    
    println!("=== SEMAPHORE BENCHMARK: MANUAL vs CROSSBEAM ===");
    println!("Ambiente di stress: {} thread concorrenti", num_threads);
    println!("Ognuno proverà ad eseguire {} volte una funzione vuota.", executions_per_thread);
    println!("Limite di esecuzioni contemporanee: {}\n", limit);

    // ==========================================
    // BENCHMARK 1: Manuale (Mutex + Condvar)
    // ==========================================
    {
        println!("Avvio test MANUALE (Mutex + Condvar)...");
        let start = Instant::now();
        
        let limiter = Arc::new(limiter_manual::ExecutionLimiter::with_capacity(limit));
        let mut handles = vec![];
        
        for _ in 0..num_threads {
            let limiter_clone = Arc::clone(&limiter);
            handles.push(thread::spawn(move || {
                for _ in 0..executions_per_thread {
                    limiter_clone.execute(|| { /* do nothing */ });
                }
            }));
        }
        
        for h in handles { h.join().unwrap(); }
        println!("> Risultato MANUALE:   {:?}", start.elapsed());
    }

    println!("------------------------------------------------");

    // ==========================================
    // BENCHMARK 2: Lock-Free (crossbeam bounded channel)
    // ==========================================
    {
        println!("Avvio test CROSSBEAM (Bounded Channel Semaphore)...");
        let start = Instant::now();
        
        let limiter = Arc::new(limiter_crossbeam::ExecutionLimiter::with_capacity(limit));
        let mut handles = vec![];
        
        for _ in 0..num_threads {
            let limiter_clone = Arc::clone(&limiter);
            handles.push(thread::spawn(move || {
                for _ in 0..executions_per_thread {
                    limiter_clone.execute(|| { /* do nothing */ });
                }
            }));
        }
        
        for h in handles { h.join().unwrap(); }
        println!("> Risultato CROSSBEAM: {:?}", start.elapsed());
    }
}
