mod shared_tests;
mod looper_mpsc;
mod looper_crossbeam;
mod looper_basic;

use std::time::Instant;
use std::sync::Arc;
use std::thread;

fn main() {
    // Riduciamo leggermente i messaggi per evitare che il test manuale impieghi troppo tempo,
    // ma manteniamo uno stress sufficiente per evidenziare le differenze.
    let msg_per_thread = 500_000;
    let num_threads = 8;
    println!("=== TRIPLE BENCHMARK: MPSC vs CROSSBEAM vs MANUAL ===");
    println!("Ambiente di altissimo stress: {} thread concorrenti", num_threads);
    println!("Ognuno invia {} messaggi sulla stessa coda!\n", msg_per_thread);

    // ==========================================
    // BENCHMARK 1: Standard Library (mpsc)
    // ==========================================
    {
        println!("Avvio test MPSC...");
        let start = Instant::now();
        
        let looper = Arc::new(looper_mpsc::Looper::new(|_msg: i32| { }, || {}));
        let mut handles = vec![];
        for _ in 0..num_threads {
            let looper_clone = Arc::clone(&looper);
            handles.push(thread::spawn(move || {
                for i in 0..msg_per_thread { looper_clone.send(i); }
            }));
        }
        for h in handles { h.join().unwrap(); }
        if let Ok(l) = Arc::try_unwrap(looper) { drop(l); }

        println!("> Risultato MPSC:      {:?}", start.elapsed());
    }

    println!("------------------------------------------------");

    // ==========================================
    // BENCHMARK 2: Lock-Free (crossbeam)
    // ==========================================
    {
        println!("Avvio test CROSSBEAM...");
        let start = Instant::now();
        
        let looper = Arc::new(looper_crossbeam::Looper::new(|_msg: i32| { }, || {}));
        let mut handles = vec![];
        for _ in 0..num_threads {
            let looper_clone = Arc::clone(&looper);
            handles.push(thread::spawn(move || {
                for i in 0..msg_per_thread { looper_clone.send(i); }
            }));
        }
        for h in handles { h.join().unwrap(); }
        if let Ok(l) = Arc::try_unwrap(looper) { drop(l); }

        println!("> Risultato CROSSBEAM: {:?}", start.elapsed());
    }

    println!("------------------------------------------------");

    // ==========================================
    // BENCHMARK 3: Manuale (Mutex + Condvar)
    // ==========================================
    {
        println!("Avvio test MANUALE (VecDeque + Mutex)...");
        let start = Instant::now();
        
        let looper = Arc::new(looper_basic::Looper::new(|_msg: i32| { }, || {}));
        let mut handles = vec![];
        for _ in 0..num_threads {
            let looper_clone = Arc::clone(&looper);
            handles.push(thread::spawn(move || {
                for i in 0..msg_per_thread { looper_clone.send(i); }
            }));
        }
        for h in handles { h.join().unwrap(); }
        if let Ok(l) = Arc::try_unwrap(looper) { drop(l); }

        println!("> Risultato MANUALE:   {:?}", start.elapsed());
    }
}
