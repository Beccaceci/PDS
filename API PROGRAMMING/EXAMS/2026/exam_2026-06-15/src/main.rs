pub mod shared_tests;
pub mod forgettable_basic;
pub mod forgettable_mpsc;

use std::thread;
use std::time::Instant;

macro_rules! run_benchmark {
    ($name:expr, $factory:path, $module:ident) => {{
        use crate::$module::{ForgettableSender, ForgettableReceiver, Forgettable};
        
        println!("==================================================");
        println!("🏁 INIZIO BENCHMARK: {}", $name);
        println!("==================================================");
        
        let (tx, rx) = $factory();
        let start = Instant::now();
        
        // Configuriamo uno scenario molto stressante:
        // 4 thread Sender che sparano 250.000 messaggi ciascuno (Totale: 1.000.000 messaggi)
        // Ognuno tenta di annullare il 10% dei propri messaggi subito dopo averli inviati.
        let num_senders = 4;
        let msgs_per_sender = 250_000;
        
        let mut handles = vec![];
        
        for _ in 0..num_senders {
            let tx_clone = tx.clone();
            handles.push(thread::spawn(move || {
                let mut forget_handles = Vec::new();
                
                for i in 0..msgs_per_sender {
                    if let Some(h) = tx_clone.send(i) {
                        // Conserviamo l'handle di 1 messaggio su 10 per provare ad annullarlo
                        if i % 10 == 0 {
                            forget_handles.push(h);
                        }
                    }
                }
                
                // Tentiamo di annullarli.
                // Molti saranno già stati processati, altri riusciremo a fermarli in tempo.
                let mut forgotten = 0;
                for h in forget_handles {
                    if h.forget() {
                        forgotten += 1;
                    }
                }
                // Ritorniamo anche tx_clone in modo da NON farlo distruggere alla fine del thread!
                // Altrimenti, a causa dell'implementazione "base" di MyChannel, distruggere un clone
                // chiuderebbe l'intero canale per tutti gli altri thread.
                (forgotten, tx_clone)
            }));
        }
        
        // Thread Ricevitore super veloce che divora messaggi
        let rx_handle = thread::spawn(move || {
            let mut received = 0;
            while let Some(_) = rx.recv() {
                received += 1;
            }
            received
        });
        
        // Aspettiamo che tutti i sender finiscano e raccogliamo i loro tx_clones
        let mut total_forgotten = 0;
        let mut kept_txs = Vec::new();
        for h in handles {
            let (forgotten, tx_clone) = h.join().unwrap();
            total_forgotten += forgotten;
            kept_txs.push(tx_clone);
        }
        
        // ORA CHE TUTTI I MESSAGGI SONO STATI INVIATI, possiamo chiudere il canale!
        // Facciamo drop di tutti i mittenti tenuti in vita e di quello originale del main.
        drop(kept_txs);
        drop(tx);
        
        // Aspettiamo che il receiver finisca di leggere
        let total_received = rx_handle.join().unwrap();
        let elapsed = start.elapsed();
        
        println!("⏱️  Tempo impiegato: {:?}", elapsed);
        println!("📨 Messaggi effettivamente ricevuti: {}", total_received);
        println!("🛑 Messaggi annullati con successo (prima della ricezione): {}", total_forgotten);
        
        let total_processed = total_received + total_forgotten;
        let expected = num_senders * msgs_per_sender;
        
        println!("📊 Totale messaggi elaborati: {} / {} (Dovrebbe essere identico!)", total_processed, expected);
        assert_eq!(total_processed, expected, "⚠️ ATTENZIONE: Perdita di messaggi rilevata!");
        println!("\n");
    }};
}

fn main() {
    println!("Inizio la campagna di Stress Test...\n");

    // Benchmark 1: L'implementazione Basic (Mutex + VecDeque + Condvar)
    run_benchmark!(
        "BASIC IMPLEMENTATION (Mutex)", 
        crate::forgettable_basic::forgettable_channel, 
        forgettable_basic
    );

    // Benchmark 2: L'implementazione Avanzata (mpsc::channel + AtomicBool)
    run_benchmark!(
        "MPSC IMPLEMENTATION (AtomicBool)", 
        crate::forgettable_mpsc::forgettable_channel, 
        forgettable_mpsc
    );
    
    println!("Tutti i benchmark sono stati completati con successo!");
}
