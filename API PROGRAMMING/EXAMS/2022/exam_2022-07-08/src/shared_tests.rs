#[macro_export]
macro_rules! generate_limiter_tests {
    ($limiter_type:path) => {
        #[cfg(test)]
        mod tests {
            use super::*;
            use std::sync::{Arc, Mutex};
            use std::thread;
            use std::time::Duration;
            use std::sync::atomic::{AtomicUsize, Ordering};

            #[test]
            fn test_limiter_concurrency() {
                let limit = 3;
                let limiter = Arc::new(<$limiter_type>::with_capacity(limit));
                
                let active = Arc::new(AtomicUsize::new(0));
                let max_active = Arc::new(AtomicUsize::new(0));
                let completed = Arc::new(AtomicUsize::new(0));
                
                let mut handles = vec![];
                
                // Lanciamo 10 thread che proveranno tutti a eseguire contemporaneamente.
                // Il limiter dovrebbe bloccarne 7, facendone entrare solo 3 alla volta.
                for _ in 0..10 {
                    let limiter_clone = Arc::clone(&limiter);
                    let active_clone = Arc::clone(&active);
                    let max_active_clone = Arc::clone(&max_active);
                    let completed_clone = Arc::clone(&completed);
                    
                    handles.push(thread::spawn(move || {
                        limiter_clone.execute(move || {
                            // 1. Incrementiamo il numero di thread attivi in questo blocco
                            let current = active_clone.fetch_add(1, Ordering::SeqCst) + 1;
                            
                            // 2. Aggiorniamo il massimo numero di thread attivati contemporaneamente
                            let mut max = max_active_clone.load(Ordering::SeqCst);
                            while current > max {
                                match max_active_clone.compare_exchange_weak(max, current, Ordering::SeqCst, Ordering::SeqCst) {
                                    Ok(_) => break,
                                    Err(x) => max = x,
                                }
                            }
                            
                            // 3. Simuliamo un lavoro lento (50ms) per dare tempo agli altri thread
                            // di schiantarsi contro il muro del limiter.
                            thread::sleep(Duration::from_millis(50));
                            
                            // 4. Lavoro finito, decrementiamo gli attivi e aumentiamo i completati
                            active_clone.fetch_sub(1, Ordering::SeqCst);
                            completed_clone.fetch_add(1, Ordering::SeqCst);
                        });
                    }));
                }
                
                for h in handles {
                    h.join().unwrap();
                }
                
                // Verifichiamo che MAI più di `limit` thread siano entrati contemporaneamente!
                assert_eq!(max_active.load(Ordering::SeqCst), limit, "Il limite di concorrenza è stato violato!");
                
                // Verifichiamo che tutte le funzioni siano state effettivamente eseguite
                assert_eq!(completed.load(Ordering::SeqCst), 10, "Non tutte le funzioni sono state eseguite!");
            }

            #[test]
            fn test_limiter_panic_unwinding() {
                let limiter = Arc::new(<$limiter_type>::with_capacity(1));
                
                // Primo thread: esegue e fa panic
                let limiter_clone = Arc::clone(&limiter);
                let handle = thread::spawn(move || {
                    limiter_clone.execute(|| {
                        panic!("Simulazione di errore fatale!");
                    });
                });
                let _ = handle.join(); // Ignoriamo il panic nel main thread

                // Secondo thread: DEVE poter entrare. Se il RAII Guard non ha funzionato,
                // il contatore è rimasto a 1 e questo blocco si bloccherà per sempre (deadlock).
                let success = limiter.execute(|| {
                    return true;
                });
                
                assert!(success, "Il limiter è rimasto bloccato dopo un panic!");
            }
        }
    };
}
