#[macro_export]
macro_rules! generate_looper_tests {
    ($looper_type:path) => {
        #[cfg(test)]
        mod tests {
            use super::*;
            use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};
            use std::thread;
            use std::time::Duration;

            // Variabili statiche atomiche perché i puntatori a funzione `fn(T)` 
            // NON possono catturare l'ambiente (le closure non possono avere stato).
            static PROCESSED_COUNT: AtomicUsize = AtomicUsize::new(0);
            static CLEANUP_CALLED: AtomicBool = AtomicBool::new(false);

            #[test]
            fn test_looper_process_and_cleanup() {
                // Resettiamo lo stato (utile se i test vengono eseguiti più volte)
                PROCESSED_COUNT.store(0, Ordering::SeqCst);
                CLEANUP_CALLED.store(false, Ordering::SeqCst);

                // Creiamo uno scope fittizio in modo da forzare la distruzione (Drop)
                // del Looper alla fine del blocco.
                {
                    // Passiamo il tipo esatto generato dalla macro
                    let looper = <$looper_type>::new(
                        |_msg: i32| {
                            // Simuliamo un'elaborazione che impiega tempo
                            thread::sleep(Duration::from_millis(10));
                            PROCESSED_COUNT.fetch_add(1, Ordering::SeqCst);
                        },
                        || {
                            // Segniamo che il cleanup è stato eseguito
                            CLEANUP_CALLED.store(true, Ordering::SeqCst);
                        }
                    );

                    // Inviamo i messaggi
                    looper.send(10);
                    looper.send(20);
                    looper.send(30);
                    
                } // <-- QUI il looper esce dallo scope. Viene chiamato drop(), 
                  //     che droppa il sender e fa join() sul thread aspettando che finisca!

                // Poiché il Drop fa `join()`, quando il codice arriva a questa riga
                // SIAMO CERTI che il thread ha finito sia di processare che di fare cleanup!
                
                // Verifichiamo che i 3 messaggi siano stati processati correttamente
                assert_eq!(PROCESSED_COUNT.load(Ordering::SeqCst), 3);
                
                // Verifichiamo che la funzione di cleanup sia stata chiamata
                assert_eq!(CLEANUP_CALLED.load(Ordering::SeqCst), true);
            }
        }
    };
}
