use std::{any::Any, collections::VecDeque, sync::{Arc, Condvar, Mutex}};

/// La struttura SharedState contiene i dati condivisi tra produttore e consumatori.
/// Viene protetta da un Mutex per garantire l'accesso in mutua esclusione.
struct SharedState<T> {
    /// Coda FIFO per memorizzare i valori. `VecDeque` e' ottimizzata per
    /// operazioni veloci sia in testa (pop_front) che in coda (push_back).
    data: VecDeque<T>,
    
    /// Se si verifica un errore tramite `fail()`, viene memorizzato qui.
    /// Usiamo `Option` perche' di default non c'e' errore (`None`).
    /// Quando si verifica, diventa `Some(error)`.
    err: Option<Box<dyn Any + Send>>,
    
    /// Flag booleano per indicare se il buffer ha terminato le operazioni
    /// in modo pulito tramite `terminate()` o a seguito del consumo di un errore.
    closed: bool
}

impl<T: Send> SharedState<T> {
    pub fn new() -> Self {
        Self {
            data: VecDeque::new(),
            err: None,
            closed: false
        }
    }

    pub fn with_one_value(val: T) -> Self {
        let mut q = VecDeque::new();
        q.push_back(val);

        Self {
            data: q,
            err: None,
            closed: false
        }
    }
}

/// Implementazione di un Buffer concorrente (pattern Produttore-Consumatore).
/// Per permettere a piu' thread di possedere un riferimento al buffer e ai suoi
/// dati interni, utilizziamo `Arc` (Atomic Reference Counting) per racchiudere
/// insieme il Mutex (che protegge lo stato) e la Condvar (che gestisce le attese).
pub struct Buffer<T> {
    data: Arc<(Mutex<SharedState<T>>, Condvar)>
}

// Implementazione manuale di Clone in modo che ogni thread possa avere una
// "copia" del puntatore intelligente (Arc) allo stesso buffer fisico in memoria.
impl<T: Send> Clone for Buffer<T> {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data)
        }
    }
}

impl<T: Send> Buffer<T> {
    pub fn new() -> Self {
        let tupla = (Mutex::new(SharedState::new()), Condvar::new());
        Self {
            data: Arc::new(tupla)
        }
    }

    /// Il produttore invoca `next` per aggiungere valori in coda al buffer (FIFO).
    pub fn next(&self, value: T) where T: Send {
        // 1. Acquisizione del lock per accedere in esclusiva allo stato
        let mut state = self.data.0.lock().unwrap();
        
        // 2. Regole di accettazione: se c'e' gia' un errore o il buffer e' chiuso,
        // la specifica richiede che l'operazione fallisca lanciando un'eccezione (panic in Rust).
        if state.err.is_some() {
            panic!("An error occurs");
        }
        else if state.closed == true {
            panic!("The buffer is closed");
        }
        else {
            let cvar = &self.data.1;
            
            // 3. Aggiunta del valore alla fine della coda
            state.data.push_back(value);
            
            // 4. Rilascio manuale del lock prima di inviare la notifica
            // (Best practice per evitare che il thread risvegliato si blocchi subito sul Mutex)
            drop(state);
            
            // 5. Notifica di *un solo* consumatore in attesa, dato che
            // abbiamo appena inserito un solo elemento da consumare.
            cvar.notify_one();
        }
    }

    /// Il produttore notifica che non inviera' piu' alcun valore.
    pub fn terminate(&self) {
        let mut state = self.data.0.lock().unwrap();
        state.closed = true;
        drop(state);
        
        let cvar = &self.data.1;
        // CRITICO: Svegliamo TUTTI i consumatori in attesa.
        // Essendo il buffer terminato, chiunque sia in attesa deve risvegliarsi,
        // vedere che `closed == true` e restituire `Ok(None)` per terminare.
        cvar.notify_all();
    }

    /// Il produttore notifica un fallimento asincrono.
    pub fn fail(&self, error: Box<dyn Any + Send>) {
        let mut state = self.data.0.lock().unwrap();
        state.err = Some(error);
        drop(state);
        
        let cvar = &self.data.1;
        // CRITICO: Anche per l'errore, tutti i consumatori devono risvegliarsi
        // per gestire la situazione eccezionale.
        cvar.notify_all();
    }

    /// Il consumatore preleva i valori inseriti (FIFO).
    pub fn consume(&self) -> Result<Option<T>, Box<dyn Any + Send>> {
        loop {
            let mut state = self.data.0.lock().unwrap();

            // CASO 1: Ci sono dati disponibili. Li consumiamo immediatamente.
            // Nota: Estraiamo sempre i dati PRIMA di controllare eventuali chiusure o errori,
            // poiche' il buffer potrebbe essere stato terminato dopo aver inserito dei valori validi,
            // che devono comunque essere estratti prima di restituire None/Errore!
            if state.data.is_empty() == false {
                let value = state.data.pop_front();
                return Ok(value);
            }
            else {
                // Il buffer e' vuoto. Verifichiamo i casi limite.
                
                // CASO 2: E' stato segnalato un errore.
                if state.err.is_some() {
                    // Mettiamo closed a true cosi' che, se l'errore viene consumato (take),
                    // i successivi consumatori non restino in deadlock ma ricevano `Ok(None)`
                    // (perche' il buffer non accetta piu' nuovi valori dopo un fail).
                    state.closed = true;
                    return Err(state.err.take().unwrap());
                }
                // CASO 3: Il buffer e' terminato pacificamente.
                else if state.closed == true {
                    return Ok(None);
                }
                // CASO 4: Il buffer e' aperto ma vuoto. 
                // Mettiamo il thread in sleep finche' non cambia qualcosa.
                else {
                    let cvar = &self.data.1;
                    state = cvar.wait_while(state, |c| {
                        // REGOLE DI ATTESA:
                        // Dormi SOLTANTO se:
                        // 1. Non ci sono dati (is_empty)
                        // 2. Il buffer NON e' chiuso (closed == false)
                        // 3. Non c'e' nessun errore (err.is_none)
                        c.data.is_empty() && c.closed == false && c.err.is_none()
                    }).unwrap();
                }
            }
        }
    }
}

// -----------------------------------------------------------------------------
// TEST CAMPAIGN
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_normal_producer_consumer() {
        let buffer = Buffer::new();
        let buf_clone = buffer.clone();

        // Consumer thread
        let consumer = thread::spawn(move || {
            let mut results = vec![];
            while let Ok(Some(val)) = buf_clone.consume() {
                results.push(val);
            }
            results
        });

        // Producer pushes values
        buffer.next(10);
        buffer.next(20);
        buffer.next(30);
        
        // Producer terminates the stream
        buffer.terminate();

        let res = consumer.join().unwrap();
        assert_eq!(res, vec![10, 20, 30]);
    }

    #[test]
    fn test_fail_behavior() {
        let buffer: Buffer<i32> = Buffer::new();
        let buf_clone = buffer.clone();

        let consumer = thread::spawn(move || {
            // First consume should fail with the error
            let result = buf_clone.consume();
            assert!(result.is_err());
            
            // Extract the actual error payload to verify it
            let err_payload = result.unwrap_err();
            let err_msg = err_payload.downcast_ref::<&str>().unwrap();
            assert_eq!(*err_msg, "Simulated Failure");

            // Second consume should gracefully return None, since the buffer 
            // is now empty and closed (due to the brilliant state.closed = true fix).
            let second_result = buf_clone.consume();
            assert!(second_result.is_ok());
            assert_eq!(second_result.unwrap(), None);
        });

        // Producer sends an error
        let err: Box<dyn Any + Send> = Box::new("Simulated Failure");
        buffer.fail(err);

        consumer.join().unwrap();
    }

    #[test]
    #[should_panic(expected = "The buffer is closed")]
    fn test_next_after_terminate_panics() {
        let buffer = Buffer::new();
        buffer.terminate();
        
        // This should panic
        buffer.next(10);
    }

    #[test]
    #[should_panic(expected = "An error occurs")]
    fn test_next_after_fail_panics() {
        let buffer = Buffer::new();
        let err: Box<dyn Any + Send> = Box::new("Error");
        buffer.fail(err);
        
        // This should panic
        buffer.next(10);
    }

    #[test]
    fn test_multiple_consumers_wakeup_on_terminate() {
        // Stress test to ensure `notify_all` was used in `terminate()`
        let buffer: Buffer<i32> = Buffer::new();
        let mut handles = vec![];

        // Spawn 5 sleeping consumers
        for _ in 0..5 {
            let buf_clone = buffer.clone();
            handles.push(thread::spawn(move || {
                let res = buf_clone.consume();
                assert!(res.is_ok());
                assert_eq!(res.unwrap(), None);
            }));
        }

        // Give them time to go to sleep on the Condvar
        thread::sleep(Duration::from_millis(50));
        
        // Terminate the buffer. If `notify_all` wasn't used, some threads
        // would hang here forever and `join()` would never return!
        buffer.terminate();

        for handle in handles {
            handle.join().unwrap();
        }
    }
}