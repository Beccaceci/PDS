use std::{collections::VecDeque, sync::{Arc, Condvar, Mutex}, thread::{self, JoinHandle}};

/// SharedState contiene la coda dei messaggi e il flag di chiusura.
/// Questa struttura e' passiva e condivisa tra i thread produttori e il thread consumatore.
struct SharedState<T> where T: Send + 'static {
    queue: VecDeque<T>,
    closed: bool
}

impl<T: Send + 'static> SharedState<T> {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            closed: false
        }
    }
}

/// Processor e' un Executor asincrono. 
/// Riceve task (oggetti di tipo T), li mette in coda, e un thread lavoratore 
/// in background li elabora applicando una closure definita dall'utente.
pub struct Processor<T> where T: Send + 'static {
    /// Lo stato condiviso e la variabile condizionale per coordinare i thread.
    data: Arc<(Mutex<SharedState<T>>, Condvar)>,
    /// Il JoinHandle del thread lavoratore. Lo mettiamo in un Option dentro un Mutex
    /// per poterlo estrarre (`take()`) e fare `join()` nel metodo `close(&self)`.
    handle: Mutex<Option<JoinHandle<()>>>
}

impl<T: Send + 'static> Processor<T> {
    /// Inizializza il Processor e avvia immediatamente il thread lavoratore in background.
    pub fn new<F>(task: F) -> Self
        where F: Fn(T) + Send + 'static {
        
        let mutex = Mutex::new(SharedState::new());
        let cvar = Condvar::new();
        let data = Arc::new((mutex, cvar));

        // Clona l'Arc PRIMA di avviare il thread, in modo da poterne spostare
        // una copia (`cloned_data`) dentro il thread tramite `move`.
        let cloned_data = Arc::clone(&data);

        let handle = thread::spawn(move || {
            // Ciclo di vita del consumatore (worker thread)
            loop {
                let (lock, cvar) = &*cloned_data;
                let mut state = lock.lock().unwrap();

                // 1. ATTESA: Se la coda e' vuota e il processor non e' chiuso, 
                // il thread si addormenta senza consumare CPU.
                state = cvar.wait_while(state, |c| {
                    c.queue.is_empty() && !c.closed
                }).unwrap();

                // 2. TERMINAZIONE: Se il processor e' stato chiuso e non ci sono 
                // piu' elementi in coda, il ciclo (e quindi il thread) termina.
                if state.queue.is_empty() && state.closed {
                    break;
                }

                // 3. ESTRAZIONE: Estraiamo un elemento dalla coda.
                let item = state.queue.pop_front().unwrap();
                
                // 4. RILASCIO LOCK: Molto importante! Rilasciamo il Mutex prima
                // di eseguire `task(item)`, cosi' i produttori possono continuare
                // ad inserire elementi mentre il task e' in esecuzione!
                drop(state);
                
                // 5. ESECUZIONE
                task(item);
            }
        });

        Self {
            data: data,
            handle: Mutex::new(Some(handle))
        }
    }

    /// I produttori usano questo metodo per inviare nuovi elementi da elaborare.
    pub fn send(&self, item: T) {
        let (lock, cvar) = &*self.data;
        let mut state = lock.lock().unwrap();

        match state.closed {
            true => {
                // Se il processor e' chiuso, non possiamo accettare altri lavori.
                panic!("The processor is closed");
            }
            false => {
                // Inseriamo in coda e svegliamo il thread consumatore nel caso stia dormendo.
                state.queue.push_back(item);
                drop(state);
                cvar.notify_one();
            }
        }
    }

    /// Chiude il Processor impedendo nuovi inserimenti e aspetta che tutti i task 
    /// attualmente in coda vengano completati dal thread in background.
    pub fn close(&self) {
        // 1. Impostiamo il flag di chiusura e SVEGLIAMO il thread consumatore.
        {
            let (lock, cvar) = &*self.data;
            let mut state = lock.lock().unwrap();

            state.closed = true;
            // E' cruciale svegliare il consumatore. Se la coda era vuota, stava dormendo.
            // Svegliandosi, vedra' `closed == true` e `is_empty() == true`, e terminera'.
            cvar.notify_all();
        } // Il Mutex viene rilasciato qui

        // 2. Facciamo JOIN sul thread consumatore. 
        // Questo garantisce che `close()` non ritorni fino a quando il thread non
        // ha finito di elaborare tutto (rispettando il requisito dell'esame).
        let mut handle_guard = self.handle.lock().unwrap();
        if let Some(handle) = handle_guard.take() {
            handle.join().unwrap();
        }
    }
}


// -----------------------------------------------------------------------------
// TEST CAMPAIGN
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn test_processor_manual_flow() {
        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        // Creiamo il processor con la closure custom
        let processor = Processor::new(move |item: i32| {
            // Simuliamo un'elaborazione lenta
            thread::sleep(Duration::from_millis(10));
            results_clone.lock().unwrap().push(item * 10);
        });

        // Invio dei messaggi
        processor.send(1);
        processor.send(2);
        processor.send(3);

        // La close aspetta il termine di tutte le elaborazioni!
        processor.close();

        // Verifichiamo i risultati
        let final_res = results.lock().unwrap();
        assert_eq!(*final_res, vec![10, 20, 30]);
    }

    #[test]
    #[should_panic(expected = "The processor is closed")]
    fn test_send_after_close_panics() {
        let processor = Processor::new(|_item: i32| {});
        processor.close();
        
        // Questo deve lanciare il panic esatto previsto in send()
        processor.send(100);
    }
}