use std::sync::{mpsc, Mutex};
use std::thread::{self, JoinHandle};

pub struct Processor<T> where T: Send + 'static {
    // Usiamo Option per poter fare .take() e forzare il drop (distruzione) del Sender in close()
    sender: Mutex<Option<mpsc::Sender<T>>>,
    
    // Usiamo Option per poter fare .take() e fare join() sull'handle in close()
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl<T: Send + 'static> Processor<T> {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(T) + Send + 'static,
    {
        // AZIONE 1: L'ALLOCAZIONE (mpsc::channel)
        // Sotto il cofano: 
        // 1. Rust alloca memoria nell'heap per uno stato condiviso (simile al tuo SharedState).
        // 2. Crea una coda thread-safe (Linked List) e un contatore atomico di sender (sender_count = 1).
        // 3. tx (Sender) e rx (Receiver) sono puntatori intelligenti a questo stato condiviso.
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            // AZIONE 2: IL SONNO (for item in rx)
            // Sotto il cofano:
            // 1. Il ciclo for chiama internamente `rx.recv()`.
            // 2. `recv()` guarda la coda. Se e' vuota e `sender_count > 0`, chiede 
            //    al Sistema Operativo di addormentare ("park") il thread.
            // 3. Il thread consuma 0% CPU finche' non viene svegliato da un Sender.
            for item in rx {
                f(item); // Elabora l'elemento
            }
        });

        Self {
            sender: Mutex::new(Some(tx)),
            handle: Mutex::new(Some(handle)),
        }
    }

    pub fn send(&self, item: T) {
        let mut sender_guard = self.sender.lock().unwrap();
        
        if let Some(ref tx) = *sender_guard {
            // AZIONE 3: IL RISVEGLIO (tx.send)
            // Sotto il cofano:
            // 1. `send()` spinge l'elemento nella coda condivisa.
            // 2. Controlla se il Receiver e' addormentato. Se lo e', chiede
            //    al Sistema Operativo di svegliarlo ("unpark").
            // 3. Il thread consumatore si sveglia, estrae l'elemento e lo passa a `f(item)`.
            let _ = tx.send(item);
        } else {
            panic!("Cannot send: the processor has been closed.");
        }
    }

    pub fn close(&self) {
        // AZIONE 4: LO SPEGNIMENTO DEFINITIVO (drop del Sender)
        {
            let mut sender_guard = self.sender.lock().unwrap();
            
            // `take()` estrae il Sender dall'Option, lasciando None.
            // Poiche' non salviamo il Sender estratto in una variabile esterna, esso 
            // esce dallo scope e viene immediatamente DROPPATO (distrutto).
            //
            // Sotto il cofano:
            // 1. Il Drop decrementa `sender_count` da 1 a 0.
            // 2. Dato che il count e' 0, mpsc sa che nessuno inviera' mai piu' nulla.
            // 3. Sveglia il Receiver un'ultima volta!
            // 4. Il Receiver si sveglia, vede la coda vuota e count == 0.
            // 5. `rx.recv()` capisce che il canale e' morto e restituisce un errore che fa
            //    terminare istantaneamente il ciclo `for item in rx`!
            let _sender = sender_guard.take(); 
        } // Il Mutex viene rilasciato qui

        // AZIONE 5: LA GARANZIA (handle.join)
        // Anche se abbiamo detto al thread di fermarsi (droppando il Sender), 
        // potrebbe essere ancora a meta' dell'elaborazione dell'ULTIMO elemento.
        // 
        // Sotto il cofano:
        // Facendo join(), il thread principale si blocca e aspetta che il thread 
        // consumatore finisca il task corrente, esca dal ciclo for, e muoia 
        // definitivamente. Questo soddisfa il requisito dell'esame.
        {
            let mut handle_guard = self.handle.lock().unwrap();
            if let Some(handle) = handle_guard.take() {
                let _ = handle.join();
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
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn test_processor_mpsc_flow() {
        let results = Arc::new(Mutex::new(Vec::new()));
        let results_clone = Arc::clone(&results);

        let processor = Processor::new(move |item: i32| {
            std::thread::sleep(Duration::from_millis(10));
            results_clone.lock().unwrap().push(item * 10);
        });

        processor.send(1);
        processor.send(2);
        processor.send(3);

        processor.close();

        let final_res = results.lock().unwrap();
        assert_eq!(*final_res, vec![10, 20, 30]);
    }

    #[test]
    #[should_panic(expected = "Cannot send: the processor has been closed.")]
    fn test_send_after_close_panics() {
        let processor = Processor::new(|_item: i32| {});
        processor.close();
        
        processor.send(100);
    }
}
