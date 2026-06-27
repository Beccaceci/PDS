use crossbeam_channel::{bounded, Receiver, Sender};

pub struct ExecutionLimiter {
    // Usiamo un bounded channel come "Semaforo"
    sender: Sender<()>,
    receiver: Receiver<()>,
}

impl ExecutionLimiter {
    pub fn with_capacity(limit: usize) -> Self {
        let (tx, rx) = bounded(limit);
        
        // Riempiamo il canale con N "token" (biglietti d'ingresso).
        for _ in 0..limit {
            tx.send(()).unwrap();
        }

        Self {
            sender: tx,
            receiver: rx,
        }
    }

    pub fn execute<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        // 1. Chiediamo un token. 
        // Se il canale è vuoto (0 token = N esecuzioni in corso),
        // `recv()` ci blocca in modo super efficiente senza consumare CPU!
        let _ = self.receiver.recv().unwrap();

        // 2. Creiamo una Guardia RAII per restituire il token
        struct TokenGuard<'a> {
            sender: &'a Sender<()>,
        }

        impl<'a> Drop for TokenGuard<'a> {
            fn drop(&mut self) {
                // Rimettiamo il token nel canale per svegliare un altro thread
                let _ = self.sender.send(());
            }
        }

        // Instanziamo la guardia sullo stack
        let _guard = TokenGuard { sender: &self.sender };

        // 3. Eseguiamo la funzione
        f()

        // Al termine, _guard viene droppata e il token torna nel canale
    }
}

crate::generate_limiter_tests!(crate::limiter_crossbeam::ExecutionLimiter);
