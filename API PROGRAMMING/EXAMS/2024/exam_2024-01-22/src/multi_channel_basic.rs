use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};
use crate::generate_multichannel_tests;

/// Stato condiviso tra il MultiChannel e il singolo ricevitore.
/// Contiene la coda dei messaggi e un flag per indicare se il ricevitore è ancora vivo.
struct SubscriberState {
    queue: VecDeque<u8>,
    is_alive: bool,
}

/// Il ricevitore custom che mima il comportamento di un mpsc::Receiver.
pub struct BasicReceiver {
    // Usiamo Arc per condividere lo stato tra il canale (che invia) e il ricevitore (che riceve).
    state: Arc<(Mutex<SubscriberState>, Condvar)>,
}

impl BasicReceiver {
    /// Estrae un messaggio dalla coda. Se la coda è vuota, blocca il thread (wait)
    /// senza consumare CPU. Ritorna Err se il canale è stato distrutto.
    pub fn recv(&self) -> Result<u8, ()> {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock().unwrap();
        
        loop {
            // Se c'è un messaggio, estrailo
            if let Some(val) = state.queue.pop_front() {
                return Ok(val);
            }
            
            // Se non ci sono messaggi ma il ricevitore è stato "ucciso" logicamente
            // (ad esempio il MultiChannel padre è stato droppato), usciamo.
            if !state.is_alive {
                return Err(());
            }
            
            // Attendi un nuovo messaggio
            state = cvar.wait(state).unwrap();
        }
    }
}

impl Drop for BasicReceiver {
    fn drop(&mut self) {
        let (lock, _) = &*self.state;
        let mut state = lock.lock().unwrap();
        // Quando il ricevitore esce dallo scope, si marca come "morto".
        // Al prossimo invio, MultiChannel se ne accorgerà e lo pulirà dalla sua lista!
        state.is_alive = false;
    }
}

/// Implementazione di MultiChannel basata esclusivamente su strutture basilari (Mutex + Condvar).
pub struct MultiChannelBasic {
    // Ogni sottoscrittore ha la sua coda e il suo Condvar dedicati.
    subscribers: Mutex<Vec<Arc<(Mutex<SubscriberState>, Condvar)>>>,
}

impl MultiChannelBasic {
    pub fn new() -> Self {
        Self { 
            subscribers: Mutex::new(Vec::new()) 
        }
    }

    pub fn subscribe(&self) -> BasicReceiver {
        let state = Arc::new((
            Mutex::new(SubscriberState {
                queue: VecDeque::new(),
                is_alive: true,
            }),
            Condvar::new(),
        ));
        
        let mut subs = self.subscribers.lock().unwrap();
        // Manteniamo una copia dell'Arc nel MultiChannel per potergli inviare messaggi
        subs.push(Arc::clone(&state));
        
        BasicReceiver { state }
    }

    pub fn send(&self, data: u8) -> Result<(), ()> {
        let mut subs = self.subscribers.lock().unwrap();
        
        // Sfoltiamo (cleanup) e inviamo in un solo colpo tramite retain!
        subs.retain(|sub| {
            let (lock, cvar) = &**sub;
            let mut state = lock.lock().unwrap();
            
            // Se il ricevitore è stato droppato, `is_alive` sarà false.
            // Retain ritorna `false`, eliminando questo Arc dal vettore (cleanup)!
            if !state.is_alive {
                return false;
            }
            
            // Se è vivo, accodiamo il dato e svegliamo il thread in attesa
            state.queue.push_back(data);
            cvar.notify_one();
            
            true // Mantieni questo ricevitore
        });
        
        // Se non è rimasto alcun iscritto, ritorna errore (come da specifiche)
        if subs.is_empty() {
            Err(())
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    pub fn get_num_subscribers(&self) -> usize {
        self.subscribers.lock().unwrap().len()
    }
}

impl Drop for MultiChannelBasic {
    fn drop(&mut self) {
        let subs = self.subscribers.lock().unwrap();
        for sub in subs.iter() {
            let (lock, cvar) = &**sub;
            let mut state = lock.lock().unwrap();
            // Se il canale master viene droppato, diciamo ai ricevitori
            // che sono morti per evitare che restino appesi per sempre in wait().
            state.is_alive = false;
            cvar.notify_all();
        }
    }
}

// Invochiamo la macro condivisa per testare questa architettura
generate_multichannel_tests!(MultiChannelBasic);
