use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};

/// Lo stato condiviso tra il Dispatcher e UNA singola Subscription.
/// Manteniamo solo i dati puri nel Mutex. La Condvar deve stare FUORI dal Mutex.
pub struct SharedState<Message: Clone> {
    queue: VecDeque<Message>,
    closed: bool, // Diventa true se il Dispatcher viene distrutto
}

/// La struttura pubblica Subscription.
/// Detiene un Arc che punta alla tupla (Mutex<Stato>, Condvar).
pub struct Subscription<Message: Clone> {
    receiver: Arc<(Mutex<SharedState<Message>>, Condvar)>,
}

impl<Message: Clone> Subscription<Message> {
    pub fn read(&self) -> Option<Message> {
        // 1. Acquisiamo il lock sullo stato condiviso
        let mut state = self.receiver.0.lock().unwrap();

        // 2. Attendiamo che ci sia almeno un messaggio OPPURE che il dispatcher sia morto
        state = self.receiver.1.wait_while(state, |s| {
            s.queue.is_empty() && !s.closed
        }).unwrap();

        // 3. Abbiamo finito di attendere. Estraiamo un messaggio (se c'è)
        if let Some(msg) = state.queue.pop_front() {
            // Se c'era un messaggio, lo ritorniamo! (Anche se closed è true, 
            // leggiamo comunque i messaggi rimasti nella coda, come richiesto dall'esame).
            return Some(msg);
        }

        // 4. Se siamo arrivati qui, la coda è vuota. 
        // Poiché wait_while ci ha svegliati, significa inevitabilmente che closed è true.
        None
    }
}

/// La struttura pubblica Dispatcher.
/// Detiene un vettore di Arc che puntano agli stati condivisi delle varie Subscription.
pub struct Dispatcher<Message: Clone> {
    subscriptions: Mutex<Vec<Arc<(Mutex<SharedState<Message>>, Condvar)>>>,
}

impl<Message: Clone> Dispatcher<Message> {
    pub fn new() -> Self {
        Self {
            subscriptions: Mutex::new(Vec::new()),
        }
    }

    pub fn subscribe(&self) -> Subscription<Message> {
        // 1. Alloca la memoria condivisa nell'Heap, protetta da Mutex e Arc
        let shared = Arc::new((
            Mutex::new(SharedState {
                queue: VecDeque::new(),
                closed: false,
            }),
            Condvar::new()
        ));
        
        // 2. Diamo un puntatore clonato al Dispatcher
        let mut state = self.subscriptions.lock().unwrap();
        state.push(Arc::clone(&shared));
        
        // 3. Diamo il puntatore originale alla Subscription
        Subscription { receiver: shared }
    }

    pub fn dispatch(&self, msg: Message) {
        let mut state = self.subscriptions.lock().unwrap();
        
        // Usiamo 'retain' per iterare, filtrare e pulire in un colpo solo!
        state.retain(|s| {
            // Se lo strong_count è 1, significa che il Dispatcher è l'UNICO proprietario rimasto.
            // La Subscription è stata droppata dall'utente!
            if Arc::strong_count(s) == 1 {
                false // Restituisce false a 'retain', eliminando questo Arc dal Vettore (Garbage Collection!)
            } else {
                // La Subscription è viva. Blocchiamo il suo stato interno.
                let mut sub_state = s.0.lock().unwrap();
                sub_state.queue.push_back(msg.clone()); // Inseriamo il messaggio
                
                // Svegliamo il thread eventualmente in attesa (se la coda era vuota)
                s.1.notify_one(); 
                
                true // Manteniamo questo Arc nel Vettore
            }
        });
    }
}

/// Implementazione vitale: Cosa succede quando il Dispatcher muore?
impl<Message: Clone> Drop for Dispatcher<Message> {
    fn drop(&mut self) {
        let state = self.subscriptions.lock().unwrap();
        
        // Per ogni subscription attiva...
        for s in state.iter() {
            // 1. Modifichiamo il flag closed
            let mut sub_state = s.0.lock().unwrap();
            sub_state.closed = true;
            
            // 2. SVEGLIAMO TUTTI I THREAD IN ATTESA!
            // Se non lo facessimo, una Subscription addormentata rimarrebbe in deadlock per sempre.
            s.1.notify_all();
        }
    }
}

// Invochiamo i test condivisi!
crate::generate_dispatcher_tests!(crate::dispatcher_manual::Dispatcher<String>);