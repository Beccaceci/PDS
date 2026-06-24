use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

/// SharedState contiene i dati condivisi tra produttori e consumatore.
/// Al contrario dei channel, qui gestiamo noi la memoria cruda (VecDeque).
struct SharedState<Message: Send + 'static> {
    queue: VecDeque<Message>, // Il vero buffer dei messaggi
    closed: bool,             // Flag per segnalare la distruzione del Looper
}

impl<Message: Send + 'static> SharedState<Message> {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
            closed: false,
        }
    }
}

pub struct Looper<Message: Send + 'static> {
    // Arc permette la condivisione. Mutex garantisce l'esclusione mutua (thread-safety),
    // e Condvar permette al thread di dormire senza consumare CPU.
    data: Arc<(Mutex<SharedState<Message>>, Condvar)>,
    
    // NESSUN Mutex qui! Avendo &mut self nel Drop, non serve interior mutability.
    handle: Option<JoinHandle<()>>,
}

impl<Message: Send + 'static> Looper<Message> {
    pub fn new(process: fn(Message), cleanup: fn()) -> Self {
        let mutex = Mutex::new(SharedState::new());
        let cvar = Condvar::new();
        let reference_counter = Arc::new((mutex, cvar));

        // Clona l'Arc per passarlo al thread in background
        let cloned_counter = Arc::clone(&reference_counter);

        let handle = thread::spawn(move || {
            loop {
                let (lock, cvar) = &*cloned_counter;
                let mut state = lock.lock().unwrap();

                // 1. IL SONNO: Se la coda è vuota e il Looper è ancora attivo, dormi.
                state = cvar.wait_while(state, |c| {
                    c.queue.is_empty() && !c.closed
                }).unwrap();

                // 2. LA TERMINAZIONE: Se ci hanno svegliato, siamo vuoti ed è chiuso -> esci dal ciclo.
                if state.queue.is_empty() && state.closed {
                    cleanup();
                    break;
                }

                // 3. ESTRAZIONE: Prendiamo UN solo messaggio alla volta.
                // Usiamo unwrap in sicurezza perché siamo certi che la coda non sia vuota qui.
                let msg = state.queue.pop_front().unwrap();

                // 4. RILASCIO LOCK (FONDAMENTALE): Lasciamo il lock PRIMA di elaborare.
                // In questo modo i produttori possono inviare nuovi messaggi
                // mentre questo thread esegue il `process`!
                drop(state);

                // 5. ELABORAZIONE: Chiamiamo la logica utente in totale parallelismo.
                process(msg);
            }
        });

        Self {
            data: reference_counter,
            handle: Some(handle),
        }
    }

    pub fn send(&self, msg: Message) {
        let (lock, cvar) = &*self.data;
        let mut state = lock.lock().unwrap();

        if state.closed {
            panic!("The looper is closed");
        }

        // 1. Inseriamo il messaggio nella coda
        state.queue.push_back(msg);
        
        // 2. Rilasciamo il lock prima di svegliare il thread per evitare 
        //    context-switch inutili se il consumatore prova subito a ri-acquisire il Mutex (ottimizzazione).
        drop(state);
        
        // 3. IL RISVEGLIO: Svegliamo il consumatore se stava dormendo.
        cvar.notify_one();
    }
}

impl<Message: Send + 'static> Drop for Looper<Message> {
    fn drop(&mut self) {
        // 1. LO SPEGNIMENTO DEFINITIVO: Impostiamo closed a true.
        {
            let (lock, cvar) = &*self.data;
            let mut state = lock.lock().unwrap();

            state.closed = true;
            // Svegliamo il thread dormiente. Vedrà closed=true e terminerà.
            cvar.notify_all();
        }

        // 2. LA GARANZIA: Aspettiamo che il thread finisca l'ultimo task e il cleanup.
        // Avendo &mut self, possiamo usare direttamente take() senza alcun Mutex!
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

// Invochiamo i test condivisi!
crate::generate_looper_tests!(crate::looper_basic::Looper<i32>);