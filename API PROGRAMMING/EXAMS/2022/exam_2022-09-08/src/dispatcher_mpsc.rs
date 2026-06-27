use std::{sync::{Mutex, mpsc::{self, Receiver, Sender}}};

/// Struttura Subscription
pub struct Subscription<Message: Clone> {
    receiver: Receiver<Message>
}

impl<Message: Clone> Subscription<Message> {
    pub fn with_receiver(receiver: Receiver<Message>) -> Self {
        Self { receiver }
    }

    /// Legge il prossimo messaggio. Si blocca in attesa se non ce ne sono.
    /// Ritorna None se il Dispatcher è stato distrutto.
    pub fn read(&self) -> Option<Message> {
        // recv() attende in blocco finché non c'è un messaggio.
        // Se TUTTI i Sender associati a questo Receiver vengono distrutti
        // (cioè quando il Dispatcher muore), recv() restituisce Err.
        // .ok() converte dolcemente Result<Msg, Err> in Option<Msg>.
        self.receiver.recv().ok()
    }
}

/// Struttura Dispatcher
pub struct Dispatcher<Message: Clone> {
    // Vettore protetto da Mutex per supportare l'interior mutability,
    // dato che subscribe() può essere chiamato da più thread in modo concorrente.
    subscriptions: Mutex<Vec<Sender<Message>>>
}

impl<Message: Clone> Dispatcher<Message> {
    pub fn new() -> Self {
        Self {
            subscriptions: Mutex::new(Vec::new())
        }
    }

    /// Sottoscrive un nuovo client e restituisce l'oggetto Subscription.
    pub fn subscribe(&self) -> Subscription<Message> {
        // 1. Creiamo una coppia (Mittente, Ricevitore)
        let (tx, rx) = mpsc::channel();
        
        // 2. Registriamo il mittente nel Dispatcher
        let mut state = self.subscriptions.lock().unwrap();
        state.push(tx);
        
        // 3. Consegniamo il ricevitore (Subscription) all'utente
        Subscription::with_receiver(rx)
    }

    /// Esegue il dispatch del messaggio a tutte le subscription attive.
    pub fn dispatch(&self, msg: Message) {
        let mut state = self.subscriptions.lock().unwrap();
        
        // Invia il messaggio a tutti i Sender. 
        // Usiamo 'retain' per mantenere nel Vettore SOLO i Sender validi.
        // Se una Subscription è stata distrutta (Receiver eliminato o caduto dallo scope), 
        // tx.send(...) fallisce restituendo Err, is_ok() diventerà false,
        // rimuovendo istantaneamente e in modo sicuro il Sender morto dalla memoria!
        state.retain(|tx| tx.send(msg.clone()).is_ok());
    }
}

// Invochiamo i test condivisi!
crate::generate_dispatcher_tests!(crate::dispatcher_mpsc::Dispatcher<String>);
