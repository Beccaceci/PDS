use std::sync::mpsc;
use std::thread::{self, JoinHandle};

pub struct Looper<Message: Send + 'static> {
    // Il Sender è intrinsecamente thread-safe. 
    // Lo avvolgiamo in un Option solo per poterlo estrarre con `.take()`
    // durante l'invocazione di `drop(&mut self)`.
    sender: Option<mpsc::Sender<Message>>,
    
    // Lo avvolgiamo in Option per poter chiamare `.join()` prendendone possesso 
    // durante il `drop(&mut self)`.
    handle: Option<JoinHandle<()>>
}

impl<Message: Send + 'static> Looper<Message> {
    
    /// Crea il Looper, istanzia la coda e avvia il thread in background.
    pub fn new(process: fn(Message), cleanup: fn()) -> Self {
        // 1. Creiamo un canale mpsc
        let (tx, rx) = mpsc::channel();

        // 2. Avviamo il thread worker
        let handle = thread::spawn(move || {
            // Il ciclo si blocca consumando 0% CPU in attesa di messaggi.
            // Quando il Sender viene distrutto (nel Drop del Looper), 
            // rx.recv() restituirà un errore e il ciclo terminerà.
            for item in rx {
                process(item);
            }

            // 3. Immediatamente dopo la fine del ciclo (quando non arriveranno più messaggi),
            // eseguiamo la funzione di cleanup prima di far morire il thread.
            cleanup();
        });

        Self {
            sender: Some(tx),
            handle: Some(handle)
        }
    }

    /// Inserisce un messaggio nella coda
    pub fn send(&self, msg: Message) {
        // Preleviamo una referenza al Sender. 
        // mpsc::Sender implementa un send(&self), quindi non ci serve alcun Mutex!
        // Usiamo unwrap() in sicurezza perché `sender` diventa None SOLO nel `Drop`,
        // quando nessuno può più chiamare `send`.
        let tx = self.sender.as_ref().unwrap();
        let _ = tx.send(msg);
    }
}

/// Gestisce la distruzione del Looper. 
/// Garantisce che il thread chiami cleanup() e poi termini.
impl<Message: Send + 'static> Drop for Looper<Message> {
    fn drop(&mut self) {
        // 1. Distruggiamo il Sender!
        // Facendo take(), togliamo il Sender dall'Option. Uscendo immediatamente dallo scope,
        // il Sender viene droppato. Il canale mpsc capisce che non ci sono più mittenti 
        // e sblocca il thread ricevitore facendo interrompere il ciclo `for`.
        self.sender.take();

        // 2. Attendiamo la fine dei lavori!
        // Facendo take(), prendiamo pieno possesso del JoinHandle.
        // Chiamiamo join() per bloccare il main thread finché il worker thread 
        // non ha finito di svuotare la coda e ha eseguito la funzione di cleanup.
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

// ==========================================
// Invochiamo la macro dei test condivisi!
// ==========================================
crate::generate_looper_tests!(crate::looper_mpsc::Looper<i32>);