use std::{thread, sync::{Arc, atomic::{AtomicBool, Ordering}, mpsc}, time::Duration};

// =================================================================================
// 1. DEFINIZIONE DEI TRAIT (FORNITI DAL TESTO)
// =================================================================================

pub trait Forgettable {
    /// Handle restituito da 'send()'.
    /// Il metodo 'forget()' tenta di annullare il messaggio corrispondente ancora in coda:
    /// - restituisce 'true' se il messaggio era ancora in attesa o il canale è stato chiuso.
    /// - restituisce 'false' se il ricevitore lo aveva già elaborato.
    fn forget(&self) -> bool;
}

pub trait ForgettableSender<T>: Clone {
    /// Lato mittente. 
    /// 'send(t)' restituisce:
    /// - 'Some(handle)' se il messaggio è stato accodato con successo
    /// - 'None' se il ricevitore non esiste più (canale chiuso)
    fn send(&self, t: T) -> Option<Box<dyn Forgettable>>;
}

pub trait ForgettableReceiver<T> {
    /// Lato ricevitore. 
    /// 'recv()' blocca finché non è disponibile un messaggio non annullato e 
    /// restituisce 'None' solo quando il canale è chiuso e la coda è vuota.
    fn recv(&self) -> Option<T>;
}

// =================================================================================
// 2. IL WRAPPER (Il nostro "Pacchetto" per MPSC)
// =================================================================================

/// Dato che std::sync::mpsc è una "scatola nera" (non possiamo sbirciarci dentro 
/// per eliminare i messaggi come facevamo con il Vec), dobbiamo inviare messaggi "intelligenti".
/// `Wrapper` avvolge il dato `T` insieme a due flag booleani atomici condivisi.
pub struct Wrapper<T> {
    /// Il dato vero e proprio.
    data: T,
    
    /// Questo flag viene messo a `true` dal Ricevitore quando elabora il messaggio.
    /// Permette all'Handle di sapere che è troppo tardi per annullarlo.
    received: Arc<AtomicBool>,
    
    /// Questo flag viene messo a `true` dall'Handle se l'utente chiama `forget()`.
    /// Permette al Ricevitore di scartare silenziosamente il messaggio quando lo estrae.
    cancelled: Arc<AtomicBool>
}
impl<T> Wrapper<T> {
    pub fn new (_data: T) -> Self {
        Self {
            data: _data,
            received: Arc::new(AtomicBool::new(false)),
            cancelled: Arc::new(AtomicBool::new(false))
        }
    }
}

// =================================================================================
// 3. IL MITTENTE E IL RICEVITORE (I wrapper di MPSC)
// =================================================================================

/// Il nostro Sender "dimenticabile" che avvolge il vero Sender di libreria standard.
/// Nota che la coda MPSC trasporterà i nostri `Wrapper<T>` invece dei semplici `T`.
pub struct MySender<T> {
    sender: mpsc::Sender<Wrapper<T>>
}
impl<T> Clone for MySender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone()
        }
    }
}

impl<T> ForgettableSender<T> for MySender<T> {
    fn send(&self, _t: T) -> Option<Box<dyn Forgettable>> {
        let wrapper = Wrapper::new(_t);
        let received = wrapper.received.clone();
        let cancelled = wrapper.cancelled.clone();

        if self.sender.send(wrapper).is_ok() {
            Some(Box::new(MyHandler {
                received: received,
                cancelled: cancelled
            }))
        }
        else {
            None
        }
    }
}

/// Il nostro Receiver "dimenticabile" che avvolge il vero Receiver di libreria standard.
pub struct MyReceiver<T> {
    receiver: mpsc::Receiver<Wrapper<T>>
}
impl<T> ForgettableReceiver<T> for MyReceiver<T> {
    fn recv(&self) -> Option<T> {
        loop {
            let result = self.receiver.recv();

            if result.is_err() {
                return None;
            }
            
            if result.is_ok()  {
                let wrapper = result.unwrap();
                if !wrapper.cancelled.load(Ordering::SeqCst) {
                    wrapper.received.store(true, Ordering::SeqCst);
                    return Some(wrapper.data);
                }
            }
        }
        
    }
}

// =================================================================================
// 4. L'HANDLE E LE IMPLEMENTAZIONI DEI TRAIT
// =================================================================================

// TODO: Definisci qui la struct Handle. 
pub struct MyHandler {
    /// Questo flag viene messo a `true` dal Ricevitore quando elabora il messaggio.
    /// Permette all'Handle di sapere che è troppo tardi per annullarlo.
    received: Arc<AtomicBool>,
    
    /// Questo flag viene messo a `true` dall'Handle se l'utente chiama `forget()`.
    /// Permette al Ricevitore di scartare silenziosamente il messaggio quando lo estrae.
    cancelled: Arc<AtomicBool>
}

impl Forgettable for MyHandler {
    fn forget(&self) -> bool {
        self.cancelled.store(true, Ordering::SeqCst);
        !self.received.load(Ordering::SeqCst)
    }
}

pub fn forgettable_channel<T: 'static>() -> (impl ForgettableSender<T>, impl ForgettableReceiver<T>) {
    let (tx, rx) = mpsc::channel::<Wrapper<T>>();
    (MySender { sender: tx }, MyReceiver { receiver: rx })
}

// =================================================================================
// 5. TEST SUITE
// =================================================================================

crate::generate_channel_tests!(forgettable_channel);