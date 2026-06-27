use std::{collections::VecDeque, sync::{Arc, Condvar, Mutex}};

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
// 2. LO STATO CONDIVISO
// =================================================================================

/// `SharedState` è la struttura che vive "sottochiave" dentro al Mutex.
/// Contiene tutti i dati necessari per far comunicare Sender, Receiver e Handle.
struct SharedState<T> {
    /// La coda dei messaggi. Usiamo un `VecDeque` perché è super efficiente 
    /// per estrarre dal fronte (pop_front) e inserire in coda (push_back).
    /// Ogni elemento è una tupla: (ID del messaggio, Dato generico T).
    messages: VecDeque<(usize, T)>,
    
    /// Flag che indica se il canale è stato chiuso.
    /// Viene impostato a 'true' se il ricevitore viene distrutto (Drop).
    closed: bool,
    
    /// Un contatore costantemente crescente. 
    /// Genera ID univoci per i messaggi, evitando conflitti quando la coda si svuota.
    next_id: usize
}

impl<T> SharedState<T> {
    pub fn new() -> Self {
        Self {
            messages: VecDeque::new(),
            closed: false,
            next_id: 1 // Partiamo da 1 per pura convenzione
        }
    }
}

// =================================================================================
// 3. IL CANALE (Mittente e Ricevitore fusi in un'unica Struct)
// =================================================================================

/// `MyChannel` funge sia da Sender che da Receiver.
/// Sotto il cofano contiene solo un `Arc` (Reference Counter Atomico)
/// che avvolge un Mutex (per i dati) e una Condvar (per far addormentare/svegliare i thread).
pub struct MyChannel<T> {
    state: Arc<(Mutex<SharedState<T>>, Condvar)>
}

impl<T> MyChannel<T> {
    pub fn new() -> Self {
        Self {
            state: Arc::new((Mutex::new(SharedState::new()), Condvar::new()))
        }
    }
}

/// Implementiamo Clone in modo che l'utente possa clonare liberamente i mittenti.
/// Clonare un `MyChannel` clona SOLO il puntatore Arc, non i dati, 
/// mantenendo tutti collegati allo stesso Mutex centrale.
impl<T> Clone for MyChannel<T> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state)
        }
    }
}

/// Il `Drop` (distruttore) viene chiamato automaticamente quando un MyChannel esce di scope.
/// Se l'utente chiude il canale (es. fa drop del ricevitore), alziamo il flag `closed`
/// in modo che i futuri `send` o `recv` sappiano che è finita.
impl<T> Drop for MyChannel<T> {
    fn drop(&mut self) {
        let (mutex, cvar) = &*self.state;
        let mut guard = mutex.lock().unwrap();
        
        guard.closed = true;
        
        // Risvegliamo eventuali thread bloccati su `recv` per fargli leggere il 'closed = true'
        cvar.notify_all(); 
    }
}

// =================================================================================
// 4. IMPLEMENTAZIONE DEL MITTENTE
// =================================================================================

impl<T: 'static> ForgettableSender<T> for MyChannel<T> {
    fn send(&self, t: T) -> Option<Box<dyn Forgettable>> {
        let (mutex, cvar) = &*self.state;
        
        // Prendiamo il possesso esclusivo dello stato condiviso
        let mut guard = mutex.lock().unwrap();
        
        // Se il ricevitore è stato disconnesso, restituiamo None
        if guard.closed {
            None
        }
        else {
            // Generiamo l'ID per questo nuovo messaggio leggendo dal contatore
            let id = guard.next_id;
            
            // Inseriamo fisicamente il messaggio in fondo alla coda
            guard.messages.push_back((id, t));
            
            // Creiamo un "bigliettino" (Handle) che contiene il clone dell'intero canale 
            // e l'ID del messaggio appena inviato, così da poterlo eventualmente annullare.
            let handle = MyHandle {
                id,
                state: Arc::clone(&self.state)
            };
            
            // Incrementiamo il contatore per il prossimo messaggio (così gli ID non si sovrappongono mai)
            guard.next_id += 1;
            
            // Rilasciamo il Mutex (molto importante rilasciarlo prima della notify_one)
            drop(guard);
            
            // Svegliamo UN thread in ascolto (il Receiver che stava bloccato su recv)
            cvar.notify_one();
            
            // Restituiamo l'Handle impacchettato nel Box
            Some(Box::new(handle))
        }
    }
}

// =================================================================================
// 5. IMPLEMENTAZIONE DEL RICEVITORE
// =================================================================================

impl<T> ForgettableReceiver<T> for MyChannel<T> {
    fn recv(&self) -> Option<T> {
        let (mutex, cvar) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        loop {
            // PROVA A LEGGERE: Cerchiamo di prelevare il primo messaggio dalla coda.
            // .pop_front() non solo ci restituisce il messaggio, ma lo RIMUOVE fisicamente
            // dal vettore. Questo è il cuore della nostra logica: se il messaggio non c'è più,
            // significa che è stato elaborato!
            if let Some((_id, message)) = guard.messages.pop_front() {
                return Some(message); // Dato recuperato, chiudiamo la funzione!
            }
            
            // SE LA CODA È VUOTA MA IL CANALE È CHIUSO:
            // Svuotiamo del tutto eventuali residui (per buona prassi) e restituiamo None.
            else if guard.closed {
                guard.messages.clear();
                return None;
            }
            
            // SE LA CODA È VUOTA MA IL CANALE È ANCORA APERTO:
            // Non possiamo far altro che aspettare. cvar.wait() addormenta questo thread
            // e rilascia *immediatamente* il Mutex, permettendo ai Sender di inviare messaggi.
            // Quando un Sender fa 'notify_one()', questo thread si risveglia, 
            // si riprende il Mutex, e il loop riparte dall'inizio!
            else {
                guard = cvar.wait(guard).unwrap();
            }
        }
    }
}

// =================================================================================
// 6. IMPLEMENTAZIONE DELL'HANDLE (Il "Bigliettino")
// =================================================================================

pub struct MyHandle<T> {
    id: usize, // L'ID univoco assegnato al messaggio al momento del send
    
    // Contiene un clone dell'intero stato del canale! Questo permette all'Handle
    // di scavalcare tutto, bloccare il Mutex, frugare nella coda, e cancellare il suo messaggio.
    state: Arc<(Mutex<SharedState<T>>, Condvar)>
}

impl<T> Forgettable for MyHandle<T> {
    fn forget(&self) -> bool {
        let (mutex, _) = &*self.state;
        let mut guard = mutex.lock().unwrap();

        // FRUGHIAMO NEL VETTORE: cerchiamo l'indice del messaggio che ha il nostro stesso ID
        let found_index = guard.messages.iter()
            .enumerate()
            .find(|(_index, (id, _message))| *id == self.id)
            .map(|(index, _)| index); // Estraiamo solo l'indice trovato

        if let Some(index) = found_index {
            // MESSAGGIO TROVATO!
            // Significa che il ricevitore NON lo ha ancora prelevato con `pop_front`.
            // Procediamo ad assassinarlo rimuovendolo fisicamente dalla coda.
            guard.messages.remove(index);
            true
        } else {
            // MESSAGGIO NON TROVATO!
            // Può significare due cose:
            // 1. Il ricevitore è stato più veloce di noi e lo ha già elaborato (`pop_front`).
            // 2. Il canale è stato chiuso e la coda è stata spazzata via.
            // In entrambi i casi, l'annullamento non è andato a buon fine, restituiamo false.
            false
        } 
    }
}

// =================================================================================
// 7. FACTORY E TEST SUITE
// =================================================================================

/// Punto di ingresso per la creazione del canale richiesto dall'esame.
pub fn forgettable_channel<T: 'static>() -> (impl ForgettableSender<T>, impl ForgettableReceiver<T>) {
    let channel = MyChannel::new();
    
    // Restituiamo due cloni identici. Il trait system si assicurerà che l'utente 
    (channel.clone(), channel)
}

crate::generate_channel_tests!(forgettable_channel);