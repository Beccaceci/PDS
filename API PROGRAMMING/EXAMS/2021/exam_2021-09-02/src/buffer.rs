use std::{sync::{Arc, Condvar, Mutex}, time::Duration};

/// SharedState incapsula i dati crudi del buffer circolare.
/// Utilizzando i Const Generics (`const N: usize`), possiamo definire la dimensione
/// dell'array a tempo di compilazione senza dover allocare dinamicamente memoria nell'heap.
struct SharedState<T, const N: usize> {
    /// Array a dimensione fissa. Dato che `T` potrebbe non implementare `Copy`,
    /// non potremmo usare `[None; N]` se non ricorrendo a un trucco con una costante associata.
    queue: [Option<T>; N],
    /// Numero attuale di elementi nel buffer. Ci serve per capire facilmente se e' pieno o vuoto.
    count: usize,

    /// Indice dal quale estrarre il prossimo elemento (testa).
    head: usize,
    /// Indice nel quale inserire il prossimo elemento (coda).
    tail: usize
}

impl<T, const N: usize> SharedState<T, N> where T: Send + 'static {
    /// TRUCCO: Definendo `None` come costante associata, forziamo il compilatore
    /// a valutarla a tempo di compilazione. Questo ci permette di inizializzare
    /// l'array aggirando il vincolo del tratto `Copy` che normalmente servirebbe per `[value; N]`.
    const NONE: Option<T> = None;

    pub fn new() -> Self {
        Self {
            queue: [Self::NONE; N],
            count: 0,
            head: 0,
            tail: 0
        }
    }

    /// Funzione di utilita' per incapsulare la logica circolare dell'inserimento.
    pub fn push_internal(&mut self, value: T) {
        self.queue[self.tail] = Some(value);
        self.count += 1;
        // Modulo aritmetico: se tail + 1 raggiunge N, torna istantaneamente a 0!
        self.tail = (self.tail + 1) % N;
    }

    /// Funzione di utilita' per incapsulare la logica circolare dell'estrazione.
    pub fn pop_internal(&mut self) -> T {
        // `take()` estrae il valore dal `Option` lasciando `None` al suo posto.
        let value = self.queue[self.head].take().unwrap();
        self.count -= 1;
        // Modulo aritmetico per avvolgere l'indice della testa.
        self.head = (self.head + 1) % N;
        value
    }
}

/// CircularBuffer implementa il pattern produttore-consumatore bounded.
/// Condivide la proprieta' attraverso un `Arc` e protegge i dati con un `Mutex`.
/// Cruciale per evitare "Lost Wakeups": usiamo DUE Condvar distinte.
pub struct CircularBuffer<T: Send + 'static, const N: usize> {
    // Ordine nella tupla: (Stato, Condvar per Produttori (not_full), Condvar per Consumatori (not_empty))
    data: Arc<(Mutex<SharedState<T, N>>, Condvar, Condvar)>
}

// Per permettere ai thread di condividere il buffer.
impl<T: Send + 'static, const N: usize> Clone for CircularBuffer<T, N> {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data)
        }
    }
}

impl<T, const N: usize> CircularBuffer<T, N> where T: Send + 'static {
    pub fn new() -> Self {
        let mutex = Mutex::new(SharedState::new());
        let cvar_full = Condvar::new();   // Segnala quando il buffer NON e' piu' pieno
        let cvar_empty = Condvar::new();  // Segnala quando il buffer NON e' piu' vuoto
        
        Self {
            data: Arc::new((mutex, cvar_full, cvar_empty))
        }
    }

    /// Inserisce un elemento. Se il buffer e' pieno (count == N), il thread si blocca
    /// senza sprecare cicli di CPU (niente busy-waiting) finche' non si libera spazio.
    pub fn insert(&self, value: T) {
        let (lock, cvar_full, cvar_empty) = &*self.data;
        let mut state = lock.lock().unwrap();

        // 1. ATTESA: Il produttore aspetta sulla condizione `not_full` se il buffer e' pieno.
        state = cvar_full.wait_while(state, |c| c.count == N).unwrap();

        // 2. AZIONE: C'e' spazio! Inseriamo il valore.
        state.push_internal(value);
        
        // 3. RILASCIO E NOTIFICA: Rilasciamo il Mutex per non bloccare il consumatore 
        // che stiamo per svegliare. Poi notifichiamo la `cvar_empty` per svegliare
        // un consumatore in attesa di dati.
        drop(state);
        cvar_empty.notify_one();
    }

    /// Estrae un elemento. Se il buffer e' vuoto (count == 0), il thread si blocca
    /// finche' un produttore non inserisce qualcosa.
    pub fn extract(&self) -> T {
        let (lock, cvar_full, cvar_empty) = &*self.data;
        let mut state = lock.lock().unwrap();

        // 1. ATTESA: Il consumatore aspetta sulla condizione `not_empty` se e' vuoto.
        state = cvar_empty.wait_while(state, |c| c.count == 0).unwrap();

        // 2. AZIONE: Ci sono dati! Estraiamo.
        let value = state.pop_internal();
        
        // 3. RILASCIO E NOTIFICA: Rilasciamo il lock e svegliamo UN produttore
        // in attesa (su `cvar_full`) dicendogli "Ehi, ho appena liberato uno spazio!".
        drop(state);
        cvar_full.notify_one();
        
        value
    }

    /// Tenta di inserire un valore per un tempo massimo `dur`.
    /// Restituisce `true` in caso di successo, `false` se scade il timeout.
    pub fn try_insert_for(&self, value: T, dur: Duration) -> bool {
        let (lock, cvar_full, cvar_empty) = &*self.data;
        let mut state = lock.lock().unwrap();
        let mut res;

        // L'API della libreria standard `wait_timeout_while` gestisce automaticamente
        // il ricalcolo del tempo rimanente anche in caso di spurious wakeups!
        (state, res) = cvar_full.wait_timeout_while(state, dur, |c| c.count == N).unwrap();

        if res.timed_out() {
            // Tempo scaduto e condizione (count == N) ancora vera: fallimento.
            false
        } else {
            // Risvegliati con successo e condizione falsa (c'e' spazio): successo!
            state.push_internal(value);
            drop(state);
            cvar_empty.notify_one();
            true
        }
    }

    /// Tenta di estrarre un valore per un tempo massimo `dur`.
    /// Restituisce la tupla (Option<T>, TimeoutResult) richiesta dall'esame.
    pub fn try_extract_for(&self, dur: Duration) -> (Option<T>, bool) {
        let (lock, cvar_full, cvar_empty) = &*self.data;
        let mut state = lock.lock().unwrap();
        let mut res;

        (state, res) = cvar_empty.wait_timeout_while(state, dur, |c| c.count == 0).unwrap();

        if res.timed_out() {
            // Tempo scaduto e condizione (count == 0) ancora vera.
            (None, false)
        } else {
            // Abbiamo i dati!
            let value = state.pop_internal();
            drop(state);
            cvar_full.notify_one();
            (Some(value), true)
        }
    }
}

// -----------------------------------------------------------------------------
// TEST CAMPAIGN
// -----------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_basic_fifo_behavior() {
        // Buffer capacity 3
        let buffer: CircularBuffer<i32, 3> = CircularBuffer::new();
        
        buffer.insert(10);
        buffer.insert(20);
        
        assert_eq!(buffer.extract(), 10); // FIFO: 10 was inserted first
        
        buffer.insert(30);
        buffer.insert(40);
        
        assert_eq!(buffer.extract(), 20);
        assert_eq!(buffer.extract(), 30);
        assert_eq!(buffer.extract(), 40);
    }

    #[test]
    fn test_timeout_insert() {
        let buffer: CircularBuffer<i32, 2> = CircularBuffer::new();
        
        // Fill the buffer
        buffer.insert(1);
        buffer.insert(2);
        
        // Third insertion should fail due to timeout (since it's full)
        let success = buffer.try_insert_for(3, Duration::from_millis(50));
        assert_eq!(success, false);
        
        // Extract one element to make space
        assert_eq!(buffer.extract(), 1);
        
        // Now try_insert should succeed instantly
        let success_now = buffer.try_insert_for(3, Duration::from_millis(50));
        assert_eq!(success_now, true);
    }

    #[test]
    fn test_timeout_extract() {
        let buffer: CircularBuffer<i32, 5> = CircularBuffer::new();
        
        // Buffer is empty, extract should timeout
        let (val, success) = buffer.try_extract_for(Duration::from_millis(50));
        assert_eq!(success, false);
        assert_eq!(val, None);
        
        // Insert a value
        buffer.insert(99);
        
        // Now try_extract should succeed instantly
        let (val_now, success_now) = buffer.try_extract_for(Duration::from_millis(50));
        assert_eq!(success_now, true);
        assert_eq!(val_now, Some(99));
    }

    #[test]
    fn test_concurrent_producers_consumers() {
        // Buffer capacity 5
        let buffer: CircularBuffer<i32, 5> = CircularBuffer::new();
        
        let mut producer_handles = vec![];
        let mut consumer_handles = vec![];

        // Spawn 3 producers that each insert 100 items (300 total)
        for p in 0..3 {
            let buf_clone = buffer.clone();
            producer_handles.push(thread::spawn(move || {
                for i in 0..100 {
                    buf_clone.insert(p * 1000 + i);
                }
            }));
        }

        // Spawn 3 consumers that each extract 100 items (300 total)
        let sum = Arc::new(Mutex::new(0));
        for _ in 0..3 {
            let buf_clone = buffer.clone();
            let sum_clone = Arc::clone(&sum);
            consumer_handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let _val = buf_clone.extract();
                    let mut lock = sum_clone.lock().unwrap();
                    *lock += 1;
                }
            }));
        }

        for h in producer_handles { h.join().unwrap(); }
        for h in consumer_handles { h.join().unwrap(); }

        // We successfully extracted exactly 300 items without deadlocking!
        assert_eq!(*sum.lock().unwrap(), 300);
    }
}