use std::sync::{Condvar, Mutex};
use std::time::Instant;

/// Coda non limitata che permette di estrarre gli elementi solo dopo
/// che è trascorso un determinato istante di scadenza.
/// Essendo una struttura monolitica, non utilizza un `Arc` interno:
/// la condivisione tra thread è delegata all'utente (tramite `Arc` esterno o thread scoped).
pub struct DelayedQueue<T: Send> {
    // Il Mutex protegge i dati condivisi: un vettore di tuple (elemento, scadenza).
    queue: Mutex<Vec<(T, Instant)>>,
    // La Condvar serve a far dormire i thread consumatori quando la coda è vuota
    // o quando nessun elemento è ancora scaduto, senza consumare cicli CPU.
    cvar: Condvar
}

impl<T: Send> DelayedQueue<T> {
    /// Crea una nuova istanza vuota della coda ritardata.
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(Vec::new()),
            cvar: Condvar::new()
        }
    }

    /// Inserisce un elemento che non potrà essere estratto prima dell'istante `i`.
    pub fn offer(&self, element: T, i: Instant) {
        let mut q = self.queue.lock().unwrap();
        q.push((element, i));
        
        // Notifichiamo TUTTI i thread in attesa.
        // Perché notify_all e non notify_one? 
        // Perché il nuovo elemento potrebbe avere una scadenza più ravvicinata 
        // rispetto a quella che stavano aspettando i thread bloccati, quindi
        // devono svegliarsi, ricalcolare il minimo, e aggiornare il loro wait_timeout!
        self.cvar.notify_all();
    }

    /// Cerca ed estrae l'elemento con la scadenza più ravvicinata.
    pub fn take(&self) -> Option<T> {
        let mut q = self.queue.lock().unwrap();

        // Se la coda è vuota subito, ritorniamo None.
        if q.is_empty() {
            return None;
        }

        loop {
            // Troviamo l'elemento con la scadenza minima.
            // Mappiamo il risultato per estrarre copie (index e time) e sganciare il borrow da `q`.
            let min_info = q.iter()
                .enumerate()
                .min_by_key(|(_, (_, time))| *time)
                .map(|(idx, (_, time))| (idx, *time));

            if let Some((index, time)) = min_info {
                let now = Instant::now();
                
                if time <= now {
                    // La scadenza è già passata: estraiamo l'elemento e lo ritorniamo.
                    // Utilizziamo q.remove(index) invece di scontrarci con le regole del borrow checker.
                    let (element, _) = q.remove(index);
                    return Some(element);
                } else {
                    // La scadenza non è ancora passata: calcoliamo quanto manca.
                    // Usiamo `saturating_duration_since` per evitare panici di underflow 
                    // nel caso rarissimo in cui il tempo sia avanzato superando `time` 
                    // tra la riga `Instant::now()` e questa riga.
                    let wait_time = time.saturating_duration_since(now);
                    
                    // Mettiamo il thread in attesa fino alla scadenza o fino a una nuova notifica.
                    // wait_timeout ritorna (MutexGuard, WaitTimeoutResult). Estraiamo il guard (.0).
                    q = self.cvar.wait_timeout(q, wait_time).unwrap().0;
                    
                    // Al risveglio, il loop riparte automaticamente:
                    // 1. Se siamo stati svegliati dal timeout, la scadenza minima sarà <= now (vittoria!).
                    // 2. Se siamo stati svegliati da un nuovo offer(), ricalcoliamo il minimo.
                }
            } else {
                // Se la coda si svuota (magari da un altro thread) mentre eravamo in attesa.
                return None;
            }
        }
    }

    /// Restituisce il numero totale di elementi in coda, scaduti e non.
    pub fn size(&self) -> usize {
        let q = self.queue.lock().unwrap();
        q.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_size_and_empty() {
        let q = DelayedQueue::new();
        assert_eq!(q.size(), 0);
        assert!(q.take().is_none());
        
        q.offer(10, Instant::now() + Duration::from_millis(100));
        assert_eq!(q.size(), 1);
    }

    #[test]
    fn test_take_expired_immediately() {
        let q = DelayedQueue::new();
        // Inseriamo un elemento già scaduto da 1 secondo.
        q.offer(42, Instant::now() - Duration::from_secs(1));
        
        assert_eq!(q.take(), Some(42));
        assert_eq!(q.size(), 0);
    }

    #[test]
    fn test_take_waits_for_expiration() {
        let q = Arc::new(DelayedQueue::new());
        let delay = Duration::from_millis(200);
        
        q.offer(99, Instant::now() + delay);
        
        let start = Instant::now();
        let res = q.take(); // Dovrebbe bloccare il thread senza consumare CPU per ~200ms
        let elapsed = start.elapsed();
        
        assert_eq!(res, Some(99));
        assert!(elapsed >= delay, "take() è ritornato troppo presto!");
    }

    #[test]
    fn test_new_offer_wakes_up_waiter() {
        let q = Arc::new(DelayedQueue::new());
        
        // Elemento con scadenza lontanissima (10 secondi).
        // Se `take()` non venisse svegliato, il test rimarrebbe appeso per 10 secondi.
        q.offer("Tardi", Instant::now() + Duration::from_secs(10));
        
        let q_clone = Arc::clone(&q);
        
        let consumer = thread::spawn(move || {
            // Il consumatore aspetterà l'elemento più vicino.
            // Quando verrà svegliato dal nuovo elemento, ricalcolerà la scadenza
            // e restituirà quello appena aggiunto (perché ha una scadenza più vicina).
            q_clone.take()
        });
        
        thread::sleep(Duration::from_millis(100));
        
        // Elemento con scadenza immediata
        q.offer("Presto", Instant::now());
        
        let res = consumer.join().unwrap();
        assert_eq!(res, Some("Presto")); // Estrae quello più vicino, confermando che il ricalcolo funziona!
        assert_eq!(q.size(), 1); // L'elemento "Tardi" è ancora in coda.
    }
    
    #[test]
    fn test_multiple_consumers() {
        let q = Arc::new(DelayedQueue::new());
        
        // Inseriamo due elementi con scadenze diverse
        let now = Instant::now();
        q.offer(1, now + Duration::from_millis(100));
        q.offer(2, now + Duration::from_millis(200));
        
        let q1 = Arc::clone(&q);
        let q2 = Arc::clone(&q);
        
        let t1 = thread::spawn(move || q1.take());
        let t2 = thread::spawn(move || q2.take());
        
        let res1 = t1.join().unwrap();
        let res2 = t2.join().unwrap();
        
        // Un thread prenderà l'1, l'altro prenderà il 2. L'ordine dipende dallo scheduler.
        assert!(res1 == Some(1) || res1 == Some(2));
        assert!(res2 == Some(1) || res2 == Some(2));
        assert_ne!(res1, res2); // Non devono mai estrarre lo stesso elemento.
        assert_eq!(q.size(), 0);
    }
}