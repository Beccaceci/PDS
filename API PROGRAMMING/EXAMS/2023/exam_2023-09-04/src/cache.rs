use std::{collections::HashMap, hash::Hash, sync::{Arc, RwLock}, time::{Duration, Instant}};

/// Una cache thread-safe che memorizza coppie chiave/valore con scadenza.
/// Permette letture simultanee e scritture esclusive.
pub struct Cache<K: Eq + Hash + Clone, V> {
    // Usiamo RwLock per favorire i lettori multipli concorrenti (get/size).
    // Il valore memorizzato è una tupla: (Dato protetto da Arc, Istante di Scadenza)
    map: RwLock<HashMap<K, (Arc<V>, Instant)>>
}

impl<K: Eq + Hash + Clone, V> Cache<K, V> {
    /// Crea una nuova istanza vuota.
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new())
        }
    }

    /// Esegue il ciclo di pulizia richiesto dal testo.
    /// Viene invocato privatamente durante le operazioni di scrittura (put, renew).
    fn cleanup(&self, guard: &mut std::sync::RwLockWriteGuard<'_, HashMap<K, (Arc<V>, Instant)>>) {
        let now = Instant::now();
        // `retain` itera su tutta la mappa e mantiene SOLO gli elementi per cui 
        // la closure restituisce `true`. Rimuove tutto ciò che è già scaduto.
        guard.retain(|_, (_, timestamp)| *timestamp > now);
    }

    /// Restituisce il numero di coppie FISICAMENTE presenti nella mappa 
    /// (incluse quelle eventualmente scadute ma non ancora rimosse dal ciclo di pulizia).
    pub fn size(&self) -> usize {
        let guard = self.map.read().unwrap();
        guard.len()
    }

    /// Inserisce la coppia chiave/valore con una durata di validità pari a `_d`.
    pub fn put(&self, _k: K, _v: V, _d: Duration) {
        let mut guard = self.map.write().unwrap();
        
        // Requisito fondamentale dell'esame: per evitare saturazione,
        // eseguiamo la pulizia prima di (o durante) ogni scrittura!
        self.cleanup(&mut guard);

        let time = Instant::now() + _d;
        
        // ERRORE PRECEDENTE: Avevi usato guard.get().replace(...) che operava 
        // su una Option temporanea e non modificava la HashMap.
        // SOLUZIONE: Il metodo `insert` della HashMap sovrascrive AUTOMATICAMENTE 
        // l'elemento se la chiave esiste già. Non servono if-else!
        guard.insert(_k, (Arc::new(_v), time));
    }

    /// Rinnova la durata dell'elemento rappresentato dalla chiave `_k`.
    pub fn renew(&self, _k: &K, _d: Duration) -> bool {
        let mut guard = self.map.write().unwrap();
        
        // Eseguiamo la pulizia per evitare saturazione, come richiesto.
        self.cleanup(&mut guard);
        
        // ERRORE PRECEDENTE: Avevi provato a usare get().replace().
        // SOLUZIONE: Usiamo `get_mut()` per ottenere una reference mutabile al valore
        // e modifichiamo direttamente l'Instant (timestamp) in-place senza toccare l'Arc!
        if let Some((_, timestamp)) = guard.get_mut(_k) {
            let now = Instant::now();
            if *timestamp > now {
                // Aggiorniamo il timestamp sul posto.
                *timestamp = now + _d;
                return true;
            }   
        }
        false
    }

    /// Restituisce il valore se presente e non scaduto.
    pub fn get(&self, _k: &K) -> Option<Arc<V>> {
        // Accesso in LETTURA (read) per permettere a più thread di cercare contemporaneamente!
        let guard = self.map.read().unwrap();

        if let Some((value, timestamp)) = guard.get(_k) {
            if *timestamp > Instant::now() {
                // Ritorna un nuovo Arc che punta allo stesso valore
                return Some(Arc::clone(value));
            }   
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_put_and_get() {
        let cache = Cache::new();
        cache.put("chiave", "valore", Duration::from_secs(5));
        
        let result = cache.get(&"chiave");
        assert_eq!(result.map(|arc| *arc), Some("valore"));
    }

    #[test]
    fn test_expiration() {
        let cache = Cache::new();
        cache.put("chiave", "valore", Duration::from_millis(100));
        
        assert!(cache.get(&"chiave").is_some());
        
        thread::sleep(Duration::from_millis(150));
        
        // Dopo l'attesa, l'elemento è logicamente scaduto
        assert!(cache.get(&"chiave").is_none());
        // Fisicamente è ancora nella mappa (dimensione 1)
        assert_eq!(cache.size(), 1);
    }

    #[test]
    fn test_cleanup_on_write() {
        let cache = Cache::new();
        cache.put("chiave1", "valore1", Duration::from_millis(100));
        
        thread::sleep(Duration::from_millis(150));
        
        // Adesso chiave1 è scaduta, ma size è ancora 1
        assert_eq!(cache.size(), 1);
        
        // Questa operazione di SCRITTURA scatenerà il cleanup()
        cache.put("chiave2", "valore2", Duration::from_secs(5));
        
        // Ora chiave1 è stata rimossa fisicamente, size è 1 (solo chiave2)
        assert_eq!(cache.size(), 1);
        assert!(cache.get(&"chiave1").is_none());
    }

    #[test]
    fn test_renew() {
        let cache = Cache::new();
        cache.put("chiave", "valore", Duration::from_millis(200));
        
        thread::sleep(Duration::from_millis(100)); // Aspetta 100ms
        
        // Rinnova per altri 300ms
        let success = cache.renew(&"chiave", Duration::from_millis(300));
        assert!(success);
        
        thread::sleep(Duration::from_millis(150)); // Aspetta 150ms
        
        // Se non avessimo rinnovato, sarebbe scaduto (100+150 = 250 > 200).
        // Poiché abbiamo rinnovato, ora ha 300ms dal rinnovo, quindi è vivo!
        assert!(cache.get(&"chiave").is_some());
    }

    #[test]
    fn test_renew_fails_if_expired() {
        let cache = Cache::new();
        cache.put("chiave", "valore", Duration::from_millis(100));
        
        thread::sleep(Duration::from_millis(150));
        
        // Il rinnovo deve fallire su elementi già scaduti
        let success = cache.renew(&"chiave", Duration::from_secs(5));
        assert_eq!(success, false);
    }

    #[test]
    fn test_concurrent_reads() {
        let cache = Arc::new(Cache::new());
        cache.put("k", "v", Duration::from_secs(10));

        let mut handles = vec![];
        for _ in 0..10 {
            let c = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                // I thread possono leggere in modo concorrente grazie all'RwLock
                for _ in 0..1000 {
                    assert!(c.get(&"k").is_some());
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }
}