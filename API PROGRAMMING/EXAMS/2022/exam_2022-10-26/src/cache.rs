use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Condvar, Mutex};

/// `EntryState` rappresenta lo stato di una specifica chiave all'interno della Cache.
/// Essendo un sistema multi-thread, non possiamo solo sapere se un valore c'è o non c'è.
/// Dobbiamo anche sapere se qualcuno lo sta attualmente calcolando!
enum EntryState<V> {
    /// Indica che un thread ha richiesto la chiave e sta attualmente eseguendo la funzione f().
    /// Gli altri thread che richiedono questa chiave dovranno mettersi in attesa.
    Pending,
    /// Indica che il calcolo è terminato ed è disponibile il risultato finale incapsulato in un Arc.
    Present(Arc<V>)
}

/// Il componente Cache.
/// Richiede che le chiavi `K` siano confrontabili (`Eq`) e hashabili (`Hash`) per poter 
/// essere usate in una `HashMap`. Devono anche essere `Clone` in modo da poter essere passate
/// sia alla mappa che alla funzione `f`.
pub struct Cache<K: Eq + Hash + Clone, V> {
    // Lo stato condiviso è protetto da un Mutex.
    // La Condvar è usata per mettere in pausa i thread che richiedono una chiave 'Pending'.
    data: Arc<(Mutex<HashMap<K, EntryState<V>>>, Condvar)>
}

impl<K: Eq + Hash + Clone, V> Cache<K, V> {
    pub fn new() -> Self {
        Self {
            data: Arc::new((Mutex::new(HashMap::new()), Condvar::new()))
        }
    }

    /// Ottiene un valore dalla cache. Se non esiste, esegue `f(k)` per calcolarlo.
    /// Garantisce che `f(k)` venga chiamata UNA SOLA VOLTA per ogni chiave `k`, 
    /// anche se molti thread la richiedono contemporaneamente.
    pub fn get(&self, _k: K, f: fn(K) -> V) -> Arc<V> {
        let (lock, cvar) = &*self.data;
        let mut state = lock.lock().unwrap();
        
        // 1. ATTESA SULLA CONDVAR
        // Usiamo wait_while per addormentare il thread se la chiave è nello stato Pending.
        // Il thread si sveglierà e controllerà nuovamente la condizione solo quando riceverà un notify.
        state = cvar.wait_while(state, |c| {
            matches!(c.get(&_k), Some(EntryState::Pending))
        }).unwrap();

        // 2. CONTROLLO DELLO STATO ATTUALE
        // A questo punto, la chiave PUÒ ESSERE solo in due stati:
        // - Presente (qualcuno l'ha calcolata o eravamo addormentati e ora è pronta)
        // - Assente (nessuno l'ha mai richiesta)
        if let Some(EntryState::Present(value)) = state.get(&_k) {
            // Il valore è pronto! Ne restituiamo un clone del puntatore (molto economico).
            value.clone()
        }
        else {
            // La chiave non c'è. SIAMO IL PRIMO THREAD a richiederla!
            
            // A) Prenotiamo la chiave inserendo Pending
            state.insert(_k.clone(), EntryState::Pending);
            
            // B) Rilasciamo il Mutex! Questo è fondamentale, altrimenti bloccheremmo
            // l'intera Cache per tutti gli altri thread mentre eseguiamo f().
            drop(state);
            
            // C) Eseguiamo il calcolo pesante
            let result = Arc::new(f(_k.clone()));
            
            // D) Ri-acquisiamo il lock per aggiornare lo stato
            let mut state = lock.lock().unwrap();
            
            // E) Sostituiamo Pending con il risultato finale Present
            state.insert(_k, EntryState::Present(result.clone()));
            
            // F) SVEGLIAMO TUTTI I THREAD! 
            // Quelli che erano in wait_while per questa chiave si sveglieranno e troveranno Present.
            cvar.notify_all();
            
            result
        }
    }
}

// ==========================================
// CAMPAGNA DI TEST
// ==========================================
#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Variabile globale per contare quante volte la funzione pesante viene eseguita
    static CALCULATION_COUNT: AtomicUsize = AtomicUsize::new(0);

    // Funzione "pesante" simulata
    fn expensive_calculation(k: String) -> String {
        CALCULATION_COUNT.fetch_add(1, Ordering::SeqCst);
        thread::sleep(Duration::from_millis(500)); // Simula mezzo secondo di lavoro
        format!("Risultato di {}", k)
    }

    #[test]
    fn test_cache_concurrency() {
        // Resettiamo il contatore
        CALCULATION_COUNT.store(0, Ordering::SeqCst);
        
        let cache = Arc::new(Cache::new());
        let mut handles = vec![];

        // Lanciamo 10 thread che richiedono contemporaneamente la STESSA CHIAVE "chiave_1"
        for _ in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                let res = cache_clone.get("chiave_1".to_string(), expensive_calculation);
                assert_eq!(*res, "Risultato di chiave_1");
            });
            handles.push(handle);
        }

        // Lanciamo 5 thread che richiedono contemporaneamente una CHIAVE DIVERSA "chiave_2"
        for _ in 0..5 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                let res = cache_clone.get("chiave_2".to_string(), expensive_calculation);
                assert_eq!(*res, "Risultato di chiave_2");
            });
            handles.push(handle);
        }

        // Attendiamo che tutti i 15 thread finiscano
        for handle in handles {
            handle.join().unwrap();
        }

        // VERIFICA CRUCIALE: La funzione pesante DEVE essere stata chiamata 
        // esattamente 2 volte (una per "chiave_1" e una per "chiave_2"), 
        // NON 15 volte!
        assert_eq!(CALCULATION_COUNT.load(Ordering::SeqCst), 2);
    }
}