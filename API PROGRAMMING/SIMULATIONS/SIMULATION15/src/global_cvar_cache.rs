//! # Implementazione 1: SingleFlightCache con Condvar Globale (`global_cvar_cache.rs`)
//!
//! Questa implementazione rappresenta l'approccio standard a stato compatto:
//! - Mantiene un `Mutex<HashMap<K, Option<V>>>` e una singola `Condvar` globale condivisa.
//! - `None` rappresenta una richiesta in corso di calcolo (*InFlight*).
//! - `Some(V)` rappresenta il valore pronto in cache (*Ready*).
//! - Durante l'esecuzione di `compute()`, il lock della mappa viene rilasciato per non bloccare altre chiavi.
//! - Al termine del calcolo, `cvar.notify_all()` risveglia tutti i thread in attesa sulla coda globale.

use super::SingleFlightCache;
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};

/// Cache con sincronizzazione basata su Condvar globale.
pub struct GlobalCvarCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    inner: Arc<(Mutex<HashMap<K, Option<V>>>, Condvar)>,
}

impl<K, V> GlobalCvarCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Inizializza una nuova cache vuota.
    pub fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new(HashMap::new()), Condvar::new())),
        }
    }
}

impl<K, V> SingleFlightCache<K, V> for GlobalCvarCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn get_or_compute(&self, key: K, compute: impl FnOnce() -> V) -> V {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        loop {
            if let Some(option_value) = guard.get(&key) {
                if let Some(value) = option_value {
                    // Cache Hit immediato
                    return value.clone();
                }
            } else {
                // Primo thread: inserisce lo stato "In corso" (None)
                guard.insert(key.clone(), None);
                // Rilascia il lock per consentire l'elaborazione concorrente su chiavi diverse
                drop(guard);

                // Calcolo costoso
                let value = compute();

                // Riacquisisce il lock e salva il risultato
                guard = mutex.lock().unwrap();
                guard.insert(key, Some(value.clone()));
                drop(guard);

                // Notifica tutti i thread in attesa
                cvar.notify_all();
                return value;
            }

            // Attende che il valore per questa chiave sia pronto
            guard = cvar
                .wait_while(guard, |c| {
                    if let Some(option_v) = c.get(&key) {
                        if option_v.is_some() {
                            return false;
                        }
                    }
                    true
                })
                .unwrap();
        }
    }
}

impl<K, V> Clone for GlobalCvarCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Costruttore pubblico per la versione a Condvar globale.
pub fn make_global_cvar_cache<K, V>() -> impl SingleFlightCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    GlobalCvarCache::new()
}
