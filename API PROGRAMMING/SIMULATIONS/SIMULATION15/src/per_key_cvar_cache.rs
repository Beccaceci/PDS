//! # Implementazione 2: SingleFlightCache con Condvar Dedicata Per-Key (`per_key_cvar_cache.rs`)
//!
//! Questa implementazione risolve alla radice il problema del **Thundering Herd** tra chiavi diverse:
//!
//! ## Architettura a Sincronizzazione Mirata:
//! 1. **Stato della Voce (`Entry<V>`)**:
//!    - `InFlight(Arc<(Mutex<Option<V>>, Condvar)>)`: quando il calcolo è in corso, memorizza una cella
//!      di sincronizzazione dedicata esclusivamente a quella specifica chiave.
//!    - `Ready(V)`: quando il calcolo è terminato, memorizza direttamente il valore finale.
//! 2. **Isolamento Totale delle Notifiche**:
//!    - I thread in attesa della chiave `"a"` si sospendono **solo e soltanto** sulla `Condvar` associata ad `"a"`.
//!    - I thread in attesa della chiave `"b"` si sospendono su una `Condvar` distinta.
//!    - Quando il calcolo di `"a"` termina, la notifica `cvar.notify_all()` risveglia **esclusivamente**
//!      i thread interessati ad `"a"`, senza alcun risveglio spurio o disturbo per i thread in attesa di altre chiavi.
//! 3. **Concorrenza di Mappa Massima**:
//!    - Il lock della mappa globale viene trattenuto per tempi brevissimi ($O(1)$) unicamente per leggere o
//!      aggiornare il puntatore `Entry<V>`, rilasciandolo immediatamente prima di attendere sulla `Condvar` locale.

use super::SingleFlightCache;
use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};

/// Rappresenta lo stato di una voce nella cache.
enum Entry<V> {
    /// Calcolo in corso: possiede un blocco Mutex+Condvar dedicato a cui agganciarsi in attesa del risultato.
    InFlight(Arc<(Mutex<Option<V>>, Condvar)>),
    /// Calcolo completato: valore pronto e memorizzato stabilmente in cache.
    Ready(V),
}

/// Cache avanzata con notifica selettiva per-chiave.
pub struct PerKeyCvarCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    map: Arc<Mutex<HashMap<K, Entry<V>>>>,
}

impl<K, V> PerKeyCvarCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Crea una nuova istanza di cache per-key vuota.
    pub fn new() -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<K, V> SingleFlightCache<K, V> for PerKeyCvarCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn get_or_compute(&self, key: K, compute: impl FnOnce() -> V) -> V {
        let mut map_guard = self.map.lock().unwrap();

        match map_guard.get(&key) {
            // 1. CACHE HIT: il valore è già calcolato e pronto in cache
            Some(Entry::Ready(val)) => val.clone(),

            // 2. IN FLIGHT: un altro thread sta già calcolando il valore per questa chiave
            Some(Entry::InFlight(cell)) => {
                let cell = Arc::clone(cell);
                // Rilascia immediatamente il lock della mappa globale per non bloccare altre chiavi
                drop(map_guard);

                // Si sospende unicamente sulla Condvar dedicata a QUESTA chiave
                let (mutex, cvar) = &*cell;
                let mut cell_guard = mutex.lock().unwrap();
                cell_guard = cvar
                    .wait_while(cell_guard, |val_opt| val_opt.is_none())
                    .unwrap();

                // Estrae il valore calcolato dall'altro thread senza rieseguire il calcolo
                cell_guard.as_ref().unwrap().clone()
            }

            // 3. MISS: nessuno sta calcolando questa chiave; questo thread diventa il Worker incaricato
            None => {
                // Alloca una cella di sincronizzazione dedicata per questa specifica chiave
                let cell = Arc::new((Mutex::new(None), Condvar::new()));
                map_guard.insert(key.clone(), Entry::InFlight(Arc::clone(&cell)));
                // Rilascia subito il lock globale della mappa prima di avviare il calcolo
                drop(map_guard);

                // Esegue il calcolo costoso SENZA MANTENERE ALCUN LOCK
                let computed = compute();

                // 1. Scrive il risultato nella cella dedicata e notifica SOLO i thread in attesa di QUESTA chiave
                let (mutex, cvar) = &*cell;
                let mut cell_guard = mutex.lock().unwrap();
                *cell_guard = Some(computed.clone());
                drop(cell_guard);
                cvar.notify_all(); // 👈 Notifica mirata: zero risvegli spuri su chiavi diverse!

                // 2. Aggiorna la voce nella mappa globale con il valore definitivo Ready
                let mut map_guard = self.map.lock().unwrap();
                map_guard.insert(key, Entry::Ready(computed.clone()));
                drop(map_guard);

                computed
            }
        }
    }
}

impl<K, V> Clone for PerKeyCvarCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            map: Arc::clone(&self.map),
        }
    }
}

/// Costruttore pubblico per la versione con Condvar dedicata per-chiave.
pub fn make_per_key_cvar_cache<K, V>() -> impl SingleFlightCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    PerKeyCvarCache::new()
}
