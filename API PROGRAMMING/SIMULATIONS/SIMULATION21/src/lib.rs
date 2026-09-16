//! # Simulazione 021 — TtlCache (Capstone)
//!
//! Una cache di risposte (ad esempio per un resolver DNS, o per risultati di query costose) deve:
//! - Evitare di ricalcolare un valore già noto e ancora valido.
//! - Evitare che più richieste concorrenti per la stessa chiave mancante scatenino calcoli duplicati (Single-Flight).
//! - Scadere automaticamente le voci più vecchie di un termine temporale dato (`ttl`).
//! - Quando la capacità massima è esaurita, fare spazio rimuovendo prima le voci scadute e, se non bastasse, quella usata meno di recente (LRU).
//!
//! Si scriva in Rust una struttura che implementi il tratto generico `TtlCache<K, V>` definito di seguito.
//!
//! ### API richiesta
//!
//! ```text
//! use std::time::Duration;
//! use std::hash::Hash;
//!
//! pub trait TtlCache<K: Eq + Hash + Clone + Send, V: Clone + Send>: Clone + Send + Sync {
//!     fn get_or_compute(&self, key: K, ttl: Duration, compute: impl FnOnce() -> V) -> V;
//!     fn len(&self) -> usize;
//! }
//!
//! pub fn make_ttl_cache<K: Eq + Hash + Clone + Send + Sync + 'static, V: Clone + Send + Sync + 'static>(
//!     capacity: usize,
//! ) -> impl TtlCache<K, V> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - Thread-safe, condivisibile (`Clone + Send + Sync`).
//! - `compute` viene invocata al più una volta per ciascun episodio di chiave mancante o scaduta.
//! - Il lock interno non deve mai essere mantenuto durante l'esecuzione di `compute`.
//! - L'espulsione per capacità deve preferire sempre una voce scaduta rispetto a una valida; a parità di validità, espelle la LRU.
//! - Letture di voci valide non devono mai bloccare.
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Trait che rappresenta una cache concorrente avanzata con scadenza temporale (TTL), coalescing delle richieste (Single-Flight) ed espulsione LRU per capacità.
pub trait TtlCache<K: Eq + Hash + Clone + Send, V: Clone + Send>: Clone + Send + Sync {
    /// Restituisce il valore associato a `key`.
    /// - Se `key` è presente e non scaduta, ritorna immediatamente e aggiorna il timestamp di accesso per LRU.
    /// - Se `key` è assente o scaduta, e nessun altro thread la sta calcolando, invoca `compute()` senza lock e memorizza il risultato con il `ttl` specificato.
    /// - Se un altro thread la sta già calcolando, attende il completamento senza consumo di CPU e ne restituisce il risultato.
    /// - Se la capacità massima è raggiunta, espelle prioritariamente una voce scaduta oppure la voce LRU.
    fn get_or_compute(&self, key: K, ttl: Duration, compute: impl FnOnce() -> V) -> V;

    /// Restituisce il numero di voci pronte effettivamente memorizzate in cache (esclusi i calcoli in corso).
    fn len(&self) -> usize;
}

// =================================================================================
// 🛠️ IMPLEMENTAZIONE TTL CACHE CON CONDVAR PER-CHIAVE (STUDENT WORKSPACE)
// =================================================================================

/// Cella di sincronizzazione dedicata al calcolo concorrente di una specifica chiave.
/// Consente ai thread follower di attendere solo il completamento di questa precisa chiave,
/// eliminando il fenomeno del "thundering herd" (risveglio a sciame di thread non correlati).
type InProgressCell<V> = Arc<(Mutex<Option<V>>, Condvar)>;

/// Rappresentazione esplicita a stati finiti per ciascuna voce della cache.
enum CacheEntry<V> {
    /// Un thread leader sta attualmente calcolando il valore per questa chiave.
    /// I follower attendono sulla `Condvar` specifica della cella.
    InProgress(InProgressCell<V>),

    /// La voce è calcolata, memorizzata e pronta all'uso.
    Ready {
        /// Il valore calcolato e memorizzato in cache.
        value: V,
        /// Istante assoluto oltre il quale il valore è considerato scaduto (`insertion_time + ttl`).
        expires_at: Instant,
        /// Istante dell'ultimo accesso effettuato su questa voce (utilizzato per la politica LRU).
        last_accessed: Instant,
    },
}

/// Cache concorrente con supporto TTL, Single-Flight e LRU.
pub struct MyTtlCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Mappa condivisa globale protetta da Mutex.
    map: Arc<Mutex<HashMap<K, CacheEntry<V>>>>,
    /// Capacità massima di voci pronte consentite in cache.
    capacity: usize,
}

impl<K, V> MyTtlCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    /// Inizializza una nuova cache con la capacità massima specificata.
    pub fn new(capacity: usize) -> Self {
        Self {
            map: Arc::new(Mutex::new(HashMap::new())),
            capacity,
        }
    }
}

impl<K, V> TtlCache<K, V> for MyTtlCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn get_or_compute(&self, key: K, ttl: Duration, compute: impl FnOnce() -> V) -> V {
        // =========================================================================
        // FASE 1: CONTROLLO DELLO STATO SOTTO IL LOCK GLOBALE DELLA MAPPA
        // =========================================================================
        let mut guard = self.map.lock().unwrap();

        let in_progress_cell = match guard.get_mut(&key) {
            // CASO 1: La voce è già pronta in cache
            Some(CacheEntry::Ready {
                value,
                expires_at,
                last_accessed,
            }) => {
                let now = Instant::now();
                if *expires_at > now {
                    // La voce è valida e non scaduta: aggiorna l'ultimo accesso per LRU e ritorna subito
                    *last_accessed = now;
                    return value.clone();
                }
                else {
                    // La voce è scaduta: questo thread diventa il leader del ricalcolo
                    let cell = Arc::new((Mutex::new(None), Condvar::new()));
                    guard.insert(key.clone(), CacheEntry::InProgress(Arc::clone(&cell)));
                    cell
                }
            }

            // CASO 2: Un altro thread sta già calcolando il valore per questa chiave (Single-Flight follower)
            Some(CacheEntry::InProgress(cell)) => {
                let cell_clone = Arc::clone(cell);
                // 👈 Rilasciamo il lock globale della mappa prima di metterci in attesa passiva
                drop(guard);

                // Attesa passiva sulla Condvar dedicata a QUESTA singola chiave
                let (cell_mutex, cell_cvar) = &*cell_clone;
                let mut cell_guard = cell_mutex.lock().unwrap();
                cell_guard = cell_cvar.wait_while(cell_guard, |opt|
                    opt.is_none()
                ).unwrap();

                // Valore calcolato dal leader pronto
                let value = cell_guard.as_ref().unwrap();
                return value.clone();
            }

            // CASO 3: La chiave è totalmente assente nella mappa
            None => {
                let cell = Arc::new((Mutex::new(None), Condvar::new()));
                guard.insert(key.clone(), CacheEntry::InProgress(Arc::clone(&cell)));
                cell
            }
        };

        // =========================================================================
        // FASE 2: ESECUZIONE DEL CALCOLO (LEADER) SENZA ALCUN LOCK
        // =========================================================================
        // Rilasciamo il lock globale della mappa prima di invocare `compute()`
        drop(guard);

        // Calcolo potenzialmente costoso eseguito fuori da qualsiasi lock
        let computed_value = compute();

        // =========================================================================
        // FASE 3: ESPULSIONE PER CAPACITÀ E AGGIORNAMENTO STATO
        // =========================================================================
        let mut guard = self.map.lock().unwrap();
        let now = Instant::now();

        // Conta quante voci "Ready" sono attualmente presenti in cache (esclude i calcoli InProgress)
        let ready_count = guard
            .values()
            .filter(|e| matches!(e, CacheEntry::Ready { .. }))
            .count();

        // Se la capacità massima è stata raggiunta, effettuiamo lo sfratto (eviction)
        if ready_count >= self.capacity {
            // 1. Priorità massima: cerca se esiste almeno una voce già scaduta
            let expired_victim = guard.iter().find_map(|(k, entry)|
                match entry {
                    CacheEntry::Ready { expires_at, .. } if *expires_at <= now => Some(k.clone()),
                    _ => None,
                }
            );

            if let Some(expired_key) = expired_victim {
                guard.remove(&expired_key);
            }
            else {
                // 2. Se non ci sono voci scadute, sfratta la voce Least Recently Used (LRU)
                let lru_victim = guard
                    .iter()
                    .filter_map(|(k, entry)| 
                        match entry {
                            CacheEntry::Ready { last_accessed, .. } => {
                                Some((k.clone(), *last_accessed))
                            }
                            _ => None,
                        }
                    )
                    .min_by_key(|(_, last_accessed)| *last_accessed)
                    .map(|(k, _)| k);

                if let Some(lru_key) = lru_victim {
                    guard.remove(&lru_key);
                }
            }
        }

        // Inserisce la voce pronta con il proprio TTL fisso e il timestamp di accesso
        guard.insert(
            key,
            CacheEntry::Ready {
                value: computed_value.clone(),
                expires_at: now + ttl,
                last_accessed: now,
            },
        );

        // Rilascia il lock globale della mappa
        drop(guard);

        // =========================================================================
        // FASE 4: NOTIFICA DEI FOLLOWER IN ATTESA SU QUESTA CHIAVE
        // =========================================================================
        let (cell_mutex, cell_cvar) = &*in_progress_cell;
        let mut cell_guard = cell_mutex.lock().unwrap();
        *cell_guard = Some(computed_value.clone());
        drop(cell_guard);

        // Risveglia solo i thread in attesa per questa specifica chiave
        cell_cvar.notify_all();

        computed_value
    }

    /// Restituisce il numero di voci pronte memorizzate in cache (esclusi i calcoli in corso).
    fn len(&self) -> usize {
        let guard = self.map.lock().unwrap();
        guard
            .values()
            .filter(|e| matches!(e, CacheEntry::Ready { .. }))
            .count()
    }
}

impl<K, V> Clone for MyTtlCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            map: Arc::clone(&self.map),
            capacity: self.capacity,
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `TtlCache` con la capacità massima specificata.
pub fn make_ttl_cache<
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
>(
    capacity: usize,
) -> impl TtlCache<K, V> {
    MyTtlCache::new(capacity)
}
