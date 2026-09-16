//! # Simulazione 015 — SingleFlightCache (Crate Root)
//!
//! Questo modulo radice definisce il tratto pubblico `SingleFlightCache` ed esporta due implementazioni concorrenti:
//!
//! 1. **[`global_cvar_cache`]**: Implementazione classica basata su `Mutex<HashMap<K, Option<V>>>` e `Condvar` globale.
//! 2. **[`per_key_cvar_cache`]**: Implementazione ad alte prestazioni con **notifica selettiva per-chiave** (`Arc<(Mutex, Condvar)>` dedicato), azzerando i risvegli spuri tra chiavi diverse.

pub mod global_cvar_cache;
pub mod per_key_cvar_cache;

/// Trait che rappresenta una cache concorrente con aggregazione delle richieste (SingleFlight / Request Coalescing).
pub trait SingleFlightCache<K, V: Clone + Send>: Clone + Send + Sync {
    /// Restituisce il valore associato a `key`.
    ///
    /// - Se il valore è già presente in cache, ritorna immediatamente, senza bloccare.
    /// - Se nessun altro thread sta già calcolando il valore per questa chiave, il chiamante stesso
    ///   diventa responsabile del calcolo: invoca `compute()` — che può richiedere un tempo arbitrario —
    ///   SENZA mantenere alcun lock durante la sua esecuzione, così da non bloccare richieste concorrenti
    ///   su chiavi diverse. Al termine, memorizza il risultato in cache e risveglia chiunque fosse in attesa
    ///   dello stesso calcolo.
    /// - Se invece un altro thread sta già calcolando il valore per la stessa chiave, il chiamante attende,
    ///   senza consumare cicli di CPU, che quel calcolo termini, quindi restituisce il risultato prodotto
    ///   da quell'altro thread, senza ricalcolarlo.
    fn get_or_compute(&self, key: K, compute: impl FnOnce() -> V) -> V;
}

// Re-export dei costruttori
pub use global_cvar_cache::make_global_cvar_cache;
pub use per_key_cvar_cache::make_per_key_cvar_cache;

/// Costruttore predefinito (punto di ingresso standard a massime prestazioni con notifica selettiva per-chiave).
pub fn make_single_flight_cache<K, V>() -> impl SingleFlightCache<K, V>
where
    K: Eq + std::hash::Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    make_per_key_cvar_cache()
}
