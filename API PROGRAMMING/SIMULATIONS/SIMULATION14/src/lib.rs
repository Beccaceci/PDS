//! # Simulazione 014 — VersionedCell
//!
//! *Rottura deliberata di un pattern che si è consolidato in tutte e 13 le simulazioni precedenti: qui non serve alcun `Condvar`, e usarne uno sarebbe un errore concettuale. Questa simulazione insegna il modello della **concorrenza ottimistica (Optimistic Concurrency Control — OCC)** e il pattern **Read-Copy-Update (RCU)** con snapshot zero-copy.*
//!
//! ---
//!
//! ## VersionedCell
//!
//! I thread di lavoro di un server leggono una configurazione condivisa con altissima frequenza e non devono mai essere rallentati da un aggiornamento in corso; un thread di amministrazione, più raro, deve poterla modificare — ma senza rischiare di sovrascrivere silenziosamente una modifica concorrente di un altro amministratore basata su uno stato ormai superato. È lo schema della **concorrenza ottimistica**: si legge sempre liberamente, e si scrive solo se nel frattempo nessun altro ha già scritto.
//!
//! ### API richiesta
//!
//! ```text
//! pub trait Snapshot<T: Clone + Send> {
//!     fn value(&self) -> &T;
//!     fn version(&self) -> u64;
//! }
//!
//! pub trait VersionedCell<T: Clone + Send>: Clone + Send + Sync {
//!     fn read(&self) -> impl Snapshot<T>;
//!     fn compare_and_update(&self, expected_version: u64, new_value: T) -> bool;
//! }
//!
//! pub fn make_versioned_cell<T: Clone + Send + Sync + 'static>(initial: T) -> impl VersionedCell<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - La struttura deve essere thread-safe e condivisibile tra più thread lettori e scrittori (da cui `Clone` sul tratto `VersionedCell`).
//! - `read()` non deve mai bloccare il chiamante in attesa di alcunché.
//! - Se due `compare_and_update` concorrenti partono dalla stessa `expected_version`, al più uno dei due deve avere successo; l'altro deve fallire restituendo `false`.
//! - La versione interna deve crescere in modo monotono di uno ad ogni aggiornamento riuscito, mai per un tentativo fallito.
//! - Nessuna attesa attiva né alcuna forma di blocco indefinito in nessun punto.

use std::sync::{Arc, RwLock};

/// Trait che rappresenta un'istantanea immutabile e point-in-time del valore contenuto nella cella.
pub trait Snapshot<T: Clone + Send> {
    /// Restituisce un riferimento al valore osservato al momento della lettura.
    fn value(&self) -> &T;

    /// Restituisce il numero di versione corrispondente a questa istantanea.
    fn version(&self) -> u64;
}

/// Trait che rappresenta una cella condivisa a controllo di concorrenza ottimistica (Optimistic Concurrency Control).
pub trait VersionedCell<T: Clone + Send>: Clone + Send + Sync {
    /// Lettura non bloccante: restituisce immediatamente un'istantanea del
    /// valore corrente e della sua versione. Non deve mai far attendere il
    /// chiamante, indipendentemente da quanti aggiornamenti sono in corso o
    /// da quanti altri thread stanno leggendo contemporaneamente.
    fn read(&self) -> impl Snapshot<T>;

    /// Tenta di aggiornare il valore a `new_value`, ma solo se nessuno ha
    /// già scritto un valore più recente di `expected_version` (tipicamente
    /// ottenuta da una `read()` precedente). Restituisce `true` se
    /// l'aggiornamento è avvenuto (la versione interna viene allora
    /// incrementata), `false` se è stato rifiutato perché `expected_version`
    /// non è più quella corrente. Non deve mai bloccare il chiamante in
    /// attesa che la versione torni ad essere quella attesa: un fallimento
    /// va segnalato immediatamente, lasciando ad un eventuale nuovo
    /// tentativo la responsabilità di chi chiama.
    fn compare_and_update(&self, expected_version: u64, new_value: T) -> bool;
}

// =================================================================================
// 🛠️ IMPLEMENTAZIONE OTTIMISTICA CON ZERO-COPY SNAPSHOTTING (RCU)
// =================================================================================

/// Istantanea immutabile e persistente di un valore associato alla sua versione.
///
/// ## Proprietà Zero-Copy e Immutabilità Garantita:
/// - Il campo `value` è racchiuso in un `Arc<T>`: la cattura di un nuovo snapshot clona solo
///   il puntatore heap (incrementando il contatore di riferimenti a costo O(1)), senza duplicare i dati di `T`.
/// - Poiché `T` non è avvolto da `Mutex` o `UnsafeCell` interni, il dato puntato da `Arc<T>` è
///   **congelato per sempre** in memoria: modifiche successive alla cella non possono in alcun modo alterarlo.
pub struct MySnap<T: Clone + Send> {
    /// Puntatore immutabile al valore congelato nell'heap al momento della lettura.
    value: Arc<T>,
    /// Numero progressivo di versione all'istante dello snapshot.
    version: u64,
}

impl<T: Clone + Send> Snapshot<T> for MySnap<T> {
    /// Restituisce un riferimento immutabile `&T` tramite dereferenziazione dell'`Arc<T>`.
    fn value(&self) -> &T {
        &self.value
    }

    /// Restituisce la versione temporale associata a questo snapshot.
    fn version(&self) -> u64 {
        self.version
    }
}

/// Implementazione concreta di `VersionedCell` basata su `RwLock` e Read-Copy-Update (RCU).
pub struct MyCell<T: Clone + Send> {
    /// Stato interno della cella protetto da un lock lettori-scrittori (`RwLock`).
    /// - Lettori multipli accedono concorrentemente tramite `read()` senza serializzarsi.
    /// - Gli scrittori acquisiscono il lock esclusivo solo per verificare la versione e scambiare il puntatore `Arc`.
    inner: Arc<RwLock<MySnap<T>>>,
}

impl<T: Clone + Send> MyCell<T> {
    /// Inizializza una nuova cella con il valore specificato alla versione iniziale `0`.
    pub fn new(initial_value: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(MySnap {
                value: Arc::new(initial_value),
                version: 0,
            })),
        }
    }
}

impl<T: Clone + Send + Sync> VersionedCell<T> for MyCell<T> {
    /// Esegue una lettura non bloccante dello stato corrente in tempo costante $O(1)$.
    ///
    /// ## Concorrenza di Lettura Massima:
    /// - Acquisisce un read-lock condiviso (`RwLock::read()`), consentendo a infiniti thread lettori
    ///   di eseguire `read()` contemporaneamente senza alcuna contesa.
    /// - Clona unicamente l'`Arc<T>` e copia l'intero `u64`: zero copie profonde del dato `T`!
    fn read(&self) -> impl Snapshot<T> {
        let guard = self.inner.read().unwrap();
        MySnap {
            value: Arc::clone(&guard.value),
            version: guard.version,
        }
    }

    /// Tenta una scrittura ottimistica (Compare-And-Swap logico a livello di versione).
    ///
    /// ## Meccanismo Copy-on-Write (RCU):
    /// 1. Acquisisce il write-lock esclusivo sullo stato della cella.
    /// 2. Confronta la versione corrente con `expected_version`:
    ///    - **Successo (`version == expected_version`)**: incrementa la versione (`version += 1`),
    ///      alloca un NUOVO `Arc::new(new_value)` e lo assegna alla cella. I vecchi snapshot
    ///      rimangono indisturbati e continuano a puntare al vecchio `Arc`. Ritorna `true`.
    ///    - **Conflitto Rilevato (`version != expected_version`)**: significa che un altro scrittore
    ///      ha già modificato la cella nel frattempo. L'operazione viene rifiutata immediatamente
    ///      senza modificare lo stato. Ritorna `false`.
    fn compare_and_update(&self, expected_version: u64, new_value: T) -> bool {
        let mut guard = self.inner.write().unwrap();

        if guard.version == expected_version {
            guard.version += 1;
            guard.value = Arc::new(new_value);
            true
        } else {
            false
        }
    }
}

impl<T: Clone + Send> Clone for MyCell<T> {
    /// Clona l'handle della cella incrementando il contatore di riferimenti dell'`Arc` condiviso.
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova `VersionedCell` con il valore iniziale specificato alla versione 0.
pub fn make_versioned_cell<T: Clone + Send + Sync + 'static>(initial: T) -> impl VersionedCell<T> {
    MyCell::new(initial)
}
