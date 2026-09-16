//! # Simulazione 011 — MultiResourceManager
//!
//! Quando un'operazione richiede l'accesso simultaneo a più risorse condivise — ad esempio più record di un database, o più periferiche di un sistema — acquisirle una alla volta espone al classico rischio dello stallo: due thread che richiedono le stesse risorse in ordine diverso possono bloccarsi reciprocamente per sempre, ciascuno in attesa di una risorsa già posseduta dall'altro.
//!
//! Si scriva in Rust una struttura che implementi il tratto generico `MultiResourceManager<T: Send>`, che gestisce un insieme fisso di risorse identificate da un indice (`0..capacity()`), e permette di acquisirne più di una contemporaneamente in un'unica operazione atomica, garantendo l'assenza di stalli indipendentemente dall'ordine in cui i chiamanti concorrenti specificano gli indici richiesti.
//!
//! ### API richiesta
//!
//! ```text
//! pub trait ResourceSet<T: Send> {
//!     fn get(&self, id: usize) -> &T;
//! }
//!
//! pub trait MultiResourceManager<T: Send>: Send + Sync {
//!     fn capacity(&self) -> usize;
//!     fn acquire_all(&self, ids: &[usize]) -> impl ResourceSet<T>;
//! }
//!
//! pub fn make_multi_resource_manager<T: Send>(items: Vec<T>) -> impl MultiResourceManager<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//!
//! - La struttura deve essere thread-safe e condivisibile tra più thread.
//! - Ogni singola risorsa deve essere posseduta da al più un `ResourceSet<T>` alla volta.
//! - Due chiamate concorrenti con insiemi di indici parzialmente sovrapposti (ad esempio un thread richiede `[1, 2]` e un altro `[2, 3]`) non devono mai produrre uno stallo permanente, qualunque sia l'ordine con cui gli indici sono passati a `acquire_all` (una chiamata con `[2, 1]` deve essere trattata in modo equivalente a `[1, 2]`).
//! - Quando l'oggetto che implementa `ResourceSet<T>` esce dallo scope, tutte le risorse che possedeva tornano disponibili contemporaneamente (RAII, tramite il tratto `Drop`).
//! - Nessuna attesa attiva in nessun punto.
//! - I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::sync::{Arc, Condvar, Mutex};

/// Trait che rappresenta un insieme di risorse acquisite contemporaneamente e atomicamente.
/// Fornisce accesso condiviso in sola lettura alle risorse appartenenti all'insieme acquisito.
/// Quando l'oggetto che implementa questo tratto esce dallo scope, tutte le risorse possedute
/// vengono automaticamente rilasciate contemporaneamente (RAII, tramite `Drop`).
pub trait ResourceSet<T: Send> {
    /// Accesso condiviso alla risorsa con l'id specificato, se presente
    /// nell'insieme correntemente posseduto da questo oggetto.
    ///
    /// # Panics
    /// Panica se l'id non fa parte dell'insieme acquisito da questo `ResourceSet`.
    fn get(&self, id: usize) -> &T;
}

/// Trait che rappresenta un gestore di risorse multiple con supporto per
/// acquisizioni atomiche e garanzia di assenza di deadlock (deadlock-free multi-resource manager).
pub trait MultiResourceManager<T: Send>: Send + Sync {
    /// Restituisce il numero totale di risorse gestite, identificate dagli indici `0..capacity()`.
    fn capacity(&self) -> usize;

    /// Acquisisce in blocco, in un'unica operazione atomica, tutte le
    /// risorse i cui indici sono elencati in `ids` (senza duplicati, tutti `< capacity()`).
    ///
    /// Blocca il chiamante, senza consumare cicli di CPU, finché tutte le risorse richieste non sono
    /// simultaneamente disponibili.
    ///
    /// L'assenza di stalli tra chiamate concorrenti con insiemi di indici sovrapposti
    /// deve valere indipendentemente dall'ordine in cui gli indici sono specificati in `ids`.
    fn acquire_all(&self, ids: &[usize]) -> impl ResourceSet<T>;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================

/// Insieme di risorse concesse in prestito esclusivo a un singolo thread chiamante.
/// Implementa il pattern RAII: quando esce dallo scope, il suo distruttore `Drop`
/// reinserisce contemporaneamente tutte le risorse nel pool condiviso in un'unica transazione.
pub struct MySet<T: Send> {
    /// Riferimento condiviso al pool centrale di risorse e alla variabile di condizione.
    inner: Arc<(Mutex<Vec<Element<T>>>, Condvar)>,
    /// Vettore locale contenente le risorse attualmente in possesso di questo set.
    items: Vec<Element<T>>,
}

impl<T: Send> ResourceSet<T> for MySet<T> {
    /// Restituisce un riferimento immutabile alla risorsa con ID specificato.
    /// Se l'ID richiesto non fa parte del set posseduto, panica secondo la specifica.
    fn get(&self, id: usize) -> &T {
        for elem in &self.items {
            if elem.id == id {
                return &elem.item;
            }
        }

        panic!("ID {} non presente nell'insieme acquisito", id);
    }
}

impl<T: Send> Drop for MySet<T> {
    /// Rilascio atomico multi-risorsa: trasferisce in blocco tutti gli elementi posseduti
    /// all'interno del pool condiviso sotto un unico lock (`Mutex`) e risveglia tutti i thread in attesa.
    fn drop(&mut self) {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        // `append` trasferisce tutti gli elementi in O(1) svuotando `self.items` senza riallocazioni
        guard.append(&mut self.items);
        drop(guard);

        // Notifica tutti i thread in attesa che potrebbero essere in grado di soddisfare la propria richiesta
        cvar.notify_all();
    }
}

/// Singolo elemento di risorsa associato al proprio identificatore univoco fisso `id`.
pub struct Element<T: Send> {
    /// Il valore della risorsa protetta.
    item: T,
    /// Identificatore statico e invariante della risorsa (nell'intervallo 0..capacity).
    id: usize,
}

/// Implementazione concreta del gestore di risorse multiple con prevenzione dei deadlock.
pub struct MyResourceManager<T: Send> {
    /// Stato condiviso: un `Mutex` che protegge il vettore delle risorse attualmente libere/disponibili,
    /// e una `Condvar` per coordinare l'attesa atomica dei chiamanti.
    inner: Arc<(Mutex<Vec<Element<T>>>, Condvar)>,
    /// Numero totale fisso di risorse gestite dal manager (`capacity`).
    capacity: usize,
}

impl<T: Send> MyResourceManager<T> {
    /// Inizializza il manager associando a ciascun elemento il proprio ID progressivo statico `0..items.len()`.
    pub fn new(items: Vec<T>) -> Self {
        let elements: Vec<Element<T>> = items
            .into_iter()
            .enumerate()
            .map(|(id, item)| Element { item, id })
            .collect();

        Self {
            capacity: elements.len(),
            inner: Arc::new((Mutex::new(elements), Condvar::new())),
        }
    }
}

impl<T: Send> MultiResourceManager<T> for MyResourceManager<T> {
    /// Restituisce la capacità totale (numero totale di risorse).
    fn capacity(&self) -> usize {
        self.capacity
    }

    /// Acquisisce atomicamente tutte le risorse richieste in `ids`.
    ///
    /// ## Strategia Anti-Deadlock:
    /// Per evitare deadlock dovuti ad acquisizioni parziali incrociate (es. Thread A chiede [0, 1]
    /// e Thread B chiede [1, 0]), `acquire_all` valuta la disponibilità di **TUTTI** gli ID richiesti
    /// sotto un unico blocco transazionale con `wait_while`. Il thread viene sospeso finché
    /// l'intero sottoinsieme non è simultaneamente presente nel pool libero.
    fn acquire_all(&self, ids: &[usize]) -> impl ResourceSet<T> {
        // Validazione preventiva: panica se un id è fuori dai limiti [0..capacity)
        if ids.iter().any(|&id| id >= self.capacity) {
            panic!("Richiesto ID fuori dai limiti consentiti (0..capacity)");
        }

        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        // Attesa atomica: il thread si blocca senza consumo di CPU finché TUTTI gli `ids` richiesti
        // non sono contemporaneamente disponibili all'interno del vettore `guard`.
        guard = cvar
            .wait_while(guard, |c| {
                !ids.iter().all(|id| c.iter().any(|elem| elem.id == *id))
            })
            .unwrap();

        // Estrazione di tutte le risorse richieste:
        // `swap_remove` estrae ciascun elemento in tempo costante O(1) senza shiftare l'intero vettore.
        let mut items = Vec::with_capacity(ids.len());
        for id in ids {
            let index = guard
                .iter()
                .position(|elem| elem.id == *id)
                .expect("Elemento verificato ma non trovato");
            items.push(guard.swap_remove(index));
        }

        MySet {
            inner: Arc::clone(&self.inner),
            items,
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce un'istanza del gestore di risorse multiple `MultiResourceManager`.
pub fn make_multi_resource_manager<T: Send + 'static>(items: Vec<T>) -> impl MultiResourceManager<T> {
    MyResourceManager::new(items)
}
