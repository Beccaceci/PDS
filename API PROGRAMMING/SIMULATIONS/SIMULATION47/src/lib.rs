//! # Simulazione 047 — Signal (capstone)
//!
//! Un sistema di notifica in cui più osservatori (slot) si registrano per essere invocati ad ogni evento
//! deve gestire un caso che si presenta regolarmente in pratica: uno slot che, nella propria logica di gestione
//! dell'evento, decide di non voler più essere notificato in futuro — disconnettendosi da sé — oppure che invalida
//! un altro slot ancora in attesa di essere invocato nella stessa tornata.
//! Nessuno dei due casi deve corrompere la tornata di notifiche in corso.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `Connection` e `Signal<T>` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! pub trait Connection {
//!     fn disconnect(&self);
//! }
//!
//! pub trait Signal<T: Clone + Send>: Clone + Send + Sync {
//!     fn connect(&self, slot: impl Fn(&T) + Send + Sync + 'static) -> impl Connection + 'static;
//!     fn emit(&self, value: T);
//! }
//!
//! pub fn make_signal<T: Clone + Send + 'static>() -> impl Signal<T> {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//! - Thread-safe, condivisibile (`Clone + Send + Sync`).
//! - `emit()` deve invocare i soli slot connessi al proprio inizio, nell'ordine di connessione — ma deve anche
//!   rispettare, per ciascuno di essi, un'eventuale disconnessione avvenuta *durante* la stessa `emit()`,
//!   prima che essa lo raggiunga.
//! - Uno slot che chiama `disconnect()` su se stesso o su un altro, dall'interno della propria invocazione,
//!   non deve causare panico, blocco, né alcuna forma di comportamento indefinito nell'`emit()` in corso.
//! - `connect()` chiamato mentre un'`emit()` è già in corso non deve essere invocato da quella stessa `emit()`,
//!   ma deve essere pienamente valido e osservabile da qualunque `emit()` successiva.
//! - `disconnect()` non deve mai bloccare, né deve bloccare o essere bloccato da un'`emit()` in corso.
//! - Nessuna attesa attiva.
//! - I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{collections::HashMap, sync::{Arc, Mutex}};

/// Trait che rappresenta la connessione a uno slot.
pub trait Connection: Send + Sync {
    /// Disconnette lo slot associato.
    /// Può essere chiamato in qualunque momento, inclusa dall'interno del callback stesso.
    /// Se già disconnesso, non ha ulteriori effetti. Non blocca mai.
    fn disconnect(&self);
}

/// Trait che rappresenta il sistema di segnali/slot.
pub trait Signal<T: Clone + Send>: Clone + Send + Sync {
    /// Registra un nuovo slot invocato ad ogni futura emissione finché non disconnesso.
    /// Restituisce un handle `Connection` per disconnetterlo.
    fn connect(&self, slot: impl Fn(&T) + Send + Sync + 'static) -> impl Connection + 'static;

    /// Emette un valore invocando nell'ordine di connessione gli slot attivi al momento dell'inizio.
    /// Rispetta le disconnessioni avvenute prima che l'iterazione raggiunga lo slot.
    fn emit(&self, value: T);
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

// ---------------------------------------------------------------------------------
// 📐 ARCHITETTURA DEL SISTEMA E RELAZIONI TRA LE STRUTTURE (SIGNAL / SLOT PATTERN):
//
// 1. `MySignalManager<T>`:
//    - È il coordinatore globale del segnale (`Signal<T>`), condivisibile tra thread (`Clone + Send + Sync`).
//    - Contiene un `Arc<Mutex<ManagerState<T>>>` che protegge la mappa viva delle connessioni.
//
// 2. `ManagerState<T>`:
//    - Memorizza:
//      * `map: HashMap<usize, Arc<Mutex<SignalState<T>>>>`: la mappa viva di tutti gli slot
//        attualmente registrati, indicizzati da un ID progressivo.
//      * `next_id: usize`: generatore di identificatori univoci e monotonicamente crescenti,
//        che definiscono rigorosamente l'ordine temporale FIFO di registrazione.
//
// 3. `SignalState<T>`:
//    - È lo stato del SINGOLO slot, condiviso tra:
//      a) L'elenco vivo presente in `ManagerState`.
//      b) La "fotografia" (snapshot) creata all'inizio di `emit()`.
//      c) L'handle `MyConnection` restituito al momento della connessione.
//    - Contiene:
//      * `slot: Option<Arc<dyn Fn(&T) + ...>>`: la closure del callback incapsulata in un `Arc`.
//        L'`Arc` consente di clonare il puntatore e rilasciare il lock PRIMA di invocare il callback,
//        prevenendo deadlock rientranti.
//      * `connected: bool`: flag atomico/condiviso che riflette istantaneamente se lo slot
//        è ancora attivo oppure se è stato invalidato da una `disconnect()`.
//
// 4. `MyConnection<T>`:
//    - Rappresenta l'handle di disconnessione (`Connection`) consegnato al chiamante di `connect`.
//    - Mantiene sia l'`id` (per ripulire la mappa globale in `ManagerState`), sia il riferimento
//      diretto ad `Arc<Mutex<SignalState<T>>>` (per invalidare lo slot a livello locale anche se
//      un'`emit()` ne ha già catturato la fotografia).
//
// 🔄 DINAMICA DI DISCONNESSIONE RIENTRANTE E RISOLUZIONE DEI DEADLOCK:
//    - `emit()` scatta una fotografia degli slot all'inizio e ordina per `id` crescente (ordine FIFO).
//    - RILASCIA il lock del manager prima di iterare.
//    - Per ciascuno slot:
//      * Acquisisce brevemente il lock dello slot per verificare `signal_guard.connected`.
//      * Clona l'`Arc` della closure e RILASCIA immediatamente il lock dello slot.
//      * Esegue `slot(&value)` completamente fuori da qualunque lock!
//      * Se la closure chiama `disconnect()` su se stessa o su un altro slot, non trova
//        nessun lock occupato dallo stesso thread: l'operazione ha successo istantaneo
//        e il flag `connected = false` viene osservato in tempo reale dall'`emit()` in corso.
// ---------------------------------------------------------------------------------

/// Handle restituito dalla registrazione di uno slot tramite `connect()`.
/// Permette di invalidare in qualsiasi momento la ricezione dei futuri eventi.
pub struct MyConnnection<T: Clone + Send> {
    id: usize,
    signal_state: Arc<Mutex<SignalState<T>>>,
    shared_manager: MySignalManager<T>,
}

impl<T: Clone + Send> Connection for MyConnnection<T> {
    /// Disconnette lo slot associato in modo non bloccante e sicuro.
    /// Imposta `connected = false` sullo stato condiviso dello slot (rendendo nulla
    /// qualunque invocazione pendente all'interno di un'`emit` in corso) e rimuove
    /// l'ID dalla mappa globale viva del manager.
    fn disconnect(&self) {
        // 1. Invalida lo stato locale condiviso con la fotografia di emit()
        let mut signal_guard = self.signal_state.lock().unwrap();
        signal_guard.connected = false;
        drop(signal_guard);

        // 2. Rimuove la registrazione dalla mappa viva del manager per le future emit()
        let mut manager_guard = self.shared_manager.inner.lock().unwrap();
        manager_guard.map.remove(&self.id);
    }
}

/// Struttura che rappresenta lo stato individuale di un singolo slot registrato.
pub struct SignalState<T: Clone + Send> {
    /// Callback incapsulato in un Arc: permette di clonare il puntatore per eseguirlo fuori lock.
    slot: Option<Arc<dyn Fn(&T) + Send + Sync + 'static>>,
    /// Flag booleano che indica se lo slot è ancora attivo o se è stato disconnesso.
    connected: bool,
}

/// Struttura interna protetta dal lock globale del gestore del segnale.
pub struct ManagerState<T: Clone + Send> {
    /// Mappa degli slot registrati (ID -> Stato condiviso dello slot).
    map: HashMap<usize, Arc<Mutex<SignalState<T>>>>,
    /// Contatore progressivo per assegnare ID monotonicamente crescenti (garantisce l'ordine FIFO).
    next_id: usize,
}

/// Implementazione concreta del gestore dei segnali (`Signal<T>`).
/// È thread-safe e condivisibile tra thread (`Clone + Send + Sync`).
pub struct MySignalManager<T: Clone + Send> {
    inner: Arc<Mutex<ManagerState<T>>>,
}

impl<T: Clone + Send> MySignalManager<T> {
    /// Crea una nuova istanza di `MySignalManager` con registro slot vuoto.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ManagerState {
                map: HashMap::new(),
                next_id: 0,
            })),
        }
    }
}

impl<T: Clone + Send + 'static> Signal<T> for MySignalManager<T> {
    /// Connette un nuovo callback `slot` al segnale.
    /// Assegna un nuovo `connection_id` progressivo, inserisce lo slot nella mappa viva
    /// e restituisce un handle `MyConnection` per consentirne la disconnessione futura.
    fn connect(&self, slot: impl Fn(&T) + Send + Sync + 'static) -> impl Connection + 'static {
        let new_connection = Arc::new(Mutex::new(SignalState {
            slot: Some(Arc::new(slot)),
            connected: true,
        }));

        let mut guard = self.inner.lock().unwrap();

        let connection_id = guard.next_id;
        guard.next_id += 1;

        guard.map.insert(connection_id, new_connection.clone());

        MyConnnection {
            id: connection_id,
            signal_state: new_connection,
            shared_manager: self.clone(),
        }
    }

    /// Emette un valore verso tutti gli slot attualmente connessi nell'ordine in cui sono stati registrati.
    /// 1. Scatta una fotografia degli slot all'inizio dell'operazione e rilascia subito il lock del manager.
    /// 2. Ordina gli slot per ID crescente per rispettare rigorosamente l'ordine FIFO di connessione.
    /// 3. Per ciascun elemento della fotografia, verifica se è ancora `connected`, clona l'`Arc` della closure,
    ///    rilascia il lock dello slot ed esegue la computazione completamente fuori da qualunque lock.
    fn emit(&self, value: T) {
        let manager_guard = self.inner.lock().unwrap();

        // Scatta la fotografia (snapshot) degli slot presenti all'inizio dell'emit
        let mut cloned_signals = Vec::new();
        manager_guard.map.iter().for_each(|(key, signal)| {
            cloned_signals.push((*key, signal.clone()));
        });
        // Ordiniamo per connection_id per preservare l'ordine rigoroso di registrazione
        cloned_signals.sort_by(|(key1, _), (key2, _)| key1.cmp(key2));

        // Rilasciamo immediatamente il lock del manager per permettere connect/disconnect concorrenti
        drop(manager_guard);

        for (_, signal) in cloned_signals {
            let signal_guard = signal.lock().unwrap();

            // Verifica se lo slot è ancora connesso (potrebbe essere stato disconnesso da uno slot precedente)
            if signal_guard.connected {
                if let Some(ref slot) = signal_guard.slot.clone() {
                    // RILASCIAMO il lock del singolo slot PRIMA di invocare il callback.
                    // Questo passaggio previene qualsiasi deadlock rientrante in caso di auto-disconnessione!
                    drop(signal_guard);

                    // Esecuzione del callback completamente fuori dal lock
                    slot(&value);
                }
            }
        }
    }
}

impl<T: Clone + Send> Clone for MySignalManager<T> {
    /// Clona il gestore del segnale condividendo l'istanza `Arc` interna.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce un nuovo `Signal`.
pub fn make_signal<T: Clone + Send + 'static>() -> impl Signal<T> {
    MySignalManager::new()
}
