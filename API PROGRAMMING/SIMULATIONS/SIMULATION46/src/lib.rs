//! # Simulazione 046 — TaskGraph (capstone)
//!
//! Un insieme di task con dipendenze reciproche — questo deve terminare prima che quello possa iniziare —
//! è la struttura naturale di una compilazione, di una pipeline di build, di un grafo di elaborazione dati.
//! Un task diventa eseguibile solo quando tutte le sue dipendenze hanno avuto successo; se anche una sola fallisce,
//! ogni task che ne dipende, direttamente o transitivamente, deve essere considerato fallito senza mai essere eseguito —
//! a cascata, per quanto lontano si estenda quella catena di dipendenze.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `TaskHandle` e `TaskGraph` definiti di seguito.
//!
//! ### API richiesta
//!
//! ```rust,ignore
//! pub type TaskId = u64;
//!
//! pub trait TaskHandle {
//!     fn cancel(&self) -> bool;
//!     fn join(self) -> bool;
//! }
//!
//! pub trait TaskGraph: Clone + Send + Sync {
//!     fn submit(
//!         &self,
//!         depends_on: &[TaskId],
//!         task: impl FnOnce() -> bool + Send + 'static,
//!     ) -> (TaskId, impl TaskHandle);
//! }
//!
//! pub fn make_task_graph(worker_count: usize) -> impl TaskGraph {
//!     ...
//! }
//! ```
//!
//! ### Requisiti
//! - Alla creazione, `worker_count` thread di lavoro devono essere avviati, ciascuno capace di eseguire qualunque task pronto trovi.
//! - Un task con `depends_on` vuoto è pronto immediatamente.
//! - Un `depends_on` che referenzia un task già terminato con successo conta come dipendenza già soddisfatta;
//!   uno che referenzia un task già fallito/annullato/saltato rende il nuovo task immediatamente saltato.
//! - La propagazione di un fallimento (per qualunque causa) deve raggiungere ogni dipendente transitivo,
//!   non solo quelli diretti, indipendentemente dalla profondità della catena.
//! - `cancel()` e la propagazione naturale di un fallimento producono esattamente lo stesso esito osservabile per i dipendenti.
//! - Thread-safe, condivisibile (`Clone + Send + Sync`).
//! - Nessuna attesa attiva.
//! - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
//! - Se il codice consegnato non compila, non verrà valutato.

use std::{collections::VecDeque, sync::{Arc, Condvar, Mutex}, thread};

pub type TaskId = u64;

/// Trait che rappresenta l'handle per controllare e attendere l'esito di un task sottomesso.
pub trait TaskHandle {
    /// Annulla il task se non è ancora iniziato (in coda o in attesa di dipendenze).
    /// Restituisce false se è già in esecuzione o già terminato.
    /// Un annullamento riuscito si propaga come fallimento a tutti i dipendenti transitivi.
    fn cancel(&self) -> bool;

    /// Consuma l'handle, bloccando il chiamante finché il task non raggiunge un esito definitivo.
    /// Restituisce true solo se il task ha eseguito con successo.
    fn join(self) -> bool;
}

/// Trait che rappresenta il grafo di esecuzione dei task.
pub trait TaskGraph: Clone + Send + Sync {
    /// Sottomette un nuovo task dipendente dai task specificati in `depends_on`.
    /// Restituisce l'id assegnato al nuovo task insieme al suo handle.
    fn submit(
        &self,
        depends_on: &[TaskId],
        task: impl FnOnce() -> bool + Send + 'static,
    ) -> (TaskId, impl TaskHandle + 'static);
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

// ---------------------------------------------------------------------------------
// 📐 ARCHITETTURA DEL SISTEMA E RELAZIONI TRA LE STRUTTURE:
//
// 1. `MyTaskManager`:
//    - È il contenitore condivisibile (`Clone + Send + Sync`) che racchiude lo stato
//      globale del grafo (`ManagerState`) tramite un `Arc<(Mutex, Condvar, Condvar)>`.
//    - Possiede due `Condvar` distinte per evitare risvegli spuri incrociati:
//      * `cvar_workers`: notificato quando un nuovo task è pronto per essere eseguito dai worker.
//      * `cvar_join`: notificato quando un task raggiunge un esito definitivo (`outcome.is_some()`).
//
// 2. `ManagerState`:
//    - Rappresenta lo stato del DAG dei task protetto dall'unico lock del manager.
//    - Contiene:
//      * `tasks: Vec<MyTask>`: l'elenco sequenziale di tutti i task sottomessi.
//      * `ready_queue: VecDeque<TaskId>`: la coda FIFO dei soli task con tutte le
//         dipendenze soddisfatte (o senza dipendenze), pronti per essere prelevati dai worker.
//
// 3. `MyTask`:
//    - Memorizza sia la relazione in ingresso sia quella in uscita del grafo (DAG):
//      * In ingresso: `remaining_deps` (quante dipendenze devono ancora completare con SUCCESSO).
//      * In uscita (relazione inversa): `dependents: Vec<TaskId>` (chi dipende da me?).
//        Questa è la relazione chiave: permette la propagazione di eventi in AVANTI
//        dal genitore ai figli (`success_cascade` o `fail_cascade`) senza dover scorrere
//        l'intero grafo.
//      * `job`: la closure `FnOnce() -> bool` racchiusa in un `Mutex<Option<Job>>` per
//        permettere al worker di prenderne possesso esclusivo con `.take()`.
//      * `outcome`: esito terminale (`None` = in attesa/in esecuzione, `Some(true)` = successo,
//        `Some(false)` = fallimento/cancellazione).
//
// 4. `MyHandle`:
//    - È un handle leggero restituito al chiamante di `submit`.
//    - Non duplica lo stato del task: mantiene solo il `task_id` e un clone del manager.
//    - Accede direttamente allo stato centralizzato per `join()` e `cancel()`, garantendo
//      la completa assenza di lock multipli nidificati o disallineamenti di stato.
// ---------------------------------------------------------------------------------

/// Handle leggero associato a uno specifico task registrato nel grafo.
/// Permette di attendere il completamento del task (`join`) o di richiederne
/// l'annullamento prima che venga avviato (`cancel`).
pub struct MyHandle {
    task_id: TaskId,
    shared_manager: MyTaskManager,
}

impl TaskHandle for MyHandle {
    /// Annulla il task se non è ancora iniziato (in coda o in attesa di dipendenze).
    /// Restituisce `false` se il task è già terminato o già in esecuzione.
    /// In caso di successo, imposta l'esito a `false`, rimuove la closure e propaga
    /// il fallimento a cascata su tutti i discendenti transitivi, notificando chi attende su `join`.
    fn cancel(&self) -> bool {
        let (manager_mutex, _, cvar_join) = &*self.shared_manager.inner;
        let mut manager_guard = manager_mutex.lock().unwrap();
        let target_task = &manager_guard.tasks[self.task_id as usize];

        // Un task può essere cancellato solo se non ha ancora raggiunto un esito definitivo
        if target_task.outcome.is_none() {
            // Propaga ricorsivamente il fallimento a tutti i discendenti transitivi
            manager_guard.fail_cascade(self.task_id);

            // Risveglia tutti i thread attualmente bloccati in `join()` sul task o sui suoi discendenti
            drop(manager_guard);
            cvar_join.notify_all();

            true
        } else {
            false
        }
    }

    /// Consuma l'handle, bloccando il chiamante finché il task non raggiunge un esito terminale.
    /// Restituisce `true` solo se il task ha eseguito ed è terminato con successo.
    fn join(self) -> bool {
        let (manager_mutex, _, cvar_join) = &*self.shared_manager.inner;
        let mut manager_guard = manager_mutex.lock().unwrap();

        // Attende in modo non attivo sul Condvar dedicato alla join finché l'esito non è disponibile
        manager_guard = cvar_join
            .wait_while(manager_guard, |c| {
                c.tasks[self.task_id as usize].outcome.is_none()
            })
            .unwrap();

        manager_guard.tasks[self.task_id as usize].outcome.unwrap()
    }
}

/// Alias per la closure del task (FnOnce thread-safe con allocazione heap).
type Job = Box<dyn FnOnce() -> bool + Send + 'static>;

/// Struttura interna che rappresenta un singolo nodo del grafo di dipendenze.
pub struct MyTask {
    /// Numero di dipendenze dirette che devono ancora completare con esito positivo.
    remaining_deps: usize,
    /// Elenco dei task che dipendono direttamente da questo (arco diretto genitore -> figlio).
    dependents: Vec<TaskId>,
    /// Closure del task da eseguire, protetta da Mutex per estrazione sicura (`take`).
    job: Mutex<Option<Job>>,
    /// Esito definitivo: `None` (non terminato), `Some(true)` (successo), `Some(false)` (fallito/cancellato).
    outcome: Option<bool>,
}

/// Stato globale del grafo protetto dall'unico `Mutex` del manager.
pub struct ManagerState {
    /// Tabella di tutti i task sottomessi, indicizzati dal loro `TaskId`.
    tasks: Vec<MyTask>,
    /// Coda FIFO dei task pronti per l'esecuzione (zero dipendenze rimaste e non falliti).
    ready_queue: VecDeque<TaskId>,
}

impl ManagerState {
    /// Inizializza un nuovo `ManagerState` vuoto.
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            ready_queue: VecDeque::new(),
        }
    }

    /// Propaga ricorsivamente il fallimento a catena lungo il grafo delle dipendenze.
    /// Imposta `outcome = Some(false)` sul task corrente e ricorsivamente su ogni discendente,
    /// scartandone i relativi job per impedire che vengano eseguiti dai worker thread.
    pub fn fail_cascade(&mut self, root_id: TaskId) {
        let target_task = &mut self.tasks[root_id as usize];
        target_task.outcome = Some(false);
        // Cloniamo la lista dei figli per svincolare il borrow mutabile su self.tasks
        let dependents = target_task.dependents.clone();

        for child_id in dependents {
            self.fail_cascade(child_id);
        }
    }

    /// Propaga il successo di un task ai suoi dipendenti diretti.
    /// Decrementa `remaining_deps` di ciascun figlio: quando raggiunge 0 (e se il figlio non è fallito),
    /// il figlio diventa idoneo all'esecuzione e viene inserito nella `ready_queue`.
    pub fn success_cascade(&mut self, root_id: TaskId) {
        let target_task = &mut self.tasks[root_id as usize];
        target_task.outcome = Some(true);
        // Cloniamo la lista dei figli per svincolare il borrow mutabile su self.tasks
        let dependents = target_task.dependents.clone();

        for child_id in dependents {
            let child_task = &mut self.tasks[child_id as usize];
            child_task.remaining_deps -= 1;

            if child_task.remaining_deps == 0 && child_task.outcome.is_none() {
                self.ready_queue.push_back(child_id);
            }
        }
    }
}

/// Implementazione concreta del coordinatore `TaskGraph`.
/// Condivisibile tra thread tramite `Arc` con lock per i dati e due Condvar dedicate.
pub struct MyTaskManager {
    inner: Arc<(
        Mutex<ManagerState>,
        Condvar, // cvar_workers: notifica i worker thread quando ci sono nuovi task in ready_queue
        Condvar, // cvar_join: notifica i thread in attesa sul completamento di un task (join)
    )>,
}

impl MyTaskManager {
    /// Crea una nuova istanza di `MyTaskManager`.
    pub fn new() -> Self {
        Self {
            inner: Arc::new((
                Mutex::new(ManagerState::new()),
                Condvar::new(),
                Condvar::new(),
            )),
        }
    }
}

impl TaskGraph for MyTaskManager {
    /// Sottomette un nuovo task al grafo specificando i task da cui dipende (`depends_on`).
    /// - Se una qualunque dipendenza è già fallita, il task viene marcato immediatamente come fallito (`outcome = Some(false)`).
    /// - Per ogni dipendenza ancora in corso, registra il nuovo task come dipendente nel genitore.
    /// - Se il task non ha dipendenze pendenti (o `depends_on` è vuoto), viene inserito subito in `ready_queue`.
    /// Restituisce l'ID univoco assegnato e il rispettivo `TaskHandle`.
    fn submit(
        &self,
        depends_on: &[TaskId],
        task: impl FnOnce() -> bool + Send + 'static,
    ) -> (TaskId, impl TaskHandle + 'static) {
        let (mutex, cvar_workers, _) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        // L'ID del nuovo task corrisponde alla sua posizione ordinata nel vettore dei task
        let task_id = guard.tasks.len() as TaskId;
        let mut remaining_deps = 0 as usize;
        let mut outcome = None;

        for id in depends_on {
            let task = &mut guard.tasks[*id as usize];

            if let Some(completed) = task.outcome {
                if !completed {
                    // Dipendenza già fallita/annullata in precedenza: il nuovo task fallisce subito
                    outcome = Some(false);
                    break;
                }
            } else {
                // Dipendenza ancora in esecuzione/in attesa: incrementiamo il contatore e registriamo l'arco inverso
                remaining_deps += 1;
                task.dependents.push(task_id);
            }
        }

        guard.tasks.push(MyTask {
            remaining_deps,
            dependents: Vec::new(),
            job: Mutex::new(Some(Box::new(task))),
            outcome,
        });

        // Se non ci sono dipendenze pendenti e il task non è fallito in partenza, è pronto per l'esecuzione
        if remaining_deps == 0 && outcome.is_none() {
            guard.ready_queue.push_back(task_id);
            drop(guard);
            cvar_workers.notify_one();
        }

        (
            task_id,
            MyHandle {
                task_id,
                shared_manager: self.clone(),
            },
        )
    }
}

impl Clone for MyTaskManager {
    /// Clona il gestore del grafo condividendo l'istanza `Arc` sottostante.
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e avvia un nuovo `TaskGraph` con `worker_count` worker thread interni.
/// Ciascun thread attende in modo bloccante (`wait_while` su `cvar_workers`) la presenza
/// di task pronti nella `ready_queue`, ne preleva la closure e la esegue fuori dal lock,
/// propagando poi l'esito (`success_cascade` o `fail_cascade`) e notificando i chiamanti di `join`.
pub fn make_task_graph(_worker_count: usize) -> impl TaskGraph {
    let manager = MyTaskManager::new();

    for _ in 0.._worker_count {
        let cloned_manager = manager.clone();

        thread::spawn(move || {
            loop {
                let (manager_mutex, cvar_workers, cvar_join) = &*cloned_manager.inner;
                let mut manager_guard = manager_mutex.lock().unwrap();

                // Attesa bloccante: dorme finché non ci sono task pronti nella coda
                manager_guard = cvar_workers
                    .wait_while(manager_guard, |c| c.ready_queue.is_empty())
                    .unwrap();

                let task_id = manager_guard.ready_queue.pop_front().unwrap();
                let task = &manager_guard.tasks[task_id as usize];

                // Verifica che il task non sia stato nel frattempo cancellato o fallito
                if task.outcome.is_none() {
                    let job = task.job.lock().unwrap().take().unwrap();
                    // Rilasciamo il lock prima di eseguire il task per non bloccare il grafo
                    drop(manager_guard);

                    // Esecuzione effettiva della computazione (fuori dal lock)
                    let outcome = job();

                    // Riacquisiamo il lock per registrare l'esito e propagarlo
                    let mut manager_guard = manager_mutex.lock().unwrap();

                    if outcome {
                        manager_guard.success_cascade(task_id);
                    } else {
                        manager_guard.fail_cascade(task_id);
                    }
                    drop(manager_guard);

                    // Svegliamo sia eventuali worker (per nuovi task sbloccati) sia i chiamanti di join()
                    cvar_workers.notify_all();
                    cvar_join.notify_all();
                }
            }
        });
    }

    manager
}
