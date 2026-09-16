//! # Simulazione 022 — JobScheduler (Capstone Concurrency System)
//!
//! Un sistema di esecuzione concorrente di lavori (*job scheduler*) riceve richieste di esecuzione di task
//! associando a ciascuna una priorità iniziale e un insieme di risorse nominate necessarie per la sua esecuzione.
//!
//! ### 🏛️ PANORAMICA ARCHITETTURALE GENERALE
//!
//! Il sistema implementa un'architettura **Multi-Worker / Multi-Resource** con sincronizzazione a due livelli:
//!
//! ```text
//!                                  [ MyScheduler ]
//!                                         │
//!                      Arc<(Mutex<SchedulerState>, Condvar)>
//!                                         │
//!            ┌────────────────────────────┴────────────────────────────┐
//!            ▼                                                         ▼
//!  ┌─────────────────────────┐                               ┌─────────────────────────┐
//!  │     SchedulerState      │                               │   Global Scheduler Cvar │
//!  │  ├── jobs: HashMap      │                               │  (Worker wakeup &       │
//!  │  ├── busy_resources     │                               │   join_all coordination)│
//!  │  ├── next_id: usize     │                               └─────────────────────────┘
//!  │  └── closed: bool       │
//!  └─────────┬───────────────┘
//!            │
//!            ├─────────────────────────┬─────────────────────────┐
//!            ▼                         ▼                         ▼
//!     ┌──────────────┐          ┌──────────────┐          ┌──────────────┐
//!     │  JobRecord 1 │          │  JobRecord 2 │          │  JobRecord 3 │
//!     │  - priority  │          │  - priority  │          │  - priority  │
//!     │  - aging     │          │  - aging     │          │  - aging     │
//!     │  - resources │          │  - resources │          │  - resources │
//!     │  - status    │          │  - status    │          │  - status    │
//!     │  - cvar (Arc)│          │  - cvar (Arc)│          │  - cvar (Arc)│
//!     └──────────────┘          └──────────────┘          └──────────────┘
//!            ▲                         ▲                         ▲
//!            │                         │                         │
//!     [ MyJobHandle 1 ]         [ MyJobHandle 2 ]         [ MyJobHandle 3 ]
//!     (join/cancel utente)      (join/cancel utente)      (join/cancel utente)
//! ```
//!
//! ### 🔄 CICLO DI VITA DI UN JOB
//!
//! ```text
//!      submit()
//!         │
//!         ▼
//!    [ PENDING ] ─── cancel() ───► [ CANCELLED ] (Terminazione anticipata)
//!         │
//!  Worker assegna risorse &
//!  estrae task sotto lock
//!         │
//!         ▼
//!  [ IN PROGRESS ] (Esecuzione task() FUORI DAL LOCK per massima concorrenza)
//!         │
//!  Worker rilascia risorse &
//!  imposta stato sotto lock
//!         │
//!         ▼
//!   [ COMPLETED ] ───► Notifica cvar privata (job.join) + cvar globale (altri worker/join_all)
//! ```

pub mod single_cvar_scheduler;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Instant;

/// Trait che rappresenta l'handle per il controllo e l'attesa di un singolo job sottomesso.
pub trait JobHandle: Send + Sync {
    /// Annulla il job se non è ancora stato preso in carico da un thread di lavoro.
    /// Restituisce `true` se l'annullamento ha avuto effetto, `false` se il job era già in esecuzione o già terminato.
    fn cancel(&self) -> bool;

    /// Blocca il chiamante in modo efficiente senza consumo di cicli di CPU finché questo specifico job non è terminato.
    fn join(&self);
}

/// Trait che rappresenta il pianificatore concorrente di job con vincoli di risorse, priorità con invecchiamento e pool di worker.
pub trait JobScheduler: Clone + Send + Sync {
    /// Registra un nuovo job con priorità iniziale `priority` e l'insieme di risorse nominate richieste.
    fn submit(
        &self,
        priority: u32,
        resources: &[String],
        job: impl FnOnce() + Send + 'static,
    ) -> impl JobHandle + 'static;

    /// Impedisce l'accodamento di nuovi job; i job già pendenti o in esecuzione proseguono fino al termine.
    fn close(&self);

    /// Blocca il chiamante finché tutti i job già sottomessi non sono terminati e i worker thread non hanno concluso.
    fn join_all(&self);
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================

/// Stato di avanzamento a stati finiti di un singolo Job.
pub enum JobState {
    /// Il job è in attesa che un worker lo prenda in carico. Contiene la chiusura `task` da eseguire.
    Pending(Box<dyn FnOnce() + Send + 'static>),
    /// Il job è attualmente in esecuzione su uno dei thread del pool di worker.
    InProgress,
    /// Il job è stato completato con successo.
    Completed,
    /// Il job è stato annullato prima dell'avvio tramite `cancel()`.
    Cancelled,
}

/// Handle restituito al chiamante per interagire con il singolo job sottomesso.
pub struct MyJobHandle {
    /// Riferimento condiviso allo scheduler globale.
    scheduler: Arc<(Mutex<SchedulerState>, Condvar)>,
    /// Identificativo univoco del job.
    job_id: usize,
}

impl JobHandle for MyJobHandle {
    /// Attende passivamente finché questo specifico job non raggiunge lo stato `Completed` o `Cancelled`.
    fn join(&self) {
        let (mutex, _) = &*self.scheduler;
        let mut guard = mutex.lock().unwrap();

        // Estrae la Condvar dedicata a questo job per non mantenere il borrow attivo su `guard`
        let cvar = match guard.jobs.get(&self.job_id) {
            Some(record) => Arc::clone(&record.cvar),
            None => return,
        };

        // Attesa mirata: solo i thread in attesa su QUESTO job verranno risvegliati al suo termine
        guard = cvar
            .wait_while(guard, |c| {
                match c.jobs.get(&self.job_id) {
                    Some(this) => {
                        !matches!(this.status, JobState::Cancelled | JobState::Completed)
                    }
                    None => false,
                }
            })
            .unwrap();
    }

    /// Annulla il job se si trova ancora nello stato `Pending`.
    fn cancel(&self) -> bool {
        let (mutex, scheduler_cvar) = &*self.scheduler;
        let mut guard = mutex.lock().unwrap();

        let job_record = match guard.jobs.get_mut(&self.job_id) {
            Some(record) => record,
            None => return false,
        };

        if matches!(job_record.status, JobState::Pending(_)) {
            // Transizione a Cancelled e distruzione della chiusura
            job_record.status = JobState::Cancelled;
            // Sveglia chi è in attesa su join() per questo job
            job_record.cvar.notify_all();
            // Sveglia anche lo scheduler globale (utile per join_all())
            scheduler_cvar.notify_all();
            true
        } else {
            false
        }
    }
}

/// Record contenente tutti i metadati e lo stato associati a un singolo Job.
pub struct JobRecord {
    /// Priorità iniziale di base assegnata al momento del `submit`.
    pub base_priority: u32,
    /// Istante temporale di sottomissione (usato per calcolare l'invecchiamento/Aging).
    pub submitted_at: Instant,
    /// Insieme delle risorse nominate richieste per l'esecuzione di questo job.
    pub resources: Vec<String>,
    /// Stato corrente del job.
    pub status: JobState,
    /// Condvar privata per l'attesa selettiva su `job.join()`.
    pub cvar: Arc<Condvar>,
}

impl JobRecord {
    /// Calcola la priorità effettiva del job sommando alla priorità di base
    /// i millisecondi trascorsi dal momento della sottomissione (meccanismo di Aging).
    ///
    /// ## Formula di Aging:
    /// `effective_priority = base_priority as u64 + elapsed_milliseconds`
    pub fn compute_effective_priority(&self) -> u64 {
        let elapsed_ms = self.submitted_at.elapsed().as_millis() as u64;
        (self.base_priority as u64).saturating_add(elapsed_ms)
    }
}

/// Stato globale condiviso del pianificatore protetto da `Mutex`.
pub struct SchedulerState {
    /// Mappa di tutti i job memorizzati indicizzati per ID.
    jobs: HashMap<usize, JobRecord>,
    /// Insieme delle risorse attualmente occupate da job in esecuzione (`InProgress`).
    busy_resources: HashSet<String>,
    /// Contatore incrementale per generare ID univoci per i nuovi job.
    next_id: usize,
    /// Flag booleano che indica se il pianificatore è stato chiuso (`close()`).
    closed: bool,
}

impl SchedulerState {
    /// Inizializza un nuovo stato vuoto per il pianificatore.
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            busy_resources: HashSet::new(),
            next_id: 0,
            closed: false,
        }
    }

    /// Verifica se esiste almeno un job pendente con tutte le risorse libere.
    pub fn has_eligible_job(&self) -> bool {
        self.jobs.values().any(|job_record| self.is_eligible(job_record))
    }

    /// Seleziona ed estrae il miglior job eleggibile (con priorità effettiva massima).
    ///
    /// Per evitare errori di doppio prestito (`borrow checker`):
    /// 1. Scansiona la mappa in modo immutabile trovando l'ID migliore.
    /// 2. Preleva il riferimento mutabile `&mut` solo su quell'ID specifico.
    pub fn pop_eligible_job(&mut self) -> Option<(usize, &mut JobRecord)> {
        let best_id = self
            .jobs
            .iter()
            .filter(|(_, r)| self.is_eligible(r))
            .max_by_key(|(_, r)| r.compute_effective_priority())
            .map(|(&id, _)| id);

        if let Some(id) = best_id {
            Some((id, self.jobs.get_mut(&id).unwrap()))
        } else {
            None
        }
    }

    /// Un job è eleggibile se e solo se:
    /// 1. È in stato `Pending`.
    /// 2. Nessuna delle sue risorse richieste compare in `busy_resources`.
    pub fn is_eligible(&self, job_record: &JobRecord) -> bool {
        if matches!(job_record.status, JobState::Pending(_)) {
            let has_conflicts = job_record
                .resources
                .iter()
                .any(|r| self.busy_resources.contains(r));
            !has_conflicts
        } else {
            false
        }
    }
}

/// Implementazione concreta e thread-safe del trait `JobScheduler`.
pub struct MyScheduler {
    inner: Arc<(Mutex<SchedulerState>, Condvar)>,
}

impl MyScheduler {
    /// Crea una nuova istanza di `MyScheduler`.
    pub fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new(SchedulerState::new()), Condvar::new())),
        }
    }
}

impl JobScheduler for MyScheduler {
    /// Registra un nuovo job nel pianificatore.
    fn submit(
        &self,
        priority: u32,
        resources: &[String],
        job: impl FnOnce() + Send + 'static,
    ) -> impl JobHandle + 'static {
        let new_job_record = JobRecord {
            base_priority: priority,
            submitted_at: Instant::now(),
            resources: resources.to_vec(),
            status: JobState::Pending(Box::new(job)),
            cvar: Arc::new(Condvar::new()),
        };

        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        let job_id = guard.next_id;
        guard.next_id += 1;

        if !guard.closed {
            guard.jobs.insert(job_id, new_job_record);
            drop(guard);
            // Risveglia i worker thread in attesa di lavoro
            cvar.notify_all();
        }

        MyJobHandle {
            scheduler: Arc::clone(&self.inner),
            job_id,
        }
    }

    /// Chiude il pianificatore: impedisce nuovi inserimenti e notifica tutti i worker.
    fn close(&self) {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        guard.closed = true;
        drop(guard);
        cvar.notify_all();
    }

    /// Blocca il chiamante finché non ci sono più job pendenti né in esecuzione.
    fn join_all(&self) {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        guard = cvar
            .wait_while(guard, |c| {
                c.jobs
                    .values()
                    .any(|j| matches!(j.status, JobState::Pending(_) | JobState::InProgress))
            })
            .unwrap();
    }
}

impl Clone for MyScheduler {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce un nuovo `JobScheduler` avviando `worker_count` thread di lavoro.
pub fn make_job_scheduler(_worker_count: usize) -> impl JobScheduler {
    let scheduler = MyScheduler::new();

    for _ in 0.._worker_count {
        let cloned_scheduler = scheduler.clone();

        thread::spawn(move || {
            loop {
                let (mutex, cvar) = &*cloned_scheduler.inner;
                let mut guard = mutex.lock().unwrap();

                // 1. Attesa passiva finché:
                //    - C'è almeno un job con risorse libere eseguibile, OPPURE
                //    - Lo scheduler è closed e non ci sono più job pendenti (uscita pulita).
                guard = cvar
                    .wait_while(guard, |c| {
                        let has_pending = c
                            .jobs
                            .values()
                            .any(|j| matches!(j.status, JobState::Pending(_)));
                        !c.has_eligible_job() && !(c.closed && !has_pending)
                    })
                    .unwrap();

                // Condizione di terminazione del thread worker
                if guard.closed
                    && !guard
                        .jobs
                        .values()
                        .any(|j| matches!(j.status, JobState::Pending(_)))
                {
                    break;
                }

                // 2. Selezione ed assegnazione atomica del Job
                if let Some((job_id, job_record)) = guard.pop_eligible_job() {
                    // Estrae la chiusura e imposta lo stato a InProgress
                    let task = match std::mem::replace(&mut job_record.status, JobState::InProgress)
                    {
                        JobState::Pending(task) => task,
                        _ => panic!("Stato inconsistente del job"),
                    };

                    let resources: Vec<String> = job_record.resources.clone();

                    // Prenota atomicamente tutte le risorse richieste
                    for resource in &resources {
                        guard.busy_resources.insert(resource.clone());
                    }

                    // 3. Rilascia il lock dello scheduler per eseguire la task
                    drop(guard);

                    // Esecuzione della chiusura utente fuori da qualsiasi lock
                    (task)();

                    // 4. Riacquisizione del lock per liberare le risorse e aggiornare lo stato
                    let mut guard = mutex.lock().unwrap();

                    // Rilascia le risorse occupate
                    for resource in &resources {
                        guard.busy_resources.remove(resource);
                    }

                    // Imposta lo stato a Completed e notifica i thread in attesa
                    let job_record = guard.jobs.get_mut(&job_id).unwrap();
                    job_record.status = JobState::Completed;

                    // Risveglia il thread bloccato su questo specifico `job.join()`
                    job_record.cvar.notify_all();

                    // Risveglia gli altri worker (ora ci sono risorse libere) e chi attende su `join_all()`
                    cvar.notify_all();
                }
            }
        });
    }

    scheduler
}
