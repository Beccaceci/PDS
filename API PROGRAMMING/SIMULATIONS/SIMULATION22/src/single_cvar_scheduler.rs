//! # Variante Architetturale: JobScheduler con Singola Condvar Globale
//!
//! In questa variante alternativa, l'intero sistema è coordinato da **un solo Mutex e una sola Condvar**.
//!
//! ### ⚖️ Confronto con l'architettura a due livelli:
//! - **Semplicità**: `JobRecord` e `MyJobHandle` non contengono campi `Arc<Condvar>` dedicati.
//! - **Funzionamento**: `join()`, i worker e `join_all()` condividono la stessa `Condvar` globale.
//!   Il predicato di ciascun `wait_while` garantisce che ogni thread si risvegli solo quando
//!   la sua specifica condizione booleana è soddisfatta.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Instant;

use crate::{JobHandle, JobScheduler, JobState};

/// Record semplificato senza `cvar` privata.
pub struct SimpleJobRecord {
    pub base_priority: u32,
    pub submitted_at: Instant,
    pub resources: Vec<String>,
    pub status: JobState,
}

impl SimpleJobRecord {
    pub fn compute_effective_priority(&self) -> u64 {
        let elapsed_ms = self.submitted_at.elapsed().as_millis() as u64;
        (self.base_priority as u64).saturating_add(elapsed_ms)
    }
}

/// Handle del singolo job che attende sulla `Condvar` globale dello scheduler.
pub struct SingleCvarJobHandle {
    scheduler: Arc<(Mutex<SingleCvarSchedulerState>, Condvar)>,
    job_id: usize,
}

impl JobHandle for SingleCvarJobHandle {
    fn join(&self) {
        let (mutex, cvar) = &*self.scheduler;
        let mut guard = mutex.lock().unwrap();

        // Attende sulla Condvar GLOBALE verificando lo stato del proprio job_id
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

    fn cancel(&self) -> bool {
        let (mutex, cvar) = &*self.scheduler;
        let mut guard = mutex.lock().unwrap();

        if let Some(record) = guard.jobs.get_mut(&self.job_id) {
            if matches!(record.status, JobState::Pending(_)) {
                record.status = JobState::Cancelled;
                drop(guard);
                cvar.notify_all();
                true
            } else {
                false
            }
        } else {
            false
        }
    }
}

/// Stato globale per lo scheduler a singola Condvar.
pub struct SingleCvarSchedulerState {
    jobs: HashMap<usize, SimpleJobRecord>,
    busy_resources: HashSet<String>,
    next_id: usize,
    closed: bool,
}

impl SingleCvarSchedulerState {
    pub fn new() -> Self {
        Self {
            jobs: HashMap::new(),
            busy_resources: HashSet::new(),
            next_id: 0,
            closed: false,
        }
    }

    pub fn has_eligible_job(&self) -> bool {
        self.jobs.values().any(|r| self.is_eligible(r))
    }

    pub fn is_eligible(&self, record: &SimpleJobRecord) -> bool {
        if matches!(record.status, JobState::Pending(_)) {
            !record.resources.iter().any(|r| self.busy_resources.contains(r))
        } else {
            false
        }
    }

    pub fn pop_eligible_job(&mut self) -> Option<(usize, &mut SimpleJobRecord)> {
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
}

/// Scheduler basato su una singola Condvar.
#[derive(Clone)]
pub struct SingleCvarScheduler {
    inner: Arc<(Mutex<SingleCvarSchedulerState>, Condvar)>,
}

impl SingleCvarScheduler {
    pub fn new(worker_count: usize) -> Self {
        let scheduler = Self {
            inner: Arc::new((Mutex::new(SingleCvarSchedulerState {
                jobs: HashMap::new(),
                busy_resources: HashSet::new(),
                next_id: 0,
                closed: false,
            }), Condvar::new())),
        };

        for _ in 0..worker_count {
            let cloned = scheduler.clone();

            thread::spawn(move || {
                loop {
                    let (mutex, cvar) = &*cloned.inner;
                    let mut guard = mutex.lock().unwrap();

                    guard = cvar
                        .wait_while(guard, |c| {
                            let has_pending = c
                                .jobs
                                .values()
                                .any(|j| matches!(j.status, JobState::Pending(_)));
                            !c.has_eligible_job() && !(c.closed && !has_pending)
                        })
                        .unwrap();

                    if guard.closed
                        && !guard
                            .jobs
                            .values()
                            .any(|j| matches!(j.status, JobState::Pending(_)))
                    {
                        break;
                    }

                    if let Some((job_id, job_record)) = guard.pop_eligible_job() {
                        let task = match std::mem::replace(&mut job_record.status, JobState::InProgress)
                        {
                            JobState::Pending(task) => task,
                            _ => panic!("Inconsistenza"),
                        };

                        let resources = job_record.resources.clone();
                        for r in &resources {
                            guard.busy_resources.insert(r.clone());
                        }

                        drop(guard);

                        // Esecuzione task fuori dal lock
                        (task)();

                        let mut guard = mutex.lock().unwrap();
                        for r in &resources {
                            guard.busy_resources.remove(r);
                        }

                        guard.jobs.get_mut(&job_id).unwrap().status = JobState::Completed;
                        drop(guard);

                        // Notifica globale: sveglia sia i worker per le risorse libere, sia i join()
                        cvar.notify_all();
                    }
                }
            });
        }

        scheduler
    }
}

impl JobScheduler for SingleCvarScheduler {
    fn submit(
        &self,
        priority: u32,
        resources: &[String],
        job: impl FnOnce() + Send + 'static,
    ) -> impl JobHandle + 'static {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        let job_id = guard.next_id;
        guard.next_id += 1;

        if !guard.closed {
            guard.jobs.insert(
                job_id,
                SimpleJobRecord {
                    base_priority: priority,
                    submitted_at: Instant::now(),
                    resources: resources.to_vec(),
                    status: JobState::Pending(Box::new(job)),
                },
            );
            drop(guard);
            cvar.notify_all();
        }

        SingleCvarJobHandle {
            scheduler: Arc::clone(&self.inner),
            job_id,
        }
    }

    fn close(&self) {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        guard.closed = true;
        drop(guard);
        cvar.notify_all();
    }

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
