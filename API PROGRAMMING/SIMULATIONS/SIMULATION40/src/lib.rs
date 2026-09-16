//! # Simulazione 046 — VirtualMemory (capstone)
//!
//! Implementazione di un gestore di memoria virtuale concorrente con page fault[cite: 8],
//! deduplicazione delle richieste concorrenti (single-flight)[cite: 8], espulsione con politica LRU[cite: 8],
//! blocco in caso di esaurimento dei frame liberi[cite: 8], pinning RAII tramite `PageGuard`[cite: 8]
//! e write-back differito su disco per le pagine modificate (dirty)[cite: 8].
//!
//! ### Requisiti
//! - Thread-safe, condivisibile tra thread (`Clone + Send + Sync`)[cite: 8].
//! - Al più `frame_count` pagine residenti contemporaneamente in memoria[cite: 8].
//! - Più lettori/scrittori possono contemporaneamente accedere alla stessa pagina residente (incrementando il pin count)[cite: 8].
//! - Una pagina non può essere espulsa finché il suo conteggio di pin è maggiore di 0[cite: 8].
//! - Le chiusure lente (`load`, `write_back`) NON devono mai essere eseguite mentre si mantiene
//!   un lock che bloccherebbe operazioni su chiavi indipendenti[cite: 8].
//! - Se non ci sono frame liberi e tutte le pagine residenti sono pinnate, la richiesta si blocca
//!   senza attesa attiva finché una pagina non viene sbloccata (unpinnata)[cite: 8].
//! - `PageGuard` implementa `Drop`: rilascia il pin e propaga lo stato dirty se `get_mut()` è stato invocato[cite: 8].

use std::{cell::UnsafeCell, collections::HashMap, hash::Hash, sync::{Arc, Condvar, Mutex}, time::Instant};

/// Guard RAII che rappresenta il prestito e il pin di una pagina residente in memoria[cite: 8].
pub trait PageGuard<V: Send> {
    /// Accesso immutabile al contenuto della pagina[cite: 8].
    fn get(&self) -> &V;

    /// Accesso esclusivo e mutabile[cite: 8].
    /// La prima invocazione di questo metodo marca la pagina come sporca (dirty)[cite: 8].
    fn get_mut(&mut self) -> &mut V;
}

/// Tratto che definisce il sottosistema di memoria virtuale[cite: 8].
pub trait VirtualMemory<K: Eq + Hash + Clone + Send, V: Send>: Clone + Send + Sync {
    /// Accede alla pagina identificata da `key`, caricandola con `load` se non residente[cite: 8].
    /// Restituisce un guard RAII che mantiene la pagina pinnata[cite: 8].
    fn fault_in(&self, key: K, load: impl FnOnce() -> V) -> impl PageGuard<V>;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

use std::cell::UnsafeCell;
use std::sync::Arc;

pub struct SyncCell<V>(UnsafeCell<V>);

impl<V> SyncCell<V> {
    pub fn new(val: V) -> Self {
        Self(UnsafeCell::new(val))
    }

    pub fn get(&self) -> *mut V {
        self.0.get()
    }
}

// SAFETY: Pinning and the memory manager guarantee safe concurrent access
unsafe impl<V: Send> Send for SyncCell<V> {}
unsafe impl<V: Send> Sync for SyncCell<V> {}

pub struct MyPageGuard<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static,
{
    key: K,
    value: Arc<SyncCell<V>>,
    shared_memory: MyVirtualMemory<K, V>,
    modified: bool,
}

impl<K, V> PageGuard<V> for MyPageGuard<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static,
{
    fn get(&self) -> &V {
        let (memory_mutex, _) = &*self.shared_memory.inner;
        let mut memory_guard = memory_mutex.lock().unwrap();

        if let Some(Some(frame_state)) = memory_guard.map.get_mut(&self.key) {
            frame_state.last_access = Instant::now();
        }

        unsafe { &*self.value.get() }
    }

    fn get_mut(&mut self) -> &mut V {
        self.modified = true;
        let (memory_mutex, _) = &*self.shared_memory.inner;
        let mut memory_guard = memory_mutex.lock().unwrap();

        if let Some(Some(frame_state)) = memory_guard.map.get_mut(&self.key) {
            frame_state.last_access = Instant::now();
            frame_state.dirty = true;
        }

        unsafe { &mut *self.value.get() }
    }
}

impl<K, V> Drop for MyPageGuard<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static,
{
    fn drop(&mut self) {
        let (memory_mutex, memory_cvar) = &*self.shared_memory.inner;
        let mut memory_guard = memory_mutex.lock().unwrap();

        if let Some(Some(frame_state)) = memory_guard.map.get_mut(&self.key) {
            frame_state.num_pins -= 1;
            if self.modified {
                frame_state.dirty = true;
            }
            memory_cvar.notify_all();
        }
    }
}

pub struct FrameState<V: Send + 'static> {
    value: Arc<UnsafeCell<V>>,
    dirty: bool,
    last_access: Instant,
    num_pins: usize,
}

pub struct MyMap<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static,
{
    map: HashMap<K, Option<FrameState<V>>>,
}

impl<K, V> MyMap<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static,
{
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    pub fn evicted_frame(&self) -> Option<K> {
        self.map
            .iter()
            .filter_map(|(key, option_frame)| {
                if let Some(frame_state) = option_frame {
                    if frame_state.num_pins == 0 {
                        return Some((key.clone(), frame_state.last_access));
                    }
                }
                None
            })
            .min_by_key(|&(_, timestamp)| timestamp)
            .map(|(key, _)| key)
    }
}

pub struct MyVirtualMemory<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static,
{
    inner: Arc<(Mutex<MyMap<K, V>>, Condvar)>,
    frame_count: usize,
    write_back: Arc<dyn Fn(&K, &V) + Send + Sync + 'static>,
}

impl<K, V> VirtualMemory<K, V> for MyVirtualMemory<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static,
{
    fn fault_in(&self, key: K, load: impl FnOnce() -> V) -> impl PageGuard<V> {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        loop {
            if let Some(option_frame) = guard.map.get(&key) {
                if let Some(frame_state) = option_frame {
                    let val = frame_state.value.clone();
                    let frame_mut = guard.map.get_mut(&key).unwrap().as_mut().unwrap();
                    frame_mut.last_access = Instant::now();
                    frame_mut.num_pins += 1;

                    return MyPageGuard {
                        key,
                        value: val,
                        shared_memory: self.clone(),
                        modified: false,
                    };
                } else {
                    // Page is currently Loading by another thread
                    guard = cvar.wait(guard).unwrap();
                }
            } else {
                // Key not present: check if we need to evict first
                if guard.map.len() >= self.frame_count {
                    guard = cvar
                        .wait_while(guard, |c| c.evicted_frame().is_none())
                        .unwrap();

                    let evicted_key = guard.evicted_frame().unwrap();
                    let evicted_frame = guard.map.remove(&evicted_key).unwrap().unwrap();
                    drop(guard);

                    if evicted_frame.dirty {
                        let val_ref = unsafe { &*evicted_frame.value.get() };
                        (self.write_back)(&evicted_key, val_ref);
                    }

                    guard = mutex.lock().unwrap();
                }

                // Reserve the slot as Loading
                guard.map.insert(key.clone(), None);
                drop(guard);

                // Run load() outside the lock
                let loaded_val = load();
                let val_arc = Arc::new(UnsafeCell::new(loaded_val));

                guard = mutex.lock().unwrap();
                guard.map.insert(
                    key.clone(),
                    Some(FrameState {
                        value: val_arc.clone(),
                        dirty: false,
                        last_access: Instant::now(),
                        num_pins: 1,
                    }),
                );

                cvar.notify_all();

                return MyPageGuard {
                    key,
                    value: val_arc,
                    shared_memory: self.clone(),
                    modified: false,
                };
            }
        }
    }
}

impl<K, V> Clone for MyVirtualMemory<K, V>
where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            frame_count: self.frame_count,
            write_back: self.write_back.clone(),
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `VirtualMemory`[cite: 8].
pub fn make_virtual_memory<K: Eq + Hash + Clone + Send + 'static, V: Send + 'static>(
    _frame_count: usize,
    _write_back: impl Fn(&K, &V) + Send + Sync + 'static,
) -> impl VirtualMemory<K, V> {
    MyVirtualMemory {
        inner: Arc::new((Mutex::new(MyMap::new()), Condvar::new())),
        frame_count: _frame_count,
        write_back: Arc::new(Box::new(_write_back))
    }
}