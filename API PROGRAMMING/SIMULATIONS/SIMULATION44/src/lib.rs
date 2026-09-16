//! # Simulazione 050 — InternPool (capstone)
//!
//! Pool di deduplicazione (*interning*) di valori immutabili basato su riferimenti deboli (`Weak<V>`)[cite: 19].
//! Garantisce che chiavi concorrenti condividano la stessa istanza[cite: 19], che `create` venga invocata
//! fuori da qualunque lock globale[cite: 19], e risolve atomicamente la "corsa alla resurrezione"
//! rimuovendo la voce dal pool esattamente nel momento in cui l'ultimo handle viene deallocato (`Drop`)[cite: 19].
//!
//! ### Requisiti
//! - Thread-safe, condivisibile tra thread (`Clone + Send + Sync`)[cite: 19].
//! - Richieste concorrenti per una chiave viva condividono lo stesso valore sottostante[cite: 19].
//! - `create` non viene mai invocata tenendo il lock del pool[cite: 19].
//! - Quando l'ultimo `Interned<V>` per una chiave viene rilasciato, la voce è rimossa dal pool (`len()` decresce)[cite: 19].
//! - Nessuna voce fantasma o handle orfano generato dalla corsa tra `intern` e `Drop`[cite: 19].
//! - Nessuna attesa attiva (`Mutex` + `Condvar` se necessario)[cite: 19].

use std::{collections::HashMap, hash::Hash, sync::{Arc, Condvar, Mutex}};

use crate::MyValueState::{Delivered, Loading};

/// Handle condiviso a un valore deduplicato (interned)[cite: 19].
pub trait Interned<V: Send> {
    /// Restituisce un riferimento immutabile al valore[cite: 19].
    fn get(&self) -> &V;
}

/// Tratto che definisce il pool di interning[cite: 19].
pub trait InternPool<K: Eq + Hash + Clone + Send, V: Send>: Clone + Send + Sync {
    /// Restituisce un handle condiviso associato a `key`, costruendolo tramite `create`
    /// solo se non è già presente un'istanza viva nel pool[cite: 19].
    fn intern(&self, key: K, create: impl FnOnce() -> V) -> impl Interned<V>;

    /// Restituisce il numero di chiavi attualmente vive nel pool[cite: 19].
    fn len(&self) -> usize;
}

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyHandle<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static
{
    key: K,
    value: Arc<V>,
    shared_pool: MyInternPool<K, V>
}

impl<K, V> Interned<V> for MyHandle<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static
{
    fn get(&self) -> &V {
        &self.value
    }
}

impl<K, V> Drop for MyHandle<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static
{
    fn drop(&mut self) {
        let (mutex, _) = &*self.shared_pool.inner;
        let mut guard = mutex.lock().unwrap();
        let value_state = guard.get_mut(&self.key).unwrap();
        if let Delivered { value: _, num_instances } = value_state {
            *num_instances -= 1;

            if *num_instances == 0 {
                guard.remove(&self.key);
            }
        }
        else {
            panic!("Expected Delivered state, found Loading")
        }
    }
}

pub enum MyValueState<V: Send + 'static> {
    Loading,
    Delivered {
        value: Arc<V>,
        num_instances: usize
    }
}

pub struct MyInternPool<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static
{ 
    inner: Arc<(Mutex<HashMap<K, MyValueState<V>>>, Condvar)>
}

impl<K, V> MyInternPool<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static
{
    pub fn new () -> Self {
        Self {
            inner: Arc::new((Mutex::new(HashMap::new()), Condvar::new()))
        }
    }
}

unsafe impl<K: Send, V: Send> Send for MyInternPool<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static
{}
unsafe impl<K: Send, V: Send> Sync for MyInternPool<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static
{}

impl<K, V> InternPool<K, V> for MyInternPool<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static
{
    fn intern(&self, key: K, create: impl FnOnce() -> V) -> impl Interned<V> {
        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();

        if guard.contains_key(&key) {
            loop {
                let value_state = guard.get_mut(&key).unwrap();
                if let Delivered { value, num_instances } = value_state {
                    *num_instances += 1;

                    return MyHandle {
                        key: key.clone(),
                        value: value.clone(),
                        shared_pool: self.clone()
                    };
                }
                else {
                    guard = cvar.wait_while(guard, |c| {
                        let value_state = c.get(&key).unwrap();
                        matches!(*value_state, Loading)
                    }).unwrap();
                }
            }
        }
        
        guard.insert(key.clone(), Loading);
        drop(guard);

        let computed_value = Arc::new(create());

        let mut guard = mutex.lock().unwrap();
        let value_state = guard.get_mut(&key).unwrap();
        *value_state = Delivered {
            value: computed_value.clone(),
            num_instances: 1
        };
        drop(guard);
        cvar.notify_all();

        MyHandle {
            key: key.clone(),
            value: computed_value,
            shared_pool: self.clone()
        }
    }

    fn len(&self) -> usize {
        let (mutex, _) = &*self.inner;
        let guard = mutex.lock().unwrap();
        guard.iter().filter(|&(_, value_state)| {
            matches!(value_state, Delivered { value: _, num_instances: _ })
        }).count()
    }
}

impl<K, V> Clone for MyInternPool<K, V> where
    K: Eq + Hash + Clone + Send + 'static,
    V: Send + 'static
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

// =================================================================================
// 🚀 FUNZIONE COSTRUTTORE PUBBLICA (FACTORY FUNCTION ENTRYPOINT)
// =================================================================================

/// Inizializza e restituisce una nuova istanza di `InternPool`[cite: 19].
pub fn make_intern_pool<K: Eq + Hash + Clone + Send + 'static, V: Send + 'static>() -> impl InternPool<K, V> {
    MyInternPool::new()
}