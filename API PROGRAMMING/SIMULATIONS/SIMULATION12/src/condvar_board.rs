//! # Implementazione 1: MetricsBoard con Condvar e Granularità Mista (`condvar_board.rs`)
//!
//! Questa implementazione segue il pattern a **granularità di lock mista** (mixed lock granularity):
//!
//! ## Architettura di Sincronizzazione:
//! 1. **Lock Globale del Registro (`Mutex<HashMap<String, Arc<MyValue>>>`)**:
//!    - Viene acquisito **esclusivamente** durante la registrazione di nuove metriche (`register()`),
//!      la loro distruzione (`Drop` su `CondvarMetricHandle`) e la cattura dello snapshot (`snapshot()`).
//!    - Garantisce mutua esclusione sul dizionario, impedendo registrazioni duplicate dello stesso nome
//!      e consentendo il riciclo immediato del nome una volta che l'handle viene distrutto.
//! 2. **Lock a Grana Fine per Singola Metrica (`MyValue::value: Mutex<f64>`)**:
//!    - Ogni metrica possiede il proprio `Mutex<f64>` e la propria `Condvar` indipendenti.
//!    - Il metodo `record()` acquisisce **solo il lock locale del singolo valore**, senza toccare il lock
//!      globale del board. In questo modo, aggiornamenti ad altissima frequenza su metriche distinte
//!      (es. "cpu", "mem", "disk") procedono in parallelo senza alcuna serializzazione o contesa reciproca.
//! 3. **Attesa Selettiva su Soglia (`wait_for_threshold`)**:
//!    - Cerca la metrica nel dizionario, ne clona l'`Arc<MyValue>` e **rilascia immediatamente il lock globale**.
//!    - Si sospende sulla `Condvar` specifica della metrica senza bloccare l'intero board per gli altri thread.

use super::{MetricHandle, MetricsBoard, NameAlreadyUsed};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex};

/// Handle RAII associato a una specifica metrica registrata nel board.
/// Quando esce dallo scope (`Drop`), rimuove la metrica dal dizionario globale,
/// rendendo il nome nuovamente disponibile per future registrazioni.
pub struct CondvarMetricHandle {
    /// Riferimento condiviso al dizionario globale delle metriche protetto da Mutex.
    shared_state: Arc<Mutex<HashMap<String, Arc<MyValue>>>>,
    /// Nome univoco assegnato a questa metrica.
    key: String,
    /// Riferimento alla struttura interna contenente il valore numerico e la Condvar.
    value: Arc<MyValue>,
}

impl MetricHandle for CondvarMetricHandle {
    /// Aggiorna il valore numerico della metrica.
    ///
    /// ## Concorrenza a Grana Fine:
    /// Blocca unicamente il `Mutex<f64>` di questa specifica metrica: nessun altro thread che aggiorna
    /// metriche diverse viene rallentato o serializzato.
    fn record(&self, value: f64) {
        let mut guard_value = self.value.value.lock().unwrap();
        *guard_value = value;
        drop(guard_value);

        // Notifica eventuali thread in attesa del superamento di una soglia su questa metrica
        self.value.cvar.notify_all();
    }
}

impl Drop for CondvarMetricHandle {
    /// Distruttore RAII: quando l'handle viene rilasciato, rimuove la chiave dal dizionario
    /// globale e notifica eventuali osservatori.
    fn drop(&mut self) {
        // Marca la metrica come non più valida
        self.value.valid.store(false, SeqCst);

        // Rimuove la metrica dal registro centrale sotto il lock globale
        let mut guard = self.shared_state.lock().unwrap();
        guard.remove(&self.key);
        drop(guard);

        // Risveglia eventuali thread in attesa su questa metrica
        self.value.cvar.notify_all();
    }
}

/// Struttura interna associata a una singola metrica registrata.
pub struct MyValue {
    /// Il valore numerico corrente della metrica, protetto da lock a grana fine.
    value: Mutex<f64>,
    /// Variabile di condizione dedicata al risveglio di thread bloccati su `wait_for_threshold`.
    cvar: Condvar,
    /// Flag atomico che indica se la metrica è ancora attiva o se l'handle è stato distrutto.
    valid: AtomicBool,
}

impl MyValue {
    /// Inizializza un nuovo valore con valore di default `0.0`.
    pub fn new() -> Self {
        Self {
            value: Mutex::new(0f64),
            cvar: Condvar::new(),
            valid: AtomicBool::new(true),
        }
    }
}

/// Implementazione concreta del pannello di controllo con Mutex a granularità mista.
pub struct CondvarMetricsBoard {
    /// Registro centrale delle metriche attive: mappa il nome della metrica al rispettivo valore.
    inner: Arc<Mutex<HashMap<String, Arc<MyValue>>>>,
}

impl CondvarMetricsBoard {
    /// Crea un nuovo pannello di controllo vuoto.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl MetricsBoard for CondvarMetricsBoard {
    /// Registra una nuova metrica con il nome specificato.
    /// Se il nome è già associato a un handle ancora vivo, restituisce `Err(NameAlreadyUsed)`.
    fn register(&self, name: &str) -> Result<impl MetricHandle + 'static, NameAlreadyUsed> {
        let mut guard = self.inner.lock().unwrap();

        if guard.contains_key(name) {
            Err(NameAlreadyUsed)
        } else {
            let new_value = Arc::new(MyValue::new());
            guard.insert(name.to_string(), Arc::clone(&new_value));
            Ok(CondvarMetricHandle {
                shared_state: Arc::clone(&self.inner),
                key: name.to_string(),
                value: new_value,
            })
        }
    }

    /// Restituisce un'istantanea atomica coerente di tutte le metriche correntemente registrate.
    ///
    /// ## Garanzia di Coerenza Istantanea:
    /// 1. Mantiene il lock sul dizionario globale (bloccando inserimenti e cancellazioni concorrenti).
    /// 2. Acquisisce contemporaneamente il lock di TUTTE le metriche presenti.
    /// 3. Legge simultaneamente i valori float in un unico istante senza possibilità di torn reads.
    fn snapshot(&self) -> Vec<(String, f64)> {
        let guard = self.inner.lock().unwrap();

        // 1. Raccoglie i riferimenti a tutte le metriche attive
        let entries: Vec<_> = guard
            .iter()
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();

        // 2. Acquisisce contemporaneamente il lock su TUTTI i valori
        let locked_guards: Vec<_> = entries
            .iter()
            .map(|(k, v)| (k, v.value.lock().unwrap()))
            .collect();

        // 3. Legge i valori simultaneamente in un unico istante coerente
        locked_guards
            .into_iter()
            .map(|(k, g)| (k.clone(), *g))
            .collect()
    }

    /// Blocca il chiamante finché il valore della metrica `name` non raggiunge o supera `threshold`.
    ///
    /// ## Assenza di Blocco Globale:
    /// Rilascia immediatamente il lock del dizionario `self.inner` prima di mettersi in attesa
    /// sulla `Condvar` locale della singola metrica, evitando di bloccare il board per gli altri thread.
    fn wait_for_threshold(&self, name: &str, threshold: f64) {
        let guard = self.inner.lock().unwrap();

        if let Some(entry) = guard.get(name) {
            let cloned_entry = Arc::clone(entry);
            let guard_value = cloned_entry.value.lock().unwrap();
            let cvar = &cloned_entry.cvar;

            // Rilascia il lock globale del dizionario prima di attendere
            drop(guard);

            // Attesa non-busy: sospende il thread senza consumo di CPU finché il valore è inferiore alla soglia
            drop(cvar.wait_while(guard_value, |c| *c < threshold).unwrap());
        } else {
            panic!("La metrica richiesta non è registrata nel board");
        }
    }
}

/// Costruttore per la versione basata su `Condvar` e granularità mista.
pub fn make_condvar_metrics_board() -> impl MetricsBoard {
    CondvarMetricsBoard::new()
}
