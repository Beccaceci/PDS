use std::{collections::HashMap, sync::{Arc, Condvar, Mutex}, thread::{self, JoinHandle}, time::{Duration, Instant}};

/// `InnerState` rappresenta lo stato condiviso tra il thread principale e il thread in background.
/// Viene protetto da un `Mutex` all'interno dell'Aggregator.
pub struct InnerState {
    /// Contiene le misure grezze raggruppate per sensore.
    /// La chiave è il `sensor_id`, il valore è un vettore di tuple (temperatura, istante di ricezione).
    measurements: HashMap<usize, Vec<(f64, Instant)>>,
    /// Flag utilizzato per controllare il ciclo di vita del thread in background. 
    /// Quando diventa `false` (nel Drop), il thread si arresta.
    running: bool,
    /// Cache delle medie calcolate nell'ultima finestra temporale.
    recent_averages: Vec<Average>
}

impl InnerState {
    pub fn new () -> Self {
        Self {
            measurements: HashMap::new(),
            running: true,
            recent_averages: Vec::new()
        }
    }
}

/// L'Aggregator è la struttura principale. Coordina l'aggiunta di misure e 
/// gestisce un thread in background che calcola periodicamente le medie.
pub struct Aggregator {
    /// L'Arc condivide la proprietà dello stato tra i vari thread. 
    /// Contiene il Mutex (per i dati) e la Condvar (per far dormire/svegliare il thread in background).
    state: Arc<(Mutex<InnerState>, Condvar)>,
    /// Maniglia del thread in background, necessaria per poterne fare il join() ed evitare memory leak alla chiusura.
    join_handle: Option<JoinHandle<()>>
}

/// Struttura che rappresenta la media calcolata per un singolo sensore.
pub struct Average {
    pub sensor_id: usize,
    pub reference_time: Instant, // istante in cui e' stata calcolata la media
    pub average_temperature: f64,
}

impl Average {
    pub fn new (_sensor_id: usize, time: Instant, avg_temp: f64) -> Self {
        Self {
            sensor_id: _sensor_id,
            reference_time: time,
            average_temperature: avg_temp
        }
    }
}

impl Aggregator {
    /// Inizializza l'Aggregator e fa partire il thread in background responsabile dei calcoli.
    pub fn new(sample_time_millis: u64) -> Self {
        let mutex = Mutex::new(InnerState::new());
        let cvar = Condvar::new();
        let reference_counter = Arc::new((mutex, cvar));
        
        // Cloniamo l'Arc per passarlo al thread in background
        let cloned_arc = Arc::clone(&reference_counter);

        // Creiamo il thread "worker" che si occuperà di calcolare le medie
        let handle = thread::spawn(move || {
            loop {
                let mut state = cloned_arc.0.lock().unwrap();
                let cvar = &cloned_arc.1;
                
                // Il thread dorme per `sample_time_millis`.
                // Usiamo `wait_timeout_while` con la condizione `c.running`: se `running` è true,
                // tornerà a dormire in caso di risvegli spuri, ma uscirà comunque allo scadere del timeout.
                // Se `running` diventa false (es. chiamato dal Drop), esce subito.
                (state, _) = cvar.wait_timeout_while(state, Duration::from_millis(sample_time_millis), |c| {
                    c.running
                }).unwrap();

                // Se l'aggregatore è stato distrutto, interrompiamo il ciclo infinito per terminare il thread
                if !state.running {
                    break;
                }

                // CALCOLO DELLE MEDIE E PULIZIA:
                // `drain()` svuota l'intera HashMap restituendo contemporaneamente i suoi elementi all'iteratore.
                // Questo è cruciale: in un solo colpo calcoliamo le medie e "resettiamo" le misure per il prossimo intervallo temporale!
                state.recent_averages = state.measurements.drain().map(|(sensor_id, vec_measures)| {
                    // Sommiamo tutte le temperature (ignoriamo _instant usando il prefisso '_' per evitare warning)
                    let sum:f64 = vec_measures.iter().map(|&(temp, _instant)| temp).sum();
                    let count = vec_measures.len() as f64;

                    Average::new(
                        sensor_id,
                        Instant::now(),
                        sum / count
                    )
                }).collect();
            }
        });

        Self {
            state: reference_counter,
            join_handle: Some(handle)
        }
    }

    /// Aggiunge una misura di temperatura per il sensore con id 'sensor_id' e temperatura 'temperature'. 
    /// Le misure sono automaticamente etichettate con l'istante temporale in cui sono comunicate.
    pub fn add_measure(&self, sensor_id: usize, temperature: f64) {
        let mut state = self.state.0.lock().unwrap();
        // Se il sensore esiste già nella HashMap, aggiungiamo la misura al suo vettore.
        if let Some(entries) = state.measurements.get_mut(&sensor_id) {
            entries.push((temperature, Instant::now()));
        }
        else {
            // Altrimenti, inseriamo una nuova entry per questo sensore.
            state.measurements.insert(sensor_id, vec![(temperature, Instant::now())]);
        }
    }

    /// Restituisce un vettore che riporta la temperatura media di ciascun sensore, calcolata durante l'ultimo periodo di campionamento.
    /// Sono presenti solo i sensori che hanno inviato almeno una misura.
    pub fn get_averages(&self) -> Vec<Average> {
        let state = self.state.0.lock().unwrap();
        // Poiché non possiamo usare `#[derive(Clone)]` su `Average` (per vincoli di traccia),
        // cloniamo i dati manualmente mappando il vettore in nuove istanze di Average identiche alle originali.
        state.recent_averages.iter().map(|a| Average::new(
            a.sensor_id,
            a.reference_time,
            a.average_temperature
        )).collect()
    }
}

/// Implementazione personalizzata della distruzione (Drop) per spegnere il thread in sicurezza.
impl Drop for Aggregator {
    fn drop(&mut self) {
        let mut state = self.state.0.lock().unwrap();
        // 1. Diciamo al thread che è ora di fermarsi
        state.running = false;
        drop(state);
        
        // 2. Svegliamo il thread che sta dormendo nella Condvar (altrimenti dovremmo aspettare la fine del suo timeout!)
        self.state.1.notify_one();

        // 3. Facciamo la join() sull'handle del thread per attendere FISICAMENTE 
        // che concluda le sue operazioni ed eviti memory leak o errori asincroni.
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.join(); 
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    // Helper per verificare gli Average (visto che Average non deriva PartialEq/Debug)
    fn check_averages(actual: &[Average], expected: &[(usize, f64)]) {
        assert_eq!(actual.len(), expected.len(), "Il numero di sensori mediati non corrisponde");
        for (exp_id, exp_temp) in expected {
            let mut found = false;
            for a in actual {
                if a.sensor_id == *exp_id {
                    // Tolleranza per f64
                    assert!((a.average_temperature - exp_temp).abs() < f64::EPSILON, 
                            "Temperatura errata per sensore {}: attesa {}, trovata {}", exp_id, exp_temp, a.average_temperature);
                    found = true;
                    break;
                }
            }
            assert!(found, "Sensore {} mancante nell'output", exp_id);
        }
    }

    #[test]
    fn test_empty_aggregator() {
        // Appena creato o senza misure, non deve restituire nulla
        let agg = Aggregator::new(100);
        let avgs = agg.get_averages();
        assert!(avgs.is_empty(), "Aggregator appena creato deve restituire vettore vuoto");
    }

    #[test]
    fn test_single_measure() {
        let agg = Aggregator::new(200);
        agg.add_measure(1, 25.5);
        
        // Aspettiamo che il thread in background finisca il periodo di campionamento
        thread::sleep(Duration::from_millis(250));
        
        let avgs = agg.get_averages();
        check_averages(&avgs, &[(1, 25.5)]);
    }

    #[test]
    fn test_multiple_measures_same_sensor() {
        let agg = Aggregator::new(300);
        agg.add_measure(42, 10.0);
        agg.add_measure(42, 20.0);
        agg.add_measure(42, 30.0); // Media attesa: 20.0
        
        thread::sleep(Duration::from_millis(350));
        
        let avgs = agg.get_averages();
        check_averages(&avgs, &[(42, 20.0)]);
    }

    #[test]
    fn test_multiple_sensors() {
        let agg = Aggregator::new(200);
        agg.add_measure(1, 15.0);
        agg.add_measure(2, 40.0);
        agg.add_measure(1, 17.0); // Media S1: 16.0
        agg.add_measure(2, 40.0); // Media S2: 40.0
        
        thread::sleep(Duration::from_millis(250));
        
        let avgs = agg.get_averages();
        check_averages(&avgs, &[(1, 16.0), (2, 40.0)]);
    }

    #[test]
    fn test_rolling_window_clears_old_data() {
        let agg = Aggregator::new(200);
        
        // Prima finestra
        agg.add_measure(1, 10.0);
        thread::sleep(Duration::from_millis(250));
        let avgs = agg.get_averages();
        check_averages(&avgs, &[(1, 10.0)]);

        // Seconda finestra: sensore 1 non invia dati, sensore 2 invia 20.0
        agg.add_measure(2, 20.0);
        thread::sleep(Duration::from_millis(250));
        
        let avgs2 = agg.get_averages();
        // S1 non deve esserci, S2 deve esserci (media 20.0)
        check_averages(&avgs2, &[(2, 20.0)]);
    }

    #[test]
    fn test_concurrent_measurements() {
        use std::sync::Arc;
        
        let agg = Arc::new(Aggregator::new(300));
        let a1 = Arc::clone(&agg);
        let a2 = Arc::clone(&agg);

        let t1 = thread::spawn(move || {
            a1.add_measure(1, 10.0);
            a1.add_measure(1, 20.0);
        });

        let t2 = thread::spawn(move || {
            a2.add_measure(2, 100.0);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // Aspettiamo la fine della finestra
        thread::sleep(Duration::from_millis(350));
        
        let avgs = agg.get_averages();
        check_averages(&avgs, &[(1, 15.0), (2, 100.0)]);
    }

    #[test]
    fn test_drop_terminates_background_thread_safely() {
        let agg = Aggregator::new(100);
        agg.add_measure(1, 50.0);
        
        // Droppiamo l'aggregator esplicitamente
        drop(agg);
        
        // Aspettiamo per dare tempo al thread di terminare (non dovrebbe generare panic)
        thread::sleep(Duration::from_millis(200));
        // Se il programma arriva qui senza blocchi o deadlock, il Drop ha chiuso bene il thread.
    }
}
