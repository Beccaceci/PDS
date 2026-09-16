# Simulazione 002 — Canale di Lavoro a Priorità Annullabile (Cancelable Priority Work Channel)

*Generata come simulazione d'esame trait-driven per il corso di Programmazione di Sistema.*

---

## Canale di Lavoro a Priorità Annullabile

In molti sistemi concorrenti a elaborazione di compiti (task processing systems), i compiti (*work item*) vengono sottomessi da thread produttori e gestiti da thread consumatori in base a una **priorità** associata. Inoltre, un produttore che ha sottomesso un compito potrebbe avere la necessità di **annullarlo** prima che esso venga prelevato ed eseguito da un consumatore, oppure di verificarne lo stato di completamento.

Si scriva in Rust una struttura di comunicazione concorrente che implementi il pattern *Producer/Consumer* a priorità con supporto all'annullamento.

### API richiesta

```rust
pub trait CancelableHandle: Send + Sync {
    /// Annulla il task associato se non è ancora stato prelevato dal consumatore.
    /// Restituisce `true` se l'annullamento ha avuto successo prima del prelievo,
    /// `false` se il task era già stato estratto dal consumatore o già annullato.
    fn cancel(&self) -> bool;

    /// Restituisce `true` se il task è stato completato (ossia estratto con successo dal consumatore).
    fn is_done(&self) -> bool;
}

pub trait WorkProducer<T: Send>: Clone + Send + Sync {
    /// Sottomette `item` con una priorità `priority` (`u8`, valori più alti = priorità maggiore).
    /// Restituisce `Some(Arc<dyn CancelableHandle>)` se inviato con successo,
    /// oppure `None` se tutti i consumatori sono stati rilasciati (channel chiuso lato consumo).
    fn submit(&self, item: T, priority: u8) -> Option<Arc<dyn CancelableHandle>>;
}

pub trait WorkConsumer<T: Send>: Clone + Send + Sync {
    /// Estrae il compito valido (non annullato) con la priorità numerica più alta.
    /// Se in coda sono presenti compiti annullati, essi vengono scartati silenziosamente.
    /// Se la coda è vuota ma vi sono ancora produttori attivi, la chiamata si blocca (senza attesa attiva).
    /// Restituisce `None` solo quando tutti i produttori sono stati rilasciati e non ci sono più compiti validi.
    fn pop_next(&self) -> Option<T>;
}

pub fn create_channel<T: Send>() -> (impl WorkProducer<T>, impl WorkConsumer<T>);
```

### Requisiti

- **Ordinamento per Priorità**: I compiti in coda vengono restituiti in ordine decrescente di priorità (valori `u8` maggiori prima). A parità di priorità, l'ordine di estrazione è FIFO.
- **Annullamento Silenzioso**: Se un task viene annullato tramite `cancel()`, `pop_next()` deve scartarlo silenziosamente senza restituirlo al chiamante. `cancel()` restituisce `true` se il task era in coda e non ancora estratto, `false` altrimenti.
- **Stato Completato**: Quando `pop_next()` estrae un task non annullato, l'handle associato a quel task deve restituire `true` alla chiamata `is_done()`.
- **Rilascio Produttori**: Quando tutte le istanze di `WorkProducer` vengono distrutte (dropped) e non ci sono più elementi in coda, `pop_next()` sblocca eventuali consumatori bloccati e restituisce `None`.
- **Rilascio Consumatori**: Quando tutte le istanze di `WorkConsumer` vengono distrutte (dropped), le chiamate `submit(...)` su qualsiasi produttore restituiscono `None`.
- **Attesa Attiva Proibita**: Non è ammesso alcun ciclo di busy-waiting (`loop`, `spinlock`, o `sleep` ripetuti). Usare le primitive di sincronizzazione standard (`Mutex`, `Condvar`).
- **Thread Safety & Multi-Threading**: Sia `WorkProducer` che `WorkConsumer` devono implementare `Clone + Send + Sync` ed essere liberamente condivisibili ed eseguibili tra più thread contemporanei.
