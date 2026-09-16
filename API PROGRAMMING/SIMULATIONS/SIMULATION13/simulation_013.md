# Simulazione 013 — AgingScheduler

*Una coda a priorità concorrente in cui la priorità degli elementi cresce col passare del tempo (invecchiamento / aging), combinata con la possibilità di annullare o promuovere elementi già accodati tramite un ticket dedicato, e con supporto per la chiusura controllata (`close`).*

---

## AgingScheduler

Nei sistemi operativi, per prevenire la starvation dei processi a bassa priorità si impiega la tecnica dell'**invecchiamento** (*aging*): col passare del tempo, la priorità effettiva di ciascun elemento in attesa cresce gradualmente, garantendo che anche i task a priorità iniziale minima vengano prima o poi serviti.

Si scriva in Rust una struttura che implementi il tratto generico `AgingScheduler<T: Send>`, una coda con priorità thread-safe in cui:
- Ogni elemento viene inserito con una priorità di base (`u32`) e un parametro globale di invecchiamento `aging_rate` ($unità/secondo$).
- `push()` restituisce un `Option<impl Ticket>`, che consente al chiamante di annullare (`cancel()`) o promuovere (`boost()`) l'elemento mentre è ancora in attesa. Se lo scheduler è già stato chiuso, `push()` restituisce `None`.
- `pop()` preleva l'elemento con la priorità effettiva più alta, bloccandosi se la coda non contiene elementi validi disponibili. Se lo scheduler è chiuso e non ci sono più elementi validi, `pop()` restituisce `None`.
- `close()` chiude lo scheduler: successive `push()` falliscono restituendo `None`, mentre `pop()` permette di drenare tutti gli elementi residui prima di restituire `None`.

### API richiesta

```rust
pub trait Ticket: Send + Sync {
    // Annulla l'elemento associato a questo ticket, se non è ancora stato
    // prelevato da `pop()`. Restituisce `true` se l'annullamento ha avuto
    // effetto, `false` se l'elemento era già stato prelevato o annullato.
    fn cancel(&self) -> bool;

    // Aumenta immediatamente, di `extra`, la priorità di base dell'elemento associato,
    // sommandosi a qualunque invecchiamento già maturato.
    // Non ha effetto se l'elemento è già stato prelevato o annullato.
    fn boost(&self, extra: u32);
}

pub trait AgingScheduler<T: Send>: Clone + Send + Sync {
    // Inserisce `value` con priorità di base `priority` (valori più alti
    // indicano priorità maggiore) e restituisce un ticket. Se lo scheduler
    // è stato chiuso, restituisce `None`.
    fn push(&self, value: T, priority: u32) -> Option<impl Ticket + 'static>;

    // Preleva l'elemento non annullato con la priorità effettiva più alta
    // al momento della chiamata, bloccando senza consumare cicli di CPU se
    // non ce ne sono. La priorità effettiva di un elemento è la sua
    // priorità di base (comprensiva di eventuali `boost`) incrementata di
    // `aging_rate` unità per ogni secondo trascorso dal suo inserimento.
    // Se lo scheduler è chiuso e non ci sono elementi validi, restituisce `None`.
    fn pop(&self) -> Option<T>;

    // Chiude lo scheduler, impedendo nuovi inserimenti e risvegliando
    // eventuali consumatori bloccati in attesa.
    fn close(&self);
}

pub fn make_aging_scheduler<T: Send + Sync + 'static>(aging_rate: f64) -> impl AgingScheduler<T> {
    ...
}
```

### Requisiti

- La struttura deve essere clonabile (`Clone`), thread-safe (`Send + Sync`) e condivisibile tra più produttori e consumatori concorrenti.
- `pop()` deve sempre restituire, tra gli elementi non annullati presenti in coda al momento della chiamata, quello con la priorità effettiva più alta secondo la formula data.
- Un `cancel()` riuscito fa sì che l'elemento non venga mai restituito da `pop()`, senza che il chiamante di `pop()` debba occuparsene esplicitamente.
- Un `boost()` deve avere effetto immediato sulle valutazioni successive di priorità effettiva.
- La chiusura con `close()` deve consentire di svuotare tutti gli elementi residui non annullati prima di restituire `None`.
- Nessuna attesa attiva in nessun punto del sistema.
- Tutti i test devono passare con `cargo test`.
