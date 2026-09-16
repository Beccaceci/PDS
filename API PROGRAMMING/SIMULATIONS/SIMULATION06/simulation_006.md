# Simulazione 006 — TransactionalQueue

*Calibrata deliberatamente più in alto delle simulazioni 002–005, per correggere un difetto strutturale: quei problemi non avevano nulla che passasse di mano da un chiamante all'altro, quindi collassavano a un solo tratto. Qui, come in `ResourcePool` e `forgettable_channel`, qualcosa **viene consegnato al chiamante e ha un proprio ciclo di vita indipendente** — il che impone un secondo tratto, e in questo caso combina entrambi i meccanismi di risoluzione già visti (RAII via `Drop` *e* un'azione esplicita) invece di usarne solo uno.*

---

## TransactionalQueue

Nei sistemi che devono garantire che un elemento venga inserito in una coda solo se l'intera operazione che lo produce va a buon fine, non basta un semplice `push`: serve prima **riservare** lo spazio (per rispettare un limite di capacità), poi decidere se **confermare** l'inserimento oppure abbandonarlo, lasciando che lo spazio riservato torni disponibile automaticamente. È lo schema delle scritture transazionali a due fasi, applicato a una coda limitata condivisa tra più produttori e consumatori.

Si scrivano in Rust le strutture che implementano i tratti generici `Reservation<T: Send>` e `TransactionalQueue<T: Send>` definiti di seguito.

### API richiesta

```rust
use std::time::Duration;

pub trait Reservation<T: Send> {
    // Consuma la prenotazione, inserendo definitivamente `value` in coda al
    // posto riservato. Se questo metodo non viene mai chiamato e l'oggetto
    // esce dallo scope, lo spazio riservato deve tornare disponibile senza
    // che nulla venga inserito in coda (RAII, tramite il tratto Drop).
    fn commit(self, value: T);
}

pub trait TransactionalQueue<T: Send>: Clone {
    // Riserva una posizione in coda. Se la coda ha già raggiunto la
    // capacità massima — contando sia gli elementi effettivamente inseriti
    // sia le prenotazioni ancora aperte — blocca il chiamante senza
    // consumare cicli di CPU finché una posizione non si libera (per una
    // pop, o per l'abbandono di un'altra prenotazione). Restituisce `None`
    // se la coda è chiusa o viene chiusa durante l'attesa.
    fn reserve(&self) -> Option<impl Reservation<T>>;

    // Variante con attesa limitata: come `reserve`, ma se non ottiene una
    // posizione entro `timeout` rinuncia e restituisce `None`. L'attesa non
    // deve consumare cicli di CPU.
    fn reserve_timeout(&self, timeout: Duration) -> Option<impl Reservation<T>>;

    // Preleva l'elemento meno recente tra quelli effettivamente committati
    // in coda, bloccando senza consumo di CPU se non ce ne sono. Restituisce
    // None solo quando la coda è stata chiusa e non contiene più elementi
    // committati né prenotazioni pendenti.
    fn pop(&self) -> Option<T>;

    // Impedisce nuove chiamate a `reserve`/`reserve_timeout`. Le
    // prenotazioni già aperte e gli elementi già committati restano validi.
    fn close(&self);

    // Numero massimo di posizioni gestite dalla coda.
    fn capacity(&self) -> usize;
}

pub fn make_transactional_queue<T: Send>(capacity: usize) -> impl TransactionalQueue<T> {
    ...
}
```

### Requisiti

- La struttura deve essere thread-safe e condivisibile tra più produttori e più consumatori (da cui `Clone` sul tratto `TransactionalQueue` stesso).
- In ogni istante, `(elementi committati in coda) + (prenotazioni aperte, non ancora committate né abbandonate) ≤ capacity`.
- Se una `Reservation<T>` viene distrutta senza che `commit` sia stato chiamato, la posizione riservata torna immediatamente disponibile per un nuovo `reserve`/`reserve_timeout`, e nessun valore viene inserito in coda.
- `pop()` deve restituire gli elementi committati nell'ordine in cui sono stati committati (non nell'ordine in cui le rispettive prenotazioni sono state aperte, se questo differisce).
- Nessuna attesa attiva in nessun punto del sistema.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

1. Rappresentare lo stato condiviso con una struttura che tenga: una `VecDeque<T>` per gli elementi committati, un contatore delle prenotazioni aperte, la `capacity` e un flag `closed`. Proteggerla con `Mutex` + `Condvar`, racchiusa in un `Arc` clonato da ogni parte del sistema che vi accede.
2. `reserve`/`reserve_timeout` devono attendere finché `committati + prenotazioni_aperte < capacity`, poi incrementare il contatore delle prenotazioni aperte prima di restituire l'oggetto `Reservation<T>`.
3. Il tipo che implementa `Reservation<T>` deve poter distinguere, al momento della propria distruzione, se `commit` è già stato chiamato oppure no — la stessa tecnica `Option<...>` + `.take()` già utilizzata per `Element<T>` in `ResourcePool` permette di farlo: `commit(mut self, value: T)` estrae ciò che serve con `.take()` prima di inserire il valore in coda, mentre `Drop` verifica se c'è ancora qualcosa da rilasciare.
4. `commit` deve decrementare il contatore delle prenotazioni aperte e incrementare la coda degli elementi committati in un'unica operazione atomica (sotto lo stesso lock), per evitare che un `reserve` concorrente osservi uno stato intermedio inconsistente.

---

## Meta-commentario

**Cosa distingue questo problema dalle simulazioni 002–005:** qui, come in `ResourcePool` e `forgettable_channel`, un oggetto "passa di mano" dal servizio al chiamante (`reserve()` restituisce una `Reservation<T>` che vive indipendentemente) e quell'oggetto ha un proprio ciclo di vita da gestire — motivo per cui serve un secondo tratto (`Reservation<T>`) oltre a quello del servizio (`TransactionalQueue<T>`), e un'architettura interna a più livelli (stato condiviso / struttura di servizio / struttura handle) anziché una singola struct piatta.

**Perché combina RAII esplicito con un'azione esplicita, invece di usarne solo uno come nei due campioni reali:** `ResourcePool` risolve il ciclo di vita del suo handle solo tramite `Drop`; `forgettable_channel` lo risolve solo tramite una chiamata esplicita (`forget()`). Qui la `Reservation<T>` ha **entrambe le vie di uscita** — un `commit()` esplicito che consuma `self`, oppure l'abbandono implicito via `Drop` — che è strutturalmente più simile a un guard RAII con "conferma opzionale", un pattern comune nella programmazione di sistema reale (transazioni, buffer di scrittura differita) ma non ancora testato in nessuno dei campioni raccolti finora.

**Difficoltà stimata:** superiore a `ResourcePool` da solo; paragonabile a `ResourcePool` più l'insidia di `Exchanger` (evitare stati intermedi inconsistenti) sommate. Budget consigliato: 90–110 minuti.
