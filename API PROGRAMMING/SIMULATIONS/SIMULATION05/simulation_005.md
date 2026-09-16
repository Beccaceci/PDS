# Simulazione 005 — BoundedQueue

*A differenza delle simulazioni 002–004, questa non è la traduzione diretta di un problema del catalogo storico C++ — è un'estensione naturale di `CBuffer<T>` (già imparentato con `forgettable_channel`) verso un angolo che né il catalogo storico né i due campioni Rust reali testano esplicitamente: **il produttore che si blocca**, non solo il consumatore. Confidenza: media — plausibile, non evidenziata.*

---

## BoundedQueue

Nei sistemi produttore/consumatore, un produttore più veloce del consumatore può accumulare dati senza limiti, esaurendo la memoria disponibile. Una coda **limitata** (bounded) risolve il problema imponendo una capacità massima: quando la coda è piena, il produttore stesso viene bloccato finché il consumatore non libera spazio. Questa pressione all'indietro è nota come **backpressure**.

Si scriva in Rust una struttura che implementi il tratto generico `BoundedQueue<T: Send>`, un canale produttore/consumatore con capacità massima fissata, in cui sia il produttore che il consumatore possono bloccarsi.

### API richiesta

```rust
pub trait BoundedQueue<T: Send>: Clone {
    // Inserisce `value` in coda. Se la coda ha raggiunto la capacità
    // massima, blocca il chiamante senza consumare cicli di CPU finché non
    // si libera spazio. Restituisce `false` se la coda è stata chiusa (il
    // valore non viene inserito), `true` altrimenti.
    fn push(&self, value: T) -> bool;

    // Preleva il valore più vecchio in coda (FIFO), bloccando senza
    // consumo di CPU se la coda è vuota. Restituisce `None` solo quando la
    // coda è stata chiusa e non contiene più valori.
    fn pop(&self) -> Option<T>;

    // Impedisce ulteriori `push`; i valori già in coda restano disponibili
    // per `pop` finché non si esauriscono.
    fn close(&self);
}

pub fn make_bounded_queue<T: Send>(capacity: usize) -> impl BoundedQueue<T> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile tra più produttori e più consumatori (da cui `Clone` sul tratto stesso).
- La coda non deve mai contenere più di `capacity` elementi.
- Un `push` bloccato per coda piena deve sbloccarsi non appena un `pop` libera spazio; un `pop` bloccato per coda vuota deve sbloccarsi non appena un `push` inserisce un valore.
- Dopo `close()`, i `push` successivi falliscono immediatamente (senza bloccare); i `pop` continuano a restituire i valori residui e poi `None`.
- Nessuna attesa attiva in nessun punto.
- I test devono passare senza modifiche; se il codice non compila, non verrà valutato.

### Suggerimenti implementativi

1. Stato condiviso: una `VecDeque<T>`, la `capacity`, e un flag `closed`, protetti da `Mutex` + **due** `Condvar` distinte (una per "coda non piena", una per "coda non vuota") oppure una sola `Condvar` condivisa risvegliata con `notify_all` a ogni cambiamento — la seconda opzione è più semplice da rendere corretta sotto pressione di tempo, la prima è più efficiente.
2. `push` attende (`wait_while`) finché `len() == capacity` e la coda non è chiusa; se chiusa, ritorna `false` senza attendere.
3. `pop` attende finché la coda è vuota e non chiusa; se vuota e chiusa, ritorna `None`.

---

## Meta-commentario

**Perché è un buon candidato nonostante non sia nel catalogo:** in tutti i problemi visti finora — reali e simulati — **solo un lato blocca** (chi consuma/acquisisce/attende un partner). Qui entrambi i lati possono bloccarsi, il che è un salto di difficoltà naturale e un'estensione plausibile di `CBuffer`/`forgettable_channel` una volta che lo studente ha dimostrato di padroneggiare la versione a un solo lato bloccante.

**Perché la confidenza è "media" e non "alta":** a differenza delle simulazioni 002–004, non ho trovato questo esatto problema nel catalogo storico — è un'estrapolazione motivata, non una traduzione.

**Difficoltà stimata:** superiore a `ResourcePool`; 75–90 minuti, per via della doppia condizione di blocco da gestire correttamente senza confonderle.
