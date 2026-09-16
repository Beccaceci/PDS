# Simulazione 019 — Broadcast

*Prima simulazione in cui l'"handle" non è un tratto inventato ad hoc, ma il tratto standard `Iterator` stesso — argomento di corso (collezioni) mai comparso finora in questa veste. Testa se si riconosce quando riusare un tratto della libreria standard è la scelta corretta, invece di inventarne uno nuovo per abitudine.*

---

## Broadcast

Un flusso di eventi che più iscritti vogliono consumare uno alla volta, in ordine, bloccandosi quando non c'è nulla di nuovo, è esattamente la semantica di un iteratore Rust — con la differenza che qui il "prossimo elemento" può non essere ancora disponibile e richiedere un'attesa.

Si scriva in Rust una struttura che implementi il tratto generico `Broadcast<T: Send + Clone>` definito di seguito.

### API richiesta

```rust
pub trait Broadcast<T: Send + Clone>: Clone {
    // Pubblica un nuovo valore, visibile a tutti gli iscritti creati prima
    // di questa chiamata.
    fn publish(&self, value: T);

    // Crea un nuovo iscritto. Il tipo restituito deve implementare anche
    // il tratto standard `Iterator<Item = T>`: ogni chiamata al suo
    // metodo `next()` deve bloccare, senza consumare cicli di CPU, finché
    // non è disponibile un nuovo valore pubblicato dopo la sottoscrizione,
    // e deve restituire `None` solo dopo che `close()` è stato chiamato e
    // non ci sono più valori residui da consegnare a questo iscritto.
    fn subscribe(&self) -> impl Iterator<Item = T> + Send;

    // Chiude il flusso: nessun iscritto (esistente o futuro) riceverà
    // ulteriori valori oltre a quelli già pubblicati prima di questa
    // chiamata.
    fn close(&self);
}

pub fn make_broadcast<T: Send + Clone>() -> impl Broadcast<T> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`).
- L'oggetto restituito da `subscribe()` deve poter essere usato direttamente in un ciclo `for`, senza chiamate esplicite a `.next()`.
- Ogni iscritto riceve tutti e soli i valori pubblicati dopo la propria sottoscrizione, nell'ordine di pubblicazione.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Il tipo concreto restituito da `subscribe()` deve implementare `Iterator<Item = T>`, il che significa scrivere `fn next(&mut self) -> Option<T>` (non `&self`, a differenza di quasi ogni altro metodo "handle" visto finora nella serie) sulla struttura che rappresenta l'iscrizione — internamente può appoggiarsi a un canale (`std::sync::mpsc`) registrato in una struttura condivisa protetta da `Mutex`, popolata da `publish()`.

---

## Meta-commentario

**Perché conta che `next()` prenda `&mut self`:** è la prima volta nella serie che il metodo "principale" di un tratto handle richiede accesso mutabile esclusivo invece di condiviso — conseguenza diretta della firma di `Iterator::next`, non di una scelta del problema. Chi ha interiorizzato "i metodi handle prendono sempre `&self`" dai problemi precedenti deve riconoscere qui l'eccezione, imposta dal tratto della libreria standard che si sta implementando, non negoziabile.

**Perché riusare `Iterator` invece di un tratto proprio è la scelta giusta:** un tratto inventato (`fn recv(&self) -> Option<T>`) avrebbe funzionato altrettanto bene per la sola logica di ricezione, ma avrebbe perso gratuitamente tutta l'ergonomia che Rust offre agli iteratori (cicli `for`, `.map()`, `.take()`, ecc.) — un segnale che, quando la semantica di un problema coincide con quella di un tratto standard, implementare quel tratto è spesso la soluzione più idiomatica, non solo una scorciatoia.

**Difficoltà stimata:** paragonabile a `forgettable_channel`; la sincronizzazione è la stessa di sempre, la novità è tutta nella forma dell'API. Budget consigliato: 60 minuti.
