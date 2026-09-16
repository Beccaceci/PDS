# Simulazione 008 — DelayQueue

*Non è una traduzione dal catalogo storico né un'estensione diretta di un campione reale — è un'estrapolazione motivata verso un meccanismo di sincronizzazione mai testato finora: l'attesa non è più "finché una condizione diventa vera", ma "finché passa un tempo che può cambiare mentre sto già aspettando". Confidenza: media, come le altre estrapolazioni non ancorate al catalogo.*

---

## DelayQueue

Molti sistemi devono rendere disponibile un dato solo dopo un certo intervallo di tempo dalla sua produzione — un tentativo di retry da ritardare dopo un fallimento, una voce di cache con scadenza, un timer di sistema. A differenza di una coda ordinaria, qui la disponibilità di un elemento non dipende solo dall'ordine di arrivo, ma da un'informazione temporale associata a ciascun elemento, che può anche essere annullata prima che scada.

Si scrivano in Rust le strutture che implementano i tratti generici `ScheduledItem<T: Send>` e `DelayQueue<T: Send>` definiti di seguito.

### API richiesta

```rust
use std::time::Duration;

pub trait ScheduledItem<T: Send> {
    // Annulla la consegna futura dell'elemento associato, se non è ancora
    // stato reso disponibile a `pop`. Restituisce `true` se l'annullamento
    // ha avuto effetto, `false` se l'elemento era già stato consegnato (o
    // il suo termine era già scaduto al momento della chiamata).
    fn cancel(&self) -> bool;
}

pub trait DelayQueue<T: Send>: Clone {
    // Inserisce `value`, che diventerà disponibile per `pop` solo dopo che
    // sarà trascorso `delay` dal momento di questa chiamata. Restituisce un
    // handle che permette di annullarne la consegna futura.
    fn push_after(&self, value: T, delay: Duration) -> impl ScheduledItem<T>;

    // Preleva l'elemento non ancora annullato con la scadenza più vicina.
    // Se il suo termine non è ancora trascorso, blocca il chiamante fino
    // alla scadenza — non oltre, e senza consumare cicli di CPU nel
    // frattempo. Se non ci sono elementi in coda, blocca indefinitamente
    // finché non ne arriva uno.
    fn pop(&self) -> T;
}

pub fn make_delay_queue<T: Send>() -> impl DelayQueue<T> {
    ...
}
```

### Requisiti

- La struttura deve essere thread-safe e condivisibile tra più produttori e più consumatori (da cui `Clone` sul tratto `DelayQueue` stesso).
- `pop()` deve restituire sempre l'elemento non annullato con la scadenza più vicina, non necessariamente il primo inserito.
- Se, mentre un thread è bloccato in `pop()` in attesa della scadenza dell'elemento più vicino conosciuto, un altro thread inserisce un elemento con scadenza ancora più vicina, il thread in attesa deve accorgersene e ridurre di conseguenza il proprio tempo di attesa residuo — non deve attendere fino alla scadenza dell'elemento che conosceva all'inizio.
- Un `cancel()` riuscito rimuove l'elemento dalla coda in modo che non venga mai restituito da `pop()`, senza che il chiamante di `pop()` debba occuparsene esplicitamente.
- Nessuna attesa attiva in nessun punto.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

1. Rappresentare lo stato condiviso con una struttura contenente gli elementi non ancora consegnati, ciascuno associato al proprio istante di scadenza (`std::time::Instant`) e a un flag di annullamento, protetta da `Mutex` + `Condvar` dentro un `Arc`.
2. In `pop()`, calcolare la durata residua fino alla scadenza dell'elemento più vicino non annullato e attendere con `wait_timeout_while` per (al più) quella durata, ricontrollando la condizione al risveglio — sia per un risveglio spurio, sia perché nel frattempo è arrivato un elemento con scadenza più vicina.
3. `push_after` deve sempre notificare (`notify_all`) dopo aver inserito il nuovo elemento: è l'unico modo per far sì che un `pop()` già in attesa su una scadenza più lontana si accorga di doverne attendere una più vicina.
4. `cancel()` deve limitarsi a marcare l'elemento come annullato (stesso principio "scartato silenziosamente" già visto in `forgettable_channel`) — non serve rimuoverlo fisicamente subito; `pop()` lo salterà quando lo incontra.

---

## Meta-commentario

**Cosa introduce di nuovo rispetto a tutti i campioni precedenti:** finora, ogni attesa era condizionata da un evento binario — una risorsa che si libera, un partner che arriva, una coda che si riempie o svuota. Qui l'attesa è condizionata dal **tempo stesso**, e quel tempo può accorciarsi mentre lo si sta già attendendo. Un `wait_timeout_while` con una durata fissata una sola volta all'ingresso — l'errore naturale, e l'esatta ripetizione dell'idioma usato correttamente in `ResourcePool::acquire_timeout` ma qui applicato nel posto sbagliato — supera i test con un solo elemento in coda ma fallisce silenziosamente (senza panico, semplicemente restituendo l'elemento sbagliato o in ritardo) non appena un secondo elemento con scadenza più vicina viene inserito mentre `pop()` è già in attesa.

**Perché il tratto `ScheduledItem` non usa `Drop`:** qui l'annullamento è, come in `forgettable_channel`, un'operazione che può fallire in modo significativo (l'elemento potrebbe essere già scaduto) — un `bool` di ritorno è più informativo di un `Drop` silenzioso, e alternare i due stili tra una simulazione e l'altra (invece di applicare sempre lo stesso) riflette meglio il fatto che i due campioni reali raccolti finora non concordano su quale usare sempre.

**Difficoltà stimata:** paragonabile a `RwCell` (simulazione 007); l'insidia principale non è l'architettura ma la correttezza della ri-valutazione del tempo di attesa. Budget consigliato: 90–110 minuti.
