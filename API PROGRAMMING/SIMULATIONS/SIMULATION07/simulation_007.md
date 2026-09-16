# Simulazione 007 — RwCell

*Stessa filosofia della simulazione 006: due tratti-handle con `Drop` proprio, oltre al tratto di servizio. Introduce due dimensioni di difficoltà non ancora toccate da nessun campione reale né da nessuna simulazione precedente: la **preferenza dello scrittore** (per evitare starvation) e la distinzione `Send` vs `Send + Sync`.*

---

## RwCell

Molte strutture dati condivise vengono lette molto più spesso di quanto vengano modificate: permettere a più lettori di accedere contemporaneamente, riservando l'accesso esclusivo solo a chi scrive, migliora sensibilmente il throughput rispetto a un semplice mutex. È il classico problema **lettori/scrittori**: più lettori possono procedere insieme, ma uno scrittore richiede accesso esclusivo, e un flusso continuo di lettori non deve poter far attendere indefinitamente uno scrittore in coda (starvation).

Si scrivano in Rust le strutture che implementano i tratti generici `ReadGuard<T>`, `WriteGuard<T>` e `RwCell<T>` definiti di seguito.

### API richiesta

```rust
pub trait ReadGuard<T: Send + Sync> {
    // Accesso condiviso al valore protetto.
    fn get(&self) -> &T;
}

pub trait WriteGuard<T: Send + Sync> {
    // Accesso condiviso al valore protetto.
    fn get(&self) -> &T;
    // Accesso esclusivo e mutabile al valore protetto.
    fn get_mut(&mut self) -> &mut T;
}

pub trait RwCell<T: Send + Sync> {
    // Blocca il chiamante, senza consumare cicli di CPU, finché non è
    // possibile concedere accesso in lettura: cioè finché nessuno
    // scrittore ha accesso attivo e nessuno scrittore è in attesa da prima
    // di questa chiamata.
    fn read(&self) -> impl ReadGuard<T>;

    // Blocca il chiamante, senza consumare cicli di CPU, finché non è
    // possibile concedere accesso esclusivo: cioè finché non ci sono né
    // lettori né scrittori attivi.
    fn write(&self) -> impl WriteGuard<T>;
}

pub fn make_rw_cell<T: Send + Sync>(value: T) -> impl RwCell<T> {
    ...
}
```

### Requisiti

- Più `ReadGuard<T>` possono coesistere ed essere attivi contemporaneamente, anche su thread diversi.
- Al più un `WriteGuard<T>` può essere attivo alla volta, e la sua presenza esclude qualunque `ReadGuard<T>` o altro `WriteGuard<T>` attivo.
- **Preferenza dello scrittore**: se uno scrittore è in attesa, nessun nuovo lettore che arrivi successivamente deve poter ottenere accesso prima di lui, anche se l'accesso in lettura sarebbe altrimenti concedibile — i lettori già attivi al momento dell'arrivo dello scrittore possono invece completare normalmente.
- Quando un `ReadGuard<T>` o un `WriteGuard<T>` esce dallo scope, l'accesso corrispondente deve essere rilasciato automaticamente (RAII, tramite il tratto `Drop`), risvegliando eventuali richieste in attesa compatibili con il nuovo stato.
- Thread-safe, condivisibile tra più thread contemporaneamente.
- Nessuna attesa attiva in nessun punto.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

1. Rappresentare lo stato condiviso con una struttura che tenga: il valore `T`, un contatore di lettori attivi, un flag di scrittore attivo, e un contatore di scrittori in attesa. Proteggerla con `Mutex` + `Condvar`, dentro un `Arc` clonato da ogni handle.
2. `read()` deve attendere finché `scrittore_attivo` è falso **e** `scrittori_in_attesa == 0`, non semplicemente finché `scrittore_attivo` è falso — è questa seconda condizione a garantire la preferenza dello scrittore.
3. `write()` deve incrementare `scrittori_in_attesa` prima di iniziare ad attendere (per rendere visibile la propria richiesta ai lettori che arrivano nel frattempo), attendere finché `lettori_attivi == 0 && scrittore_attivo == false`, poi decrementare `scrittori_in_attesa` e impostare `scrittore_attivo`.
4. `Drop` per `ReadGuard<T>` decrementa `lettori_attivi` e sveglia gli attendenti solo se il contatore raggiunge zero; `Drop` per `WriteGuard<T>` azzera `scrittore_attivo` e sveglia sempre tutti gli attendenti (`notify_all`), perché sia lettori che scrittori potrebbero ora procedere.

---

## Meta-commentario

**Perché `Send + Sync` e non solo `Send` come negli altri campioni:** `ResourcePool` e `forgettable_channel` non hanno mai bisogno che due thread tengano contemporaneamente un riferimento condiviso allo *stesso* valore `T` — un elemento del pool va a un solo thread alla volta, un messaggio viene mosso interamente. Qui, invece, più `ReadGuard<T>` su thread diversi puntano realmente allo stesso `T` nello stesso istante: perché ciò sia sicuro, `T` deve essere anche `Sync`, non solo `Send`. È il primo problema della serie che costringe a ragionare esplicitamente sulla differenza tra i due marker trait, invece di scrivere `T: Send` per abitudine.

**Perché la preferenza dello scrittore è la parte difficile:** un'implementazione ingenua (lettori bloccati solo da uno scrittore *attivo*, non da uno *in attesa*) compila, supera i test con pochi thread, e produce starvation solo sotto carico sostenuto — esattamente il tipo di bug che un test nascosto con molti lettori concorrenti e un solo scrittore è pensato per scoprire.

**Difficoltà stimata:** la più alta finora tra i campioni generati; due tipi di guard con due implementazioni di `Drop` distinte, più l'invariante di fairness da mantenere sotto lock. Budget consigliato: 90–120 minuti.
