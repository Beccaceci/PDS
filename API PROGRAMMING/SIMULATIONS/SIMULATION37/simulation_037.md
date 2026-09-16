# Simulazione 037 — PriorityLock (capstone)

*Argomento nuovo e classico della programmazione di sistema: l'ereditarietà di priorità (*priority inheritance*), la tecnica che previene l'inversione di priorità — resa famosa dal bug che quasi compromise la missione Mars Pathfinder della NASA. Diverso da ogni problema precedente in una proprietà specifica: qui **il solo fatto di mettersi in attesa** ha un effetto immediato e osservabile sullo stato condiviso, prima ancora che l'attesa termini — non solo l'esito finale di un'operazione, come in tutta la serie fino ad ora.*

---

## PriorityLock

Se un thread a bassa priorità detiene un lock che un thread a priorità più alta sta aspettando, un sistema che non se ne accorge rischia l'inversione di priorità: il thread a bassa priorità, non essendo esso stesso prioritario, può essere tenuto in attesa da thread a priorità intermedia che non hanno nulla a che fare con quel lock — ritardando indirettamente anche il thread ad alta priorità che aspetta il rilascio. La soluzione classica è elevare temporaneamente la priorità del possessore del lock a quella del richiedente più prioritario in attesa, finché non lo rilascia.

Si scrivano in Rust le strutture che implementano i tratti `PriorityGuard<T>` e `PriorityLock<T>` definiti di seguito.

### API richiesta

```rust
pub trait PriorityGuard<T: Send> {
    fn get(&self) -> &T;
    fn get_mut(&mut self) -> &mut T;
}

pub trait PriorityLock<T: Send>: Clone + Send + Sync {
    // Acquisisce l'accesso esclusivo al valore protetto, con la priorità
    // del chiamante indicata da `caller_priority` (valori più alti = più
    // prioritario). Blocca il chiamante, senza consumare cicli di CPU, se
    // il lock è già posseduto da un altro thread.
    //
    // Nel momento stesso in cui questo metodo inizia ad attendere (senza
    // attendere che l'attesa finisca), se `caller_priority` è maggiore
    // della priorità effettiva corrente del possessore, quella del
    // possessore deve elevarsi immediatamente a `caller_priority` —
    // osservabile subito tramite current_holder_priority(). Quando il
    // possessore rilascia il lock, se questo chiamante non è quello a cui
    // viene concesso, la sua priorità smette di contribuire al calcolo
    // della priorità effettiva del nuovo possessore.
    fn acquire(&self, caller_priority: u32) -> impl PriorityGuard<T>;

    // Priorità effettiva corrente del possessore del lock — il massimo
    // tra la priorità con cui l'ha acquisito e le priorità di tutti i
    // richiedenti attualmente in attesa — oppure None se il lock è
    // libero.
    fn current_holder_priority(&self) -> Option<u32>;
}

pub fn make_priority_lock<T: Send + 'static>(value: T) -> impl PriorityLock<T> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone + Send + Sync`); al più un `PriorityGuard<T>` attivo alla volta.
- `current_holder_priority()` deve riflettere in ogni istante il massimo tra la priorità di acquisizione del possessore corrente e le priorità di tutti i richiedenti attualmente bloccati in `acquire()` — aggiornato immediatamente ad ogni nuovo richiedente che inizia ad attendere, non solo quando qualcuno ottiene effettivamente il lock.
- Quando il possessore rilascia il lock (uscita dallo scope del `PriorityGuard<T>`, RAII tramite `Drop`), se ci sono richiedenti in attesa, il lock deve essere concesso a quello con la priorità di richiesta più alta tra essi (non necessariamente il primo arrivato).
- Dopo il rilascio, `current_holder_priority()` deve riflettere correttamente il nuovo stato: la priorità di acquisizione del nuovo possessore combinata con le priorità degli eventuali richiedenti ancora in attesa (che potrebbe risultare inferiore al valore osservato un istante prima, se il richiedente con la priorità più alta era proprio quello appena diventato possessore).
- Nessuna attesa attiva.
- I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con: il valore protetto, la priorità di acquisizione del possessore corrente (se posseduto), e una collezione delle priorità dei richiedenti attualmente in attesa (ad esempio un `Vec<u32>` o un `BinaryHeap<u32>`), protetti da `Mutex` + `Condvar` dentro un `Arc`. `acquire(priority)`: se il lock è libero, diventa immediatamente possessore; altrimenti inserisce la propria priorità nella collezione degli attendenti — **prima** di iniziare ad attendere, cosicché `current_holder_priority()` la veda subito — poi attende (`wait_while`) finché non è lui stesso a diventare possessore, rimuovendo a quel punto la propria priorità dalla collezione degli attendenti. Il `Drop` del guardiano, se la collezione degli attendenti non è vuota, sceglie quello con la priorità più alta, lo rimuove dalla collezione e lo imposta come nuovo possessore, quindi notifica (`notify_all`, dato che ogni attendente deve ricontrollare individualmente se è stato scelto lui).

---

## Meta-commentario

**Perché questo problema è diverso da ogni altro della serie in una proprietà specifica:** in tutti i problemi precedenti, un thread in attesa era osservabile solo indirettamente (tramite l'eventuale esito della sua chiamata) — lo stato condiviso rifletteva sempre e solo ciò che era già *accaduto*. Qui, il solo atto di iniziare ad attendere altera immediatamente uno stato osservabile da chiunque altro (`current_holder_priority()`), prima che quell'attesa produca alcun esito. Un'implementazione che aggiornasse la priorità effettiva del possessore solo al momento del rilascio (un errore naturale, per abitudine, dato che quasi ogni altro problema della serie "fa qualcosa" solo quando un'operazione si completa) non previene affatto l'inversione di priorità — l'intero scopo del pattern richiede che l'elevazione sia immediata, non differita.

**Perché `Vec<u32>`/`BinaryHeap<u32>` invece di un semplice contatore massimo aggiornato incrementalmente:** un massimo tenuto per pura incrementazione (aggiornato solo quando un nuovo attendente arriva con priorità più alta) si romperebbe al momento della rimozione — se l'attendente con la priorità più alta ottiene il lock o smette di attendere, occorre poter ricalcolare il nuovo massimo tra quelli *rimasti*, non solo saperlo confrontare con uno nuovo in ingresso.

**Perché conta come esercizio capstone:** la combinazione di un effetto collaterale immediato dell'attesa stessa, di una struttura dati che deve supportare sia inserimento sia rimozione con ricalcolo del massimo, e dell'assegnazione per priorità al rilascio, eccede probabilmente il tempo di un singolo appello. Budget stimato: 110–140 minuti.
