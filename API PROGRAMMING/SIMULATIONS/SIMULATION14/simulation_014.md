# Simulazione 014 — VersionedCell

*Rottura deliberata di un pattern che si è consolidato in tutte e 13 le simulazioni precedenti e in entrambi i campioni reali: qui non serve alcun `Condvar`, e usarne uno sarebbe un errore concettuale, non solo di stile. Se le simulazioni precedenti hanno insegnato "quando vedi stato condiviso concorrente, pensa a `Mutex` + `Condvar`", questa insegna a riconoscere quando la seconda metà di quella regola non si applica.*

---

## VersionedCell

I thread di lavoro di un server leggono una configurazione condivisa con altissima frequenza e non devono mai essere rallentati da un aggiornamento in corso; un thread di amministrazione, più raro, deve poterla modificare — ma senza rischiare di sovrascrivere silenziosamente una modifica concorrente di un altro amministratore basata su uno stato ormai superato. È lo schema della **concorrenza ottimistica**: si legge sempre liberamente, e si scrive solo se nel frattempo nessun altro ha già scritto.

Si scrivano in Rust le strutture che implementano i tratti generici `Snapshot<T>` e `VersionedCell<T>` definiti di seguito.

### API richiesta

```rust
pub trait Snapshot<T: Clone + Send> {
    // Il valore osservato al momento della lettura.
    fn value(&self) -> &T;

    // La versione a cui corrisponde questo valore.
    fn version(&self) -> u64;
}

pub trait VersionedCell<T: Clone + Send>: Clone {
    // Lettura non bloccante: restituisce immediatamente un'istantanea del
    // valore corrente e della sua versione. Non deve mai far attendere il
    // chiamante, indipendentemente da quanti aggiornamenti sono in corso o
    // da quanti altri thread stanno leggendo o scrivendo contemporaneamente.
    fn read(&self) -> impl Snapshot<T>;

    // Tenta di aggiornare il valore a `new_value`, ma solo se nessuno ha
    // già scritto un valore più recente di `expected_version` (tipicamente
    // ottenuta da una `read()` precedente). Restituisce `true` se
    // l'aggiornamento è avvenuto (la versione interna viene allora
    // incrementata), `false` se è stato rifiutato perché `expected_version`
    // non è più quella corrente. Non deve mai bloccare il chiamante in
    // attesa che la versione torni ad essere quella attesa: un fallimento
    // va segnalato immediatamente, lasciando ad un eventuale nuovo
    // tentativo la responsabilità di chi chiama.
    fn compare_and_update(&self, expected_version: u64, new_value: T) -> bool;
}

pub fn make_versioned_cell<T: Clone + Send>(initial: T) -> impl VersionedCell<T> {
    ...
}
```

### Requisiti

- La struttura deve essere thread-safe e condivisibile tra più thread lettori e scrittori (da cui `Clone` sul tratto `VersionedCell`).
- `read()` non deve mai bloccare il chiamante in attesa di alcunché — è accettabile trattenere brevemente uno stato interno protetto, purché mai per un'operazione che possa a sua volta restare in attesa.
- Se due `compare_and_update` concorrenti partono dalla stessa `expected_version`, al più uno dei due deve avere successo; l'altro deve fallire restituendo `false`, senza che il proprio aggiornamento venga applicato né che la versione interna cambi due volte.
- La versione interna deve crescere in modo monotono di uno ad ogni aggiornamento riuscito, mai per un tentativo fallito.
- Nessuna attesa attiva né alcuna forma di blocco indefinito in nessun punto — questo problema non richiede, e non deve usare, alcun meccanismo di attesa su condizione.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso come una coppia `(versione, Arc<T>)` protetta da un semplice `Mutex` (senza alcun `Condvar` associato) dentro un `Arc` clonato da ogni handle del tratto `VersionedCell`; `read()` acquisisce il lock solo per clonare la coppia corrente (un'operazione O(1), mai un'attesa) e lo rilascia immediatamente; `compare_and_update` acquisisce il lock, confronta la versione memorizzata con `expected_version`, e in caso di corrispondenza sostituisce il valore e incrementa la versione, altrimenti rilascia il lock senza modificare nulla e restituisce `false` — in nessun punto del codice deve comparire un ciclo che ritenta internamente: il ritentativo, se voluto, spetta a chi chiama `compare_and_update`, non all'implementazione.

---

## Meta-commentario

**Perché è l'esercizio di variazione più importante finora:** ogni problema precedente ha rinforzato un'associazione — "più thread, stato condiviso, deve bloccare senza busy-waiting" → `Condvar`. Qui quell'associazione va riconosciuta come **non applicabile**: non c'è nulla per cui aspettare, perché ogni operazione o ha successo immediatamente o fallisce immediatamente. Uno studente che, per abitudine maturata sui problemi precedenti, aggiunge un `Condvar` e fa sì che `compare_and_update` attenda che la versione torni valida prima di ritentare da sola, viola direttamente il requisito esplicito ("non deve mai bloccare... un fallimento va segnalato immediatamente") — un errore concettualmente opposto a quelli visti finora (qui il rischio è *aggiungere* un blocco non richiesto, non *dimenticare* di aggiungerne uno necessario).

**Radicamento nel dominio reale:** questo è esattamente lo schema della concorrenza ottimistica usato in database, configurazioni distribuite (etcd, ZooKeeper) e tecniche RCU nei kernel — un tema naturale per un corso di programmazione di sistema, e concettualmente distante a sufficienza da tutto ciò che precede da valere come test genuino, non solo come variazione di superficie.

**Difficoltà stimata:** l'architettura è più semplice di `RwCell` o `MultiResourceManager` — nessun `Condvar`, nessuna generazione da tracciare — ma il rischio di introdurre inavvertitamente un blocco non richiesto (per abitudine) è la vera insidia. Budget consigliato: 60–75 minuti.
