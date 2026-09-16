# Simulazione 029 — AppendLog

*Dominio nuovo: un log ad accodamento con checkpoint, basato sulla tecnica della sincronizzazione per epoche (la stessa idea alla base di RCU e della rotazione dei log nei database) — mai comparsa nella serie. La difficoltà è tutta nel decidere a quale "epoca" appartiene un'operazione già in corso quando un checkpoint la chiude sotto di essa, e nel far sì che questa appartenenza resti coerente per tutta la durata dell'operazione anche se altre epoche si aprono e chiudono nel frattempo.*

---

## AppendLog

Un log a cui più thread accodano scritture in continuazione deve poter essere periodicamente "chiuso a chiave" (checkpoint) — per essere scritto su disco, ad esempio — in un momento preciso: le scritture già in corso in quel momento vanno considerate parte del checkpoint e attese prima di procedere, ma le scritture che iniziano *dopo* l'avvio del checkpoint devono poter procedere immediatamente, senza attendere che il checkpoint finisca, e senza essere conteggiate in esso.

Si scrivano in Rust le strutture che implementano i tratti `AppendGuard` e `AppendLog` definiti di seguito.

### API richiesta

```rust
pub trait AppendGuard {}

pub trait AppendLog: Clone {
    // Inizia una nuova scrittura, registrandola come appartenente
    // all'epoca corrente al momento di questa chiamata. Non blocca mai il
    // chiamante. Restituisce un guardiano: quando esce dallo scope, la
    // scrittura è considerata terminata.
    fn begin_append(&self) -> impl AppendGuard;

    // Chiude l'epoca corrente e ne apre immediatamente una nuova: da
    // questo momento, ogni begin_append() — anche se chiamato mentre
    // questo stesso checkpoint() è ancora in attesa — appartiene alla
    // nuova epoca, non a quella appena chiusa. Blocca il chiamante, senza
    // consumare cicli di CPU, finché tutte le scritture appartenenti
    // all'epoca appena chiusa non sono terminate (i rispettivi
    // AppendGuard usciti dallo scope), quindi restituisce quante
    // scritture in totale sono appartenute a quell'epoca.
    fn checkpoint(&self) -> usize;
}

pub fn make_append_log() -> impl AppendLog {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`).
- `begin_append()` non deve mai bloccare, in nessuna circostanza — nemmeno mentre un `checkpoint()` è in corso.
- Un `checkpoint()` deve attendere esclusivamente le scritture iniziate *prima* della propria chiamata; scritture iniziate dopo (anche durante la sua attesa) appartengono alla nuova epoca e non influenzano né sono influenzate da quel `checkpoint()`.
- Checkpoint successivi sono indipendenti: ciascuno attende solo le scritture della propria epoca, mai quelle di un'epoca già chiusa da un checkpoint precedente.
- Il valore restituito da `checkpoint()` è il numero totale di scritture che sono appartenute a quell'epoca, non il numero di quelle ancora attive al momento della chiamata (che potrebbe essere già inferiore, se alcune erano già terminate).
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con un numero di epoca corrente (`u64`) e, per ciascuna epoca non ancora completamente "drenata", un conteggio totale e un conteggio ancora attivo, ad esempio in una `HashMap<u64, (usize, usize)>` (totale, attive) protetta da `Mutex` + `Condvar` dentro un `Arc`. `begin_append()` legge l'epoca corrente e incrementa entrambi i conteggi per quell'epoca, ricordando il numero di epoca nel guardiano restituito; `checkpoint()` incrementa il numero di epoca corrente (così che ogni `begin_append()` successivo, anche concorrente, veda già la nuova epoca), poi attende che il conteggio "attive" dell'epoca appena chiusa raggiunga zero, quindi rimuove quella voce dalla mappa restituendone il conteggio "totale". Il `Drop` del guardiano decrementa il conteggio "attive" della *propria* epoca (memorizzata al momento della creazione, non l'epoca corrente al momento della distruzione, che potrebbe già essere cambiata) e notifica.

---

## Meta-commentario

**Perché un guardiano deve "ricordarsi" la propria epoca invece di leggere quella corrente al momento del `Drop`:** se `Drop` leggesse l'epoca corrente invece di quella con cui è stato creato, decrementerebbe il conteggio dell'epoca sbagliata non appena un `checkpoint()` fosse intervenuto nel frattempo — esattamente lo stesso principio della generazione memorizzata in `LeaseLockManager` (024) e in `Rendezvous` (002), qui applicato per far sopravvivere correttamente l'identità di un'operazione attraverso il cambio di epoca, non per invalidare un handle scaduto.

**Perché serve un conteggio "totale" oltre a quello "attive":** `checkpoint()` deve restituire quante scritture sono appartenute all'epoca, ma se aspettasse a leggerlo solo dopo che tutte sono terminate, il conteggio "attive" sarebbe già a zero e non informativo — va quindi tenuto un secondo numero, mai decrementato, aggiornato solo da `begin_append()`.

**Perché `begin_append()` non deve mai bloccare, nemmeno durante un `checkpoint()` in corso:** è la proprietà che rende questo pattern utile in pratica (permette scritture continue senza mai fermare il sistema per un checkpoint) — un'implementazione che facesse attendere `begin_append()` finché un `checkpoint()` in corso non finisce risolverebbe la sincronizzazione correttamente ma tradirebbe lo scopo stesso del problema, un errore di lettura del requisito piuttosto che di codice.

**Difficoltà stimata:** concettualmente tra le più impegnative della serie nonostante il codice risultante sia relativamente compatto — il ragionamento sulle epoche è il punto, non il volume implementativo. Budget consigliato: 90–120 minuti.
