# Simulazione 036 — Reactor (capstone)

*Argomento completamente diverso da `Elezione del leader` (035): non consenso distribuito, ma multiplexing di I/O — il meccanismo dietro `epoll`/`kqueue`/`select`, di sistema operativo puro. Diverso da tutto il resto della serie in una direzione precisa: ogni problema di attesa finora era "aspetta finché TUTTE le condizioni di un insieme sono soddisfatte" (`Rendezvous`, `Phaser`) o "aspetta finché LA condizione è soddisfatta" (quasi tutto il resto). Qui è "aspetta finché ALMENO UNA di un insieme dinamico è pronta", e una volta risvegliati bisogna decidere con precisione quali, tra tutte quelle pronte, consegnare adesso e quali lasciare in sospeso.*

---

## Reactor

Un ciclo di eventi che deve sorvegliare più sorgenti di I/O contemporaneamente (descrittori di file, socket) non può bloccarsi su ciascuna singolarmente: deve invece attendere che *almeno una* tra tutte quelle registrate diventi pronta, restituendo l'insieme di quelle pronte in quel momento — dando priorità, quando presenti, alle sorgenti marcate come urgenti rispetto alle altre.

Si scrivano in Rust le strutture che implementano i tratti `Registration` e `Reactor` definiti di seguito.

### API richiesta

```rust
use std::time::Duration;

pub trait Registration {
    // Annulla questa registrazione. Se l'interesse corrispondente era già
    // segnalato come pronto, quella segnalazione viene scartata: non
    // comparirà in nessuna chiamata a poll() successiva.
    fn deregister(self);
}

pub trait Reactor: Clone + Send + Sync {
    // Registra un nuovo interesse, identificato da `id` (univoco tra le
    // registrazioni correntemente attive — comportamento non specificato
    // in caso contrario). Se `urgent` è true, questo interesse ha
    // precedenza sugli altri in poll(). Inizialmente non pronto.
    fn register(&self, id: u64, urgent: bool) -> impl Registration;

    // Segnala che l'interesse `id` è pronto. Se `id` non è attualmente
    // registrato, non ha alcun effetto. Non blocca mai il chiamante. Una
    // volta segnalato, resta pronto finché non viene consumato da poll()
    // o annullato da deregister().
    fn mark_ready(&self, id: u64);

    // Blocca il chiamante, senza consumare cicli di CPU, finché almeno un
    // interesse registrato non è pronto — inclusi interessi registrati
    // dopo l'inizio di questa chiamata, se segnalati pronti prima che
    // essa ritorni. Se, al risveglio, almeno un interesse pronto è
    // urgente, restituisce SOLO gli id urgenti pronti in quel momento,
    // consumandoli, lasciando intatti quelli non urgenti eventualmente
    // pronti per una chiamata successiva. Se nessun interesse pronto è
    // urgente, restituisce tutti gli id non urgenti pronti, consumandoli.
    // Ciascun id pronto viene restituito da al più una chiamata a poll(),
    // mai duplicato tra chiamate concorrenti.
    fn poll(&self) -> Vec<u64>;

    // Variante con attesa limitata: come poll(), ma se nessun interesse
    // diventa pronto entro timeout rinuncia e restituisce un vettore
    // vuoto. L'attesa non deve consumare cicli di CPU.
    fn poll_timeout(&self, timeout: Duration) -> Vec<u64>;
}

pub fn make_reactor() -> impl Reactor {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone + Send + Sync`).
- `register()`/`deregister()` possono avvenire in qualunque momento, anche mentre una o più chiamate a `poll()`/`poll_timeout()` sono già bloccate in attesa.
- Un id deregistrato non deve mai comparire in un risultato di `poll()` successivo, nemmeno se era già segnalato pronto al momento della deregistrazione.
- `poll()` consuma esattamente e soltanto gli id che restituisce: se restituisce solo gli urgenti (perché ve n'erano di pronti), quelli non urgenti eventualmente pronti restano tali per una chiamata successiva.
- Se più chiamate a `poll()`/`poll_timeout()` sono bloccate concorrentemente su thread diversi, ciascun id pronto deve essere consegnato a una sola di esse.
- Nessuna attesa attiva.
- I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con una mappa degli id registrati (verso il proprio flag `urgent`) e due insiemi separati di id pronti — uno per gli urgenti, uno per i non urgenti — protetti da `Mutex` + `Condvar` dentro un `Arc`. `poll()` attende (`wait_while`) finché entrambi gli insiemi di pronti sono vuoti; al risveglio, se l'insieme dei pronti urgenti non è vuoto, lo svuota interamente e lo restituisce, lasciando intatto quello dei non urgenti; altrimenti svuota e restituisce quello dei non urgenti. `mark_ready(id)`, se `id` è tra i registrati, lo aggiunge all'insieme dei pronti corrispondente al proprio flag `urgent` e notifica (`notify_all`, dato che più `poll()` in attesa potrebbero risvegliarsi ma solo una porzione dell'insieme pronto sarà disponibile per ciascuna).

---

## Meta-commentario

**Perché "aspetta finché almeno una è pronta" è strutturalmente nuovo:** ogni barrier della serie (`Rendezvous`, `Phaser`) aspettava che *tutte* le condizioni di un insieme fossero soddisfatte; ogni altro problema aspettava *una* condizione singola e fissa. Qui la condizione di risveglio è una disgiunzione su un insieme che può crescere e restringersi mentre l'attesa è già in corso — un nuovo `register()` seguito da un `mark_ready()` durante una `poll()` già bloccata deve poter risvegliarla, esattamente come `Phaser` doveva accorgersi di un `deregister()` durante un'attesa già in corso, ma nella direzione opposta (qui si aggiunge una via per soddisfare la condizione, non se ne rimuove una).

**Perché la separazione tra pronti urgenti e non urgenti deve avvenire "durante" il drenaggio, non prima:** un'implementazione che controllasse "ci sono urgenti pronti?" e poi, in un passo separato, svuotasse l'insieme corrispondente rischia la stessa finestra di corsa già vista in `CircuitBreaker` (029) e `LoadBalancer` (036) — un nuovo elemento urgente potrebbe diventare pronto proprio in quella finestra, e la decisione "quale insieme restituire" deve restare valida rispetto a ciò che viene effettivamente svuotato, non a ciò che era vero un istante prima.

**Perché conta come esercizio capstone nonostante l'assenza di più tratti complessi:** la difficoltà non è nel numero di componenti (una sola struttura di stato, come in `AppendLog`), ma nella precisione richiesta dalla condizione di attesa e dalla logica di consumo — un'attesa scritta con un solo `wait_while` "banale" (insieme pronti vuoto o no) supera i test più semplici ma non rispetta la precedenza urgente/non urgente se non progettata esplicitamente in quel punto.

**Difficoltà stimata:** paragonabile a `Phaser`/`AppendLog`; eccede probabilmente il tempo di un singolo appello per la disciplina richiesta nel gestire correttamente la coesistenza di registrazione dinamica e consumo a livelli. Budget stimato: 100–130 minuti.
