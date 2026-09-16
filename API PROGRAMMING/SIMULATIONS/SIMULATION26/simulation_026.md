# Simulazione 026 — TwoPhaseCommitCoordinator (capstone)

*Dominio nuovo, e il primo in tutta la serie a richiedere `dyn Trait` invece di `impl Trait`: i partecipanti arrivano come una collezione eterogenea fornita dal chiamante (`Vec<Box<dyn Participant>>`), non come un tipo opaco scelto dall'implementazione. Fonde l'esecuzione parallela con raccolta di risultati (`TicketLock`/`WaitGroup` in spirito) con un timeout complessivo su un insieme di operazioni indipendenti — mai visto insieme finora — e introduce un problema nuovo: cosa fare di una risposta che arriva dopo che la decisione è già stata presa.*

---

## TwoPhaseCommitCoordinator

Una transazione distribuita su più partecipanti indipendenti (ad esempio più sottosistemi che devono aggiornare il proprio stato in modo coerente) non può limitarsi a chiedere a ciascuno di confermare direttamente: se anche uno solo fallisse dopo che gli altri hanno già confermato, il sistema resterebbe in uno stato incoerente. Il protocollo a due fasi risolve il problema chiedendo prima a tutti se *sarebbero* in grado di confermare (fase di preparazione), e solo se tutti rispondono positivamente entro un tempo limite procede con la conferma definitiva; altrimenti annulla presso tutti quelli che si erano dichiarati pronti.

Si scrivano in Rust le strutture che implementano i tratti `Participant` e `Coordinator` definiti di seguito.

### API richiesta

```rust
use std::time::Duration;

pub trait Participant: Send {
    // Chiede al partecipante di prepararsi. Restituisce true se pronto a
    // confermare, false se deve annullare. Invocato al più una volta per
    // transazione.
    fn prepare(&self) -> bool;

    // Conferma definitivamente. Invocato solo se TUTTI i partecipanti
    // hanno risposto true a prepare() entro il timeout della fase di
    // preparazione.
    fn commit(&self);

    // Annulla, riportando il partecipante allo stato precedente a
    // prepare(). Invocato su ciascun partecipante che aveva risposto true
    // a prepare(), se la transazione nel suo complesso non viene
    // confermata.
    fn abort(&self);
}

pub trait Coordinator: Clone {
    // Esegue una transazione a due fasi sui partecipanti dati. Invoca
    // prepare() su ciascuno in parallelo, su thread distinti. Se tutti
    // rispondono true entro prepare_timeout dall'inizio della fase,
    // invoca commit() su tutti e restituisce true. Se anche un solo
    // partecipante risponde false, o se il timeout scade prima che tutti
    // abbiano risposto, invoca abort() su ciascun partecipante che aveva
    // già risposto true (non su quelli che hanno risposto false, né su
    // quelli la cui risposta non è ancora arrivata al momento della
    // decisione), e restituisce false. In ogni caso, il metodo restituisce
    // il controllo solo dopo che tutte le chiamate a commit()/abort()
    // rilevanti sono state completate.
    fn run_transaction(&self, participants: Vec<Box<dyn Participant>>, prepare_timeout: Duration) -> bool;
}

pub fn make_coordinator() -> impl Coordinator {
    ...
}
```

### Requisiti

- `prepare()` deve essere invocato su ciascun partecipante in parallelo (un thread per partecipante), non in sequenza: il timeout si applica alla fase nel suo complesso, non per singolo partecipante.
- Una risposta di `prepare()` che arriva dopo che il timeout è già scaduto (o dopo che un altro partecipante ha già risposto false, rendendo l'esito complessivo già deciso) non deve alterare l'esito già determinato, né causare panico o comportamento indefinito: va semplicemente ignorata ai fini della decisione.
- `commit()`/`abort()` vanno invocati esattamente una volta per ciascun partecipante rilevante.
- `Coordinator` deve essere condivisibile (`Clone`) e utilizzabile per transazioni concorrenti indipendenti: lo stato di una chiamata a `run_transaction` non deve in alcun modo interferire con un'altra chiamata concorrente sullo stesso `Coordinator` clonato.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Lo stato condiviso per raccogliere le risposte della fase di preparazione va creato **da capo ad ogni chiamata** a `run_transaction` — un `Arc<Mutex<...>>` locale alla singola transazione, non uno stato persistente del `Coordinator` — proprio per garantire l'isolamento tra transazioni concorrenti richiesto sopra. Una struttura ragionevole: un `Vec<Option<bool>>` (una cella per partecipante) più un contatore di risposte pervenute, protetti da `Mutex` + `Condvar`; ogni thread spawnato per un partecipante scrive il proprio risultato nella cella corrispondente e notifica; il thread principale attende con `wait_timeout_while` finché tutte le celle sono `Some` oppure il timeout scade, poi decide. I thread di `prepare()` la cui risposta arriva dopo che il thread principale ha già deciso continuano comunque ad eseguire fino in fondo (scrivendo il proprio risultato, che verrà semplicemente ignorato) invece di essere interrotti forzatamente — Rust non offre un meccanismo per terminare un thread dall'esterno, ed è bene che il testo non lo richieda.

---

## Meta-commentario

**Perché `dyn Trait` qui, quando ogni altro problema della serie usa `impl Trait`:** i partecipanti non sono un tipo scelto dall'implementazione (come ogni handle visto finora), ma una collezione eterogenea di implementazioni fornite dal *chiamante* di `run_transaction` — l'implementazione di `Coordinator` non può conoscerne il tipo concreto a priori, né può essere generica su di esso in modo utile (il chiamante potrebbe voler passare partecipanti di tipi diversi nella stessa transazione). `dyn Trait` è qui la scelta corretta, non un'alternativa stilistica a `impl Trait`: è l'unico modo di esprimere "una collezione di tipi diversi che condividono un'interfaccia comune", cosa che `impl Trait` non può fare.

**Perché lo stato per-transazione, e non nello stato condiviso del `Coordinator`, è la decisione architetturale centrale:** ogni problema precedente con `Clone` sul tratto di servizio (`EventBus`, `TransactionalQueue`, `AgingScheduler`...) aveva un *unico* stato condiviso, a cui tutti i cloni accedevano insieme — l'abitudine naturale, a questo punto della serie, è mettere tutto dietro un solo `Arc` creato dalla funzione factory. Qui sarebbe sbagliato: farlo significherebbe che due chiamate concorrenti a `run_transaction` sullo stesso `Coordinator` condividerebbero lo stesso `Vec` di risposte, corrompendosi a vicenda. Il `Coordinator` stesso può restare quasi privo di stato; è `run_transaction` a dover costruire uno stato temporaneo, scoped alla singola chiamata.

**Perché i "ritardatari" dopo il timeout sono un problema genuinamente nuovo:** nessun problema precedente doveva decidere cosa fare di un risultato che arriva *dopo* che una decisione basata sulla sua assenza è già stata presa — qui è inevitabile, perché Rust non permette di interrompere forzatamente un thread, quindi un `prepare()` lento continuerà comunque fino alla fine anche se il coordinatore ha già deciso di procedere senza di lui.

**Difficoltà stimata:** tra le più alte della serie; eccede il tempo di un singolo appello. Budget stimato: 130–160 minuti.
