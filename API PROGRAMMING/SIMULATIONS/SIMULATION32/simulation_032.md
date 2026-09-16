# Simulazione 032 — Phaser

*Direzione nuova: un barrier multi-fase a partecipazione dinamica (ispirato a `java.util.concurrent.Phaser`), diverso da `Rendezvous` (003) o `Barrier` (006) nella premessa di fondo — lì N era fissato alla creazione; qui i partecipanti si registrano e si deregistrano nel tempo, e il numero di "arrivi" richiesti per completare una fase cambia di conseguenza. La difficoltà: due metodi diversi (`arrive_and_wait` e `deregister`) possono ciascuno, indipendentemente, essere quello che fa scattare il completamento di una fase, e devono condividere esattamente la stessa logica di completamento senza duplicarla in modo incoerente.*

---

## Phaser

Un'elaborazione a fasi sincronizzate, in cui un numero di partecipanti non fisso nel tempo deve completare ciascuno il proprio lavoro prima che chiunque possa procedere alla fase successiva, non può usare un contatore fissato a priori come in un barrier classico: i partecipanti possono unirsi o ritirarsi durante l'esecuzione, e l'ultimo evento che soddisfa i requisiti della fase corrente — sia esso un arrivo o un ritiro — deve far avanzare la fase.

Si scrivano in Rust le strutture che implementano i tratti `PhaserParty` e `Phaser` definiti di seguito.

### API richiesta

```rust
pub trait PhaserParty {
    // Segnala che questo partecipante ha completato il proprio lavoro per
    // la fase corrente, e blocca il chiamante, senza consumare cicli di
    // CPU, finché tutti i partecipanti attualmente registrati non hanno
    // fatto lo stesso per questa fase — a quel punto la fase avanza e la
    // chiamata ritorna.
    fn arrive_and_wait(&self);

    // Deregistra definitivamente questo partecipante. Se non aveva ancora
    // chiamato arrive_and_wait per la fase corrente, la sua assenza non
    // deve più essere richiesta per completarla — se ciò fa sì che tutti
    // gli altri partecipanti registrati abbiano già effettuato il proprio
    // arrivo, la fase deve avanzare immediatamente, come se l'ultimo
    // arrivo mancante fosse appena giunto.
    //
    // Si assuma che un partecipante non chiami mai deregister()
    // concorrentemente a una propria chiamata a arrive_and_wait() già in
    // corso.
    fn deregister(self);
}

pub trait Phaser: Clone {
    // Registra un nuovo partecipante, aumentando di uno il numero di
    // arrivi richiesti per completare la fase corrente (quella non ancora
    // completata al momento della chiamata).
    fn register(&self) -> impl PhaserParty;

    // Numero della fase corrente, a partire da 0.
    fn phase(&self) -> u64;
}

pub fn make_phaser() -> impl Phaser {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`).
- Una fase si completa quando il numero di `arrive_and_wait` ricevuti per essa eguaglia il numero di partecipanti attualmente registrati — che può cambiare nel tempo per effetto di `register()` e `deregister()`, anche mentre altri partecipanti sono già bloccati in attesa del completamento della fase corrente.
- Registrare un nuovo partecipante mentre altri sono già in attesa del completamento della fase corrente obbliga anche loro ad attendere l'arrivo del nuovo partecipante prima che la fase avanzi.
- `deregister()` può, da solo, causare il completamento della fase corrente, esattamente come `arrive_and_wait()` può.
- Un partecipante bloccato in `arrive_and_wait()` deve essere sicuro di risvegliarsi solo al completamento della fase che stava effettivamente attendendo, non per un falso positivo dovuto a un conteggio che torna momentaneamente a coincidere in una fase successiva.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con tre contatori — partecipanti registrati, arrivi ricevuti per la fase corrente, numero di fase — protetti da `Mutex` + `Condvar` dentro un `Arc`. Fattorizzare la logica "il completamento è stato raggiunto" (arrivi == registrati) in un unico punto interno richiamato sia da `arrive_and_wait` (dopo aver incrementato gli arrivi) sia da `deregister` (dopo aver decrementato i registrati), per evitare di duplicare — e potenzialmente far divergere — la stessa logica in due posti. Come in `Barrier` (006), un partecipante che attende deve memorizzare il numero di fase osservato *all'ingresso* e attendere (`wait_while`) finché il numero di fase corrente non è diverso da quello, non semplicemente finché "arrivi == registrati" (condizione che potrebbe tornare vera per coincidenza in una fase futura senza che quella specifica chiamata sia mai stata effettivamente soddisfatta).

---

## Meta-commentario

**Perché due metodi devono condividere la stessa logica di completamento:** è la novità strutturale rispetto a ogni barrier precedente della serie. In `Rendezvous`, un solo metodo (`join_with`) poteva mai causare l'avanzamento della fase. Qui, un partecipante può "sparire" (`deregister`) invece di arrivare, e se era l'ultimo mancante, la fase deve comunque avanzare — un'implementazione che controlla il completamento solo dentro `arrive_and_wait` lascerebbe gli altri partecipanti bloccati per sempre in questo caso, un deadlock che nessun test con partecipazione fissa (come in `Rendezvous`) potrebbe mai far emergere.

**Perché la fase catturata all'ingresso resta necessaria anche con un numero di partecipanti dinamico:** il rischio di rientro prematuro visto in `Barrier` non scompare per il fatto che ora anche il denominatore (`registrati`) cambia — anzi, con un denominatore mobile, il solo confronto "arrivi == registrati" è ancora meno affidabile come condizione di risveglio, dato che potrebbe diventare vero per ragioni completamente indipendenti dalla fase che un particolare chiamante stava aspettando.

**Difficoltà stimata:** paragonabile a `LeaseLockManager` (024) per la disciplina richiesta nel far convergere più punti di ingresso sulla stessa transizione di stato. Budget consigliato: 90–120 minuti.
