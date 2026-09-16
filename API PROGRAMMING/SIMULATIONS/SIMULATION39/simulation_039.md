# Simulazione 045 — PoisonableBarrier

*Estende `Rendezvous` (002) in una direzione mai esplorata: la propagazione di un fallimento attraverso una barriera. Se un solo partecipante non può contribuire il proprio valore per un guasto, l'intero round va considerato fallito per tutti — non solo per chi lo segnala — e chi era già in attesa non deve restare bloccato aspettando un completamento che non arriverà mai.*

---

## PoisonableBarrier

In un round sincronizzato tra più partecipanti, se anche uno solo di essi non può completare il proprio contributo per un errore irreversibile, aspettare comunque gli altri non ha senso: l'intero round va "avvelenato", risvegliando immediatamente chiunque fosse già in attesa con un esito di fallimento invece che con i valori del round — che, essendo incompleto, non esistono comunque.

Si scriva in Rust una struttura che implementi il tratto generico `PoisonableBarrier<T>` definito di seguito.

### API richiesta

```rust
#[derive(Clone)]
pub enum ArriveResult<T: Clone> {
    // Tutti i partecipanti sono arrivati con successo in questo round;
    // contiene l'insieme completo dei valori, nell'ordine di arrivo.
    Complete(Vec<T>),
    // Il round è stato avvelenato prima del proprio completamento.
    Poisoned,
}

pub trait PoisonableBarrier<T: Clone + Send> {
    // Come in un barrier ordinario: consegna value per il round corrente e
    // blocca il chiamante, senza consumare cicli di CPU, finché il round
    // non si conclude — per completamento normale (tutti i partecipanti
    // arrivati) o per avvelenamento. Se il round era già avvelenato nel
    // momento stesso in cui questa chiamata lo osserva, restituisce
    // Poisoned immediatamente, senza attendere.
    fn join_with(&self, value: T) -> ArriveResult<T>;

    // Avvelena immediatamente il round corrente: tutti i partecipanti già
    // in attesa del suo completamento ricevono Poisoned senza ulteriore
    // attesa, e il round successivo inizia pulito. Si assuma che venga
    // chiamato al più una volta per round, e che il numero complessivo di
    // chiamate a join_with in un dato round non superi mai il numero di
    // partecipanti previsto.
    fn poison(&self);
}

pub fn make_poisonable_barrier<T: Clone + Send>(participants: usize) -> impl PoisonableBarrier<T> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile tra `participants` thread.
- In assenza di `poison()`, il comportamento è identico a un barrier ordinario: un round si completa quando esattamente `participants` valori sono stati consegnati, e tutti i chiamanti bloccati su quel round ricevono lo stesso `Complete(valori)`.
- `poison()` chiamato mentre alcuni partecipanti sono già bloccati in attesa del round corrente li risveglia immediatamente tutti con `Poisoned`, senza attendere gli arrivi mancanti.
- Dopo un avvelenamento, il round successivo riparte pulito: non è già avvelenato, e accetta normalmente nuovi arrivi verso un nuovo completamento.
- Un `join_with` la cui chiamata osserva un round già avvelenato (perché `poison()` è già stato eseguito prima che questa chiamata iniziasse ad attendere) restituisce `Poisoned` immediatamente, senza bloccarsi.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con: il numero di fase corrente (`u64`), i valori raccolti finora nel round corrente, e l'esito dell'ultimo round appena concluso (`Option<ArriveResult<T>>`, aggiornato esattamente nel momento in cui la fase avanza — sia per completamento sia per avvelenamento), protetti da `Mutex` + `Condvar` dentro un `Arc`, sullo stesso principio della fase catturata all'ingresso già visto in `Rendezvous`. `poison()` imposta l'esito dell'ultimo round a `Poisoned`, incrementa la fase, svuota i valori raccolti, e notifica (`notify_all`) — la stessa sequenza di chiusura di un round usata da un completamento normale, con un esito diverso registrato.

---

## Meta-commentario

**Perché serve un esito a tre stati (in attesa / completato / avvelenato) invece del solo binomio già visto altrove:** ogni barrier o rendezvous precedente aveva un solo modo di concludersi. Qui il round può concludersi in due modi diversi, ed entrambi devono essere distinguibili da chi era in attesa — non basta che la fase cambi (come in `Rendezvous`, dove il solo cambio di fase implicava sempre lo stesso tipo di esito), serve anche *cosa* è successo a quella specifica fase, registrato nello stesso istante in cui la fase avanza, non recuperabile altrimenti in un secondo momento.

**Perché `poison()` riusa la stessa sequenza di chiusura di un completamento normale invece di un percorso separato:** un'implementazione con due funzioni di chiusura del round distinte (una per il completamento, una per l'avvelenamento) rischia di farle divergere nel tempo — dimenticare di svuotare i valori raccolti in un solo dei due percorsi, o di notificare in uno solo. Fattorizzare "chiudi il round corrente con questo esito" in un solo punto, parametrizzato sull'esito, è la stessa disciplina già raccomandata in `Phaser` (038) per `arrive_and_wait` e `deregister`.

**Difficoltà stimata:** paragonabile a `Rendezvous`, con l'onere concettuale aggiuntivo di propagare correttamente un secondo tipo di esito attraverso la stessa infrastruttura di attesa. Budget consigliato: 75–90 minuti.
