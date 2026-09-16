# Simulazione 035 — Elezione del leader (capstone)

*Dominio interamente nuovo e mai avvicinato nella serie: elezione di un leader distribuito, il nucleo di protocolli di consenso come Raft. Semplificato rispetto a un vero Raft (nessuna replica di log, nessun timer di elezione autonomo — le richieste di voto sono guidate esplicitamente, come farebbe un harness di test), ma realistico in ogni altro aspetto: un termine condiviso che nessun lock centrale governa, un conteggio di maggioranza che deve terminare presto sia in caso di vittoria sia di sconfitta garantita, e uno stato per-nodo che deve restare coerente sotto chiamate concorrenti arbitrarie.*

---

## Elezione del leader

In un cluster di nodi paritari, esattamente un nodo alla volta deve potersi considerare "leader" per un dato termine — un contatore logico condiviso, incrementato ogni volta che un nodo tenta una nuova elezione. Un nodo diventa leader solo se ottiene il voto di una maggioranza stretta degli altri nodi per quel termine; ogni nodo concede al più un voto per termine, indipendentemente da chi lo richiede, e riconosce automaticamente un termine più recente del proprio osservato in qualunque messaggio ricevuto, rinunciando a qualunque pretesa di leadership per termini ormai superati.

Si scriva in Rust una struttura che implementi il tratto `Node` definito di seguito.

### API richiesta

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteResult {
    Granted,
    Denied,
}

pub trait Node: Send + Sync {
    // Termine corrente conosciuto da questo nodo.
    fn current_term(&self) -> u64;

    // true se questo nodo si considera attualmente leader per il proprio
    // termine corrente.
    fn is_leader(&self) -> bool;

    // Un candidato richiede il voto per `term` a questo nodo (il votante).
    // Se `term` è maggiore del termine corrente del votante, questo
    // aggiorna il proprio termine a `term`, dimentica qualunque voto già
    // espresso in termini precedenti, concede il voto e restituisce
    // Granted. Se `term` è uguale al termine corrente e il votante non ha
    // ancora votato in questo termine, concede e restituisce Granted.
    // Altrimenti (term inferiore, o voto già espresso in questo termine
    // per una richiesta precedente) restituisce Denied.
    fn request_vote(&self, term: u64) -> VoteResult;

    // Un leader per `term` segnala il proprio battito cardiaco a questo
    // nodo. Se `term` è maggiore o uguale al termine corrente del nodo,
    // questo aggiorna il proprio termine (se maggiore) e rinuncia a
    // qualunque pretesa di leadership propria per quel termine o
    // precedenti. Se `term` è inferiore al termine corrente, non ha
    // alcun effetto.
    fn receive_heartbeat(&self, term: u64);

    // Questo nodo tenta di diventare leader per un nuovo termine
    // (current_term() + 1 al momento della chiamata), richiedendo il voto
    // a ciascuno dei nodi in `others` in parallelo (contando anche il
    // proprio voto implicito a se stesso). Termina non appena raggiunta
    // una maggioranza stretta dei voti totali (others.len() + 1) —
    // diventando leader e restituendo true — oppure non appena una
    // maggioranza diventa matematicamente impossibile anche nella
    // migliore delle ipotesi sulle risposte ancora mancanti — restando (o
    // tornando) Follower e restituendo false — senza necessariamente
    // attendere tutte le risposte in nessuno dei due casi.
    fn start_election(&self, others: &[&dyn Node]) -> bool;
}

pub fn make_node() -> impl Node {
    ...
}
```

### Requisiti

- Lo stato di ciascun nodo (termine corrente, se ha già votato in questo termine, se si considera leader) è indipendente da quello di ogni altro nodo.
- `request_vote` e `receive_heartbeat` devono poter essere chiamati concorrentemente sullo stesso nodo da più chiamanti (rappresentando messaggi arrivati "in parallelo"), con un esito coerente con un qualche ordine di serializzazione effettivo — mai uno stato intermedio inconsistente.
- `start_election` deve contattare tutti gli `others` in parallelo (un thread per nodo), non in sequenza.
- Il ritorno anticipato — sia per maggioranza raggiunta sia per maggioranza ormai impossibile — è obbligatorio quando applicabile: non è corretto attendere sempre tutte le risposte prima di decidere.
- Risposte che arrivano dopo che `start_election` ha già deciso e restituito il controllo non devono alterare l'esito già determinato, né causare panico.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Lo stato interno di un nodo (termine, voto espresso in questo termine, se leader) va protetto da un semplice `Mutex` — nessun `Condvar` qui: nessuno dei tre metodi `current_term`/`request_vote`/`receive_heartbeat` attende mai su questo stato. Per `start_election`, creare uno stato temporaneo **locale alla singola chiamata** (non condiviso né con lo stato del nodo né tra chiamate concorrenti a `start_election`) — voti concessi e voti negati, protetti da `Mutex` + `Condvar` dentro un `Arc` creato da capo ad ogni chiamata, sullo stesso principio già visto in `TwoPhaseCommitCoordinator` (032). Ogni thread spawnato per contattare un altro nodo scrive il proprio esito in questo stato e notifica; il thread principale attende con `wait_while` finché non è vera almeno una tra "maggioranza già raggiunta" e "maggioranza ormai impossibile anche vincendo tutte le risposte mancanti", poi decide senza attendere oltre.

---

## Meta-commentario

**Perché il termine non è protetto da un lock condiviso, a differenza di ogni contatore di generazione visto finora:** in `Rendezvous`, `LeaseLockManager`, `AppendLog`, un unico contatore viveva dentro un'unica struttura, protetto da un unico lock. Qui il "termine" è un concetto condiviso tra N nodi indipendenti, ciascuno con il proprio lock separato — non esiste un lock che li governi tutti insieme, né potrebbe esisterne uno senza rinunciare all'indipendenza dei nodi che è il punto stesso di un sistema distribuito. La coerenza emerge dalla regola locale "un termine maggiore osservato in qualunque messaggio sovrascrive il mio", applicata indipendentemente da ciascun nodo, non da un arbitro centrale.

**Perché il doppio ritorno anticipato è più ricco di quello visto in `TwoPhaseCommitCoordinator` (032):** lì l'unica decisione era "tutti hanno risposto true entro il timeout, o no" — un solo modo di concludere presto (il timeout), un solo esito negativo possibile. Qui esistono **due** condizioni di uscita anticipata indipendenti — maggioranza raggiunta (vittoria) e maggioranza resa impossibile dai voti negati già ricevuti (sconfitta garantita, anche con risposte ancora mancanti) — ed entrambe vanno espresse nella stessa condizione di attesa, non gestite come casi separati verificati in sequenza.

**Perché le risposte tardive dopo la decisione sono di nuovo un problema di progettazione, non di sfortuna:** come in `SagaOrchestrator` (033) e nello stesso `TwoPhaseCommitCoordinator`, Rust non offre modo di interrompere un thread già avviato — un voto che arriva dopo che l'elezione è già stata decisa deve poter essere scritto senza panico in uno stato che nessuno leggerà più, non deve tentare di "correggere" una decisione già presa.

**Difficoltà stimata:** tra le più alte, se non la più alta, della serie — eccede quasi certamente il tempo di un singolo appello. Budget stimato: 130–160 minuti.
