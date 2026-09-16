# Simulazione 043 — CancelScope (capstone)

*Dominio nuovo: un albero di ambiti di cancellazione, come `context.Context` in Go o `CancellationToken` in .NET/Tokio. Riusa `Weak<T>` (già visto in `Publisher`/`ChildHandle`, 023) ma in una configurazione molto più ricca — non un solo genitore e un solo figlio, ma un albero potenzialmente profondo, dove la cancellazione deve propagarsi verso il basso a tutti i discendenti vivi, senza mai tenere più di un lock alla volta durante la discesa.*

---

## CancelScope

Un'operazione lunga suddivisa in sotto-operazioni annidate (una richiesta HTTP che avvia sotto-richieste, ciascuna delle quali potrebbe avviarne altre) deve poter essere annullata nel suo complesso con un solo comando, senza che chi la avvia debba conoscere o tenere traccia di ogni sotto-operazione attiva in quel momento: annullare l'ambito radice deve annullare automaticamente ogni discendente, presente o futuro, senza che nessun ambito debba restare in vita più a lungo del necessario solo per poter essere raggiunto da una futura cancellazione.

Si scriva in Rust una struttura che implementi il tratto generico `CancelScope` definito di seguito.

### API richiesta

```rust
pub trait CancelScope: Clone {
    // true se questo ambito, o un qualunque suo antenato, è stato
    // cancellato.
    fn is_cancelled(&self) -> bool;

    // Cancella questo ambito e, ricorsivamente, tutti i suoi discendenti
    // attualmente esistenti. Ogni discendente futuro di un ambito già
    // cancellato — creato dopo questa chiamata — deve risultare
    // immediatamente cancellato al momento stesso della propria
    // creazione, senza bisogno che cancel() venga richiamato di nuovo.
    fn cancel(&self);

    // Crea un nuovo ambito figlio di questo. Se questo ambito è già
    // cancellato al momento della chiamata, il figlio nasce già
    // cancellato.
    fn child(&self) -> impl CancelScope;
}

pub fn make_root_scope() -> impl CancelScope {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone` condivide lo stesso ambito, non ne crea uno nuovo — a differenza di `child()`).
- `cancel()` su un ambito deve rendere `is_cancelled()` vero, immediatamente, per ogni discendente attualmente vivo (figli, nipoti, e oltre), non solo per i figli diretti.
- Un ambito il cui `Drop` avviene prima che un proprio antenato venga cancellato non deve in alcun modo impedire quell'antenato di deallocare la propria porzione di struttura relativa a quell'ambito, né `cancel()` deve tentare di raggiungere un discendente già distrutto.
- `cancel()` non deve mai mantenere il lock di un ambito mentre accede al lock di un altro ambito (né antenato né discendente): ogni livello dell'albero va toccato con il proprio lock, mai due livelli contemporaneamente sotto lock.
- `is_cancelled()` non deve mai bloccare né percorrere l'albero: deve rispondere leggendo solo lo stato locale dell'ambito su cui è chiamato.
- Nessuna attesa attiva (qui non c'è nulla su cui attendere in senso stretto).
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare ogni ambito con uno stato condiviso contenente un flag `cancellato: bool` e un elenco di riferimenti deboli ai propri figli diretti (`Vec<Weak<...>>`), protetti da un unico `Mutex` per ciascun ambito, dentro un `Arc` che ogni handle `CancelScope` clona. `child()`: sotto il lock del genitore, crea il nuovo stato del figlio con `cancellato` inizializzato al valore corrente del genitore, registra un `Weak` al nuovo figlio nell'elenco del genitore, poi rilascia il lock — tutto nella stessa sezione critica, per evitare che una cancellazione del genitore intervenga proprio nella finestra tra la lettura del suo stato e la registrazione del figlio. `cancel()`: sotto il proprio lock, imposta `cancellato = true` e ottiene una copia dei riferimenti `Arc` ancora validi tra i propri figli (tentando l'`upgrade` di ciascun `Weak`, scartando quelli ormai morti); rilascia il proprio lock; solo a quel punto richiama ricorsivamente `cancel()` su ciascun figlio ancora vivo, uno alla volta, mai tenendo il lock di questo livello mentre si opera su un livello sottostante.

---

## Meta-commentario

**Perché servono riferimenti deboli verso i figli, non verso il genitore come in `Publisher`/`ChildHandle` (023):** lì un solo figlio doveva poter sopravvivere alla distruzione del genitore senza impedirla — da cui `Weak` puntava dal figlio verso il genitore. Qui la direzione è invertita: è il *genitore* che deve poter raggiungere i propri figli per propagare la cancellazione, senza che il solo fatto di essere elencato in quella struttura tenga in vita un figlio che altrimenti sarebbe già stato distrutto altrove nel programma — da cui `Weak` punta dal genitore verso ciascun figlio, non il contrario.

**Perché non tenere mai due lock contemporaneamente durante la discesa è la vera disciplina del problema, non un dettaglio prestazionale:** un albero può essere profondo quanto si vuole a runtime; un'implementazione che, per comodità, mantenesse il lock del genitore mentre ricorre nel figlio (magari perché è più semplice da scrivere con i riferimenti già "a portata di mano") accumula un lock per ogni livello di profondità mentre scende — oltre a rischiare uno stallo se un'altra chiamata a `cancel()` o `child()` su un discendente cercasse di risalire nello stesso momento, per quanto qui la struttura ad albero puro renda lo scenario meno immediato di un vero ciclo di attesa, resta comunque una violazione della disciplina di scope del lock centrale a tutta la serie.

**Perché il figlio deve nascere già cancellato sotto lo stesso lock della registrazione:** senza quella atomicità, un `child()` chiamato esattamente mentre un `cancel()` concorrente sul genitore è in corso rischierebbe di leggere `cancellato = false` (perché non ancora aggiornato) e registrarsi comunque per tempo nell'elenco dei figli — nel qual caso la propagazione lo raggiungerebbe comunque un istante dopo, correggendo la situazione; ma se leggesse `false` *dopo* che l'aggiornamento è già avvenuto ma *prima* di essersi registrato, la propagazione già in corso non lo troverebbe più in tempo, lasciandolo permanentemente non cancellato nonostante un antenato lo sia. Leggere lo stato e registrarsi sono per questo un'unica operazione atomica, non due passi separati.

**Difficoltà stimata:** paragonabile a `VirtualMemory` (046) per la disciplina di scope del lock, con la ricorsione su una struttura ad albero come ulteriore dimensione. Budget stimato: 110–140 minuti.
