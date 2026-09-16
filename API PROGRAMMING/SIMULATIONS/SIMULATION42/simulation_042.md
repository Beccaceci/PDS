# Simulazione 042 — TransactionalPair (capstone)

*Dominio nuovo, e genuinamente più ricco di `047`: una memoria transazionale software (STM) in miniatura — lo stesso principio dietro `STM` di Haskell o i `ref` di Clojure — applicato a una coppia di variabili condivise. Diversa da ogni forma di concorrenza ottimistica già vista (`VersionedCell`, 014) in una dimensione precisa: qui **due** valori devono essere letti e scritti insieme atomicamente, e un fallimento non deve né ritentare a ciclo stretto né bloccarsi pessimisticamente — deve bloccarsi solo abbastanza a lungo da aspettare che qualcosa cambi davvero.*

---

## TransactionalPair

Il trasferimento di un importo tra due conti condivisi è l'esempio canonico per cui bloccare entrambe le risorse con lock pessimistici, in un ordine prestabilito, funziona ma serializza inutilmente ogni coppia di operazioni anche quando i conti coinvolti sono quasi sempre diversi tra thread diversi. Un approccio ottimistico legge entrambi i valori, calcola il nuovo stato senza mantenere alcun lock, e tenta di scriverlo — riprovando automaticamente, in modo invisibile al chiamante, se nel frattempo qualcun altro ha già modificato l'uno o l'altro valore.

Si scriva in Rust una struttura che implementi il tratto generico `TransactionalPair<V: Clone + Send>` definito di seguito.

### API richiesta

```rust
pub trait TransactionalPair<V: Clone + Send>: Clone {
    // Legge i valori correnti della coppia, atomicamente insieme (mai il
    // primo aggiornato da una scrittura concorrente insieme al secondo
    // non ancora aggiornato, o viceversa). Non blocca mai.
    fn read_both(&self) -> (V, V);

    // Applica `transaction` ai valori correnti della coppia, letti
    // atomicamente insieme, e tenta di scrivere atomicamente i due nuovi
    // valori che restituisce. Se, tra la lettura e il tentativo di
    // scrittura, una scrittura concorrente (da un'altra chiamata a questo
    // stesso metodo) ha modificato l'uno o l'altro valore, il tentativo
    // viene scartato e l'intera operazione ripetuta da capo, in modo
    // invisibile al chiamante — bloccando prima di rileggere e ritentare,
    // senza consumare cicli di CPU, finché almeno uno dei due valori non
    // cambia rispetto a quelli appena letti — finché un tentativo non
    // riesce a confermarsi senza conflitti. Restituisce i valori
    // effettivamente confermati.
    fn atomically(&self, transaction: impl Fn(V, V) -> (V, V)) -> (V, V);
}

pub fn make_transactional_pair<V: Clone + Send>(first: V, second: V) -> impl TransactionalPair<V> {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`).
- `transaction` può essere invocata più di una volta per una singola chiamata ad `atomically` (una per ogni tentativo fallito, oltre a quello che riesce), e non deve mai essere invocata mentre è mantenuto il lock condiviso.
- Un tentativo ha successo solo se nessuna scrittura concorrente ha modificato il primo o il secondo valore tra la lettura effettuata da quel tentativo e il suo momento di conferma.
- In caso di fallimento di un tentativo, il ritentativo non deve avvenire in un ciclo stretto: l'implementazione deve bloccare, senza consumare cicli di CPU, finché almeno uno dei due valori non cambia effettivamente rispetto a quelli letti dal tentativo appena fallito, prima di rileggere e ritentare.
- `read_both()` non blocca mai.
- Nessuna attesa attiva in nessun punto (né nel senso di polling, né di ritentativi non necessari).
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con i due valori e le rispettive versioni (`u64` ciascuna, incrementate ad ogni scrittura confermata), protetti da `Mutex` + `Condvar` dentro un `Arc`. `atomically`, in un ciclo: legge sotto lock entrambi i valori e le rispettive versioni, rilascia il lock, invoca `transaction` sui valori letti; riacquisisce il lock e verifica se le versioni sono ancora quelle lette — in tal caso applica i nuovi valori, incrementa entrambe le versioni, notifica (`notify_all`) e restituisce; altrimenti, prima di tornare all'inizio del ciclo, attende (`wait_while`) finché almeno una delle due versioni non è diversa da quella letta in questo tentativo fallito — non limitandosi a rilasciare il lock e ritentare immediatamente.

---

## Meta-commentario

**Perché limitato a una coppia dello stesso tipo, invece di un numero arbitrario di variabili di tipi diversi come una vera STM generale:** una STM generale richiederebbe di registrare, all'interno di una transazione, un numero non noto a priori di variabili di tipi eterogenei — in Rust, ciò richiederebbe tipicamente cancellazione di tipo (`Box<dyn Any>` e downcasting), una tecnica mai comparsa in nessun campione reale né richiesta da alcuna simulazione precedente. Restringere il problema a una coppia dello stesso tipo conserva l'essenza concettuale della memoria transazionale — atomicità multi-variabile, rilevamento ottimistico dei conflitti, ritentativo automatico — restando dentro gli strumenti già consolidati nella serie.

**Perché il ritentativo deve bloccare invece di rileggere immediatamente:** un ciclo che, dopo un conflitto, rilascia il lock e rilegge subito i valori correnti per ritentare è tecnicamente corretto (converge prima o poi) ma viola direttamente il divieto di attesa attiva — sotto conflitto sostenuto, degenererebbe in un ciclo stretto che consuma cicli di CPU a ripetizione. Il requisito che il ritentativo attenda un cambiamento effettivo delle versioni prima di rileggere è ciò che distingue una concorrenza ottimistica corretta da un polling travestito.

**Perché `transaction` deve essere `Fn`, in contrasto diretto con `compute` di `SingleFlightCache` (015):** lì il calcolo andava eseguito al più una volta per l'intero episodio di deduplicazione, da cui `FnOnce`. Qui lo stesso calcolo può essere legittimamente rieseguito più volte a causa di conflitti — la cardinalità delle invocazioni è parte della semantica del problema, non un dettaglio implementativo, ed è esattamente il tipo di distinzione che vale la pena riconoscere sotto pressione d'esame invece di scegliere per abitudine dal problema precedente.

**Difficoltà stimata:** tra le più alte della serie, paragonabile a `VirtualMemory` (046). Budget stimato: 130–160 minuti.
