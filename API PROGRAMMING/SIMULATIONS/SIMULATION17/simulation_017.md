*Seconda simulazione a presentazione deliberatamente variata (dopo `simulation_012.md`), questa volta in modo diverso: nessuna intestazione di sezione separata per i vincoli, che sono incorporati nel paragrafo descrittivo invece che elencati a parte; nessuna sezione di suggerimenti; blocco "API richiesta" collocato a metà testo invece che dopo tutta la descrizione. Chiude anche un vuoto sull'asse D mai toccato: una rotazione rigida e vincolata all'identità dei partecipanti, non solo un ordine di arrivo o una priorità.*

---

# Simulazione 017 — TurnManager

In una simulazione multi-agente, o in un gioco a turni, un insieme fisso di partecipanti identificati da un indice deve agire uno alla volta, rispettando rigidamente un ordine di rotazione prestabilito: dopo il partecipante 0 tocca sempre al partecipante 1, poi al 2, e così via ciclicamente, indipendentemente da quale partecipante arrivi per primo a richiedere il proprio turno — è lo stesso principio di un token che circola in una rete ad anello, dove solo chi possiede il token può agire.

```rust
pub trait Turn {
    // Indice del partecipante a cui è stato concesso questo turno.
    fn participant(&self) -> usize;
}

pub trait TurnManager {
    // Numero di partecipanti nella rotazione, identificati dagli indici
    // 0..participants().
    fn participants(&self) -> usize;

    // Blocca il chiamante finché non è esattamente il turno del
    // partecipante `id`, quindi restituisce un oggetto che rappresenta il
    // turno ottenuto.
    fn wait_for_turn(&self, id: usize) -> impl Turn;
}

pub fn make_turn_manager(participants: usize) -> impl TurnManager {
    ...
}
```

Ogni partecipante che chiama `wait_for_turn` con il proprio indice resta sospeso, senza consumare cicli di CPU, finché la rotazione non raggiunge esattamente quell'indice — anche se avesse chiamato il metodo con largo anticipo rispetto al proprio turno, e anche se altri partecipanti "in ritardo" lo chiamassero nel frattempo: l'ordine è quello della rotazione, mai quello di arrivo delle chiamate. Una volta ottenuto il turno, il partecipante lo mantiene finché l'oggetto restituito da `wait_for_turn` non esce dallo scope: solo a quel punto, non prima, la rotazione avanza automaticamente al partecipante successivo — non esiste alcun metodo esplicito per terminare il proprio turno, è interamente una questione di RAII tramite il tratto `Drop`. La struttura va condivisa tra tutti i partecipanti contemporaneamente; si assuma che ciascun indice da `0` a `participants() - 1` sia gestito da un solo thread alla volta. Come sempre, nessuna attesa attiva, tutti i test forniti devono superare senza modifiche, e una consegna che non compila non ottiene alcuna valutazione.

---

## Meta-commentario

**In cosa differisce da `Rendezvous` (simulazione 002), l'unico altro problema con più partecipanti fissi:** lì l'obiettivo era simmetrico e anonimo — tutti contribuiscono, poi tutti procedono insieme, l'identità di chi contribuisce cosa non contava ai fini della sincronizzazione. Qui è l'esatto opposto: **un solo** partecipante alla volta, in una sequenza rigidamente legata all'identità di ciascuno, non al momento di arrivo. Un'implementazione che tratta `wait_for_turn` come una normale coda FIFO (chi chiama per primo passa per primo) supera banalmente i test con chiamate in ordine di indice crescente, ma fallisce silenziosamente non appena un test invoca i partecipanti in un ordine di arrivo diverso dall'ordine di rotazione richiesto.

**Perché serve comunque `Drop` e non un contatore semplice come in `WaitGroup`:** qui la "fine" di un turno è un evento singolo e ben preciso (l'uscita dallo scope dell'oggetto `Turn`), non un accumulo di completamenti; il parallelo più vicino è `RwCell` (`WriteGuard`), non `WaitGroup`.

**Difficoltà stimata:** architetturalmente più semplice di `Rendezvous` (un solo contatore "di chi è il turno", nessuna generazione da tracciare), ma la scelta della condizione di attesa corretta (`turno_corrente == id`, non "la coda è vuota" o simili) è l'unico punto realmente delicato. Budget consigliato: 60–75 minuti.
