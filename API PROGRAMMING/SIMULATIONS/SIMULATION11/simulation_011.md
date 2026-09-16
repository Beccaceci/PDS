# Simulazione 011 — MultiResourceManager

*Resta interamente in `std::sync`/`std::thread`, come confermato. Introduce una dimensione mai toccata da nessun campione precedente: finora ogni handle rappresentava **una sola** risorsa alla volta; qui un chiamante può richiedere **più risorse contemporaneamente**, il che apre la porta al classico problema dello stallo (deadlock) da acquisizione multipla — materia d'esame tipica di un corso di sistemi operativi.*

---

## MultiResourceManager

Quando un'operazione richiede l'accesso simultaneo a più risorse condivise — ad esempio più record di un database, o più periferiche di un sistema — acquisirle una alla volta espone al classico rischio dello stallo: due thread che richiedono le stesse risorse in ordine diverso possono bloccarsi reciprocamente per sempre, ciascuno in attesa di una risorsa già posseduta dall'altro.

Si scriva in Rust una struttura che implementi il tratto generico `MultiResourceManager<T: Send>`, che gestisce un insieme fisso di risorse identificate da un indice (`0..capacity()`), e permette di acquisirne più di una contemporaneamente in un'unica operazione atomica, garantendo l'assenza di stalli indipendentemente dall'ordine in cui i chiamanti concorrenti specificano gli indici richiesti.

### API richiesta

```rust
pub trait ResourceSet<T: Send> {
    // Accesso condiviso alla risorsa con l'id specificato, se presente
    // nell'insieme correntemente posseduto da questo oggetto. Panica se
    // l'id non fa parte dell'insieme acquisito.
    fn get(&self, id: usize) -> &T;
}

pub trait MultiResourceManager<T: Send> {
    // Numero totale di risorse gestite, identificate dagli indici
    // 0..capacity().
    fn capacity(&self) -> usize;

    // Acquisisce in blocco, in un'unica operazione atomica, tutte le
    // risorse i cui indici sono elencati in `ids` (senza duplicati, tutti
    // < capacity()). Blocca il chiamante, senza consumare cicli di CPU,
    // finché tutte le risorse richieste non sono simultaneamente
    // disponibili. L'assenza di stalli tra chiamate concorrenti con
    // insiemi di indici sovrapposti deve valere indipendentemente
    // dall'ordine in cui gli indici sono specificati in `ids`.
    fn acquire_all(&self, ids: &[usize]) -> impl ResourceSet<T>;
}

pub fn make_multi_resource_manager<T: Send>(items: Vec<T>) -> impl MultiResourceManager<T> {
    ...
}
```

### Requisiti

- La struttura deve essere thread-safe e condivisibile tra più thread.
- Ogni singola risorsa deve essere posseduta da al più un `ResourceSet<T>` alla volta.
- Due chiamate concorrenti con insiemi di indici parzialmente sovrapposti (ad esempio un thread richiede `[1, 2]` e un altro `[2, 3]`) non devono mai produrre uno stallo permanente, qualunque sia l'ordine con cui gli indici sono passati a `acquire_all` (una chiamata con `[2, 1]` deve essere trattata in modo equivalente a `[1, 2]`).
- Quando l'oggetto che implementa `ResourceSet<T>` esce dallo scope, tutte le risorse che possedeva tornano disponibili contemporaneamente (RAII, tramite il tratto `Drop`).
- Nessuna attesa attiva in nessun punto.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

1. La tecnica standard per evitare lo stallo da acquisizione multipla è imporre un **ordine totale** di acquisizione: prima di verificarne la disponibilità, ordinare sempre gli indici richiesti (ad esempio in ordine crescente), indipendentemente dall'ordine con cui sono stati passati dal chiamante.
2. Rappresentare lo stato condiviso con un vettore che indichi, per ciascun indice, se la risorsa è attualmente libera o in prestito, protetto da `Mutex` + `Condvar` dentro un `Arc`.
3. `acquire_all` deve attendere, con un'unica condizione valutata sotto lo stesso lock, finché **tutti** gli indici richiesti (ordinati) non sono simultaneamente liberi — non acquisirli uno alla volta rilasciando e ri-tentando: farlo ricreerebbe esattamente il rischio di stallo che si sta cercando di evitare.
4. Il tipo che implementa `ResourceSet<T>` deve conservare l'insieme degli indici posseduti e un riferimento allo stato condiviso; il suo `Drop` deve liberare tutte le risorse contemporaneamente in un'unica operazione sotto lock, e notificare gli eventuali attendenti.

---

## Meta-commentario

**Perché questo problema è strutturalmente nuovo:** ogni campione precedente — reale o simulato — coinvolge un handle che rappresenta **una singola** unità di risorsa. Qui, per la prima volta, un singolo handle (`ResourceSet<T>`) può rappresentare un numero variabile di risorse scelte a runtime dal chiamante, il che introduce un problema di correttezza che non esiste quando si acquisisce una risorsa alla volta: l'ordine di acquisizione conta, e un'implementazione che acquisisce gli indici uno alla volta nell'ordine fornito dal chiamante (invece di ordinarli prima) supera i test con un solo `ResourceSet` alla volta, ma produce uno stallo non appena due chiamate concorrenti richiedono insiemi sovrapposti in ordine opposto — un fallimento silenzioso (nessun panico, nessun errore: semplicemente due thread che non progrediscono più) dello stesso tipo già penalizzato pesantemente nel feedback di correzione reale su `ResourcePool`.

**Perché è "più complesso" nel senso richiesto:** non si tratta di aggiungere più tratti o più `Drop` (qui ce n'è uno solo, come in molte delle simulazioni precedenti), ma di un'insidia di correttezza concettualmente più profonda — il tipo di problema che negli esami di sistemi operativi viene tipicamente introdotto dopo, non prima, dei problemi a singola risorsa già visti.

**Difficoltà stimata:** tra le più alte della serie; la parte implementativa non è più complessa di `ResourcePool`, ma il ragionamento necessario per evitare lo stallo è del tutto nuovo. Budget consigliato: 90–110 minuti.
