# Simulazione 001 — EventBus

*Generata come simulazione: non è un esame reale del professore. Ricalca il meta-pattern estratto da `exam_reverse_engineering.md` (contratto tramite trait, funzione factory opaca, un vincolo di concorrenza esplicito, un oggetto "handle" con semantica di lifecycle), applicato a un dominio non ancora visto nei due campioni reali, con una variante deliberata: la RAII questa volta vive lato *consumatore* invece che lato *produttore*, e il trait di servizio richiede esso stesso `Clone`.*

---

## EventBus

Molti sistemi devono notificare più componenti indipendenti quando accade qualcosa, senza che questi componenti debbano interrogare continuamente lo stato per accorgersene — si pensi a una dashboard di monitoraggio che deve reagire a letture di sensori, o a più pannelli di un'interfaccia che devono aggiornarsi quando cambia un dato condiviso. È lo schema **publish/subscribe** (pub-sub), usato ad esempio nei bus di eventi dei sistemi operativi e nei framework GUI.

Si scriva in Rust una struttura che implementi il tratto generico `EventBus<E: Send + Clone>`, che permette a più *publisher* di notificare un evento a tutti gli iscritti correnti, e a più iscritti di ricevere, nell'ordine di pubblicazione, tutti e soli gli eventi pubblicati successivamente alla propria iscrizione. Ogni iscrizione deve essere rappresentata da un tipo che implementi il tratto generico `Subscription<E: Send + Clone>`; quando tale oggetto esce dallo scope, l'iscrizione deve essere automaticamente rimossa dal bus, utilizzando il concetto di RAII (tramite il tratto `Drop`).

### API richiesta

```rust
pub trait Subscription<E: Send + Clone> {
    // Attende il prossimo evento pubblicato dopo l'iscrizione. Il chiamante
    // non deve consumare cicli di CPU durante l'attesa.
    // Restituisce None solo quando il bus è stato chiuso e non ci sono più
    // eventi in coda per questa iscrizione.
    fn next_event(&self) -> Option<E>;
}

pub trait EventBus<E: Send + Clone>: Clone {
    // Consegna `event` a tutti gli iscritti correntemente attivi. 
    // Non blocca il chiamante.
    fn publish(&self, event: E);

    // Crea una nuova iscrizione. L'iscritto ricevera' solo gli eventi pubblicati dopo la chiamata a `subscribe`.
    fn subscribe(&self) -> impl Subscription<E>;

    // Numero di iscrizioni correntemente attive.
    fn subscriber_count(&self) -> usize;

    // Chiude il bus: ogni `next_event()` pendente o futura restituisce None
    // non appena la coda del rispettivo iscritto si esaurisce.
    fn close(&self);
}

pub fn make_event_bus<E: Send + Clone>() -> impl EventBus<E> {
    ...
}
```

### Suggerimenti implementativi

1. Rappresentare lo stato condiviso del bus con una struttura che tenga un insieme di canali (uno per ogni iscritto attivo), protetta da un `Mutex`, racchiusa in un `Arc` clonato sia dal bus sia da ogni `Subscription`.
2. Per ogni nuova iscrizione, creare una coppia mittente/ricevitore (ad esempio tramite `std::sync::mpsc::channel`) e registrare il mittente nello stato condiviso; il ricevitore resta nell'oggetto `Subscription`.
3. In `publish`, iterare sui mittenti correntemente registrati e inoltrare l'evento a ciascuno.
4. Implementare `Drop` per il tipo che rappresenta `Subscription<E>` in modo che rimuova il proprio mittente dallo stato condiviso, aggiornando di conseguenza `subscriber_count()`.

### Requisiti

- La struttura deve essere thread-safe e condivisibile tra più publisher e più iscritti contemporaneamente (da cui il vincolo `Clone` sul tratto `EventBus` stesso).
- Ogni iscrizione deve ricevere tutti e soli gli eventi pubblicati dopo la propria creazione, nell'ordine di pubblicazione.
- Quando un oggetto che implementa `Subscription<E>` viene distrutto, l'iscrizione corrispondente deve essere rimossa dal bus.
- Nessuna attesa attiva (niente busy-waiting): `next_event()` deve bloccare senza consumare cicli di CPU.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

---

## Meta-commentario (rimuovi prima di usarlo come vera simulazione a tempo)

**Perché questa combinazione:** riusa gli ingredienti già visti (`Arc<Mutex<...>>`, canale come primitivo di blocco, `Drop` come RAII) ma li ricompone in un ordine nuovo — la `Drop` questa volta *rimuove* una registrazione da uno stato condiviso invece di *restituire* una risorsa a un pool, e lo stato condiviso è una collezione di canali (non un singolo `Vec<T>` né una singola coda), il che ti costringe a decidere autonomamente la struttura dati di registro (`Vec<Sender<E>>`? `HashMap<id, Sender<E>>`?) — nessuno dei due campioni reali te lo dice esplicitamente.

**Difficoltà stimata:** leggermente superiore a `ResourcePool`, per via del registro multi-canale e del vincolo di ordinamento ("solo gli eventi successivi all'iscrizione"), che introduce una race condition sottile se `subscribe()` e `publish()` non sono serializzati correttamente sullo stesso lock. Budget consigliato: 70–90 minuti.

**Concetti mirati:** `Arc<Mutex<_>>` su una collezione (non un singolo valore), `mpsc::channel` come primitivo di blocco riutilizzato "as-is" invece di reimplementato con `Condvar`, `Drop` lato-consumatore, `Clone` richiesto sul tratto di servizio.

Quando hai un tentativo di soluzione, condividilo e lo revisiono esattamente come il professore (Agent 7): correttezza, ownership, architettura, concorrenza, idiomaticità, complessità superflua — con punteggio e feedback dettagliato.
