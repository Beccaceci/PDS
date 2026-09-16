# Simulazione 031 — CreditedChannel

*Dominio nuovo: controllo di flusso a crediti, lo stesso principio usato dalle finestre di TCP o da HTTP/2 — un canale in cui chi riceve concede esplicitamente al mittente una quota di invii permessi, invece di lasciare che sia la sola capacità della coda a determinare quando bloccare. La differenza concettuale rispetto a `BoundedQueue` (005) è il punto centrale: lì la contropressione era automatica e legata alla dimensione della coda; qui è una politica esplicita, disaccoppiata dalla coda, decisa da chi riceve.*

---

## CreditedChannel

Un mittente che invia dati più velocemente di quanto il ricevente possa elaborarli va rallentato — ma non necessariamente in base a quanti elementi sono già in coda: talvolta chi riceve preferisce concedere una quota di invii permessi in anticipo (un "credito"), indipendentemente da quanti ne siano già arrivati, per controllare esplicitamente il ritmo del mittente invece di limitarsi a reagire a una coda piena.

Si scrivano in Rust le strutture che implementano i tratti generici `CreditedSender<T>` e `CreditedReceiver<T>` definiti di seguito.

### API richiesta

```rust
pub trait CreditedSender<T: Send>: Clone {
    // Invia `value`, consumando un'unità di credito disponibile. Blocca il
    // chiamante, senza consumare cicli di CPU, se il credito disponibile
    // in questo momento è zero, finché il ricevente non ne concede altro.
    fn send(&self, value: T);
}

pub trait CreditedReceiver<T: Send> {
    // Riceve il valore meno recente ancora in coda, bloccando senza
    // consumo di CPU se non ce ne sono. Restituisce None solo quando il
    // canale è stato chiuso e non ci sono più valori residui.
    fn recv(&self) -> Option<T>;

    // Concede `amount` unità di credito aggiuntive: permette ai mittenti
    // di inviare fino a `amount` ulteriori valori prima di bloccarsi di
    // nuovo, indipendentemente da quanti valori sono attualmente in coda.
    fn grant_credit(&self, amount: usize);

    // Chiude il canale: i mittenti bloccati o futuri non possono più
    // inviare (si assuma che, dopo close(), send() non venga più
    // chiamato); recv() continua a restituire i valori residui, poi None.
    fn close(&self);
}

pub fn make_credited_channel<T: Send>(
    initial_credit: usize,
) -> (impl CreditedSender<T>, impl CreditedReceiver<T>) {
    ...
}
```

### Requisiti

- `CreditedSender<T>` deve essere condivisibile tra più mittenti (`Clone`); `CreditedReceiver<T>` rappresenta un solo ricevente, non condivisibile.
- Il credito disponibile parte da `initial_credit` e non è mai legato automaticamente al numero di elementi in coda: cresce solo per effetto di `grant_credit()`, diminuisce solo per effetto di un `send()` riuscito.
- Se il credito disponibile è insufficiente per più mittenti in attesa contemporaneamente, un `grant_credit(amount)` deve permettere di procedere ad **al più** `amount` di essi (uno per unità di credito concessa), lasciando gli altri in attesa.
- `recv()` non consuma né richiede credito: è `send()`, non `recv()`, l'unica operazione soggetta al vincolo di credito.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

Rappresentare lo stato condiviso con una coda (`VecDeque<T>`), un contatore di credito disponibile, e un flag di chiusura, protetti da `Mutex` + `Condvar` dentro un `Arc`; `send()` attende (`wait_while`) finché il credito è zero, poi lo decrementa e inserisce il valore in coda, notificando; `recv()` attende finché la coda è vuota (e il canale non chiuso), poi preleva; `grant_credit()` incrementa il credito e notifica (`notify_all`, dato che più mittenti in attesa verificano la stessa condizione ma solo alcuni di essi devono effettivamente procedere in base a quanto credito è stato concesso — ciascuno lo scoprirà ricontrollando la condizione dopo il risveglio).

---

## Meta-commentario

**Perché disaccoppiare credito e occupazione della coda è la scelta di design, non un dettaglio:** in `BoundedQueue`, "la coda è piena" e "il produttore deve attendere" erano la stessa condizione osservata da due lati diversi. Qui sono deliberatamente due cose distinte: un ricevente potrebbe concedere credito generosamente pur elaborando lentamente (permettendo alla coda di crescere), oppure concederlo con parsimonia anche con la coda quasi vuota (rallentando il mittente per ragioni che nulla hanno a che fare con lo spazio disponibile — ad esempio per non saturare una risorsa a valle non rappresentata nel canale stesso). Un'implementazione che, per abitudine presa da `BoundedQueue`, facesse dipendere il blocco di `send()` anche dalla dimensione della coda invece che dal solo credito tradirebbe questa distinzione.

**Perché non serve un limite di capacità sulla coda:** a differenza di `BoundedQueue`, qui non esiste un parametro di capacità — il credito *è* il meccanismo di contropressione, per intero; una coda che crescesse oltre ogni limite in presenza di credito generosamente concesso è un comportamento corretto secondo la specifica, non un bug da correggere aggiungendo un vincolo non richiesto.

**Difficoltà stimata:** paragonabile a `BoundedQueue`, con l'insidia concettuale di non reintrodurre per abitudine un vincolo di capacità che qui non esiste. Budget consigliato: 60–75 minuti.
