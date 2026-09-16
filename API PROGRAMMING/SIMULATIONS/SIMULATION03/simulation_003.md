# Simulazione 003 — Exchanger

*Traduzione diretta di "Exchanger\<T\>" dall'archivio storico C++ del corso. Confidenza: alta (stesso meccanismo di base di `Rendezvous`, ma pairwise anziché a N fissato — un angolo non ancora coperto).*

---

## Exchanger

Due thread che devono scambiarsi un valore senza passare da uno stato condiviso esplicito possono affidarsi a un punto di incontro (**rendezvous**): il primo che arriva depone il proprio valore e attende; quando il secondo arriva, i due valori vengono scambiati istantaneamente e entrambi i thread ripartono. A differenza di un canale produttore/consumatore, qui i due ruoli sono simmetrici e non è fissato in anticipo chi scambia con chi.

Si scriva in Rust una struttura che implementi il tratto generico `Exchanger<T: Send>`, che permette a due thread alla volta di scambiarsi un valore di tipo `T`. La struttura deve poter essere riutilizzata per scambi successivi tra coppie diverse di thread.

### API richiesta

```rust
pub trait Exchanger<T: Send> {
    // Blocca il thread chiamante, senza consumare cicli di CPU, finché un
    // altro thread non chiama a sua volta `exchange` sulla stessa istanza.
    // Restituisce il valore fornito dall'altro thread.
    fn exchange(&self, value: T) -> T;

    // Variante con attesa limitata: come `exchange`, ma se nessun altro
    // thread si presenta entro `timeout` rinuncia e restituisce il proprio
    // valore invariato, incapsulato in `Err`.
    fn exchange_timeout(&self, value: T, timeout: Duration) -> Result<T, T>;
}

pub fn make_exchanger<T: Send>() -> impl Exchanger<T> {
    ...
}
```

### Requisiti

- Se due thread chiamano `exchange` "in contemporanea" (nell'ordine in cui il runtime li serializza), il primo depone il proprio valore e attende; il secondo lo preleva, deposita il proprio, e ripartono entrambi con il valore dell'altro.
- Un terzo thread che chiama `exchange` mentre uno scambio è già "in sospeso" tra altri due deve semplicemente formare una nuova coppia con il prossimo arrivato — non deve né interferire con lo scambio in corso né restare bloccato più a lungo del necessario.
- Se nessun altro thread arriva, il chiamante di `exchange` resta bloccato indefinitamente: è comportamento corretto, non un errore.
- Nessuna attesa attiva.
- Thread-safe, condivisibile.
- I test devono passare senza modifiche; se il codice non compila, non verrà valutato.

### Suggerimenti implementativi

1. Stato condiviso: uno "slot" che può contenere al più un valore in attesa (`Option<T>`), protetto da `Mutex` + `Condvar` dentro un `Arc`.
2. Se lo slot è vuoto quando arrivo: depongo il mio valore, e attendo (`wait_while`) finché lo slot non torna vuoto (segno che qualcuno l'ha prelevato) — a quel punto, però, serve un secondo canale per ricevere *il valore dell'altro*, non semplicemente sapere che il mio è stato preso. Una generazione/id per scambio, simile alla tecnica usata in `Rendezvous`, evita ambiguità tra scambi consecutivi.
3. Se lo slot è pieno quando arrivo: prelevo il valore lì presente, deposito il mio al suo posto, e sveglio chi era in attesa con `notify_all`.

---

## Meta-commentario

**Perché è distinto da `Rendezvous` (simulazione 002):** `Joiner`/`Rendezvous` sincronizza un numero *fisso e noto* di partecipanti (N) ad ogni round. `Exchanger` è invece *pairwise e non pianificato*: due thread qualsiasi, in un momento qualsiasi, in numero potenzialmente superiore a due nel sistema complessivo. Questo costringe a un design dello stato condiviso diverso — uno slot singolo con gestione esplicita di chi-ha-consegnato-cosa, invece di un contatore verso N.

**Perché ho aggiunto `exchange_timeout`:** entrambi i campioni reali analizzati includono una variante con timeout come "seconda metà" del problema (`acquire_timeout` in `ResourcePool`); è ragionevole aspettarselo anche qui.

**Difficoltà stimata:** paragonabile a `ResourcePool`, con un'insidia specifica: distinguere correttamente scambi consecutivi senza che i valori di round diversi si mescolino.
