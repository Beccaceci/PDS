# Simulazione 020 — TickerService

*Argomento di corso esplicitamente coperto (chiusure e la gerarchia `Fn`/`FnMut`/`FnOnce`) mai stato il fulcro di una simulazione: `TaskExecutor` (004) usava `FnOnce` di passaggio, ma qui la scelta tra `FnMut` e `FnOnce` — e le conseguenze di quella scelta su come la chiusura va memorizzata e invocata — è il punto centrale del problema. Prima simulazione in cui una chiusura viene conservata come stato a lungo termine invece di essere invocata subito.*

---

## TickerService

Un servizio che scandisce il tempo a intervalli regolari (un *ticker*) permette ad altri componenti di registrare callback da eseguire ad ogni intervallo — che tipicamente devono accumulare stato tra un'invocazione e l'altra, come un contatore — e, separatamente, un callback di pulizia da eseguire una sola volta, quando il servizio viene fermato.

Si scriva in Rust una struttura che implementi il tratto generico `TickerService` definito di seguito.

### API richiesta

```rust
pub trait TickerService: Clone {
    // Registra un callback da invocare ad ogni chiamata futura a tick(),
    // finché il servizio non viene fermato. Deve poter catturare e mutare
    // stato tra un'invocazione e la successiva.
    fn on_tick(&self, callback: impl FnMut() + Send + 'static);

    // Registra un callback da invocare esattamente una volta, quando
    // stop() viene chiamato. Se stop() non viene mai chiamato, il
    // callback non viene mai invocato. Se stop() viene chiamato più volte,
    // ogni callback registrato con on_stop viene comunque invocato una
    // sola volta.
    fn on_stop(&self, callback: impl FnOnce() + Send + 'static);

    // Invoca, una volta ciascuno e nell'ordine di registrazione, tutti i
    // callback registrati tramite on_tick.
    fn tick(&self);

    // Ferma il servizio: invoca, una sola volta ciascuno, tutti i
    // callback registrati tramite on_stop, quindi impedisce che ulteriori
    // chiamate a tick() abbiano effetto.
    fn stop(&self);
}

pub fn make_ticker_service() -> impl TickerService {
    ...
}
```

### Requisiti

- Thread-safe, condivisibile (`Clone`).
- I callback registrati con `on_tick` devono poter mutare uno stato catturato (ad esempio un contatore) tra un `tick()` e il successivo, mantenendo quello stato tra le invocazioni.
- Ogni callback registrato con `on_stop` deve essere invocato esattamente una volta nella vita del servizio, indipendentemente da quante volte `stop()` viene chiamato.
- Dopo `stop()`, ulteriori chiamate a `tick()` non devono invocare i callback di `on_tick`.
- Nessuna attesa attiva.
- I test in fondo al file `src/lib.rs` devono passare senza modifiche (`cargo test`).
- Se il codice consegnato non compila, non verrà valutato.

### Suggerimenti implementativi

I callback di `on_tick` vanno memorizzati come `Box<dyn FnMut() + Send>` in un `Vec`, protetto da `Mutex`: invocarli richiede di ottenere `&mut` su ciascuno attraverso il lock, che è l'unico modo per chiamare un `FnMut` immagazzinato dietro un riferimento condiviso (`&self` sul tratto). I callback di `on_stop` vanno invece memorizzati come `Vec<Box<dyn FnOnce() + Send>>`; `stop()` deve *estrarli* dalla struttura (ad esempio con `std::mem::take` sul `Vec`, o svuotandolo) prima di invocarli, così che una seconda chiamata a `stop()` non trovi più nulla da invocare — la stessa logica del pattern `Option<T>` + `.take()` già visto più volte nella serie, qui applicata a un `Vec` intero invece che a un singolo valore.

---

## Meta-commentario

**Perché `FnMut` costringe a `Box<dyn FnMut() + Send>` dietro un `Mutex`, e non solo dietro un `Arc`:** un `FnMut` deve essere chiamato attraverso un riferimento `&mut` — ma `on_tick` e `tick()` prendono entrambi `&self`, non `&mut self` (necessario per restare coerenti con `Clone` e con l'uso condiviso tra thread visto in tutta la serie). L'unico modo per ottenere un `&mut` temporaneo a partire da un `&self` è la mutabilità interna — qui sotto forma di `Mutex`, non di `RefCell` come in `Memoizer` (025), perché la struttura resta condivisibile tra thread.

**Perché `FnOnce` non può essere semplicemente "chiamato e poi ignorato" come `FnMut`:** un `FnOnce` consuma se stesso quando invocato — non può restare nel `Vec` dopo la chiamata, né essere richiamato una seconda volta nemmeno per errore. Estrarlo dalla struttura condivisa prima di invocarlo (invece di invocarlo mentre è ancora al suo posto) non è solo più pulito: è l'unico modo per rispettare il requisito "esattamente una volta" anche in presenza di chiamate concorrenti a `stop()`.

**Difficoltà stimata:** paragonabile a `TaskExecutor`; la parte concettualmente nuova è la scelta e la giustificazione del tipo di boxing per ciascuna categoria di callback, non la sincronizzazione in sé. Budget consigliato: 60–75 minuti.
