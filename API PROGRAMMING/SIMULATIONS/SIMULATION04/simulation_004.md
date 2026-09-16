# Simulazione 004 — TaskExecutor

*Traduzione diretta di "SingleThreadExecutor" dall'archivio storico C++ del corso. Confidenza: alta. Introduce un elemento architetturale non ancora visto: la funzione factory deve avviare un thread di lavoro in background al momento della costruzione, non limitarsi a incapsulare stato passivo.*

---

## TaskExecutor

Molte applicazioni concorrenti isolano operazioni sequenziali (ad esempio scritture su un file di log, o su una risorsa che non tollera accessi concorrenti) delegandole a un unico thread dedicato, a cui il resto del programma invia lavoro da eseguire in ordine. È lo schema del **thread pool a thread singolo**.

Si scriva in Rust una struttura che implementi il tratto generico `TaskExecutor`, che gestisce un singolo thread di lavoro interno a cui vengono sottoposti, in ordine FIFO, dei task da eseguire.

### API richiesta

```rust
pub trait TaskExecutor {
    // Accoda `task` per l'esecuzione sul thread di lavoro interno. Se
    // l'executor è già stato chiuso, il task viene rifiutato e la funzione
    // restituisce `false`; altrimenti restituisce `true`.
    fn submit<F: FnOnce() + Send + 'static>(&self, task: F) -> bool;

    // Impedisce l'accodamento di nuovi task (submit successive
    // restituiranno `false`). I task già in coda continuano ad essere
    // eseguiti normalmente. Chiamate ripetute non hanno ulteriori effetti.
    fn close(&self);

    // Blocca il chiamante, senza consumare cicli di CPU, finché il thread
    // di lavoro non ha eseguito tutti i task rimanenti in coda e si è
    // fermato.
    fn join(&self);
}

pub fn make_task_executor() -> impl TaskExecutor {
    ...
}
```

### Requisiti

- Alla chiamata di `make_task_executor()`, deve essere avviato immediatamente un thread di lavoro che estrae ed esegue i task dalla coda in ordine FIFO, bloccandosi senza consumo di CPU quando la coda è vuota e l'executor non è ancora stato chiuso.
- `submit`, `close` e `join` devono poter essere chiamati da più thread contemporaneamente.
- `join()` deve restituire il controllo solo dopo che il thread di lavoro ha terminato definitivamente (coda vuota **e** `close()` già chiamato).
- Nessuna attesa attiva in nessun punto.
- I test devono passare senza modifiche; se il codice non compila, non verrà valutato.

### Suggerimenti implementativi

1. Stato condiviso: una coda di task tipizzati come `Box<dyn FnOnce() + Send>`, più un flag `closed: bool`, protetti da `Mutex` + `Condvar` dentro un `Arc`.
2. Nella funzione factory, dopo aver creato lo stato condiviso, avviare subito un `thread::spawn` che esegue un ciclo: blocca su `wait_while` finché la coda è vuota e l'executor non è chiuso; se la coda è vuota e l'executor è chiuso, esce dal ciclo; altrimenti preleva ed esegue il prossimo task.
3. Il `JoinHandle` restituito da `thread::spawn` va conservato in un punto accessibile da `join(&self)` pur essendo un tipo che si consuma con `.join()` — lo stesso trucco `Option<T>` + `.take()` già usato per estrarre `T` da `Element<T>` in `Drop` (simulazione: qui applicato a `Option<JoinHandle<()>>` dentro un `Mutex`) risolve il problema.

---

## Meta-commentario

**Cosa introduce di nuovo:** in tutti i problemi precedenti (`ResourcePool`, `forgettable_channel`, `Rendezvous`, `Exchanger`) la funzione factory costruisce solo *stato passivo* — nessun thread viene mai avviato dal codice dello studente stesso. Qui, invece, `make_task_executor()` deve avviare attivamente un thread in background, il che introduce un secondo livello di gestione del ciclo di vita: non solo lo stato condiviso deve essere protetto correttamente, ma il thread di lavoro stesso ha un ciclo di vita (avvio → esecuzione → terminazione) che va coordinato con `close()` e osservato da `join()`.

**Perché la generalizzazione col riuso di `Option<JoinHandle>` è rilevante:** è la stessa idea architetturale già vista per `Element<T>` in `ResourcePool` (estrarre un valore posseduto da dietro un riferimento condiviso), applicata qui a un contesto diverso — buon segnale che il "trucco" `Option<T>` + `.take()` non è specifico di RAII/`Drop`, ma un pattern generale di questo stile di esame ogni volta che serve "consumare" qualcosa dietro `&self`.

**Difficoltà stimata:** leggermente superiore agli altri campioni per via della gestione del thread di lavoro; 75–90 minuti.
