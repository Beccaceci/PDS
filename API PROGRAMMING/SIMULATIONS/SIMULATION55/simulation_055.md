# Simulazione 055 — InterruptController

*Calibratura esatta esami 2026: complessità 6.0 punti, budget 60–75 minuti, testo compatto e concentrato. Due tratti pubblici (`IrqSession` e `InterruptController`), pattern a tre livelli con `Arc<(Mutex<SharedState>, Condvar)>`, arbitraggio delle linee di interrupt hardware con priorità, mascheramento dinamico (IPL - Interrupt Priority Level), handshake End-Of-Interrupt (EOI) e gestione RAII (`Drop`) del ripristino del livello di mascheramento.*

---

## InterruptController

Nelle architetture di calcolo e nei kernel dei sistemi operativi (come i controller x86 APIC/PIC, ARM GIC o RISC-V PLIC), i dispositivi periferici (timer, schede di rete, dischi) segnalano eventi asincroni alla CPU asserendo linee di interrupt hardware (`IrqNumber`), ciascuna caratterizzata da un livello di priorità ($P \in [1, 255]$).

La CPU mantiene un livello di mascheramento attivo, denominato **IPL (Interrupt Priority Level)**:
- Soltanto gli interrupt con priorità strettamente superiore all'IPL corrente ($P > \text{current\_ipl}$) possono interrompere la CPU.
- Quando la CPU accetta e prende in carico un interrupt (`wait_irq`), riceve una sessione di servizio (`IrqSession`): l'IPL del controller viene temporaneamente elevato alla priorità dell'interrupt in corso, impedendo a interrupt di priorità inferiore o uguale di causare interferenze.
- Al termine dell'elaborazione dell'interrupt, la CPU invia un segnale di completamento **EOI (End Of Interrupt)** invocando `eoi()` o rilasciando l'handle (`Drop`): l'IPL viene ripristinato al livello precedente e gli interrupt pendenti di priorità idonea vengono risvegliati e serviti.

Si scrivano in Rust le strutture che implementano i tratti `IrqSession` e `InterruptController` definiti di seguito.

---

### API richiesta

```rust
use std::time::Duration;

pub type IrqNumber = u8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PicError {
    /// Il controller è stato arrestato: non accetta nuovi interrupt e le attese si sbloccano con errore.
    Shutdown,
    /// La linea di interrupt specificata è già pendente e non ancora servita.
    AlreadyPending,
    /// Priorità non valida (es. priorità pari a zero).
    InvalidPriority,
}

pub trait IrqSession: Send {
    /// Restituisce il numero della linea di interrupt attualmente gestita.
    fn irq_number(&self) -> IrqNumber;

    /// Restituisce la priorità associata all'interrupt attualmente gestito.
    fn priority(&self) -> u8;

    /// Conclude esplicitamente la gestione dell'interrupt (End of Interrupt):
    /// ripristina il livello IPL precedente del controller e risveglia eventuali
    /// interrupt pendenti ora idonei.
    fn eoi(self);
}

pub trait InterruptController: Clone + Send + Sync {
    /// Asserisce un interrupt sulla linea indicata con la priorità specificata (1..=255).
    /// Se la stessa linea è già pendente e non ancora servita, restituisce Err(PicError::AlreadyPending).
    /// Se priority è 0, restituisce Err(PicError::InvalidPriority).
    /// Se il controller è arrestato, restituisce Err(PicError::Shutdown).
    fn raise_irq(&self, irq: IrqNumber, priority: u8) -> Result<(), PicError>;

    /// Blocca il thread chiamante (la CPU), senza consumare cicli di CPU, finché non è
    /// disponibile almeno un interrupt pendente con priorità strettamente maggiore
    /// dell'IPL corrente.
    /// Estrae l'interrupt con priorità più alta, eleva temporaneamente l'IPL a tale
    /// priorità e restituisce l'handle IrqSession.
    /// Se il controller viene arrestato, restituisce Err(PicError::Shutdown).
    fn wait_irq(&self) -> Result<impl IrqSession + 'static, PicError>;

    /// Variante con timeout di wait_irq: se scade il tempo specificato senza che sia
    /// disponibile un interrupt idoneo, rinuncia e restituisce Ok(None).
    fn wait_irq_timeout(&self, timeout: Duration) -> Result<Option<impl IrqSession + 'static>, PicError>;

    /// Imposta manualmente il livello di mascheramento base dell'IPL (0..=255).
    fn set_base_ipl(&self, ipl: u8);

    /// Restituisce il livello IPL attualmente attivo (può essere elevato a causa di un interrupt in corso).
    fn current_ipl(&self) -> u8;

    /// Restituisce il numero totale di interrupt attualmente pendenti (non ancora presi in carico).
    fn pending_irq_count(&self) -> usize;

    /// Arresta definitivamente il controller: risveglia tutti i thread bloccati in wait_irq
    /// con Err(PicError::Shutdown) e rifiuta ulteriori chiamate a raise_irq.
    fn shutdown(&self);
}

pub fn make_pic() -> impl InterruptController {
    ...
}
```

---

### Requisiti

- **Thread-Safety e Condivisione**:
  - Il controller deve essere thread-safe e condivisibile tra più thread produttori di interrupt e la CPU (`Clone + Send + Sync`).
- **Arbitraggio a Priorità e Mascheramento IPL**:
  - Un interrupt può essere servito solo se la sua priorità è strettamente superiore all'IPL corrente (`priority > current_ipl()`).
  - Se più interrupt sono pendenti e idonei, deve essere servito per primo quello con la **priorità numerica più elevata**. A parità di priorità, prevale l'ordine di inserimento FIFO.
- **Handshake EOI e Gestione RAII (`Drop`)**:
  - Quando un interrupt viene preso in carico via `wait_irq()`, l'IPL corrente sale alla priorità dell'interrupt.
  - La chiamata a `eoi(self)` ripristina il livello IPL precedente e notifica la `Condvar`.
  - Se l'handle `IrqSession` esce dallo scope (`Drop`) senza che `eoi()` sia stato chiamato esplicitamente, il distruttore deve inviare automaticamente l'EOI per garantire la correttezza RAII.
- **Assenza di Busy-Waiting e Timeout**:
  - `wait_irq()` e `wait_irq_timeout()` devono sospendere il chiamante su `Condvar` senza consumare cicli di CPU.
- **Arresto Pulito (`shutdown`)**:
  - `shutdown()` risveglia immediatamente tutti i thread in attesa con `Err(PicError::Shutdown)`.
- **Suite di Test**:
  - I test in `tests/shared_tests.rs` devono passare senza modifiche (`cargo test`).
  - Se il codice consegnato non compila, non verrà valutato.

---

### Suggerimenti implementativi

1. Rappresentare lo stato condiviso con una struttura protetta da `Arc<(Mutex<SharedState>, Condvar)>`.
2. Memorizzare gli interrupt pendenti mantenendo traccia di `(irq, priority, sequence_id)`.
3. Mantenere una pila degli IPL (`ipl_stack: Vec<u8>`) o il livello base unito allo storico dei livelli precedenti, così che un interrupt più prioritario possa nidificarsi e, al rilascio di ciascuna sessione, l'IPL torni esattamente al livello sottostante.
4. Nella struttura che realizza `IrqSession`, memorizzare un flag `eoi_sent: bool` e invocare il ripristino dell'IPL sia nel metodo `eoi()` che nell'implementazione di `Drop` (se non ancora inviato).
