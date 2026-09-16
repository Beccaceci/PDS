//! # Simulazione 055 — InterruptController
//!
//! Nelle architetture di calcolo e nei kernel dei sistemi operativi (come i controller
//! x86 APIC/PIC, ARM GIC o RISC-V PLIC), i dispositivi periferici segnalano eventi asincroni
//! alla CPU asserendo linee di interrupt hardware (`IrqNumber`), ciascuna caratterizzata
//! da un livello di priorità (1..=255).
//!
//! La CPU mantiene un livello di mascheramento attivo (IPL - Interrupt Priority Level):
//! - Soltanto gli interrupt con priorità strettamente superiore all'IPL corrente possono
//!   interrompere la CPU.
//! - Quando la CPU accetta e prende in carico un interrupt (`wait_irq`), riceve una sessione
//!   di servizio (`IrqSession`): l'IPL viene temporaneamente elevato alla priorità dell'interrupt,
//!   prevenendo interferenze da interrupt di priorità inferiore o uguale.
//! - Al termine dell'elaborazione, la CPU invia un segnale EOI (End of Interrupt) o rilascia
//!   l'handle (`Drop`): l'IPL viene ripristinato al livello precedente e gli interrupt
//!   pendenti idonei vengono risvegliati e serviti.
//!
//! Si scrivano in Rust le strutture che implementano i tratti `IrqSession` e `InterruptController`.

use std::{collections::BinaryHeap, sync::{Arc, Condvar, Mutex}, time::Duration};

use crate::PicError::{AlreadyPending, InvalidPriority, Shutdown};

pub type IrqNumber = u8;

/// Errori operativi restituiti durante le transazioni del controller di interrupt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PicError {
    /// Il controller è stato arrestato: non accetta nuovi interrupt e le attese si sbloccano con errore.
    Shutdown,
    /// La linea di interrupt specificata è già pendente e non ancora servita.
    AlreadyPending,
    /// Priorità non valida (es. priorità pari a zero).
    InvalidPriority,
}

/// Tratto che rappresenta la sessione attiva di gestione di un interrupt da parte della CPU.
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

/// Tratto che rappresenta il coordinatore controller degli interrupt hardware.
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

// =================================================================================
// 🛠️ SPAZIO RISERVATO ALLO STUDENTE (STUDENT WORKSPACE)
// =================================================================================
// Inserisci in questa sezione le tue strutture private e le relative implementazioni.
// =================================================================================

pub struct MyIrqSession {
    irq_priority: u8,
    old_priority: u8,
    irq_number: IrqNumber,
    shared_pic: MyPic
}

pub struct PicState {
    actual_priority: u8,
    available: bool,
    closed: bool,
    pending_irqs: BinaryHeap<MyIrqSession>,
    executing_irq: Option<MyIrqSession>
}

impl PicState {
    pub fn new () -> Self {
        Self {
            actual_priority: 0,
            available: true,
            closed: false,
            pending_irqs: BinaryHeap::new()
        }
    }
}

pub struct MyPic {
    inner: Arc<(Mutex<PicState>, Condvar)>
}

impl MyPic {
    pub fn new () -> Self {
        Self {
            inner: Arc::new((Mutex::new(PicState::new()), Condvar::new()))
        }
    }
}

impl InterruptController for MyPic {
    fn raise_irq(&self, irq: IrqNumber, priority: u8) -> Result<(), PicError> {
        if priority == 0 {
            return Err(InvalidPriority);
        }

        let (mutex, cvar) = &*self.inner;
        let mut guard = mutex.lock().unwrap();
        
        if guard.closed {
            return Err(Shutdown);
        }

        if guard.pending_irqs.iter().any(|irq_session| irq_session.irq_number == irq) {
            return Err(AlreadyPending);
        }

        let my_irq = MyIrqSession {
            irq_priority: irq,
            old_priority: q
        }
    }

    fn wait_irq(&self) -> Result<impl IrqSession + 'static, PicError> {
        todo!()
    }

    fn wait_irq_timeout(&self, timeout: Duration) -> Result<Option<impl IrqSession + 'static>, PicError> {
        todo!()
    }

    fn set_base_ipl(&self, ipl: u8) {
        todo!()
    }

    fn current_ipl(&self) -> u8 {
        todo!()
    }

    fn pending_irq_count(&self) -> usize {
        todo!()
    }

    fn shutdown(&self) {
        todo!()
    }
}

impl Clone for MyPic {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone()
        }
    }
}

/// Inizializza un nuovo controller programmabile degli interrupt (PIC).
pub fn make_pic() -> impl InterruptController {
    // Stub per consentire la verifica di compilazione delle interfacce.
    // Lo studente deve sostituire questo stub con la propria implementazione completa.
    struct DummySession;
    impl IrqSession for DummySession {
        fn irq_number(&self) -> IrqNumber { todo!() }
        fn priority(&self) -> u8 { todo!() }
        fn eoi(self) { todo!() }
    }

    #[derive(Clone)]
    struct DummyPic;
    impl InterruptController for DummyPic {
        fn raise_irq(&self, _irq: IrqNumber, _priority: u8) -> Result<(), PicError> { todo!() }
        fn wait_irq(&self) -> Result<impl IrqSession + 'static, PicError> {
            Ok(DummySession)
        }
        fn wait_irq_timeout(&self, _timeout: Duration) -> Result<Option<impl IrqSession + 'static>, PicError> {
            Ok(Some(DummySession))
        }
        fn set_base_ipl(&self, _ipl: u8) { todo!() }
        fn current_ipl(&self) -> u8 { todo!() }
        fn pending_irq_count(&self) -> usize { todo!() }
        fn shutdown(&self) { todo!() }
    }

    DummyPic
}
