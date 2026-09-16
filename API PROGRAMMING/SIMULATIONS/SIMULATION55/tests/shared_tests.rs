use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use simulation_055::{
    make_pic, InterruptController, IrqSession, PicError,
};

/// 1. Verifica statica dei bounds Send e Sync per i tratti.
#[test]
fn test_send_sync_bounds() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_clone<T: Clone>() {}

    fn check<P: InterruptController, S: IrqSession>() {
        assert_send::<P>();
        assert_sync::<P>();
        assert_clone::<P>();
        assert_send::<S>();
    }

    let pic = make_pic();
    pic.raise_irq(1, 10).unwrap();
    let session = pic.wait_irq().unwrap();
    check_val(&pic, &session);
    fn check_val<P: InterruptController, S: IrqSession>(_p: &P, _s: &S) {
        check::<P, S>();
    }
}

/// 2. Ciclo di vita base: sollevamento interrupt, attesa CPU, elevazione IPL e handshake EOI.
#[test]
fn test_basic_raise_and_wait_irq_with_eoi() {
    let pic = make_pic();
    assert_eq!(pic.current_ipl(), 0);
    assert_eq!(pic.pending_irq_count(), 0);

    pic.raise_irq(4, 25).unwrap();
    assert_eq!(pic.pending_irq_count(), 1);

    let session = pic.wait_irq().unwrap();
    assert_eq!(session.irq_number(), 4);
    assert_eq!(session.priority(), 25);
    assert_eq!(pic.pending_irq_count(), 0);

    // Durante l'elaborazione dell'interrupt, l'IPL è elevato al livello della priorità dell'interrupt
    assert_eq!(pic.current_ipl(), 25);

    // Esecuzione dell'EOI esplicito
    session.eoi();

    // Al termine dell'EOI, l'IPL torna al livello base precedente
    assert_eq!(pic.current_ipl(), 0);
}

/// 3. Arbitraggio a priorità: viene servito prioritariamente l'interrupt con priorità più alta.
#[test]
fn test_priority_arbitration_highest_priority_first() {
    let pic = make_pic();

    pic.raise_irq(1, 10).unwrap();
    pic.raise_irq(2, 50).unwrap(); // Massima priorità
    pic.raise_irq(3, 20).unwrap();

    assert_eq!(pic.pending_irq_count(), 3);

    // 1. Deve essere estratto prima IRQ 2 (priorità 50)
    let s1 = pic.wait_irq().unwrap();
    assert_eq!(s1.irq_number(), 2);
    assert_eq!(s1.priority(), 50);
    s1.eoi();

    // 2. Successivamente IRQ 3 (priorità 20)
    let s2 = pic.wait_irq().unwrap();
    assert_eq!(s2.irq_number(), 3);
    assert_eq!(s2.priority(), 20);
    s2.eoi();

    // 3. Infine IRQ 1 (priorità 10)
    let s3 = pic.wait_irq().unwrap();
    assert_eq!(s3.irq_number(), 1);
    assert_eq!(s3.priority(), 10);
    s3.eoi();

    assert_eq!(pic.pending_irq_count(), 0);
}

/// 4. Mascheramento IPL: gli interrupt con priorità <= IPL rimangono bloccati.
#[test]
fn test_masking_blocks_lower_priority_irq() {
    let pic = make_pic();
    pic.set_base_ipl(30);
    assert_eq!(pic.current_ipl(), 30);

    // IRQ 1 con priorità 20 <= 30: mascherato!
    pic.raise_irq(1, 20).unwrap();
    let res_timeout = pic.wait_irq_timeout(Duration::from_millis(30)).unwrap();
    assert!(res_timeout.is_none());

    // IRQ 2 con priorità 40 > 30: non mascherato, scatta subito!
    pic.raise_irq(2, 40).unwrap();
    let s_high = pic.wait_irq().unwrap();
    assert_eq!(s_high.irq_number(), 2);
    s_high.eoi();

    // IRQ 1 è ancora pendente perché mascherato dall'IPL base (30)
    assert_eq!(pic.pending_irq_count(), 1);

    // Abbassiamo l'IPL base a 15: ora IRQ 1 (20 > 15) diventa idoneo e si sblocca!
    pic.set_base_ipl(15);
    let s_low = pic.wait_irq().unwrap();
    assert_eq!(s_low.irq_number(), 1);
    s_low.eoi();

    assert_eq!(pic.pending_irq_count(), 0);
}

/// 5. Gestione RAII: il Drop dell'handle IrqSession ripristina automaticamente l'IPL.
#[test]
fn test_raii_drop_automatically_sends_eoi() {
    let pic = make_pic();
    pic.raise_irq(7, 18).unwrap();

    {
        let session = pic.wait_irq().unwrap();
        assert_eq!(session.irq_number(), 7);
        assert_eq!(pic.current_ipl(), 18);
        // Non chiamiamo session.eoi(), lasciamo che esca dallo scope!
    }

    // Il Drop deve aver inviato automaticamente l'EOI e ripristinato l'IPL a 0
    assert_eq!(pic.current_ipl(), 0);
}

/// 6. Rifiuto di linee già pendenti e priorità zero.
#[test]
fn test_already_pending_and_invalid_priority() {
    let pic = make_pic();

    // Priorità 0 non valida
    assert_eq!(pic.raise_irq(3, 0), Err(PicError::InvalidPriority));

    // Primo inserimento valido
    assert_eq!(pic.raise_irq(3, 10), Ok(()));

    // Secondo inserimento su linea già pendente deve fallire
    assert_eq!(pic.raise_irq(3, 15), Err(PicError::AlreadyPending));

    // Serviamo l'interrupt
    let s = pic.wait_irq().unwrap();
    s.eoi();

    // Ora che è stato servito, la linea 3 può essere nuovamente sollevata
    assert_eq!(pic.raise_irq(3, 15), Ok(()));
}

/// 7. Chiusura pulita: shutdown() risveglia i thread in attesa e rigetta nuovi interrupt.
#[test]
fn test_shutdown_wakes_waiters_and_rejects_raise() {
    let pic = make_pic();
    let p_clone = pic.clone();

    let unblocked = Arc::new(AtomicBool::new(false));
    let u_clone = unblocked.clone();

    let h = thread::spawn(move || {
        let res = p_clone.wait_irq();
        assert!(matches!(res, Err(PicError::Shutdown)));
        u_clone.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(30));
    assert!(!unblocked.load(Ordering::SeqCst));

    pic.shutdown();
    h.join().unwrap();
    assert!(unblocked.load(Ordering::SeqCst));

    // Nuove chiamate a raise_irq o wait_irq devono fallire con Shutdown
    assert_eq!(pic.raise_irq(1, 10), Err(PicError::Shutdown));
    assert!(matches!(pic.wait_irq(), Err(PicError::Shutdown)));
}

/// 8. Stress test concorrente: 4 periferiche asseriscono interrupt contemporaneamente.
#[test]
fn test_concurrent_multi_device_stress() {
    let pic = make_pic();
    const NUM_DEVICES: u8 = 4;
    const ROUNDS: usize = 20;

    let mut dev_handles = Vec::new();

    for dev_id in 0..NUM_DEVICES {
        let p = pic.clone();
        dev_handles.push(thread::spawn(move || {
            for _ in 0..ROUNDS {
                let priority = (dev_id + 1) * 10;
                // Attendiamo che la linea non sia occupata prima di sollevarla
                while let Err(PicError::AlreadyPending) = p.raise_irq(dev_id, priority) {
                    thread::sleep(Duration::from_millis(1));
                }
                thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    // Thread CPU che consuma tutti gli interrupt
    let total_expected = NUM_DEVICES as usize * ROUNDS;
    let mut serviced = 0;

    while serviced < total_expected {
        if let Ok(Some(session)) = pic.wait_irq_timeout(Duration::from_millis(50)) {
            serviced += 1;
            // Simuliamo breve tempo di servizio dell'ISR
            thread::sleep(Duration::from_micros(100));
            session.eoi();
        }
    }

    for h in dev_handles {
        h.join().unwrap();
    }

    assert_eq!(serviced, total_expected);
    assert_eq!(pic.pending_irq_count(), 0);
    assert_eq!(pic.current_ipl(), 0);
}
