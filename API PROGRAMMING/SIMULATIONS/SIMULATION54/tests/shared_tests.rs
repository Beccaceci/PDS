use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use simulation_054::{
    make_watchdog, Watchdog, WatchdogError, WatchdogLease,
};

/// 1. Verifica statica dei bounds Send e Sync per i tratti.
#[test]
fn test_send_sync_bounds() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_clone<T: Clone>() {}

    fn check<W: Watchdog, L: WatchdogLease>() {
        assert_send::<W>();
        assert_sync::<W>();
        assert_clone::<W>();
        assert_send::<L>();
    }

    let watchdog = make_watchdog();
    let lease = watchdog.register("test_svc", Duration::from_millis(100)).unwrap();
    check_val(&watchdog, &lease);
    fn check_val<W: Watchdog, L: WatchdogLease>(_w: &W, _l: &L) {
        check::<W, L>();
    }
}

/// 2. Ciclo di vita base: battiti regolari mantengono in vita il servizio senza allarmi.
#[test]
fn test_basic_heartbeat_keeps_service_alive() {
    let watchdog = make_watchdog();
    assert_eq!(watchdog.active_service_count(), 0);

    let lease = watchdog.register("worker_1", Duration::from_millis(60)).unwrap();
    assert_eq!(watchdog.active_service_count(), 1);
    assert!(!lease.is_expired());

    // Due battiti regolari prima della scadenza
    thread::sleep(Duration::from_millis(25));
    assert!(lease.heartbeat());
    thread::sleep(Duration::from_millis(25));
    assert!(lease.heartbeat());

    // Nessun allarme deve scattare
    let res = watchdog.wait_alarm_timeout(Duration::from_millis(20)).unwrap();
    assert_eq!(res, None);
    assert!(!lease.is_expired());
    assert_eq!(watchdog.active_service_count(), 1);
}

/// 3. Rilevamento anomalia: se il servizio manca la scadenza, scatta l'allarme.
#[test]
fn test_service_misses_deadline_triggers_alarm() {
    let watchdog = make_watchdog();
    let lease = watchdog.register("stuck_worker", Duration::from_millis(40)).unwrap();

    let start = Instant::now();
    // wait_alarm si sospende e si sblocca non appena scade la tolleranza
    let failed_service = watchdog.wait_alarm().unwrap();
    let elapsed = start.elapsed();

    assert_eq!(failed_service, "stuck_worker");
    assert!(elapsed >= Duration::from_millis(35));
    assert!(lease.is_expired());
    assert_eq!(watchdog.active_service_count(), 0);

    // Un lease scaduto non può più rinnovarsi
    assert!(!lease.heartbeat());
}

/// 4. Gestione RAII: il Drop del lease deregistra pulitamente il servizio senza generare allarmi.
#[test]
fn test_raii_drop_lease_clean_shutdown_no_alarm() {
    let watchdog = make_watchdog();
    let lease = watchdog.register("clean_worker", Duration::from_millis(40)).unwrap();
    assert_eq!(watchdog.active_service_count(), 1);

    // Il servizio termina e rilascia il proprio lease prima della scadenza
    thread::sleep(Duration::from_millis(15));
    drop(lease);
    assert_eq!(watchdog.active_service_count(), 0);

    // Anche attendendo oltre la scadenza originale, nessun allarme deve scattare
    let res = watchdog.wait_alarm_timeout(Duration::from_millis(50)).unwrap();
    assert_eq!(res, None);
}

/// 5. Monitoraggio concorrente: scatta per primo il servizio con la scadenza più imminente.
#[test]
fn test_multiple_services_earliest_deadline_first() {
    let watchdog = make_watchdog();

    // Servizio A con timeout lungo (120ms)
    let lease_a = watchdog.register("service_slow", Duration::from_millis(120)).unwrap();
    // Servizio B con timeout breve (35ms)
    let _lease_b = watchdog.register("service_fast", Duration::from_millis(35)).unwrap();

    assert_eq!(watchdog.active_service_count(), 2);

    // L'allarme deve scattare per service_fast
    let failed = watchdog.wait_alarm().unwrap();
    assert_eq!(failed, "service_fast");

    // Nel frattempo service_slow è ancora attivo
    assert_eq!(watchdog.active_service_count(), 1);
    assert!(!lease_a.is_expired());
    assert!(lease_a.heartbeat());
}

/// 6. Rifiuto di timeout non validi e registrazioni post-shutdown.
#[test]
fn test_invalid_timeout_and_post_shutdown_registration() {
    let watchdog = make_watchdog();

    // Timeout zero deve fallire
    let res_zero = watchdog.register("bad_svc", Duration::ZERO);
    assert_eq!(res_zero.err(), Some(WatchdogError::InvalidTimeout));

    watchdog.shutdown();

    // Registrazione dopo shutdown deve fallire
    let res_post = watchdog.register("any_svc", Duration::from_millis(50));
    assert_eq!(res_post.err(), Some(WatchdogError::Shutdown));
}

/// 7. Chiusura pulita: shutdown() risveglia tutti i thread bloccati in wait_alarm con Shutdown.
#[test]
fn test_shutdown_wakes_waiters_with_error() {
    let watchdog = make_watchdog();
    let w_clone = watchdog.clone();

    let unblocked = Arc::new(AtomicBool::new(false));
    let unblocked_clone = unblocked.clone();

    let h = thread::spawn(move || {
        let res = w_clone.wait_alarm();
        assert_eq!(res, Err(WatchdogError::Shutdown));
        unblocked_clone.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(30));
    assert!(!unblocked.load(Ordering::SeqCst));

    watchdog.shutdown();
    h.join().unwrap();
    assert!(unblocked.load(Ordering::SeqCst));

    // Nuove chiamate a wait_alarm devono fallire immediatamente
    assert_eq!(watchdog.wait_alarm(), Err(WatchdogError::Shutdown));
}

/// 8. Stress test concorrente: 4 worker sani e 1 worker difettoso.
#[test]
fn test_concurrent_multi_service_stress() {
    let watchdog = make_watchdog();
    let mut handles = Vec::new();

    // 4 worker che inviano battiti per 100ms e poi escono pulitamente
    for i in 0..4 {
        let name = format!("healthy_{}", i);
        let lease = watchdog.register(&name, Duration::from_millis(40)).unwrap();
        handles.push(thread::spawn(move || {
            for _ in 0..4 {
                thread::sleep(Duration::from_millis(20));
                if !lease.heartbeat() {
                    break;
                }
            }
            // drop del lease al termine
        }));
    }

    // 1 worker che smette di battere dopo 15ms (timeout 30ms)
    let faulty_lease = watchdog.register("faulty_node", Duration::from_millis(30)).unwrap();
    handles.push(thread::spawn(move || {
        thread::sleep(Duration::from_millis(15));
        // Smette di inviare heartbeat e non droppa il lease
        thread::sleep(Duration::from_millis(100));
        let _ = faulty_lease;
    }));

    // Il watchdog deve intercettare l'anomalia di faulty_node
    let alarmed = watchdog.wait_alarm().unwrap();
    assert_eq!(alarmed, "faulty_node");

    for h in handles {
        h.join().unwrap();
    }
}
