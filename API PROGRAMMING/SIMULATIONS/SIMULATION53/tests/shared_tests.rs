use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use simulation_053::{
    make_gpu_dispatcher, GpuCommand, GpuDispatcher, GpuError, GpuQueue, QueueType,
};

fn make_cmd(cost_ms: u64) -> GpuCommand {
    GpuCommand {
        execution_cost_ms: cost_ms,
        action: Box::new(move || {
            if cost_ms > 0 {
                thread::sleep(Duration::from_millis(cost_ms));
            }
        }),
    }
}

/// 1. Verifica statica dei bounds Send e Sync per i tratti pubblici.
#[test]
fn test_send_sync_bounds() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}
    fn assert_clone<T: Clone>() {}

    fn check<D: GpuDispatcher, Q: GpuQueue>() {
        assert_send::<D>();
        assert_sync::<D>();
        assert_clone::<D>();
        assert_send::<Q>();
        assert_sync::<Q>();
    }

    let dispatcher = make_gpu_dispatcher();
    let (_qid, queue) = dispatcher.create_queue(QueueType::Graphics, 1024);
    check_val(&dispatcher, &queue);
    fn check_val<D: GpuDispatcher, Q: GpuQueue>(_d: &D, _q: &Q) {
        check::<D, Q>();
    }
}

/// 2. Ciclo di vita base: sottomissione batch, esecuzione ed avanzamento del semaforo timeline.
#[test]
fn test_basic_queue_submission_and_signal() {
    let dispatcher = make_gpu_dispatcher();
    let sem0 = dispatcher.create_semaphore(0);
    assert_eq!(dispatcher.read_semaphore(sem0), Ok(0));

    let (_qid, queue) = dispatcher.create_queue(QueueType::Compute, 1024);
    assert_eq!(queue.pending_batches_count(), 0);
    assert_eq!(queue.allocated_ring_bytes(), 0);

    let cmd = make_cmd(20);

    let batch_res = queue.submit(vec![cmd], &[], &[(sem0, 1)], 128);
    assert!(batch_res.is_ok());

    // Attesa host dell'avanzamento del semaforo a 1
    let wait_res = dispatcher.host_wait_semaphore(sem0, 1);
    assert_eq!(wait_res, Ok(()));
    assert_eq!(dispatcher.read_semaphore(sem0), Ok(1));

    // A completamento, il ring buffer deve essere tornato a 0
    dispatcher.wait_idle();
    assert_eq!(queue.pending_batches_count(), 0);
    assert_eq!(queue.allocated_ring_bytes(), 0);
}

/// 3. Risoluzione autonoma delle dipendenze inter-queue:
/// Compute calcola la fisica e avanza physics_sem a 1;
/// Graphics attende physics_sem == 1 e poi avanza render_sem a 1.
#[test]
fn test_inter_queue_dependency_resolution() {
    let dispatcher = make_gpu_dispatcher();
    let physics_sem = dispatcher.create_semaphore(0);
    let render_sem = dispatcher.create_semaphore(0);

    let (_q_comp_id, q_comp) = dispatcher.create_queue(QueueType::Compute, 1024);
    let (_q_gfx_id, q_gfx) = dispatcher.create_queue(QueueType::Graphics, 1024);

    // Compute esegue un calcolo da 40ms e segnala physics_sem = 1
    let cmd_comp = make_cmd(40);
    q_comp
        .submit(vec![cmd_comp], &[], &[(physics_sem, 1)], 64)
        .unwrap();

    // Graphics è sottomessa contemporaneamente ma DEVE attendere physics_sem >= 1
    let cmd_gfx = make_cmd(10);
    q_gfx
        .submit(vec![cmd_gfx], &[(physics_sem, 1)], &[(render_sem, 1)], 64)
        .unwrap();

    // Prima che Compute finisca, render_sem deve essere ancora 0
    assert_eq!(dispatcher.read_semaphore(render_sem), Ok(0));

    // Attendiamo il completamento della pipeline grafica
    dispatcher.host_wait_semaphore(render_sem, 1).unwrap();
    assert_eq!(dispatcher.read_semaphore(physics_sem), Ok(1));
    assert_eq!(dispatcher.read_semaphore(render_sem), Ok(1));
}

/// 4. Contropressione su ring buffer:
/// Sottomissioni che eccedono la capacità istantanea si bloccano senza attesa attiva.
/// Sottomissioni che eccedono la capacità totale vengono rigettate immediatamente con BatchTooLarge.
#[test]
fn test_ring_buffer_backpressure_and_batch_too_large() {
    let dispatcher = make_gpu_dispatcher();
    let (_qid, queue) = dispatcher.create_queue(QueueType::Transfer, 100);

    // 150 > 100 -> BatchTooLarge immediato
    let err = queue.submit(vec![], &[], &[], 150);
    assert_eq!(err, Err(GpuError::BatchTooLarge));

    let sem = dispatcher.create_semaphore(0);

    // Batch 1 occupa 80 bytes su 100 per 50ms
    let cmd1 = make_cmd(50);
    queue.submit(vec![cmd1], &[], &[(sem, 1)], 80).unwrap();

    // Batch 2 richiede 40 bytes: 80 + 40 = 120 > 100 -> si blocca finché Batch 1 non finisce!
    let batch2_started = Arc::new(AtomicBool::new(false));
    let batch2_done = Arc::new(AtomicBool::new(false));

    let q_clone = dispatcher.create_queue(QueueType::Transfer, 200).1;
    let _ = q_clone; // Dummy clone test

    let b2_started = batch2_started.clone();
    let b2_done = batch2_done.clone();
    let q_ref = &queue;

    // Per poter usare thread::scope o inviare riferimento thread-safe
    thread::scope(|s| {
        s.spawn(|| {
            b2_started.store(true, Ordering::SeqCst);
            // Questa sottomissione deve attendere che Batch 1 rilasci gli 80 byte
            let cmd2 = make_cmd(10);
            let res = q_ref.submit(vec![cmd2], &[], &[(sem, 2)], 40);
            assert!(res.is_ok());
            b2_done.store(true, Ordering::SeqCst);
        });

        // Verifichiamo che dopo 20ms il thread sia bloccato nella submit
        thread::sleep(Duration::from_millis(20));
        assert!(batch2_started.load(Ordering::SeqCst));
        assert!(!batch2_done.load(Ordering::SeqCst));

        // Attendiamo che entrambi completino
        dispatcher.host_wait_semaphore(sem, 2).unwrap();
        assert!(batch2_done.load(Ordering::SeqCst));
    });

    assert_eq!(queue.allocated_ring_bytes(), 0);
}

/// 5. Sincronizzazione CPU Host con semafori timeline:
/// Avanzamento non monotono rigettato, `host_wait_semaphore` sospende senza busy-waiting.
#[test]
fn test_host_signal_and_wait_synchronization() {
    let dispatcher = make_gpu_dispatcher();
    let sem = dispatcher.create_semaphore(10);

    // Segnalare un valore non strettamente superiore deve fallire
    assert_eq!(
        dispatcher.host_signal_semaphore(sem, 9),
        Err(GpuError::NonMonotonicTimeline)
    );
    assert_eq!(
        dispatcher.host_signal_semaphore(sem, 10),
        Err(GpuError::NonMonotonicTimeline)
    );

    let unblocked = Arc::new(AtomicBool::new(false));
    let unblocked_clone = unblocked.clone();
    let disp_clone = dispatcher.clone();

    let h = thread::spawn(move || {
        let res = disp_clone.host_wait_semaphore(sem, 20);
        assert_eq!(res, Ok(()));
        unblocked_clone.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(40));
    assert!(!unblocked.load(Ordering::SeqCst));

    // L'host avanza il semaforo a 20, sbloccando il thread in attesa
    dispatcher.host_signal_semaphore(sem, 20).unwrap();
    h.join().unwrap();
    assert!(unblocked.load(Ordering::SeqCst));
    assert_eq!(dispatcher.read_semaphore(sem), Ok(20));
}

/// 6. `wait_idle` sincronizza il chiamante fino al completamento di tutte le code.
#[test]
fn test_wait_idle_synchronization() {
    let dispatcher = make_gpu_dispatcher();
    let (_q1_id, q1) = dispatcher.create_queue(QueueType::Compute, 512);
    let (_q2_id, q2) = dispatcher.create_queue(QueueType::Graphics, 512);

    let sem1 = dispatcher.create_semaphore(0);
    let sem2 = dispatcher.create_semaphore(0);

    q1.submit(
        vec![make_cmd(30)],
        &[],
        &[(sem1, 1)],
        64,
    )
    .unwrap();

    q2.submit(
        vec![make_cmd(50)],
        &[],
        &[(sem2, 1)],
        64,
    )
    .unwrap();

    dispatcher.wait_idle();

    assert_eq!(q1.pending_batches_count(), 0);
    assert_eq!(q2.pending_batches_count(), 0);
    assert_eq!(dispatcher.read_semaphore(sem1), Ok(1));
    assert_eq!(dispatcher.read_semaphore(sem2), Ok(1));
}

/// 7. Reset hardware TDR (Timeout Detection and Recovery):
/// Una coda resettata abortisce i batch con DeviceLost e risveglia le attese con errore.
#[test]
fn test_reset_queue_tdr_error_propagation() {
    let dispatcher = make_gpu_dispatcher();
    let (qid, queue) = dispatcher.create_queue(QueueType::Compute, 512);
    let blocker_sem = dispatcher.create_semaphore(0);
    let target_sem = dispatcher.create_semaphore(0);

    // Batch che attende blocker_sem == 1 prima di segnalare target_sem == 1
    queue
        .submit(
            vec![make_cmd(10)],
            &[(blocker_sem, 1)],
            &[(target_sem, 1)],
            64,
        )
        .unwrap();

    let waiter_done = Arc::new(AtomicBool::new(false));
    let wd = waiter_done.clone();
    let d_clone = dispatcher.clone();

    let h = thread::spawn(move || {
        let res = d_clone.host_wait_semaphore(target_sem, 1);
        assert_eq!(res, Err(GpuError::DeviceLost));
        wd.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(30));
    assert!(!waiter_done.load(Ordering::SeqCst));

    // Eseguiamo il reset TDR della coda
    dispatcher.reset_queue(qid).unwrap();

    h.join().unwrap();
    assert!(waiter_done.load(Ordering::SeqCst));

    // Ulteriori sottomissioni su questa coda devono fallire con DeviceLost
    let res = queue.submit(vec![], &[], &[], 10);
    assert_eq!(res, Err(GpuError::DeviceLost));
}

/// 8. Gestione RAII: Distruzione di una GpuQueue pulisce la registrazione e decrementa active_queue_count.
#[test]
fn test_raii_drop_queue_decrements_count() {
    let dispatcher = make_gpu_dispatcher();
    assert_eq!(dispatcher.active_queue_count(), 0);

    let (_q1_id, q1) = dispatcher.create_queue(QueueType::Transfer, 512);
    assert_eq!(dispatcher.active_queue_count(), 1);

    {
        let (_q2_id, _q2) = dispatcher.create_queue(QueueType::Graphics, 512);
        assert_eq!(dispatcher.active_queue_count(), 2);
    } // _q2 viene droppata qui

    assert_eq!(dispatcher.active_queue_count(), 1);
    drop(q1);
    assert_eq!(dispatcher.active_queue_count(), 0);
}

/// 9. Stress test concorrente: pipeline a 3 code (Transfer -> Compute -> Graphics)
/// con 10 frame coordinati da semafori timeline.
#[test]
fn test_multi_queue_cross_stream_pipeline_stress() {
    let dispatcher = make_gpu_dispatcher();
    let (_q_t_id, q_transfer) = dispatcher.create_queue(QueueType::Transfer, 1024);
    let (_q_c_id, q_compute) = dispatcher.create_queue(QueueType::Compute, 1024);
    let (_q_g_id, q_graphics) = dispatcher.create_queue(QueueType::Graphics, 1024);

    let t_sem = dispatcher.create_semaphore(0);
    let c_sem = dispatcher.create_semaphore(0);
    let g_sem = dispatcher.create_semaphore(0);

    const NUM_FRAMES: u64 = 8;

    for frame in 1..=NUM_FRAMES {
        // Transfer carica i dati del frame
        q_transfer
            .submit(
                vec![make_cmd(2)],
                &[(t_sem, frame - 1)],
                &[(t_sem, frame)],
                32,
            )
            .unwrap();

        // Compute calcola la logica del frame una volta terminato Transfer
        q_compute
            .submit(
                vec![make_cmd(3)],
                &[(t_sem, frame), (c_sem, frame - 1)],
                &[(c_sem, frame)],
                32,
            )
            .unwrap();

        // Graphics renderizza il frame una volta terminato Compute
        q_graphics
            .submit(
                vec![make_cmd(2)],
                &[(c_sem, frame), (g_sem, frame - 1)],
                &[(g_sem, frame)],
                32,
            )
            .unwrap();
    }

    // L'host attende il completamento dell'ultimo frame renderizzato
    dispatcher.host_wait_semaphore(g_sem, NUM_FRAMES).unwrap();

    assert_eq!(dispatcher.read_semaphore(t_sem), Ok(NUM_FRAMES));
    assert_eq!(dispatcher.read_semaphore(c_sem), Ok(NUM_FRAMES));
    assert_eq!(dispatcher.read_semaphore(g_sem), Ok(NUM_FRAMES));

    dispatcher.wait_idle();
    assert_eq!(q_transfer.pending_batches_count(), 0);
    assert_eq!(q_compute.pending_batches_count(), 0);
    assert_eq!(q_graphics.pending_batches_count(), 0);
}
