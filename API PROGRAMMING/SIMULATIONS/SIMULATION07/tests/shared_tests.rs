use simulation_007::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[test]
fn test_basic_read_and_write() {
    let cell = make_rw_cell(10);

    {
        let r1 = cell.read();
        assert_eq!(*r1.get(), 10);
    }

    {
        let mut w1 = cell.write();
        assert_eq!(*w1.get(), 10);
        *w1.get_mut() = 20;
    }

    {
        let r2 = cell.read();
        assert_eq!(*r2.get(), 20);
    }
}

#[test]
fn test_multiple_concurrent_readers() {
    let cell = Arc::new(make_rw_cell(100));
    const NUM_READERS: usize = 10;

    let active_readers = Arc::new(AtomicUsize::new(0));
    let max_readers = Arc::new(AtomicUsize::new(0));

    let mut handles = vec![];
    for _ in 0..NUM_READERS {
        let c = Arc::clone(&cell);
        let act = Arc::clone(&active_readers);
        let max_r = Arc::clone(&max_readers);
        handles.push(thread::spawn(move || {
            let guard = c.read();
            let current = act.fetch_add(1, Ordering::SeqCst) + 1;

            // Track maximum concurrent active readers
            max_r.fetch_max(current, Ordering::SeqCst);

            thread::sleep(Duration::from_millis(50));
            assert_eq!(*guard.get(), 100);

            act.fetch_sub(1, Ordering::SeqCst);
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(
        max_readers.load(Ordering::SeqCst) > 1,
        "Multiple readers should be able to hold read lock concurrently"
    );
}

#[test]
fn test_writer_excludes_readers() {
    let cell = Arc::new(make_rw_cell(0));

    let c_clone = Arc::clone(&cell);
    let writer_active = Arc::new(AtomicBool::new(false));
    let wa_clone = Arc::clone(&writer_active);

    let writer_handle = thread::spawn(move || {
        let mut guard = c_clone.write();
        wa_clone.store(true, Ordering::SeqCst);
        thread::sleep(Duration::from_millis(100));
        *guard.get_mut() = 42;
        wa_clone.store(false, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(30));

    // Reader attempts to read while writer is active
    let r_guard = cell.read();
    assert!(
        !writer_active.load(Ordering::SeqCst),
        "Reader should only acquire lock after writer finished"
    );
    assert_eq!(*r_guard.get(), 42);

    writer_handle.join().unwrap();
}

#[test]
fn test_writer_preference_prevents_reader_starvation() {
    let cell = Arc::new(make_rw_cell(0));

    // 1. First reader acquires read lock
    let c1 = Arc::clone(&cell);
    let r1_active = Arc::new(AtomicBool::new(false));
    let r1_active_clone = Arc::clone(&r1_active);

    let r1_handle = thread::spawn(move || {
        let _guard = c1.read();
        r1_active_clone.store(true, Ordering::SeqCst);
        thread::sleep(Duration::from_millis(150));
    });

    thread::sleep(Duration::from_millis(30));
    assert!(r1_active.load(Ordering::SeqCst));

    // 2. Writer arrives and starts waiting (writer-preference should block new readers)
    let c2 = Arc::clone(&cell);
    let writer_done = Arc::new(AtomicBool::new(false));
    let wd_clone = Arc::clone(&writer_done);

    let writer_handle = thread::spawn(move || {
        let mut guard = c2.write();
        *guard.get_mut() = 999;
        wd_clone.store(true, Ordering::SeqCst);
    });

    thread::sleep(Duration::from_millis(30));

    // 3. Second reader arrives AFTER the writer is in queue.
    // Due to writer preference, Reader 2 must NOT be granted access before the writer!
    let c3 = Arc::clone(&cell);
    let r2_handle = thread::spawn(move || {
        let guard = c3.read();
        assert_eq!(
            *guard.get(),
            999,
            "Reader 2 arriving after waiting writer must see writer's value (writer-preference)"
        );
    });

    r1_handle.join().unwrap();
    writer_handle.join().unwrap();
    r2_handle.join().unwrap();
}

#[test]
fn test_concurrent_read_write_stress() {
    let cell = Arc::new(make_rw_cell(0usize));
    const READERS: usize = 12;
    const WRITERS: usize = 4;
    const WRITES_PER_THREAD: usize = 25;

    let stop_flag = Arc::new(AtomicBool::new(false));

    let mut writer_handles = vec![];
    for _ in 0..WRITERS {
        let c = Arc::clone(&cell);
        writer_handles.push(thread::spawn(move || {
            for _ in 0..WRITES_PER_THREAD {
                let mut guard = c.write();
                *guard.get_mut() += 1;
                thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    let mut reader_handles = vec![];
    for _ in 0..READERS {
        let c = Arc::clone(&cell);
        let sf = Arc::clone(&stop_flag);
        reader_handles.push(thread::spawn(move || {
            while !sf.load(Ordering::SeqCst) {
                let guard = c.read();
                let _val = *guard.get();
                thread::sleep(Duration::from_micros(100));
            }
        }));
    }

    for h in writer_handles {
        h.join().unwrap();
    }

    stop_flag.store(true, Ordering::SeqCst);

    for h in reader_handles {
        h.join().unwrap();
    }

    let final_guard = cell.read();
    assert_eq!(*final_guard.get(), WRITERS * WRITES_PER_THREAD);
}
