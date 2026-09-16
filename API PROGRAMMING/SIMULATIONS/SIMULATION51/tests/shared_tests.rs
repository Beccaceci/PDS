use simulation_051::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

fn assert_send<T: Send>(_val: &T) {}
fn assert_sync<T: Sync>(_val: &T) {}

#[test]
fn test_send_sync_bounds() {
    let bus = make_memory_bus();
    assert_send(&bus);
    assert_sync(&bus);

    let (_id, cache) = bus.attach_core();
    assert_send(&cache);
    assert_sync(&cache);
}

#[test]
fn test_single_core_read_miss_to_exclusive_and_write_to_modified() {
    let bus = make_memory_bus();
    let (_id, cache) = bus.attach_core();
    assert_eq!(bus.active_core_count(), 1);

    // Initial state of unseen line is Invalid
    assert_eq!(cache.line_state(0x10), MesiState::Invalid);

    // Read Miss on Invalid: since no other core has it, installs as Exclusive
    let val = cache.read(0x10).unwrap();
    assert_eq!(val, 0); // default initial RAM value
    assert_eq!(cache.line_state(0x10), MesiState::Exclusive);
    assert_eq!(bus.total_bus_transactions(), 1); // 1 BusRd

    // Write on Exclusive: transitions silently to Modified without extra bus transaction
    cache.write(0x10, 42).unwrap();
    cache.memory_barrier();

    assert_eq!(cache.line_state(0x10), MesiState::Modified);
    assert_eq!(cache.read(0x10).unwrap(), 42);
    // Silent upgrade means no extra bus transaction
    assert_eq!(bus.total_bus_transactions(), 1);
}

#[test]
fn test_two_cores_shared_read_coherency() {
    let bus = make_memory_bus();
    let (_id0, core0) = bus.attach_core();
    let (_id1, core1) = bus.attach_core();
    assert_eq!(bus.active_core_count(), 2);

    // Core 0 reads 0x20 -> installs as Exclusive
    assert_eq!(core0.read(0x20).unwrap(), 0);
    assert_eq!(core0.line_state(0x20), MesiState::Exclusive);

    // Core 1 reads 0x20 -> miss, broadcasts BusRd.
    // Core 0 snoops and downgrades Exclusive -> Shared. Core 1 installs as Shared.
    assert_eq!(core1.read(0x20).unwrap(), 0);

    assert_eq!(core0.line_state(0x20), MesiState::Shared);
    assert_eq!(core1.line_state(0x20), MesiState::Shared);
    assert_eq!(bus.total_bus_transactions(), 2); // BusRd from core0, BusRd from core1
}

#[test]
fn test_snoop_invalidation_on_write() {
    let bus = make_memory_bus();
    let (_id0, core0) = bus.attach_core();
    let (_id1, core1) = bus.attach_core();

    // Both read 0x30 so both hold it in Shared
    core0.read(0x30).unwrap();
    core1.read(0x30).unwrap();
    assert_eq!(core0.line_state(0x30), MesiState::Shared);
    assert_eq!(core1.line_state(0x30), MesiState::Shared);
    assert_eq!(bus.total_invalidations(), 0);

    // Core 0 writes 0x30 -> emits BusUpgr
    core0.write(0x30, 99).unwrap();
    core0.memory_barrier();

    // Core 0 is now Modified; Core 1 must have been invalidated by snooping!
    assert_eq!(core0.line_state(0x30), MesiState::Modified);
    assert_eq!(core1.line_state(0x30), MesiState::Invalid);
    assert_eq!(bus.total_invalidations(), 1);

    // When Core 1 reads 0x30 again, it misses in I, snoops Core 0 (flushing dirty 99),
    // and reads the updated value 99!
    assert_eq!(core1.read(0x30).unwrap(), 99);
}

#[test]
fn test_dirty_line_flush_on_read_miss() {
    let bus = make_memory_bus();
    let (_id0, core0) = bus.attach_core();
    let (_id1, core1) = bus.attach_core();

    // Core 0 writes 0x40 with value 500 (starts in I, becomes Modified)
    core0.write(0x40, 500).unwrap();
    core0.memory_barrier();
    assert_eq!(core0.line_state(0x40), MesiState::Modified);

    // Main memory is still 0 before any flush
    assert_eq!(bus.read_main_memory(0x40), 0);

    // Core 1 reads 0x40: triggers BusRd snoop on Core 0 -> flushes dirty value 500 to main memory!
    assert_eq!(core1.read(0x40).unwrap(), 500);

    // Main memory now holds the flushed value 500
    assert_eq!(bus.read_main_memory(0x40), 500);

    // Both cores now hold the line in Shared
    assert_eq!(core0.line_state(0x40), MesiState::Shared);
    assert_eq!(core1.line_state(0x40), MesiState::Shared);
}

#[test]
fn test_store_buffer_and_memory_barrier() {
    let bus = make_memory_bus();
    let (_id0, core0) = bus.attach_core();

    core0.write(0x100, 10).unwrap();
    core0.write(0x108, 20).unwrap();
    core0.write(0x110, 30).unwrap();

    // Calling memory barrier must block without CPU spin until store buffer is completely empty
    core0.memory_barrier();
    assert_eq!(core0.pending_store_count(), 0);

    assert_eq!(core0.read(0x100).unwrap(), 10);
    assert_eq!(core0.read(0x108).unwrap(), 20);
    assert_eq!(core0.read(0x110).unwrap(), 30);
}

#[test]
fn test_raii_drop_core_flushes_modified_lines_to_main_memory() {
    let bus = make_memory_bus();

    {
        let (_id0, core0) = bus.attach_core();
        assert_eq!(bus.active_core_count(), 1);

        core0.write(0x50, 777).unwrap();
        core0.memory_barrier();
        assert_eq!(core0.line_state(0x50), MesiState::Modified);
        assert_eq!(bus.read_main_memory(0x50), 0);

        // core0 dropped here -> Drop must flush modified line 0x50 to main memory!
    }

    assert_eq!(bus.active_core_count(), 0);
    assert_eq!(
        bus.read_main_memory(0x50),
        777,
        "Drop must flush dirty lines to main memory"
    );
}

#[test]
fn test_concurrent_competing_writers_serialization() {
    let bus = make_memory_bus();
    let (_id0, core0) = bus.attach_core();
    let (_id1, core1) = bus.attach_core();

    let c0 = Arc::new(core0);
    let c1 = Arc::new(core1);

    // Both read 0x60 -> both hold in Shared
    c0.read(0x60).unwrap();
    c1.read(0x60).unwrap();

    let barrier = Arc::new(Barrier::new(2));

    let c0_clone = Arc::clone(&c0);
    let b0 = Arc::clone(&barrier);
    let h0 = thread::spawn(move || {
        b0.wait();
        c0_clone.write(0x60, 111).unwrap();
        c0_clone.memory_barrier();
    });

    let c1_clone = Arc::clone(&c1);
    let b1 = Arc::clone(&barrier);
    let h1 = thread::spawn(move || {
        b1.wait();
        c1_clone.write(0x60, 222).unwrap();
        c1_clone.memory_barrier();
    });

    h0.join().unwrap();
    h1.join().unwrap();

    // After both finish, exactly one core must have Modified (the last writer),
    // while the other must be Invalid!
    let s0 = c0.line_state(0x60);
    let s1 = c1.line_state(0x60);

    assert!(
        (s0 == MesiState::Modified && s1 == MesiState::Invalid)
            || (s1 == MesiState::Modified && s0 == MesiState::Invalid),
        "MESI invariant: exactly one core should hold Modified, other Invalid"
    );
}

#[test]
fn test_stress_multi_core_snooping_workload() {
    let bus = make_memory_bus();
    let core_count = 8;
    let mut cores = Vec::new();

    for _ in 0..core_count {
        let (_id, c) = bus.attach_core();
        cores.push(Arc::new(c));
    }

    let iterations = 25;
    let address_space = 4; // 0..4 addresses, high contention
    let completed_ops = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for c_idx in 0..core_count {
        let core = Arc::clone(&cores[c_idx]);
        let co = Arc::clone(&completed_ops);
        handles.push(thread::spawn(move || {
            for i in 0..iterations {
                let addr = ((c_idx + i) % address_space) as Address;
                if (c_idx + i) % 3 == 0 {
                    // Write
                    let val = (c_idx * 100 + i) as Value;
                    let _ = core.write(addr, val);
                    core.memory_barrier();
                } else {
                    // Read
                    let _ = core.read(addr);
                }
                co.fetch_add(1, Ordering::SeqCst);
                thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(completed_ops.load(Ordering::SeqCst), core_count * iterations);
    assert!(bus.total_bus_transactions() > 0);
}
