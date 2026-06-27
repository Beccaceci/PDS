#![allow(non_snake_case)]

#[macro_use]
pub mod shared_tests;

pub mod multiChannel_mpsc;
pub mod multi_channel_basic;
pub mod multi_channel_crossbeam;
use std::time::Instant;
use std::thread;

// Importiamo le tre varianti architetturali create
use multiChannel_mpsc::MultiChannelMpsc;
use multi_channel_crossbeam::MultiChannelCrossbeam;
use multi_channel_basic::MultiChannelBasic;

fn main() {
    let num_messages = 500_000;
    let num_subscribers = 10;
    
    println!("=== Benchmark MultiChannel ===");
    println!("Messaggi per sottoscrittore: {}", num_messages);
    println!("Numero Sottoscrittori: {}", num_subscribers);
    println!("Totale messaggi recapitati: {}", num_messages * num_subscribers as u64);
    println!("----------------------------------");

    // 1. MPSC Standard (La tua implementazione originale ottimizzata)
    let dur_mpsc = benchmark_mpsc(num_messages, num_subscribers);
    println!("1. MPSC Standard    : {:.2?}", dur_mpsc);

    // 2. Basic Structures (Condvar + Mutex)
    let dur_basic = benchmark_basic(num_messages, num_subscribers);
    println!("2. Basic (Condvar)  : {:.2?}", dur_basic);

    // 3. Crossbeam
    let dur_crossbeam = benchmark_crossbeam(num_messages, num_subscribers);
    println!("3. Crossbeam        : {:.2?}", dur_crossbeam);
}

// --- Funzioni Helper per eseguire il benchmark di ogni implementazione ---

fn benchmark_mpsc(num_messages: u64, num_subscribers: usize) -> std::time::Duration {
    let channel = MultiChannelMpsc::new();
    let mut consumers = vec![];
    
    for _ in 0..num_subscribers {
        let rx = channel.subscribe();
        consumers.push(thread::spawn(move || {
            let mut count = 0;
            while count < num_messages {
                if rx.recv().is_ok() { count += 1; }
            }
        }));
    }

    let start = Instant::now();
    for i in 0..num_messages {
        let _ = channel.send((i % 256) as u8);
    }
    
    for c in consumers {
        c.join().unwrap();
    }
    
    start.elapsed()
}

fn benchmark_crossbeam(num_messages: u64, num_subscribers: usize) -> std::time::Duration {
    let channel = MultiChannelCrossbeam::new();
    let mut consumers = vec![];
    
    for _ in 0..num_subscribers {
        let rx = channel.subscribe();
        consumers.push(thread::spawn(move || {
            let mut count = 0;
            while count < num_messages {
                if rx.recv().is_ok() { count += 1; }
            }
        }));
    }

    let start = Instant::now();
    for i in 0..num_messages {
        let _ = channel.send((i % 256) as u8);
    }
    
    for c in consumers {
        c.join().unwrap();
    }
    
    start.elapsed()
}

fn benchmark_basic(num_messages: u64, num_subscribers: usize) -> std::time::Duration {
    let channel = MultiChannelBasic::new();
    let mut consumers = vec![];
    
    for _ in 0..num_subscribers {
        let rx = channel.subscribe();
        consumers.push(thread::spawn(move || {
            let mut count = 0;
            while count < num_messages {
                if rx.recv().is_ok() { count += 1; }
            }
        }));
    }

    let start = Instant::now();
    for i in 0..num_messages {
        let _ = channel.send((i % 256) as u8);
    }
    
    for c in consumers {
        c.join().unwrap();
    }
    
    start.elapsed()
}
