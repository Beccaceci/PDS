mod exchanger;

use exchanger::Exchanger;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() {
    println!("--- Testing Exchanger ---");
    
    // We wrap the Exchanger in an Arc so multiple threads can share ownership of it.
    let exchanger = Arc::new(Exchanger::new());

    // Clone the Arcs for the threads
    let ex1 = Arc::clone(&exchanger);
    let ex2 = Arc::clone(&exchanger);

    let t1 = thread::spawn(move || {
        println!("Thread 1: Arriving with data 'A'");
        let result = ex1.exchange("A");
        println!("Thread 1: Received {:?}", result);
    });

    // We add a slight delay to ensure Thread 1 consistently arrives first
    thread::sleep(Duration::from_millis(100));

    let t2 = thread::spawn(move || {
        println!("Thread 2: Arriving with data 'B'");
        let result = ex2.exchange("B");
        println!("Thread 2: Received {:?}", result);
    });

    // Wait for both threads to finish
    t1.join().unwrap();
    t2.join().unwrap();
    
    println!("--- Exchange Complete ---");
}