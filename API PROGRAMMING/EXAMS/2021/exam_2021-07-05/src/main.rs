mod buffer;

use buffer::Buffer;
use std::thread;
use std::time::Duration;

fn main() {
    println!("--- Testing Buffer<T> ---");

    let buffer = Buffer::new();
    let buf_clone = buffer.clone();

    // Consumer thread
    let consumer = thread::spawn(move || {
        println!("Consumer: Waiting for data...");
        while let Ok(Some(val)) = buf_clone.consume() {
            println!("Consumer: Processed value: {}", val);
            thread::sleep(Duration::from_millis(100)); // Simulate work
        }
        println!("Consumer: Buffer closed or terminated gracefully.");
    });

    // Producer main thread
    for i in 1..=5 {
        println!("Producer: Sending value: {}", i);
        buffer.next(i);
        thread::sleep(Duration::from_millis(50));
    }

    println!("Producer: Terminating buffer...");
    buffer.terminate();

    consumer.join().unwrap();
    println!("Main thread: Execution finished successfully!");
}
