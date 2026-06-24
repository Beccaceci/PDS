mod buffer;

use buffer::CircularBuffer;
use std::thread;
use std::time::Duration;

fn main() {
    println!("--- Testing CircularBuffer<T, N> ---");

    // We create a buffer with a maximum capacity of 3 items
    let buffer: CircularBuffer<String, 3> = CircularBuffer::new();
    let buf_clone = buffer.clone();

    let consumer = thread::spawn(move || {
        println!("Consumer: Starting to read...");
        for _ in 0..5 {
            let item = buf_clone.extract();
            println!("Consumer: Extracted '{}'", item);
            thread::sleep(Duration::from_millis(150)); // Simulating slow processing
        }
    });

    for i in 1..=5 {
        let item = format!("Message {}", i);
        println!("Producer: Trying to insert '{}'...", item);
        
        let start = std::time::Instant::now();
        // Since capacity is 3, and the consumer is slow, the 4th and 5th 
        // insertions will naturally block here and wait for space!
        buffer.insert(item);
        let elapsed = start.elapsed().as_millis();
        
        if elapsed > 10 {
            println!("Producer: Blocked for {}ms waiting for space!", elapsed);
        } else {
            println!("Producer: Inserted immediately.");
        }
    }

    consumer.join().unwrap();
    println!("Main thread: Execution finished successfully!");
}
