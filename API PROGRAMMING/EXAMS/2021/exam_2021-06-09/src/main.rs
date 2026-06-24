mod executor;

use executor::SingleThreadExecutor;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() {
    println!("--- Testing SingleThreadExecutor ---");

    let executor = SingleThreadExecutor::new();
    let results = Arc::new(Mutex::new(Vec::new()));

    // Submit some slow tasks
    for i in 1..=5 {
        let results_clone = Arc::clone(&results);
        executor.submit(move || {
            println!("Task {} is starting...", i);
            thread::sleep(Duration::from_millis(500));
            results_clone.lock().unwrap().push(i * 10);
            println!("Task {} finished!", i);
        }).unwrap();
    }

    println!("Main thread: All tasks submitted. Closing executor...");
    executor.close();

    // Trying to submit a task after closing should fail
    match executor.submit(|| println!("Too late!")) {
        Ok(_) => println!("Error: Task was accepted after close!"),
        Err(e) => println!("Correct: Task rejected because '{}'", e),
    }

    println!("Main thread: Joining executor...");
    executor.join();

    let final_results = results.lock().unwrap();
    println!("Main thread: Executor finished! Final results: {:?}", *final_results);
}
