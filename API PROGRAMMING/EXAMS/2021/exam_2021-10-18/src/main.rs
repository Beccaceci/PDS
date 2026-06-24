mod processor;
mod processor_mpsc; // Mantiene anche il file alternativo a scopo di studio

use processor::Processor;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() {
    println!("--- Testing Manual Condvar Processor<T> ---");

    let results = Arc::new(Mutex::new(Vec::new()));
    let results_clone = Arc::clone(&results);

    // Inizializziamo l'executor passando la closure che gestisce i dati in background
    let processor = Processor::new(move |item: String| {
        println!("Worker thread: Inizio elaborazione di '{}'...", item);
        thread::sleep(Duration::from_millis(200)); // Simuliamo lavoro pesante
        results_clone.lock().unwrap().push(item.clone());
        println!("Worker thread: Completata elaborazione di '{}'.", item);
    });

    println!("Main thread: Invio messaggi...");
    processor.send("Task A".to_string());
    processor.send("Task B".to_string());
    processor.send("Task C".to_string());
    println!("Main thread: Tutti i messaggi inviati.");

    println!("Main thread: Chiamata a close(). Attendo fine lavori...");
    // Questa chiamata blocca il main thread finche' il worker non finisce
    processor.close();
    
    let final_results = results.lock().unwrap();
    println!("Main thread: Processor chiuso con successo. Risultati: {:?}", *final_results);
}
