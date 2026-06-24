use crossbeam_channel::{self, Sender, unbounded};
use std::thread::{self, JoinHandle};

pub struct Looper<Message: Send + 'static> {
    sender: Option<crossbeam_channel::Sender<Message>>,
    handle: Option<JoinHandle<()>>
}

impl<Message: Send + 'static> Looper<Message> {
    
    pub fn new(process: fn(Message), cleanup: fn()) -> Self {
        // TODO: 
        // 1. Usa `crossbeam_channel::unbounded()` per creare il canale
        let (tx, rx) = unbounded::<Message>();

        // 2. Avvia il thread 
        let handle = thread::spawn(move || {
            // 3. Esegui il ciclo di ricezione e chiama process()
            for item in rx {
                process(item);
            }

            // 4. Chiama cleanup() all'uscita dal ciclo
            cleanup()
        });
        
        Self {
            sender: Some(tx),
            handle: Some(handle)
        }
    }

    pub fn send(&self, msg: Message) {
        let sender = self.sender.as_ref().unwrap();
        let _ = sender.send(msg);
    }
}

impl<Message: Send + 'static> Drop for Looper<Message> {
    fn drop(&mut self) {
        // 1. Distruggi il sender per far terminare il ciclo for
        self.sender.take();

        // 2. Fai join() per attendere che il cleanup() finisca
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

// Invochiamo i test condivisi!
crate::generate_looper_tests!(crate::looper_crossbeam::Looper<i32>);
