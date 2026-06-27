use crossbeam_channel::{self, Receiver, Sender};
use std::sync::Mutex;

pub struct Subscription<Message: Clone> {
    receiver: Receiver<Message>
}

impl<Message: Clone> Subscription<Message> {
    pub fn with_receiver(receiver: Receiver<Message>) -> Self {
        Self { receiver }
    }

    pub fn read(&self) -> Option<Message> {
        self.receiver.recv().ok()
    }
}

pub struct Dispatcher<Message: Clone> {
    subscriptions: Mutex<Vec<Sender<Message>>>
}

impl<Message: Clone> Dispatcher<Message> {
    pub fn new() -> Self {
        Self {
            subscriptions: Mutex::new(Vec::new())
        }
    }

    pub fn subscribe(&self) -> Subscription<Message> {
        // crossbeam_channel::unbounded() è la versione crossbeam 
        // equivalente a std::sync::mpsc::channel()
        let (tx, rx) = crossbeam_channel::unbounded();
        
        let mut state = self.subscriptions.lock().unwrap();
        state.push(tx);
        
        Subscription::with_receiver(rx)
    }

    pub fn dispatch(&self, msg: Message) {
        let mut state = self.subscriptions.lock().unwrap();
        
        // La logica di retain è IDENTICA a quella di mpsc!
        // Se send() fallisce significa che il Receiver è stato distrutto,
        // quindi retain() rimuove automaticamente il Sender dalla lista.
        state.retain(|tx| tx.send(msg.clone()).is_ok());
    }
}

// Invochiamo i test condivisi!
crate::generate_dispatcher_tests!(crate::dispatcher_crossbeam::Dispatcher<String>);