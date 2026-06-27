use crossbeam::channel::{self, Receiver};
use std::sync::Mutex;

/// Implementazione di MultiChannel basata sull'alta performance di `crossbeam-channel`.
pub struct MultiChannelCrossbeam {
    // Usiamo il Sender di crossbeam invece di quello std::sync::mpsc
    senders: Mutex<Vec<channel::Sender<u8>>>
}

impl MultiChannelCrossbeam {
    pub fn new() -> Self {
        Self {
            senders: Mutex::new(Vec::new())
        }
    }

    pub fn subscribe(&self) -> Receiver<u8> {
        // Creiamo un canale unbounded (illimitato) come richiesto dal comportamento di mpsc::channel()
        let (tx, rx) = channel::unbounded::<u8>();
        let mut state = self.senders.lock().unwrap();
        state.push(tx);
        rx
    }

    pub fn send(&self, data: u8) -> Result<(), ()> {
        let mut state = self.senders.lock().unwrap();
        
        // Puliamo i sender morti iterando.
        // Identico alla versione mpsc, ma con i canali crossbeam che sono più veloci.
        state.retain(|sender| {
            sender.send(data).is_ok()
        });
        
        if state.is_empty() {
            Err(())
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    pub fn get_num_subscribers(&self) -> usize {
        self.senders.lock().unwrap().len()
    }
}

// Generiamo il test suite anche per crossbeam!
generate_multichannel_tests!(MultiChannelCrossbeam);
