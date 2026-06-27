use std::sync::{Mutex, mpsc::{self, Receiver, SendError}};



pub struct MultiChannelMpsc {
    senders: Mutex<Vec<mpsc::Sender<u8>>>
}
impl MultiChannelMpsc {
    pub fn new () -> Self {
        Self {
            senders: Mutex::new(Vec::new())
        }
    }

    pub fn subscribe (&self) -> Receiver<u8> {
        let (tx, rx) = mpsc::channel::<u8>();
        let mut state = self.senders.lock().unwrap();
        state.push(tx);
        rx
    }

    pub fn send (&self, _data: u8) -> Result<(), SendError<u8>> {
        // 1. Prendi il lock
        let mut state = self.senders.lock().unwrap();
        // 2. Iteri, invii, e pulisci in un solo colpo!
        state.retain(|sender| {
            // Questo esegue l'invio. 
            // .is_ok() ritornerà true se l'invio ha successo (e il sender viene tenuto).
            // .is_ok() ritornerà false se l'invio fallisce (e il sender viene ELIMINATO!).
            sender.send(_data).is_ok()
        });
        // 3. Controlli se dopo la pulizia il vettore è vuoto
        if state.is_empty() {
            Err(SendError(_data))
        } else {
            Ok(())
        }
    }
    #[cfg(test)]
    pub fn get_num_subscribers(&self) -> usize {
        self.senders.lock().unwrap().len()
    }
}

generate_multichannel_tests!(MultiChannelMpsc);