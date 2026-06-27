use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

pub struct MpMcChannel<E: Send> {
    // MPSC significa "Multi-Producer, SINGLE-Consumer".
    // Quindi il Sender può essere clonato, ma il Receiver no!
    // Per avere consumatori multipli, dobbiamo avvolgere il Receiver in un Mutex.
    sender: Mutex<Option<SyncSender<E>>>,
    receiver: Mutex<Option<Receiver<E>>>,
    closed: std::sync::atomic::AtomicBool,
}

impl<E: Send> MpMcChannel<E> {
    pub fn new(n: usize) -> Self {
        let (tx, rx) = sync_channel(n);
        Self {
            sender: Mutex::new(Some(tx)),
            receiver: Mutex::new(Some(rx)),
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn send(&self, element: E) -> Option<()> {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return None;
        }

        let tx = {
            let guard = self.sender.lock().unwrap();
            if let Some(t) = guard.as_ref() {
                t.clone() // Cloniamo il sender per non tenere il lock
            } else {
                return None;
            }
        };

        // mpsc::SyncSender::send() BLOCCA. Se il canale si riempie, il thread dorme.
        // PROBLEMA: std::sync::mpsc NON ha una macro select!. 
        // Se il thread dorme qui e chiamiamo shutdown(), non si sveglierà finché
        // non facciamo drop(Receiver) in shutdown().
        if tx.send(element).is_ok() {
            Some(())
        } else {
            None
        }
    }

    pub fn recv(&self) -> Option<E> {
        // Usiamo un backoff poll per simulare il recv() senza tenere il Mutex bloccato
        // per sempre, altrimenti shutdown() andrebbe in deadlock.
        loop {
            let mut is_empty = false;
            {
                let guard = self.receiver.lock().unwrap();
                if let Some(rx) = guard.as_ref() {
                    match rx.try_recv() {
                        Ok(val) => return Some(val),
                        Err(std::sync::mpsc::TryRecvError::Empty) => {
                            is_empty = true;
                        }
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            return None;
                        }
                    }
                } else {
                    return None;
                }
            } // Lock rilasciato

            if self.closed.load(std::sync::atomic::Ordering::SeqCst) && is_empty {
                return None;
            }

            // Simuliamo l'attesa. Questo consuma un po' di CPU (polling), che viola i requisiti
            // ma è l'unico modo sicuro per fare MPMC con mpsc standard senza deadlock.
            thread::sleep(Duration::from_millis(5));
        }
    }

    pub fn shutdown(&self) -> Option<()> {
        if self.closed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return None;
        }

        // Droppando il Receiver, sblocchiamo tutti i Sender addormentati!
        // Ma purtroppo perdiamo anche i messaggi non ancora letti. 
        // mpsc standard non è in grado di risolvere questo paradosso per MPMC in modo pulito.
        let mut r_guard = self.receiver.lock().unwrap();
        r_guard.take(); 

        let mut s_guard = self.sender.lock().unwrap();
        s_guard.take();

        Some(())
    }
}

// Invochiamo i test condivisi
// ATTENZIONE: Il Test 1 (drain_after_shutdown) fallirà sistematicamente
// perché mpsc non permette di mantenere il receiver vivo per il drain
// e contemporaneamente svegliare i sender bloccati (Test 3) senza select!.
crate::generate_channel_tests!(crate::mpmc_mpsc::MpMcChannel<i32>);
