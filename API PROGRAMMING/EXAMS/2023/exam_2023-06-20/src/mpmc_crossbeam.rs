use crossbeam::channel::{bounded, Receiver, Sender};
use std::sync::Mutex;

pub struct MpMcChannel<E: Send> {
    sender: Sender<E>,
    receiver: Receiver<E>,
    // Usiamo un canale aggiuntivo di capacità 0 come "segnale" di chiusura.
    // Nessuno manderà mai messaggi qui. La chiusura avviene droppando il sender.
    shutdown_rx: Receiver<()>,
    shutdown_tx: Mutex<Option<Sender<()>>>,
}

impl<E: Send> MpMcChannel<E> {
    pub fn new(n: usize) -> Self {
        let (tx, rx) = bounded(n);
        let (shut_tx, shut_rx) = bounded(0);
        Self {
            sender: tx,
            receiver: rx,
            shutdown_rx: shut_rx,
            shutdown_tx: Mutex::new(Some(shut_tx)),
        }
    }

    pub fn send(&self, element: E) -> Option<()> {
        // La macro select! di crossbeam permette di bloccare il thread
        // su PIU' canali contemporaneamente!
        crossbeam::select! {
            send(self.sender, element) -> res => {
                if res.is_ok() { Some(()) } else { None }
            }
            recv(self.shutdown_rx) -> _ => {
                // Se shutdown_rx si sblocca, significa che shutdown_tx è stato droppato.
                // Usciamo dal blocco!
                None
            }
        }
    }

    pub fn recv(&self) -> Option<E> {
        crossbeam::select! {
            recv(self.receiver) -> res => {
                res.ok()
            }
            recv(self.shutdown_rx) -> _ => {
                // Abbiamo ricevuto il segnale di shutdown.
                // Il canale è chiuso, MA proviamo ad estrarre eventuali messaggi rimasti
                // usando try_recv (che non blocca).
                self.receiver.try_recv().ok()
            }
        }
    }

    pub fn shutdown(&self) -> Option<()> {
        // Rimuoviamo e droppiamo l'unico shutdown_tx esistente.
        // Questo chiude il canale shutdown_rx, il che causa lo sblocco immediato
        // di TUTTI i select! in attesa su shutdown_rx.
        let mut guard = self.shutdown_tx.lock().unwrap();
        if guard.take().is_some() {
            Some(())
        } else {
            None // Già chiuso
        }
    }
}

// Invochiamo i test condivisi per provare l'efficacia di questa architettura
crate::generate_channel_tests!(crate::mpmc_crossbeam::MpMcChannel<i32>);