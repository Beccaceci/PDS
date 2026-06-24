#![allow(dead_code)]
//! Esercizio 1 - AsymmetricRendezvous
//!
//! Primitiva di sincronizzazione mono-uso che fa incontrare due thread con
//! ruoli diversi, scambiando dati di tipo `Req` in una direzione e `Resp`
//! nell'altra.
//!
//! Vedi il file `Lab7.md` per la specifica completa.

use std::marker::PhantomData;

/// Estremo "cliente" del rendezvous. Possiede il metodo per inviare la richiesta
/// e ricevere la risposta. Mono-uso: il metodo consuma `self`.
pub struct ClientEnd<Req: Send, Resp: Send> {
    _marker: PhantomData<(Req, Resp)>,
}

/// Estremo "servitore" del rendezvous. Possiede il metodo per ricevere la
/// richiesta, elaborarla con un handler e restituire la risposta al cliente.
pub struct ServerEnd<Req: Send, Resp: Send> {
    _marker: PhantomData<(Req, Resp)>,
}

/// Crea una nuova coppia di estremi di rendezvous.
pub fn make_rendezvous<Req: Send, Resp: Send>() -> (ClientEnd<Req, Resp>, ServerEnd<Req, Resp>) {
    todo!("Implementare make_rendezvous")
}

impl<Req: Send, Resp: Send> ClientEnd<Req, Resp> {
    /// Consegna `req` al lato servente e attende bloccandosi (senza consumare
    /// CPU) fino a che il servente non produce la risposta corrispondente.
    /// Restituisce `Some(resp)` in caso di successo, `None` se il `ServerEnd`
    /// è stato distrutto senza aver invocato `accept`.
    pub fn request(self, _req: Req) -> Option<Resp> {
        todo!("Implementare ClientEnd::request")
    }
}

impl<Req: Send, Resp: Send> ServerEnd<Req, Resp> {
    /// Attende bloccandosi (senza consumare CPU) la richiesta del cliente,
    /// la elabora con `handler` e consegna il valore restituito al cliente.
    /// Restituisce `Some(())` in caso di successo, `None` se il `ClientEnd`
    /// è stato distrutto senza aver invocato `request`.
    pub fn accept<F>(self, _handler: F) -> Option<()>
    where
        F: FnOnce(Req) -> Resp,
    {
        todo!("Implementare ServerEnd::accept")
    }
}

impl<Req: Send, Resp: Send> Drop for ClientEnd<Req, Resp> {
    fn drop(&mut self) {
        todo!("Implementare Drop per ClientEnd")
    }
}

impl<Req: Send, Resp: Send> Drop for ServerEnd<Req, Resp> {
    fn drop(&mut self) {
        todo!("Implementare Drop per ServerEnd")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    fn run_with_timeout<F>(timeout: Duration, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let handle = thread::spawn(f);
        let start = Instant::now();
        while start.elapsed() < timeout {
            if handle.is_finished() {
                handle.join().unwrap();
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("test non terminato entro {:?} (probabile deadlock)", timeout);
    }

    #[test]
    fn rendezvous_richiesta_e_risposta_base() {
        run_with_timeout(Duration::from_secs(5), || {
            let (client, server) = make_rendezvous::<i32, String>();

            let server_handle = thread::spawn(move || {
                server.accept(|n| format!("ricevuto {n}"))
            });

            let resp = client.request(42);
            assert_eq!(resp, Some("ricevuto 42".to_string()));
            assert_eq!(server_handle.join().unwrap(), Some(()));
        });
    }

    #[test]
    fn rendezvous_tipi_asimmetrici() {
        // Verifica che i tipi delle due direzioni siano effettivamente diversi.
        run_with_timeout(Duration::from_secs(5), || {
            let (client, server) = make_rendezvous::<Vec<u8>, (usize, u32)>();
            let server_handle = thread::spawn(move || {
                server.accept(|bytes: Vec<u8>| {
                    let len = bytes.len();
                    let sum = bytes.iter().map(|b| *b as u32).sum();
                    (len, sum)
                })
            });
            let resp = client.request(vec![1u8, 2, 3, 4]);
            assert_eq!(resp, Some((4usize, 10u32)));
            server_handle.join().unwrap();
        });
    }

    #[test]
    fn rendezvous_client_arriva_prima() {
        // Il cliente invoca `request` prima che il servente sia pronto: deve
        // bloccarsi (senza CPU) fino a quando arriva il servente.
        run_with_timeout(Duration::from_secs(5), || {
            let (client, server) = make_rendezvous::<i32, i32>();
            let sbloccato = Arc::new(AtomicBool::new(false));

            let sb = Arc::clone(&sbloccato);
            let client_handle = thread::spawn(move || {
                let r = client.request(10);
                sb.store(true, Ordering::SeqCst);
                r
            });

            thread::sleep(Duration::from_millis(150));
            assert!(
                !sbloccato.load(Ordering::SeqCst),
                "il client doveva essere bloccato in attesa del server"
            );

            let server_handle = thread::spawn(move || server.accept(|x| x * 2));
            let resp = client_handle.join().unwrap();
            assert_eq!(resp, Some(20));
            assert_eq!(server_handle.join().unwrap(), Some(()));
        });
    }

    #[test]
    fn rendezvous_server_arriva_prima() {
        // Il servente invoca `accept` prima che il cliente sia pronto: deve
        // bloccarsi (senza CPU) fino a quando arriva il cliente.
        run_with_timeout(Duration::from_secs(5), || {
            let (client, server) = make_rendezvous::<i32, i32>();
            let sbloccato = Arc::new(AtomicBool::new(false));

            let sb = Arc::clone(&sbloccato);
            let server_handle = thread::spawn(move || {
                let r = server.accept(|x| x + 1);
                sb.store(true, Ordering::SeqCst);
                r
            });

            thread::sleep(Duration::from_millis(150));
            assert!(
                !sbloccato.load(Ordering::SeqCst),
                "il server doveva essere bloccato in attesa del client"
            );

            let client_handle = thread::spawn(move || client.request(99));
            assert_eq!(client_handle.join().unwrap(), Some(100));
            assert_eq!(server_handle.join().unwrap(), Some(()));
        });
    }

    #[test]
    fn rendezvous_drop_del_server_sblocca_il_client() {
        // Se il server viene distrutto senza invocare `accept`, il client
        // bloccato su `request` deve sbloccarsi restituendo `None`.
        run_with_timeout(Duration::from_secs(5), || {
            let (client, server) = make_rendezvous::<i32, i32>();
            let client_handle = thread::spawn(move || client.request(7));
            thread::sleep(Duration::from_millis(150));
            drop(server);
            let resp = client_handle.join().unwrap();
            assert_eq!(resp, None);
        });
    }

    #[test]
    fn rendezvous_drop_del_client_sblocca_il_server() {
        // Se il client viene distrutto senza invocare `request`, il server
        // bloccato su `accept` deve sbloccarsi restituendo `None`.
        run_with_timeout(Duration::from_secs(5), || {
            let (client, server) = make_rendezvous::<i32, i32>();
            let server_handle = thread::spawn(move || server.accept(|x| x + 1));
            thread::sleep(Duration::from_millis(150));
            drop(client);
            let resp = server_handle.join().unwrap();
            assert_eq!(resp, None);
        });
    }

    #[test]
    fn rendezvous_drop_immediato_di_entrambi_non_blocca() {
        run_with_timeout(Duration::from_secs(2), || {
            for _ in 0..100 {
                let (client, server) = make_rendezvous::<i32, i32>();
                drop(client);
                drop(server);
            }
        });
    }

    #[test]
    fn rendezvous_handler_non_invocato_se_client_droppa_prima() {
        // Se il client droppa senza chiamare `request`, il server deve
        // sbloccarsi senza invocare l'handler.
        run_with_timeout(Duration::from_secs(5), || {
            let (client, server) = make_rendezvous::<i32, i32>();
            let chiamato = Arc::new(AtomicBool::new(false));

            let ch = Arc::clone(&chiamato);
            let server_handle = thread::spawn(move || {
                server.accept(move |x| {
                    ch.store(true, Ordering::SeqCst);
                    x
                })
            });

            thread::sleep(Duration::from_millis(150));
            drop(client);

            assert_eq!(server_handle.join().unwrap(), None);
            assert!(
                !chiamato.load(Ordering::SeqCst),
                "l'handler non doveva essere invocato"
            );
        });
    }

    #[test]
    fn ends_sono_send() {
        // I due estremi devono essere spostabili tra thread (e' come vengono
        // tipicamente utilizzati: un estremo per il thread "client" e uno per
        // il thread "server").
        fn assert_send<T: Send>() {}
        assert_send::<ClientEnd<i32, String>>();
        assert_send::<ServerEnd<Vec<u8>, (usize, u32)>>();
    }

    #[test]
    fn rendezvous_molte_coppie_indipendenti() {
        // Stress test: molte coppie indipendenti, ognuna usata correttamente.
        run_with_timeout(Duration::from_secs(15), || {
            let n = 200;
            let mut handles = Vec::with_capacity(n);
            for i in 0..n {
                let (client, server) = make_rendezvous::<i32, i32>();
                let server_handle = thread::spawn(move || server.accept(|x| x * x));
                let client_handle = thread::spawn(move || client.request(i as i32));
                handles.push((i, client_handle, server_handle));
            }
            for (i, ch, sh) in handles {
                assert_eq!(ch.join().unwrap(), Some((i as i32) * (i as i32)));
                assert_eq!(sh.join().unwrap(), Some(()));
            }
        });
    }
}
