// Questa macro genera un'intera suite di test per una specifica implementazione del canale!
#[macro_export]
macro_rules! generate_channel_tests {
    ($factory:path) => {
        #[cfg(test)]
        mod channel_tests {
            use super::*;


            #[test]
            fn test_basic_send_receive() {
                let (tx, rx) = $factory();
                let handle = tx.send(42);
                assert!(handle.is_some(), "Il send dovrebbe avere successo");
                let val = rx.recv();
                assert_eq!(val, Some(42), "Il ricevitore dovrebbe ricevere il messaggio");
            }

            #[test]
            fn test_forget_before_recv() {
                let (tx, rx) = $factory();
                let handle = tx.send(100).unwrap();
                let forgot = handle.forget();
                assert!(forgot, "forget() dovrebbe restituire true se il messaggio era in coda");
                tx.send(200).unwrap();
                let val = rx.recv();
                assert_eq!(val, Some(200), "Il messaggio 100 doveva essere scartato silenziosamente");
            }

            #[test]
            fn test_forget_after_recv() {
                let (tx, rx) = $factory();
                let handle = tx.send(300).unwrap();
                let val = rx.recv();
                assert_eq!(val, Some(300));
                let forgot = handle.forget();
                assert!(!forgot, "forget() dovrebbe restituire false se il messaggio è già stato elaborato");
            }

            #[test]
            fn test_multiple_senders() {
                let (tx1, rx) = $factory();
                let tx2 = tx1.clone();
                tx1.send("A");
                tx2.send("B");
                let v1 = rx.recv().unwrap();
                let v2 = rx.recv().unwrap();
                assert!((v1 == "A" && v2 == "B") || (v1 == "B" && v2 == "A"));
            }

            #[test]
            fn test_receiver_disconnect() {
                let (tx, rx) = $factory();
                drop(rx);
                let handle = tx.send(10);
                assert!(handle.is_none(), "send dovrebbe restituire None se il ricevitore non esiste più");
            }

            #[test]
            fn test_sender_disconnect() {
                let (tx, rx) = $factory();
                tx.send(1).unwrap();
                drop(tx);
                assert_eq!(rx.recv(), Some(1));
                assert_eq!(rx.recv(), None, "recv dovrebbe restituire None se il canale è chiuso e vuoto");
            }

            #[test]
            fn test_forget_mixed_messages() {
                let (tx, rx) = $factory();
                let _h1 = tx.send(1).unwrap();
                let h2 = tx.send(2).unwrap();
                let _h3 = tx.send(3).unwrap();
                assert!(h2.forget(), "Il messaggio 2 dovrebbe essere stato annullato");
                assert_eq!(rx.recv(), Some(1));
                assert_eq!(rx.recv(), Some(3), "Il ricevitore deve aver saltato il messaggio 2");
            }

            #[test]
            fn test_forget_after_channel_closed() {
                let (tx, rx) = $factory();
                let handle = tx.send(99).unwrap();
                drop(rx);
                let forgot = handle.forget();
                assert!(forgot, "forget() deve restituire true se il canale è stato chiuso prima della ricezione");
            }
        }
    };
}