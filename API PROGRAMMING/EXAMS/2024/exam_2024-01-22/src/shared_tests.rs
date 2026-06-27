#[macro_export]
macro_rules! generate_multichannel_tests {
    ($ChannelType:ident) => {
        #[cfg(test)]
        mod tests {
            use super::*;

            #[test]
            fn test_single_subscriber() {
                let channel = $ChannelType::new();
                let rx = channel.subscribe();
                
                let res = channel.send(42);
                assert!(res.is_ok(), "L'invio a un singolo iscritto dovrebbe avere successo");
                assert_eq!(rx.recv().unwrap(), 42, "Il ricevitore dovrebbe ricevere il byte");
            }

            #[test]
            fn test_multiple_subscribers() {
                let channel = $ChannelType::new();
                let rx1 = channel.subscribe();
                let rx2 = channel.subscribe();
                let rx3 = channel.subscribe();
                
                let res = channel.send(100);
                assert!(res.is_ok(), "L'invio a più iscritti dovrebbe avere successo");
                
                assert_eq!(rx1.recv().unwrap(), 100, "Ricevitore 1 deve ricevere");
                assert_eq!(rx2.recv().unwrap(), 100, "Ricevitore 2 deve ricevere");
                assert_eq!(rx3.recv().unwrap(), 100, "Ricevitore 3 deve ricevere");
            }

            #[test]
            fn test_send_with_zero_subscribers_fails() {
                let channel = $ChannelType::new();
                
                let res = channel.send(55);
                assert!(res.is_err(), "Se non c'è mai stato alcun iscritto, deve ritornare Err");
            }

            #[test]
            fn test_drop_one_subscriber_keeps_others_alive() {
                let channel = $ChannelType::new();
                let rx1 = channel.subscribe();
                let rx2 = channel.subscribe();
                
                // Droppiamo il primo ricevitore
                drop(rx1);
                
                // Il canale deve continuare a funzionare inviando ai restanti
                let res = channel.send(77);
                assert!(res.is_ok(), "L'invio dovrebbe avere successo perché c'è ancora un ricevitore attivo");
                
                assert_eq!(rx2.recv().unwrap(), 77, "Il ricevitore superstite deve ricevere il dato");
            }

            #[test]
            fn test_drop_all_subscribers_returns_error() {
                let channel = $ChannelType::new();
                let rx1 = channel.subscribe();
                let rx2 = channel.subscribe();
                
                // Droppiamo TUTTI i ricevitori
                drop(rx1);
                drop(rx2);
                
                // Il canale ora non ha più iscritti vivi. 
                // Il testo dice: "altrimenti ritornera' un errore"
                let res = channel.send(88);
                assert!(
                    res.is_err(), 
                    "Se TUTTI i ricevitori sono stati eliminati, l'invio deve fallire"
                );
            }

            #[test]
            fn test_cleanup_of_dead_subscribers() {
                let channel = $ChannelType::new();
                let rx1 = channel.subscribe();
                
                // Verifica che ci sia 1 iscritto
                assert_eq!(channel.get_num_subscribers(), 1);
                
                // Droppiamo il ricevitore
                drop(rx1);
                
                // Un tentativo di invio a un ricevitore morto scatenerà il cleanup tramite retain
                let _ = channel.send(99);
                
                // La lunghezza FISICA del vettore dovrebbe scendere a 0 grazie al retain!
                assert_eq!(
                    channel.get_num_subscribers(), 
                    0, 
                    "ERRORE GRAVE DI MEMORIA: Il vettore interno non è stato ripulito dai Sender morti!"
                );
            }
        }
    };
}
