#[macro_export]
macro_rules! generate_channel_tests {
    ($channel_type:path) => {
        #[cfg(test)]
        mod tests {
            use super::*;
            use std::sync::Arc;
            use std::thread;
            use std::time::Duration;

            #[test]
            fn test_1_drain_after_shutdown() {
                let channel = <$channel_type>::new(5);
                
                channel.send(10).unwrap();
                channel.send(20).unwrap();
                channel.send(30).unwrap();

                channel.shutdown().unwrap();

                assert_eq!(channel.recv(), Some(10), "Errore: Hai perso il primo messaggio dopo lo shutdown!");
                assert_eq!(channel.recv(), Some(20), "Errore: Hai perso il secondo messaggio dopo lo shutdown!");
                assert_eq!(channel.recv(), Some(30), "Errore: Hai perso il terzo messaggio dopo lo shutdown!");
                
                assert_eq!(channel.recv(), None);
            }

            #[test]
            fn test_2_lost_wakeup_deadlock() {
                let channel = Arc::new(<$channel_type>::new(1));
                
                let mut producers = vec![];
                for i in 0..3 {
                    let ch = Arc::clone(&channel);
                    producers.push(thread::spawn(move || {
                        for j in 0..100 {
                            ch.send(i * 100 + j);
                        }
                    }));
                }

                let mut consumers = vec![];
                for _ in 0..3 {
                    let ch = Arc::clone(&channel);
                    consumers.push(thread::spawn(move || {
                        let mut count = 0;
                        while let Some(_) = ch.recv() {
                            count += 1;
                            if count == 100 { break; }
                        }
                    }));
                }

                for p in producers { p.join().unwrap(); }
                channel.shutdown();
                for c in consumers { c.join().unwrap(); }
            }

            #[test]
            fn test_3_shutdown_wakes_senders() {
                // Testiamo se un sender bloccato su un canale pieno viene 
                // svegliato correttamente (e ritorna None) quando si fa shutdown.
                let channel = Arc::new(MpMcChannel::<i32>::new(1));
                channel.send(99).unwrap(); // Riempiamo il canale

                let ch_clone = Arc::clone(&channel);
                let handle = thread::spawn(move || {
                    // Questo dovrebbe bloccarsi finché non chiamiamo shutdown
                    ch_clone.send(100)
                });

                thread::sleep(Duration::from_millis(50));
                channel.shutdown();

                // Se shutdown è implementato correttamente, il thread non è in deadlock e ritornerà None.
                assert_eq!(handle.join().unwrap(), None);
            }
        }
    };
}
