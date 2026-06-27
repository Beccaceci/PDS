#[macro_export]
macro_rules! generate_dispatcher_tests {
    ($dispatcher_type:path) => {
        #[cfg(test)]
        mod tests {
            use super::*;
            use std::thread;
            use std::time::Duration;

            #[test]
            fn test_pubsub_architecture() {
                let dispatcher = <$dispatcher_type>::new();
                
                // 1. Creiamo due iscritti
                let sub1 = dispatcher.subscribe();
                let sub2 = dispatcher.subscribe();

                // 2. Il dispatcher invia un messaggio
                dispatcher.dispatch("Messaggio 1".to_string());

                // Entrambi dovrebbero riceverlo
                assert_eq!(sub1.read(), Some("Messaggio 1".to_string()));
                assert_eq!(sub2.read(), Some("Messaggio 1".to_string()));

                // 3. Sganciamo il secondo iscritto (droppandolo)
                drop(sub2);

                // 4. Invia a chi è rimasto
                dispatcher.dispatch("Messaggio 2".to_string());
                assert_eq!(sub1.read(), Some("Messaggio 2".to_string()));

                // 5. Distruggiamo il dispatcher
                drop(dispatcher);

                // Le read successive devono restituire None (il dispatcher è morto!)
                assert_eq!(sub1.read(), None);
            }
        }
    };
}
