use super::error::ServerError;

use super::state::*;
use crate::{
    ClientMessage::{self, Login, Register, SendText, UpdatePosition},
    ServerMessage,
};
use tokio::io::{self, AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::watch;

/// Serializes and sends a ServerMessage over an asynchronous writer as a newline-delimited JSON string.
async fn send_server_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    msg: &ServerMessage,
) -> io::Result<()> {
    let mut json_str = serde_json::to_string(msg)?;
    json_str.push('\n');

    writer.write_all(json_str.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

/// Validates that the current session is authenticated, returning the username slice or an error message.
fn require_auth<'a>(user: &'a Option<String>) -> Result<&'a str, ServerMessage> {
    match user.as_deref() {
        Some(username) => Ok(username),
        None => Err(ServerMessage::ErrorMessage(
            super::error::ServerError::Unauthenticated.to_string(),
        )),
    }
}

/// Handles Register and Login authentication actions uniformly.
async fn handle_auth_action<W, Fut>(
    authenticated_user: &mut Option<String>,
    client_rx: &mut Option<tokio::sync::mpsc::Receiver<ServerMessage>>,
    write_half: &mut W,
    username: String,
    action: Fut,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    Fut: std::future::Future<
            Output = Result<tokio::sync::mpsc::Receiver<ServerMessage>, ServerError>,
        >,
{
    if authenticated_user.is_some() {
        let err_msg = ServerMessage::ErrorMessage(ServerError::AlreadyAuthenticated.to_string());
        send_server_message(write_half, &err_msg).await?;
        return Ok(());
    }

    let result = action
        .await
        .map(|rx| {
            *client_rx = Some(rx);
            *authenticated_user = Some(username);
        })
        .map_err(|error| error.to_string());

    let server_response = ServerMessage::AuthResult(result);
    send_server_message(write_half, &server_response).await?;
    Ok(())
}

/// Executes an action requiring authentication and transmits the resulting response.
async fn handle_authenticated_action<W, F, Fut>(
    authenticated_user: &Option<String>,
    write_half: &mut W,
    action: F,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Option<ServerMessage>>,
{
    let username = match require_auth(authenticated_user) {
        Ok(u) => u,
        Err(err_msg) => {
            send_server_message(write_half, &err_msg).await?;
            return Ok(());
        }
    };

    if let Some(response) = action(username.to_string()).await {
        send_server_message(write_half, &response).await?;
    }
    Ok(())
}

/// Runs the client connection event loop on the specified socket.
pub async fn run_client_session<S>(
    socket: S,
    state: &ServerState,
    authenticated_user: &mut Option<String>,
    shutdown: &mut watch::Receiver<bool>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Split the stream into generic read and write halves
    let (read_half, mut write_half) = tokio::io::split(socket);
    let mut reader = tokio::io::BufReader::new(read_half);

    let mut client_rx: Option<tokio::sync::mpsc::Receiver<ServerMessage>> = None;
    let mut line = String::new();

    loop {
        line.clear();

        tokio::select! {
            // Branch 1: Process incoming data from the stream
            bytes_res = reader.read_line(&mut line) => {
                let bytes = bytes_res?;

                if bytes == 0 {
                    // Client closed the connection (EOF).
                    return Ok(());
                }

                // Deserialize JSON line into a ClientMessage enum early
                let client_msg = match serde_json::from_str::<ClientMessage>(&line) {
                    Ok(msg) => msg,
                    Err(err) => {
                        let err_msg = ServerMessage::ErrorMessage(format!("Invalid JSON payload format: {}", err));
                        send_server_message(&mut write_half, &err_msg).await?;
                        continue;
                    }
                };

                match client_msg {
                    Register { username, password } => {
                        let state = state.clone();
                        let u = username.clone();
                        handle_auth_action(
                            authenticated_user,
                            &mut client_rx,
                            &mut write_half,
                            username,
                            async move { state.register_user(&u, &password).await },
                        )
                        .await?;
                    }
                    Login { username, password } => {
                        let state = state.clone();
                        let u = username.clone();
                        handle_auth_action(
                            authenticated_user,
                            &mut client_rx,
                            &mut write_half,
                            username,
                            async move { state.user_login(&u, &password).await },
                        )
                        .await?;
                    }
                    UpdatePosition(position) => {
                        handle_authenticated_action(
                            &authenticated_user,
                            &mut write_half,
                            move |usr| async move {
                                state.update_user_position(&usr, &position)
                                    .await
                                    .err()
                                    .map(|error| ServerMessage::ErrorMessage(error.to_string()))
                            },
                        )
                        .await?;
                    }
                    SendText { content } => {
                        handle_authenticated_action(
                            &authenticated_user,
                            &mut write_half,
                            move |usr| async move {
                                state.process_message(&usr, &content)
                                    .await
                                    .err()
                                    .map(|error| ServerMessage::ErrorMessage(error.to_string()))
                            },
                        )
                        .await?;
                    }
                }
            }

            // Branch 2: Receive and flush outgoing messages from the MPSC channel (Direct / Broadcast)
            Some(msg) = async {
                match client_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                send_server_message(&mut write_half, &msg).await?;
            }

            // Branch 3: Finish the current action, then close this session when
            // the owning server requests shutdown or drops the signal sender.
            _ = shutdown.changed() => return Ok(()),
        }
    }
}

/// Handles the full lifecycle of a single connected client.
pub async fn handle_client<S>(
    socket: S,
    state: ServerState,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut authenticated_user = None;
    let result =
        run_client_session(socket, &state, &mut authenticated_user, &mut shutdown).await;

    // The session worker returns on EOF and on every read/write failure.
    if let Some(username) = authenticated_user.as_deref() {
        state.remove_active_client(username).await;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::super::error::ServerError;
    use super::super::state::tests::{VALID_PASSWORD, state_with_users};
    use super::*;
    use crate::{Position, UserState};
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::time::{Duration, timeout};
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadBuf},
        task::JoinHandle,
    };

    type ClientStream = tokio::io::DuplexStream;
    type ClientReader = BufReader<tokio::io::ReadHalf<ClientStream>>;
    type ClientWriter = tokio::io::WriteHalf<ClientStream>;

    fn start_session(
        state: ServerState,
    ) -> (JoinHandle<io::Result<()>>, ClientReader, ClientWriter) {
        let (server, client) = tokio::io::duplex(1024);
        let (shutdown_guard, shutdown) = watch::channel(false);
        let task = tokio::spawn(async move {
            let _shutdown_guard = shutdown_guard;
            handle_client(server, state, shutdown).await
        });
        let (read_half, write_half) = tokio::io::split(client);
        (task, BufReader::new(read_half), write_half)
    }

    async fn close_session(mut writer: ClientWriter, task: JoinHandle<io::Result<()>>) {
        writer.shutdown().await.unwrap();
        assert!(task.await.unwrap().is_ok());
    }

    async fn read_message(reader: &mut ClientReader) -> ServerMessage {
        let mut line = String::new();
        timeout(Duration::from_secs(5), reader.read_line(&mut line))
            .await
            .expect("server did not respond")
            .expect("could not read server response");
        serde_json::from_str(&line).expect("server response must be JSON")
    }

    async fn log_in(
        username: &str,
        reader: &mut ClientReader,
        writer: &mut ClientWriter,
    ) -> ServerMessage {
        send_client(
            writer,
            &ClientMessage::Login {
                username: username.to_string(),
                password: VALID_PASSWORD.to_string(),
            },
        )
        .await;

        read_message(reader).await
    }

    async fn send_client<W: AsyncWrite + Unpin>(writer: &mut W, message: &ClientMessage) {
        let json = serde_json::to_string(message).unwrap();
        writer
            .write_all(format!("{json}\n").as_bytes())
            .await
            .unwrap();
    }

    async fn register(
        username: &str,
        reader: &mut ClientReader,
        writer: &mut ClientWriter,
    ) -> ServerMessage {
        send_client(
            writer,
            &ClientMessage::Register {
                username: username.to_string(),
                password: VALID_PASSWORD.to_string(),
            },
        )
        .await;

        read_message(reader).await
    }

    struct WriteFails {
        stream: tokio::io::DuplexStream,
    }

    impl AsyncRead for WriteFails {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.stream).poll_read(cx, buffer)
        }
    }

    impl AsyncWrite for WriteFails {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "write failed",
            )))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "flush failed",
            )))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn rejects_unauthenticated_position() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (task, mut reader, mut write_half) = start_session(state.clone());

        send_client(
            &mut write_half,
            &ClientMessage::UpdatePosition(Position {
                latitude: 45.0,
                longitude: 9.0,
                timestamp: 1,
            }),
        )
        .await;
        assert_eq!(
            read_message(&mut reader).await,
            ServerMessage::ErrorMessage(ServerError::Unauthenticated.to_string())
        );

        close_session(write_half, task).await;
        assert!(state.registry.read().await.active_clients.is_empty());
    }

    #[tokio::test]
    async fn rejects_unauthenticated_text() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (task, mut reader, mut write_half) = start_session(state);

        send_client(
            &mut write_half,
            &ClientMessage::SendText {
                content: "ready".to_string(),
            },
        )
        .await;
        assert_eq!(
            read_message(&mut reader).await,
            ServerMessage::ErrorMessage(ServerError::Unauthenticated.to_string())
        );

        close_session(write_half, task).await;
    }

    #[tokio::test]
    async fn rejects_bad_login() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (task, mut reader, mut write_half) = start_session(state.clone());

        assert_eq!(
            log_in("invalid", &mut reader, &mut write_half).await,
            ServerMessage::AuthResult(Err(ServerError::InvalidCredentials.to_string()))
        );

        close_session(write_half, task).await;
        assert!(state.registry.read().await.active_clients.is_empty());
    }

    #[tokio::test]
    async fn registers_session() {
        let (state, _db) = state_with_users(&[]);
        let (task, mut reader, mut write_half) = start_session(state.clone());

        assert_eq!(
            register("driver17", &mut reader, &mut write_half).await,
            ServerMessage::AuthResult(Ok(()))
        );
        assert!(
            state
                .registry
                .read()
                .await
                .active_clients
                .contains_key("driver17")
        );

        close_session(write_half, task).await;
    }

    #[tokio::test]
    async fn updates_position() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (task, mut reader, mut write_half) = start_session(state.clone());
        assert_eq!(
            log_in("driver17", &mut reader, &mut write_half).await,
            ServerMessage::AuthResult(Ok(()))
        );

        let position = Position {
            latitude: 45.0,
            longitude: 9.0,
            timestamp: 100,
        };
        send_client(&mut write_half, &ClientMessage::UpdatePosition(position)).await;
        close_session(write_half, task).await;

        assert_eq!(
            state.registry.read().await.map["driver17"].positions,
            vec![position]
        );
    }

    #[tokio::test]
    async fn relays_driver_text() {
        let (state, _db) = state_with_users(&["driver17"]);
        let mut driver_messages = state.driver_message_sender.subscribe();
        let (task, mut reader, mut write_half) = start_session(state);
        assert_eq!(
            log_in("driver17", &mut reader, &mut write_half).await,
            ServerMessage::AuthResult(Ok(()))
        );

        send_client(
            &mut write_half,
            &ClientMessage::SendText {
                content: "at the depot".to_string(),
            },
        )
        .await;
        assert_eq!(
            timeout(Duration::from_secs(1), driver_messages.recv())
                .await
                .unwrap()
                .unwrap(),
            ("driver17".to_string(), "at the depot".to_string())
        );

        close_session(write_half, task).await;
    }

    #[tokio::test]
    async fn rejects_empty_text() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (task, mut reader, mut write_half) = start_session(state);
        assert_eq!(
            log_in("driver17", &mut reader, &mut write_half).await,
            ServerMessage::AuthResult(Ok(()))
        );

        send_client(
            &mut write_half,
            &ClientMessage::SendText {
                content: " \t".to_string(),
            },
        )
        .await;
        assert_eq!(
            read_message(&mut reader).await,
            ServerMessage::ErrorMessage(ServerError::EmptyMessage.to_string())
        );

        close_session(write_half, task).await;
    }

    #[tokio::test]
    async fn rejects_out_of_order_position() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (task, mut reader, mut write_half) = start_session(state);
        assert_eq!(
            log_in("driver17", &mut reader, &mut write_half).await,
            ServerMessage::AuthResult(Ok(()))
        );

        for position in [
            Position {
                latitude: 45.0,
                longitude: 9.0,
                timestamp: 100,
            },
            Position {
                latitude: 45.1,
                longitude: 9.1,
                timestamp: 99,
            },
        ] {
            send_client(&mut write_half, &ClientMessage::UpdatePosition(position)).await;
        }
        assert_eq!(
            read_message(&mut reader).await,
            ServerMessage::ErrorMessage(ServerError::OutOfOrderPosition.to_string())
        );

        close_session(write_half, task).await;
    }

    #[tokio::test]
    async fn delivers_direct_message() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (task, mut reader, mut write_half) = start_session(state.clone());

        assert_eq!(
            log_in("driver17", &mut reader, &mut write_half).await,
            ServerMessage::AuthResult(Ok(()))
        );

        state
            .send_admin_direct("driver17", "return to depot")
            .await
            .unwrap();
        assert_eq!(
            read_message(&mut reader).await,
            ServerMessage::TextMessage {
                sender: "FleetAdmin".to_string(),
                content: "return to depot".to_string()
            }
        );

        close_session(write_half, task).await;
    }

    #[tokio::test]
    async fn broadcasts_to_session() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (task, mut reader, mut write_half) = start_session(state.clone());

        assert_eq!(
            log_in("driver17", &mut reader, &mut write_half).await,
            ServerMessage::AuthResult(Ok(()))
        );
        assert_eq!(state.send_admin_broadcast("road closed").await, 1);
        assert_eq!(
            read_message(&mut reader).await,
            ServerMessage::TextMessage {
                sender: "FleetAdmin".to_string(),
                content: "road closed".to_string(),
            }
        );

        close_session(write_half, task).await;
    }

    #[tokio::test]
    async fn rejects_repeat_login() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (task, mut reader, mut write_half) = start_session(state);

        assert_eq!(
            log_in("driver17", &mut reader, &mut write_half).await,
            ServerMessage::AuthResult(Ok(()))
        );

        assert_eq!(
            log_in("driver17", &mut reader, &mut write_half).await,
            ServerMessage::ErrorMessage(ServerError::AlreadyAuthenticated.to_string())
        );

        close_session(write_half, task).await;
    }

    #[tokio::test]
    async fn rejects_duplicate_registration() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (task, mut reader, mut write_half) = start_session(state.clone());

        send_client(
            &mut write_half,
            &ClientMessage::Register {
                username: "driver17".to_string(),
                password: VALID_PASSWORD.to_string(),
            },
        )
        .await;
        assert_eq!(
            read_message(&mut reader).await,
            ServerMessage::AuthResult(Err(ServerError::UsernameTaken.to_string()))
        );
        assert!(state.registry.read().await.active_clients.is_empty());

        close_session(write_half, task).await;
    }

    #[tokio::test]
    async fn rejects_second_login() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (first_task, mut first_reader, mut first_write_half) = start_session(state.clone());
        assert_eq!(
            log_in("driver17", &mut first_reader, &mut first_write_half).await,
            ServerMessage::AuthResult(Ok(()))
        );

        let (second_task, mut second_reader, mut second_write_half) = start_session(state.clone());
        assert_eq!(
            log_in("driver17", &mut second_reader, &mut second_write_half).await,
            ServerMessage::AuthResult(Err(ServerError::AlreadyLoggedIn.to_string()))
        );

        close_session(first_write_half, first_task).await;

        assert_eq!(
            log_in("driver17", &mut second_reader, &mut second_write_half).await,
            ServerMessage::AuthResult(Ok(()))
        );

        close_session(second_write_half, second_task).await;

        assert!(state.registry.read().await.active_clients.is_empty());
    }

    #[tokio::test]
    async fn cleans_up_on_eof() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (task, mut reader, mut write_half) = start_session(state.clone());

        assert_eq!(
            log_in("driver17", &mut reader, &mut write_half).await,
            ServerMessage::AuthResult(Ok(()))
        );

        close_session(write_half, task).await;

        let registry = state.registry.read().await;
        assert!(!registry.active_clients.contains_key("driver17"));
        assert_eq!(registry.map["driver17"].state, UserState::Disconnected);
    }

    #[tokio::test]
    async fn recovers_after_bad_json() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (task, mut reader, mut write_half) = start_session(state);

        write_half.write_all(b"not json\n").await.unwrap();
        assert!(matches!(
            read_message(&mut reader).await,
            ServerMessage::ErrorMessage(message) if message.starts_with("Invalid JSON payload format:")
        ));
        assert_eq!(
            log_in("driver17", &mut reader, &mut write_half).await,
            ServerMessage::AuthResult(Ok(()))
        );

        close_session(write_half, task).await;
    }

    #[tokio::test]
    async fn cleans_up_after_write_error() {
        let (state, _db) = state_with_users(&["driver17"]);
        let (stream, mut peer) = tokio::io::duplex(1024);
        let (_shutdown_guard, shutdown) = watch::channel(false);
        let task = tokio::spawn(handle_client(
            WriteFails { stream },
            state.clone(),
            shutdown,
        ));

        send_client(
            &mut peer,
            &ClientMessage::Login {
                username: "driver17".to_string(),
                password: VALID_PASSWORD.to_string(),
            },
        )
        .await;
        peer.shutdown().await.unwrap();

        assert!(task.await.unwrap().is_err());
        let registry = state.registry.read().await;
        assert!(!registry.active_clients.contains_key("driver17"));
        assert_eq!(registry.map["driver17"].state, UserState::Disconnected);
    }
}
