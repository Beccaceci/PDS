//! Client application setup and orchestration.

use super::cli::{ClientCli, ClientCliEvent};
use super::movement::{FilePositionProvider, PositionProvider};
use super::network::{ServerReader, send_message};
use super::state::{ClientState, VehicleStateChange};
use crate::terminal::CliTerminal;
use crate::{ClientMessage, Position, ServerMessage, SystemTimeSource, TimeSource};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufRead, BufReader};
use tokio::net::{TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf};

/// Runtime settings used to start the vehicle client application.
pub struct ClientConfig {
    pub addr: String,
    pub route: String,
    pub interval: Duration,
}

/// An authenticated client connection with its route replay and movement state.
pub struct Client {
    server_reader: ServerReader<OwnedReadHalf>,
    server_writer: OwnedWriteHalf,
    position_provider: FilePositionProvider,
    state: ClientState,
}

impl Client {
    /// Connects and performs the interactive authentication flow.
    pub async fn connect(config: &ClientConfig) -> Result<Option<Self>, Box<dyn Error>> {
        let mut stdin = BufReader::new(tokio::io::stdin());
        Self::connect_with_input(config, &mut stdin).await
    }

    /// Connects and authenticates using an injected line-oriented input stream.
    pub async fn connect_with_input<I>(
        config: &ClientConfig,
        input: &mut I,
    ) -> Result<Option<Self>, Box<dyn Error>>
    where
        I: AsyncBufRead + Unpin,
    {
        Self::connect_with_input_and_time_source(config, input, Arc::new(SystemTimeSource)).await
    }

    /// Connects and authenticates using injected input and position timestamps.
    pub async fn connect_with_input_and_time_source<I>(
        config: &ClientConfig,
        input: &mut I,
        time_source: Arc<dyn TimeSource>,
    ) -> Result<Option<Self>, Box<dyn Error>>
    where
        I: AsyncBufRead + Unpin,
    {
        println!("=== GEORUGGINE CLIENT (VEHICLE TERMINAL) ===");
        println!("Attempting to connect to the central server...");

        // Establish asynchronous TCP connection to the central server.
        let stream = TcpStream::connect(&config.addr).await?;
        println!("Successfully connected to the server!\n");

        // Split TCP socket into read and write halves for concurrent bidirectional I/O:
        // 1. read_half -> incoming messages from server
        // 2. write_half -> outgoing messages to server
        let (read_half, mut write_half) = stream.into_split();
        let mut server_reader = ServerReader::new(read_half);

        // Launch the interactive authentication CLI (Login or Register).
        let logged_in_user =
            match ClientCli::authenticate_with_input(input, &mut server_reader, &mut write_half)
                .await
            {
                Some(user) => user,
                None => return Ok(None),
            };

        println!("\nWelcome to the system, {}!", logged_in_user);

        let position_provider = FilePositionProvider::with_time_source(&config.route, time_source)
            .expect("An error occurred while opening the CSV route file");

        Ok(Some(Self {
            server_reader,
            server_writer: write_half,
            position_provider,
            state: ClientState::default(),
        }))
    }

    /// Runs the interactive message and GPS update loop.
    pub async fn run(mut self, interval: Duration) -> Result<(), Box<dyn Error>> {
        println!("\n[SYSTEM] You can write messages to the Admin. Press ENTER to send.");
        let client_cli = ClientCli::new()?;
        self.run_with_cli(interval, client_cli).await
    }

    /// Runs the client with an injected terminal implementation.
    pub async fn run_with_terminal<T>(
        mut self,
        interval: Duration,
        terminal: T,
    ) -> Result<(), Box<dyn Error>>
    where
        T: CliTerminal,
    {
        terminal.write_plain(
            "\n[SYSTEM] You can write messages to the Admin. Press ENTER to send.\r\n",
        );
        let client_cli = ClientCli::with_terminal(terminal);
        self.run_with_cli(interval, client_cli).await
    }

    async fn run_with_cli<T>(
        &mut self,
        interval: Duration,
        mut client_cli: ClientCli<T>,
    ) -> Result<(), Box<dyn Error>>
    where
        T: CliTerminal,
    {
        let mut interval = tokio::time::interval(interval);

        // Main event loop where tokio::select! waits -> the first event that occurs is handled, then the loop restarts.
        loop {
            tokio::select! {
                // Arrival of a message from the server.
                server_message = self.server_reader.read_message() => {
                    match server_message {
                        Ok(Some(ServerMessage::TextMessage { sender, content })) => {
                            let msg = format!("[ADMIN MESSAGE from '{}']: {}", sender, content);
                            client_cli.write(&msg);
                        }
                        Ok(Some(ServerMessage::ErrorMessage(err))) => {
                            let msg = format!("[SERVER ERROR]: {}", err);
                            client_cli.write(&msg);
                        }
                        Ok(Some(_)) => {} // Ignored messages
                        Ok(None) => {
                            // The server closed the connection.
                            client_cli.write("[CLIENT] Disconnected from server.");
                            break;
                        }
                        Err(err) => {
                            let msg = format!("[CLIENT ERROR] Failed to read server message: {}", err);
                            client_cli.write(&msg);
                            break;
                        }
                    }
                }

                client_event = client_cli.read() => {
                    match client_event {
                        Ok(ClientCliEvent::Message(message)) => {
                            if let Err(err) = send_message(&mut self.server_writer, &message).await {
                                let message = format!("[CLIENT ERROR] Network error while sending message: {}", err);
                                client_cli.write(&message);
                                break;
                            }
                        }
                        Ok(ClientCliEvent::Exit) => {
                            client_cli.exit();
                            break;
                        }
                        Err(err) => {
                            let message = format!("[CLIENT ERROR] Keyboard stream error: {}", err);
                            client_cli.write(&message);
                            break;
                        }
                    }
                }

                // GPS timer management.
                _ = interval.tick() => {
                    if let Some((pos, state_change)) = self.next_position() {
                        match state_change {
                            Some(VehicleStateChange::Moving) => {
                                client_cli.write("[VEHICLE STATE]: MOVING");
                            }
                            Some(VehicleStateChange::Stopped) => {
                                client_cli.write("[VEHICLE STATE]: STOPPED");
                            }
                            None => {}
                        }

                        if let Err(err) = self.send_position(pos).await {
                            let msg = format!("[CLIENT ERROR] Failed to send GPS position: {}", err);
                            client_cli.write(&msg);
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Reads the next route position and updates the local movement state.
    pub fn next_position(&mut self) -> Option<(Position, Option<VehicleStateChange>)> {
        let pos = self.position_provider.next_position()?;
        let state_change = self.state.update(pos);

        Some((pos, state_change))
    }

    /// Sends a position update to the server.
    pub async fn send_position(&mut self, position: Position) -> tokio::io::Result<()> {
        send_message(
            &mut self.server_writer,
            &ClientMessage::UpdatePosition(position),
        )
        .await
    }
}

/// Starts the interactive client application with the supplied settings.
pub async fn run(config: ClientConfig) -> Result<(), Box<dyn Error>> {
    let interval = config.interval;
    let Some(client) = Client::connect(&config).await? else {
        return Ok(());
    };

    client.run(interval).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::terminal::TerminalEvent;
    use std::future::Future;
    use std::io::Write;
    use std::pin::Pin;
    use tempfile::NamedTempFile;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    struct ExitTerminal;

    impl CliTerminal for ExitTerminal {
        fn show_prompt(&self) {}

        fn write(&self, _message: &str) {}

        fn write_plain(&self, _message: &str) {}

        fn read(&mut self) -> Pin<Box<dyn Future<Output = tokio::io::Result<TerminalEvent>> + '_>> {
            Box::pin(async { Ok(TerminalEvent::Exit) })
        }
    }

    async fn client_for(route: &NamedTempFile) -> (Client, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connect = TcpStream::connect(listener.local_addr().unwrap());
        let accept = listener.accept();
        let (client_stream, accepted) = tokio::join!(connect, accept);
        let (server_stream, _) = accepted.unwrap();
        let (read_half, write_half) = client_stream.unwrap().into_split();

        (
            Client {
                server_reader: ServerReader::new(read_half),
                server_writer: write_half,
                position_provider: FilePositionProvider::new(route.path().to_str().unwrap())
                    .unwrap(),
                state: ClientState::default(),
            },
            server_stream,
        )
    }

    #[tokio::test]
    async fn run_fails_without_server() {
        let result = run(ClientConfig {
            addr: "127.0.0.1:0".to_string(),
            route: "unused-route.csv".to_string(),
            interval: Duration::from_secs(1),
        })
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn next_position_updates_state() {
        let mut route = NamedTempFile::new().unwrap();
        writeln!(route, "45.0703,7.6869").unwrap();
        route.flush().unwrap();

        let (mut client, _) = client_for(&route).await;

        let (position, state_change) = client.next_position().unwrap();
        assert_eq!(state_change, Some(VehicleStateChange::Stopped));
        assert_eq!((position.latitude, position.longitude), (45.0703, 7.6869));
    }

    #[tokio::test]
    async fn send_position_writes_update() {
        let route = NamedTempFile::new().unwrap();
        let (mut client, server_stream) = client_for(&route).await;
        let position = Position {
            latitude: 45.0703,
            longitude: 7.6869,
            timestamp: 1,
        };

        client.send_position(position).await.unwrap();

        let mut line = String::new();
        let mut reader = BufReader::new(server_stream);
        reader.read_line(&mut line).await.unwrap();
        let message: ClientMessage = serde_json::from_str(&line).unwrap();

        assert_eq!(message, ClientMessage::UpdatePosition(position));
    }

    #[tokio::test]
    async fn injected_terminal_can_exit_the_client_loop() {
        let route = NamedTempFile::new().unwrap();
        let (client, _server_stream) = client_for(&route).await;

        timeout(
            Duration::from_secs(1),
            client.run_with_terminal(Duration::from_secs(60), ExitTerminal),
        )
        .await
        .expect("injected exit must stop the client")
        .unwrap();
    }
}
