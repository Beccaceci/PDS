//! Server application setup and orchestration.

use super::cli::{start_admin_cli, start_admin_cli_with_terminal};
use super::handler::handle_client;
use super::logger::{CpuTracker, start_cpu_logger};
use super::state::ServerState;
use crate::terminal::CliTerminal;
use crate::{SystemTimeSource, TimeSource};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};

/// Runtime settings used to start the server application.
pub struct ServerConfig {
    pub addr: String,
    pub db_path: PathBuf,
    pub cpu_logger: CpuLoggerConfig,
}

/// Settings for periodic CPU usage logging and in-memory history.
pub struct CpuLoggerConfig {
    pub log_path: PathBuf,
    pub history_capacity: usize,
    pub sample_interval: Duration,
}

impl Default for CpuLoggerConfig {
    fn default() -> Self {
        Self {
            log_path: "cpu_usage.log".into(),
            history_capacity: 10,
            sample_interval: Duration::from_secs(120),
        }
    }
}

/// The running server application, including its TCP listener and shared state.
pub struct Server {
    state: ServerState,
    listener: TcpListener,
    cpu_tracker: Arc<Mutex<CpuTracker>>,
    logger_task: JoinHandle<()>,
}

/// Controls a server started in the background.
///
/// This gives integration tests a way to finish the accept loop and clean up
/// its listener without aborting the test task.
pub struct ServerHandle {
    stop: tokio::sync::watch::Sender<bool>,
    task: JoinHandle<tokio::io::Result<()>>,
}

impl ServerHandle {
    /// Requests shutdown and waits until the server has stopped.
    pub async fn stop(mut self) -> tokio::io::Result<()> {
        self.stop.send_replace(true);

        match (&mut self.task).await {
            Ok(result) => result,
            Err(error) => Err(tokio::io::Error::new(tokio::io::ErrorKind::Other, error)),
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::terminal::TerminalEvent;
    use crate::{ClientMessage, ServerMessage, UserState};
    use std::future::Future;
    use std::pin::Pin;
    use tempfile::TempDir;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpStream;
    use tokio::time::{Duration, timeout};

    struct ExitTerminal;

    #[test]
    fn cpu_logger_config_uses_existing_defaults() {
        let config = CpuLoggerConfig::default();

        assert_eq!(config.log_path, PathBuf::from("cpu_usage.log"));
        assert_eq!(config.history_capacity, 10);
        assert_eq!(config.sample_interval, Duration::from_secs(120));
    }

    impl CliTerminal for ExitTerminal {
        fn show_prompt(&self) {}

        fn write(&self, _message: &str) {}

        fn write_plain(&self, _message: &str) {}

        fn read(&mut self) -> Pin<Box<dyn Future<Output = std::io::Result<TerminalEvent>> + '_>> {
            Box::pin(async { Ok(TerminalEvent::Exit) })
        }
    }

    async fn server() -> Server {
        let database_directory = TempDir::new().unwrap();
        let log_directory = TempDir::new().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

        Server {
            state: ServerState::new(database_directory.path().join("server.db")),
            listener,
            cpu_tracker: Arc::new(Mutex::new(CpuTracker::new(
                10,
                log_directory.path().join("cpu.log"),
            ))),
            logger_task: tokio::spawn(async {}),
        }
    }

    #[tokio::test]
    async fn stop_signals_task() {
        let (stop, mut stop_receiver) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async move {
            stop_receiver
                .changed()
                .await
                .expect("handle must retain the stop sender");
            assert!(*stop_receiver.borrow());
            Ok::<(), tokio::io::Error>(())
        });

        ServerHandle { stop, task }.stop().await.unwrap();
    }

    #[tokio::test]
    async fn stop_returns_task_error() {
        let (stop, _) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(async { Err(tokio::io::Error::other("server task failed")) });

        let error = ServerHandle { stop, task }.stop().await.unwrap_err();

        assert_eq!(error.kind(), tokio::io::ErrorKind::Other);
    }

    #[tokio::test]
    async fn drop_requests_stop() {
        let (stop, mut stop_receiver) = tokio::sync::watch::channel(false);
        let (stopped, stopped_receiver) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            stop_receiver
                .changed()
                .await
                .expect("handle must retain the stop sender");
            assert!(*stop_receiver.borrow());
            stopped.send(()).unwrap();
            Ok::<(), tokio::io::Error>(())
        });

        drop(ServerHandle { stop, task });

        timeout(Duration::from_secs(1), stopped_receiver)
            .await
            .expect("server task must observe the stop signal")
            .expect("server task must confirm shutdown");
    }

    #[tokio::test]
    async fn accept_loop_stops() {
        let database_directory = TempDir::new().unwrap();
        let state = ServerState::new(database_directory.path().join("server.db"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (stop, stop_receiver) = tokio::sync::watch::channel(false);

        let task = tokio::spawn(Server::accept_clients(listener, state, stop_receiver));
        stop.send_replace(true);

        let result = timeout(Duration::from_secs(1), task)
            .await
            .expect("accept loop must react to the stop signal")
            .expect("accept loop task must not panic");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn accept_loop_handles_client() {
        const AUTH_TIMEOUT: Duration = Duration::from_secs(10);

        let database_directory = TempDir::new().unwrap();
        let state = ServerState::new(database_directory.path().join("server.db"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stop_receiver) = tokio::sync::watch::channel(false);
        let task = tokio::spawn(Server::accept_clients(
            listener,
            state.clone(),
            stop_receiver,
        ));

        let stream = TcpStream::connect(address).await.unwrap();
        let (read_half, mut write_half) = stream.into_split();
        let request = serde_json::to_string(&ClientMessage::Register {
            username: "driver17".to_string(),
            password: "Pass1234".to_string(),
        })
        .unwrap();
        write_half
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();

        let mut response = String::new();
        let mut reader = BufReader::new(read_half);
        let response_result = timeout(AUTH_TIMEOUT, reader.read_line(&mut response)).await;

        // Always stop and await the accept loop before evaluating the response.
        // Registration performs production-cost bcrypt hashing, which can take
        // longer when the test suite is running many hashing tests in parallel.
        stop.send_replace(true);
        timeout(Duration::from_secs(2), task)
            .await
            .expect("accept loop must stop")
            .expect("accept loop task must not panic")
            .expect("accept loop must exit successfully");

        response_result
            .expect("server must respond within the authentication timeout")
            .expect("registration response must be readable");
        assert_eq!(
            serde_json::from_str::<ServerMessage>(&response).unwrap(),
            ServerMessage::AuthResult(Ok(()))
        );

        let registry = state.registry.read().await;
        assert!(!registry.active_clients.contains_key("driver17"));
        assert_eq!(registry.map["driver17"].state, UserState::Disconnected);
    }

    #[tokio::test]
    async fn bind_uses_configured_addr() {
        let directory = TempDir::new().unwrap();
        let server = Server::bind(ServerConfig {
            addr: "127.0.0.1:0".to_string(),
            db_path: directory.path().join("server.db"),
            cpu_logger: CpuLoggerConfig {
                log_path: directory.path().join("cpu.log"),
                ..Default::default()
            },
        })
        .await
        .unwrap();

        let address = server.addr().unwrap();
        assert!(address.ip().is_loopback());
        assert_ne!(address.port(), 0);
        server.logger_task.abort();
    }

    #[tokio::test]
    async fn bind_rejects_invalid_addr() {
        let directory = TempDir::new().unwrap();
        let result = Server::bind(ServerConfig {
            addr: "not an address".to_string(),
            db_path: directory.path().join("server.db"),
            cpu_logger: CpuLoggerConfig {
                log_path: directory.path().join("cpu.log"),
                ..Default::default()
            },
        })
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn addr_returns_listener_addr() {
        let server = server().await;
        let expected_addr = server.listener.local_addr().unwrap();

        assert_eq!(server.addr().unwrap(), expected_addr);
    }

    #[tokio::test]
    async fn start_stops() {
        let server = server().await;

        server.start().stop().await.unwrap();
    }

    #[tokio::test]
    async fn injected_terminal_can_stop_the_server() {
        timeout(
            Duration::from_secs(1),
            server().await.run_with_terminal(ExitTerminal),
        )
        .await
        .expect("injected exit must stop the server")
        .unwrap();
    }
}

impl Server {
    /// Initializes the server resources using the supplied settings.
    pub async fn bind(config: ServerConfig) -> tokio::io::Result<Self> {
        Self::bind_with_time_source(config, Arc::new(SystemTimeSource)).await
    }

    /// Initializes server resources using an injected source for state and metric timestamps.
    pub async fn bind_with_time_source(
        config: ServerConfig,
        time_source: Arc<dyn TimeSource>,
    ) -> tokio::io::Result<Self> {
        let ServerConfig {
            addr,
            db_path,
            cpu_logger,
        } = config;

        println!("[SERVER] Starting Georuggine server...");

        // Compute and log the server executable binary size on boot.
        CpuTracker::log_executable_size();

        let state = ServerState::with_time_source(db_path, time_source.clone());
        let cpu_tracker = Arc::new(Mutex::new(CpuTracker::with_time_source(
            cpu_logger.history_capacity,
            cpu_logger.log_path,
            time_source,
        )));

        // Bind TCP listener to the configured network address.
        let listener = TcpListener::bind(&addr).await?;
        println!("[SERVER] Listening on {}...", addr);

        // Spawn the background CPU logging task only after startup succeeds.
        let logger_task = tokio::spawn(start_cpu_logger(
            cpu_tracker.clone(),
            cpu_logger.sample_interval,
        ));

        Ok(Self {
            state,
            listener,
            cpu_tracker,
            logger_task,
        })
    }

    /// Returns a clone of the shared server state.
    pub fn state(&self) -> ServerState {
        self.state.clone()
    }

    /// Returns the address on which the server listener is bound.
    pub fn addr(&self) -> tokio::io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    /// Starts the server accept loop in a background task and returns its lifecycle control.
    ///
    /// This runs the TCP listener and logger task in the background without launching
    /// the interactive admin CLI, making it suitable for integration tests and embedded operation.
    pub fn start(self) -> ServerHandle {
        let (stop, stop_receiver) = tokio::sync::watch::channel(false);
        let Self {
            state,
            listener,
            cpu_tracker: _,
            logger_task,
        } = self;

        let task = tokio::spawn(async move {
            let res = Self::accept_clients(listener, state, stop_receiver).await;
            logger_task.abort();
            let _ = logger_task.await;
            res
        });

        ServerHandle { stop, task }
    }

    /// Runs the administrator CLI and TCP listener until the administrator exits.
    pub async fn run(self) -> tokio::io::Result<()> {
        let state = self.state.clone();
        let cpu_tracker = self.cpu_tracker.clone();
        let handle = self.start();

        start_admin_cli(state, cpu_tracker).await;
        println!("[SERVER] Fleet Admin CLI closed. Shutting down server gracefully.");

        handle.stop().await
    }

    /// Runs the TCP listener with an injected administrator terminal.
    pub async fn run_with_terminal<T>(self, terminal: T) -> tokio::io::Result<()>
    where
        T: CliTerminal,
    {
        let state = self.state.clone();
        let cpu_tracker = self.cpu_tracker.clone();
        let handle = self.start();

        start_admin_cli_with_terminal(state, cpu_tracker, terminal).await;
        println!("[SERVER] Fleet Admin CLI closed. Shutting down server gracefully.");

        handle.stop().await
    }

    /// Accepts connections and starts a session task for each client.
    async fn accept_clients(
        listener: TcpListener,
        state: ServerState,
        mut stop: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::io::Result<()> {
        let mut clients = JoinSet::new();

        loop {
            tokio::select! {
                // Accept incoming client connection.
                accepted = listener.accept() => {
                    let (socket, _) = accepted?;

                    // Spawn a Tokio task to handle each client asynchronously.
                    let state_clone = state.clone();
                    let client_stop = stop.clone();
                    clients.spawn(async move {
                        if let Err(e) = handle_client(socket, state_clone, client_stop).await {
                            eprintln!("[SERVER ERROR] Client handler error: {}", e);
                        }
                    });
                }
                Some(_) = clients.join_next(), if !clients.is_empty() => {}
                _ = stop.changed() => {
                    // Each handler observes the same signal and performs its own
                    // authenticated-session cleanup before it finishes.
                    while clients.join_next().await.is_some() {}
                    return Ok(());
                }
            }
        }
    }
}
