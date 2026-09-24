#![allow(dead_code)]

use georuggine::client::network::{ServerReader, send_message};
use georuggine::server::handler::handle_client;
use georuggine::server::runtime::{CpuLoggerConfig, Server, ServerConfig, ServerHandle};
use georuggine::server::state::ServerState;
use georuggine::{ClientMessage, Position, ServerMessage, TimeSource};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tempfile::TempDir;
use tokio::io::{self, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};

/// Standard valid test password meeting all validation criteria.
pub const VALID_PASSWORD: &str = "Password1";
pub const TEST_TIMESTAMP: u64 = 1_700_000_000;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const STATE_POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Mutable deterministic clock shared by integration-test collaborators.
#[derive(Debug)]
pub struct TestTimeSource(AtomicU64);

impl TestTimeSource {
    pub fn new(now: u64) -> Arc<Self> {
        Arc::new(Self(AtomicU64::new(now)))
    }

    pub fn set(&self, now: u64) {
        self.0.store(now, Ordering::Relaxed);
    }
}

impl TimeSource for TestTimeSource {
    fn now_secs(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

type SessionReader = ServerReader<ReadHalf<DuplexStream>>;
type SessionWriter = WriteHalf<DuplexStream>;

/// In-memory client connection backed by the real server session handler.
pub struct TestSession {
    reader: SessionReader,
    writer: SessionWriter,
    task: SessionTask,
    _shutdown_guard: watch::Sender<bool>,
}

/// Aborts a session handler if its owning test exits before awaiting it.
struct SessionTask {
    handle: Option<JoinHandle<io::Result<()>>>,
}

impl SessionTask {
    fn new(handle: JoinHandle<io::Result<()>>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    async fn join(mut self) -> Result<io::Result<()>, tokio::task::JoinError> {
        let result = self
            .handle
            .as_mut()
            .expect("session task must be present")
            .await;
        self.handle = None;
        result
    }
}

impl Drop for SessionTask {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl TestSession {
    /// Starts a server handler connected to an in-memory client stream.
    pub fn start(state: ServerState) -> Self {
        let (server_stream, client_stream) = tokio::io::duplex(1024);
        let (shutdown_guard, shutdown) = watch::channel(false);
        let task = SessionTask::new(tokio::spawn(handle_client(server_stream, state, shutdown)));
        let (read_half, writer) = tokio::io::split(client_stream);

        Self {
            reader: ServerReader::new(read_half),
            writer,
            task,
            _shutdown_guard: shutdown_guard,
        }
    }

    /// Sends a typed client message through the production JSON encoder.
    pub async fn send(&mut self, message: &ClientMessage) {
        send_message(&mut self.writer, message)
            .await
            .expect("client message must be sent");
    }

    /// Sends unencoded bytes for malformed-payload scenarios.
    pub async fn send_raw(&mut self, payload: &[u8]) {
        self.writer
            .write_all(payload)
            .await
            .expect("raw payload must be sent");
    }

    /// Reads the next typed server response with a bounded wait.
    pub async fn next_message(&mut self) -> ServerMessage {
        timeout(RESPONSE_TIMEOUT, self.reader.read_message())
            .await
            .expect("server did not respond")
            .expect("server response could not be read")
            .expect("server closed the session")
    }

    /// Registers the session with the standard valid test password.
    pub async fn register(&mut self, username: &str) {
        self.send(&ClientMessage::Register {
            username: username.to_string(),
            password: VALID_PASSWORD.to_string(),
        })
        .await;

        assert_eq!(self.next_message().await, ServerMessage::AuthResult(Ok(())));
    }

    /// Disconnects the client and returns the server handler's result.
    pub async fn disconnect(self) -> io::Result<()> {
        let Self {
            reader,
            writer,
            task,
            _shutdown_guard,
        } = self;
        drop(reader);
        drop(writer);

        timeout(SHUTDOWN_TIMEOUT, task.join())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "server handler did not stop"))?
            .map_err(io::Error::other)?
    }

    /// Closes the client stream and expects a clean server shutdown.
    pub async fn close(self) {
        self.disconnect()
            .await
            .expect("server handler must stop cleanly after EOF");
    }
}

/// Builds the protocol message emitted by Fleet Admin.
pub fn admin_message(content: &str) -> ServerMessage {
    ServerMessage::TextMessage {
        sender: "FleetAdmin".to_string(),
        content: content.to_string(),
    }
}

/// Temporary SQLite database wrapper automatically deleted on drop.
pub struct TestDatabase {
    _directory: TempDir,
    path: PathBuf,
}

impl TestDatabase {
    /// Creates a new isolated temporary SQLite database for testing.
    pub fn new() -> Self {
        let directory = TempDir::with_prefix("georuggine-test-")
            .expect("temporary database directory must be created");
        let path = directory.path().join("test.db");
        Self {
            _directory: directory,
            path,
        }
    }

    /// Returns the filesystem path to the temporary test database.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Creates server state backed by this database and the standard test time.
    pub fn state(&self) -> ServerState {
        ServerState::with_time_source(self.path(), TestTimeSource::new(TEST_TIMESTAMP))
    }
}

/// Starts the production server runtime on an ephemeral loopback port.
pub async fn start_test_server(db_path: &Path) -> (ServerHandle, SocketAddr, ServerState) {
    let server = Server::bind_with_time_source(
        ServerConfig {
            addr: "127.0.0.1:0".to_string(),
            db_path: db_path.to_path_buf(),
            cpu_logger: CpuLoggerConfig {
                log_path: db_path.with_extension("cpu.log"),
                ..Default::default()
            },
        },
        TestTimeSource::new(TEST_TIMESTAMP),
    )
    .await
    .expect("server must bind to an ephemeral loopback port");

    let addr = server.addr().expect("server address must be readable");
    let state = server.state();
    let handle = server.start();
    (handle, addr, state)
}

/// Returns a sample GPS position located in Turin (Italy).
pub fn sample_position_torino() -> Position {
    Position {
        latitude: 45.0703,
        longitude: 7.6869,
        timestamp: 1700000000,
    }
}

/// Returns a sample GPS position located in Asti (Italy).
pub fn sample_position_asti() -> Position {
    Position {
        latitude: 44.9000,
        longitude: 8.2069,
        timestamp: 1700001800,
    }
}

/// Waits until a driver has the expected number of recorded positions.
pub async fn wait_for_position_count(state: &ServerState, username: &str, expected: usize) {
    let mut last_observed = None;
    let result = timeout(Duration::from_secs(2), async {
        loop {
            let count = state
                .get_driver_list()
                .await
                .into_iter()
                .find(|(name, _, _)| name == username)
                .map(|(_, _, count)| count);
            last_observed = count;

            if count == Some(expected) {
                return;
            }

            tokio::time::sleep(STATE_POLL_INTERVAL).await;
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "server did not record {expected} positions for {username}; last observed count: {last_observed:?}"
    );
}

/// Waits until a driver has recorded at least the requested number of positions.
pub async fn wait_for_position_count_at_least(
    state: &ServerState,
    username: &str,
    minimum: usize,
) {
    let mut last_observed = None;
    let result = timeout(Duration::from_secs(2), async {
        loop {
            let count = state
                .get_driver_list()
                .await
                .into_iter()
                .find(|(name, _, _)| name == username)
                .map(|(_, _, count)| count);
            last_observed = count;

            if count.is_some_and(|count| count >= minimum) {
                return;
            }

            tokio::time::sleep(STATE_POLL_INTERVAL).await;
        }
    })
    .await;

    assert!(
        result.is_ok(),
        "server did not record at least {minimum} positions for {username}; last observed count: {last_observed:?}"
    );
}
