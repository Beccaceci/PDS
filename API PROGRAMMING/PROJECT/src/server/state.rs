use super::error::ServerError;
use crate::*;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::sync::RwLock;

// GPS noise threshold in kilometers (1 meter) to filter coordinate jitter while parked
const GPS_NOISE_THRESHOLD_KM: f64 = 0.001;

/// Alias for the Tokio MPSC channel sender used to communicate with an active client.
pub type ClientSender = tokio::sync::mpsc::Sender<ServerMessage>;

/// Represents a single registered user account and its geographic position history.
#[derive(Debug, Clone)]
pub struct UserProfile {
    pub username: String,
    pub password: String,
    pub state: UserState,
    pub positions: Vec<Position>,
    pub last_session_position: Option<Position>,
}

/// Central collection mapping usernames to UserProfiles and active Tokio channels.
pub struct UserRegistry {
    pub map: HashMap<String, UserProfile>,
    pub active_clients: HashMap<String, ClientSender>,
}

impl UserRegistry {
    /// Creates a new, empty user registry.
    pub fn new() -> Self {
        Self {
            map: HashMap::<String, UserProfile>::new(),
            active_clients: HashMap::<String, ClientSender>::new(),
        }
    }
}

/// Global shared server state with SQLite embedded database persistence support.
#[derive(Clone)]
pub struct ServerState {
    pub registry: Arc<RwLock<UserRegistry>>,
    db_path: PathBuf,
    pub driver_message_sender: tokio::sync::broadcast::Sender<(String, String)>,
    time_source: Arc<dyn TimeSource>,
}

impl ServerState {
    /// Creates a new server state, restoring profiles and position histories from `db_path`.
    pub fn new(db_path: impl Into<PathBuf>) -> Self {
        Self::with_time_source(db_path, Arc::new(SystemTimeSource))
    }

    /// Creates server state using an injected source for session and statistics timestamps.
    pub fn with_time_source(db_path: impl Into<PathBuf>, time_source: Arc<dyn TimeSource>) -> Self {
        let mut registry = UserRegistry::new();
        let db_path = db_path.into();
        let (driver_message_sender, _) = tokio::sync::broadcast::channel(128);

        // Initialize SQLite tables and restore state from the configured database.
        if let Ok(connection) = sqlite::open(&db_path) {
            let _ = Self::init_schema(&connection);
            // Restore registered users from SQLite database
            let users_query = "SELECT username, password FROM users;";
            if let Ok(mut statement) = connection.prepare(users_query) {
                while let Ok(sqlite::State::Row) = statement.next() {
                    if let (Ok(username), Ok(password)) = (
                        statement.read::<String, _>("username"),
                        statement.read::<String, _>("password"),
                    ) {
                        registry.map.insert(
                            username.clone(),
                            UserProfile {
                                username,
                                password,
                                state: UserState::Disconnected, // Initial state on server boot
                                positions: Vec::new(),
                                last_session_position: None,
                            },
                        );
                    }
                }
            }

            // Restore historical position points from SQLite database
            let positions_query = "SELECT username, latitude, longitude, timestamp FROM positions ORDER BY timestamp ASC;";
            let mut loaded_pos_count = 0;
            if let Ok(mut statement) = connection.prepare(positions_query) {
                while let Ok(sqlite::State::Row) = statement.next() {
                    let username_res = statement.read::<String, _>("username");
                    let lat_res = statement.read::<f64, _>("latitude");
                    let lon_res = statement.read::<f64, _>("longitude");
                    let ts_res = statement.read::<i64, _>("timestamp");

                    if let (Ok(username), Ok(latitude), Ok(longitude), Ok(timestamp)) =
                        (username_res, lat_res, lon_res, ts_res)
                    {
                        if let Some(user_profile) = registry.map.get_mut(&username) {
                            user_profile.positions.push(Position {
                                latitude,
                                longitude,
                                timestamp: timestamp.max(0) as u64,
                            });
                            loaded_pos_count += 1;
                        }
                    }
                }
            }

            println!(
                "[SQLITE PERSISTENCE] Loaded {} user accounts and {} positions from {}.",
                registry.map.len(),
                loaded_pos_count,
                db_path.display()
            );
        } else {
            eprintln!(
                "[SQLITE PERSISTENCE ERROR] Failed to open database {}",
                db_path.display()
            );
        }

        Self {
            registry: Arc::new(RwLock::new(registry)),
            db_path: db_path,
            driver_message_sender,
            time_source,
        }
    }

    /// Initializes the SQLite database schema if the tables do not already exist.
    fn init_schema(conn: &sqlite::Connection) -> Result<(), sqlite::Error> {
        let q = "
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            timestamp INTEGER NOT NULL,
            FOREIGN KEY(username) REFERENCES users(username)
        );
        ";
        conn.execute(q)
    }

    /// Creates a new server state with a pre-populated user registry for testing purposes.
    #[cfg(test)]
    pub(crate) fn with_registry(
        registry: UserRegistry,
        db_path: PathBuf,
        time_source: Arc<dyn TimeSource>,
    ) -> Self {
        let conn = sqlite::open(&db_path).unwrap();
        Self::init_schema(&conn).unwrap();

        for (username, profile) in &registry.map {
            let hashed_password = &profile.password;
            let query = "INSERT OR IGNORE INTO users (username, password) VALUES (?, ?);";
            let mut stmt = conn.prepare(query).unwrap();
            stmt.bind((1, username.as_str())).unwrap();
            stmt.bind((2, hashed_password.as_str())).unwrap();
            stmt.next().unwrap();

            for pos in &profile.positions {
                let query = "INSERT INTO positions (username, latitude, longitude, timestamp) VALUES (?, ?, ?, ?);";
                let mut stmt = conn.prepare(query).unwrap();
                stmt.bind((1, username.as_str())).unwrap();
                stmt.bind((2, pos.latitude)).unwrap();
                stmt.bind((3, pos.longitude)).unwrap();
                stmt.bind((4, pos.timestamp as i64)).unwrap();
                stmt.next().unwrap();
            }
        }

        let (driver_message_sender, _) = tokio::sync::broadcast::channel(128);
        Self {
            registry: Arc::new(RwLock::new(registry)),
            db_path,
            driver_message_sender,
            time_source,
        }
    }

    /// Helper method to execute asynchronous SQLite database write operations in a blocking thread.
    pub async fn execute_sqlite_write<F>(&self, action: F) -> Result<(), ServerError>
    where
        F: FnOnce(&sqlite::Connection) -> Result<(), ServerError> + Send + 'static,
    {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = sqlite::open(&*db_path).map_err(|error| {
                eprintln!(
                    "[SQLITE WRITE ERROR] Failed to open database {}: {error}",
                    db_path.display()
                );
                ServerError::PersistenceFailed
            })?;
            action(&conn)
        })
        .await
        .map_err(|error| {
            eprintln!("[SQLITE WRITE ERROR] Blocking task failed: {error}");
            ServerError::BackgroundTaskFailed
        })?
    }

    /// Registers a new user account into the server state and persists it to SQLite.
    pub async fn register_user(
        &self,
        username: &str,
        password: &str,
    ) -> Result<tokio::sync::mpsc::Receiver<ServerMessage>, ServerError> {
        let username = username.trim();
        validate_credentials(username, password)?;

        let hashed_password = hash_password(password)?;

        let u = username.to_string();
        let p = hashed_password.clone();

        let mut reg = self.registry.write().await;

        if reg.map.contains_key(username) {
            Err(ServerError::UsernameTaken)
        } else {
            // Persist new user row into SQLite database using the hashed password
            self.execute_sqlite_write(move |conn| {
                let mut stmt = conn
                    .prepare("INSERT INTO users (username, password) VALUES (?, ?);")
                    .map_err(|error| {
                        eprintln!(
                            "[SQLITE WRITE ERROR] Failed to prepare user insert for '{u}': {error}"
                        );
                        ServerError::PersistenceFailed
                    })?;
                stmt.bind((1, u.as_str())).map_err(|error| {
                    eprintln!("[SQLITE WRITE ERROR] Failed to bind user '{u}': {error}");
                    ServerError::PersistenceFailed
                })?;
                stmt.bind((2, p.as_str())).map_err(|error| {
                    eprintln!("[SQLITE WRITE ERROR] Failed to bind password for '{u}': {error}");
                    ServerError::PersistenceFailed
                })?;
                stmt.next().map_err(|error| {
                    eprintln!("[SQLITE WRITE ERROR] Failed to persist user '{u}': {error}");
                    ServerError::PersistenceFailed
                })?;
                Ok(())
            })
            .await?;

            let (tx, rx) = tokio::sync::mpsc::channel::<ServerMessage>(32);
            reg.active_clients.insert(username.to_string(), tx);

            reg.map.insert(
                username.to_string(),
                UserProfile {
                    username: username.to_string(),
                    password: hashed_password,
                    state: UserState::Stopped(self.time_source.now_secs()),
                    positions: Vec::new(),
                    last_session_position: None,
                },
            );

            Ok(rx)
        }
    }

    /// Authenticates an existing user and updates its state to Stopped(timestamp).
    pub async fn user_login(
        &self,
        username: &str,
        password: &str,
    ) -> Result<tokio::sync::mpsc::Receiver<ServerMessage>, ServerError> {
        let username = username.trim();
        if validate_credentials(username, password).is_err() {
            return Err(ServerError::InvalidCredentials);
        }

        let mut reg = self.registry.write().await;

        // Prevent double login: check if the user is already connected from another device/session
        if reg.active_clients.contains_key(username) {
            return Err(ServerError::AlreadyLoggedIn);
        }

        // Verify the plain text password against the stored bcrypt hash
        if let Some(user_profile) = reg.map.get_mut(username) {
            if verify_password(password, &user_profile.password)? {
                user_profile.state = UserState::Stopped(self.time_source.now_secs());
                user_profile.last_session_position = None;

                let (tx, rx) = tokio::sync::mpsc::channel::<ServerMessage>(32);
                reg.active_clients.insert(username.to_string(), tx);
                return Ok(rx);
            }
        };

        Err(ServerError::InvalidCredentials)
    }

    /// Updates an authenticated user's geographic position and persists the new position point to SQLite.
    pub async fn update_user_position(
        &self,
        username: &str,
        new_pos: &Position,
    ) -> Result<(), ServerError> {
        let mut reg = self.registry.write().await;

        let profile = match reg.map.get(username) {
            Some(profile) => profile,
            None => return Err(ServerError::UserNotFound),
        };

        if let Some(last_known_position) = profile.positions.last()
            && new_pos.timestamp < last_known_position.timestamp
        {
            return Err(ServerError::OutOfOrderPosition);
        }

        let next_state = if let Some(last_pos) = profile.last_session_position {
            if (new_pos.latitude != last_pos.latitude) || (new_pos.longitude != last_pos.longitude)
            {
                UserState::Moving(new_pos.timestamp)
            } else if let UserState::Moving(last_moved_timestamp) = profile.state {
                if new_pos.timestamp.saturating_sub(last_moved_timestamp) >= STOPPED_AFTER_SECS {
                    UserState::Stopped(new_pos.timestamp)
                } else {
                    profile.state.clone()
                }
            } else {
                profile.state.clone()
            }
        } else {
            UserState::Stopped(new_pos.timestamp)
        };

        // Persist new position point into SQLite database asynchronously via execute_sqlite_write
        let u = username.to_string();
        let lat = new_pos.latitude;
        let lon = new_pos.longitude;
        let ts = new_pos.timestamp as i64;

        self.execute_sqlite_write(move |conn| {
            let query = "INSERT INTO positions (username, latitude, longitude, timestamp) VALUES (?, ?, ?, ?);";
            let mut stmt = conn.prepare(query).map_err(|error| {
                eprintln!("[SQLITE WRITE ERROR] Failed to prepare position insert for '{u}': {error}");
                ServerError::PersistenceFailed
            })?;
            stmt.bind((1, u.as_str())).map_err(|error| {
                eprintln!("[SQLITE WRITE ERROR] Failed to bind position user '{u}': {error}");
                ServerError::PersistenceFailed
            })?;
            stmt.bind((2, lat)).map_err(|error| {
                eprintln!("[SQLITE WRITE ERROR] Failed to bind latitude for '{u}': {error}");
                ServerError::PersistenceFailed
            })?;
            stmt.bind((3, lon)).map_err(|error| {
                eprintln!("[SQLITE WRITE ERROR] Failed to bind longitude for '{u}': {error}");
                ServerError::PersistenceFailed
            })?;
            stmt.bind((4, ts)).map_err(|error| {
                eprintln!("[SQLITE WRITE ERROR] Failed to bind timestamp for '{u}': {error}");
                ServerError::PersistenceFailed
            })?;
            stmt.next().map_err(|error| {
                eprintln!("[SQLITE WRITE ERROR] Failed to persist position for '{u}': {error}");
                ServerError::PersistenceFailed
            })?;
            Ok(())
        }).await?;

        let user_profile = reg
            .map
            .get_mut(username)
            .expect("user profile was checked while the registry write lock is held");
        user_profile.state = next_state;
        user_profile.positions.push(*new_pos);
        user_profile.last_session_position = Some(*new_pos);

        Ok(())
    }

    /// Processes incoming text messages sent from a truck driver (Client) to Fleet Admin (Server).
    pub async fn process_message(&self, sender: &str, content: &str) -> Result<(), ServerError> {
        if content.trim().is_empty() {
            return Err(ServerError::EmptyMessage);
        }

        // Notify the active Admin CLI console or print if no subscribers
        let _ = self
            .driver_message_sender
            .send((sender.to_string(), content.to_string()));
        Ok(())
    }

    /// Sends a direct (Unicast) text message from Fleet Admin to a specific connected truck driver.
    pub async fn send_admin_direct(
        &self,
        recipient: &str,
        content: &str,
    ) -> Result<(), ServerError> {
        let recipient = recipient.trim();
        let sender_channel = {
            let reg = self.registry.read().await;
            reg.active_clients.get(recipient).cloned()
        };

        if let Some(sender_channel) = sender_channel {
            let msg = ServerMessage::TextMessage {
                sender: "FleetAdmin".to_string(),
                content: content.to_string(),
            };
            sender_channel.try_send(msg).map_err(|err| match err {
                tokio::sync::mpsc::error::TrySendError::Full(_) => ServerError::MailboxFull {
                    recipient: recipient.to_string(),
                },
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    ServerError::RecipientOffline {
                        recipient: recipient.to_string(),
                    }
                }
            })
        } else {
            Err(ServerError::RecipientOffline {
                recipient: recipient.to_string(),
            })
        }
    }

    /// Sends a Broadcast text message from Fleet Admin to ALL currently connected truck drivers.
    pub async fn send_admin_broadcast(&self, content: &str) -> usize {
        let reg = self.registry.read().await;
        let mut count = 0;

        let msg = ServerMessage::TextMessage {
            sender: "FleetAdmin".to_string(),
            content: content.to_string(),
        };

        for sender_channel in reg.active_clients.values() {
            if matches!(
                sender_channel.try_send(msg.clone()),
                Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_))
            ) {
                count += 1;
            }
        }

        count
    }

    /// Calculates all movement analytics (distance in km, average speed, movement duration, pause duration in STOPPED state)
    /// for a given user and time window in a single pass over the positions vector.
    pub async fn calculate_user_stats(
        &self,
        username: &str,
        time_window: TimeWindow,
    ) -> Result<UserStats, ServerError> {
        let now = self.time_source.now_secs();
        let lower_bound_secs = time_window.lower_bound_timestamp_at(now);

        // Extract a snapshot of only the relevant positions within the time window and state while holding read lock
        let (positions, state) = {
            let reg = self.registry.read().await;
            let profile = match reg.map.get(username) {
                Some(profile) => profile,
                None => return Err(ServerError::UserNotFound),
            };

            // O(log N) binary search for the start index of the requested time window
            let idx = profile
                .positions
                .partition_point(|p| p.timestamp < lower_bound_secs);
            let start_idx = idx.saturating_sub(1);
            (
                profile.positions[start_idx..].to_vec(),
                profile.state.clone(),
            )
        };

        let mut total_distance_km: f64 = 0.0;
        let mut movement_duration_secs: u64 = 0;
        let mut pause_duration_secs: u64 = 0;

        for pair in positions.windows(2).rev() {
            let p1 = &pair[0];
            let p2 = &pair[1];

            if p1.timestamp < lower_bound_secs {
                if p2.timestamp >= lower_bound_secs {
                    // Interpolate the distance and duration for the portion of the segment that falls within the time window
                    let dt = p2.timestamp.saturating_sub(lower_bound_secs);
                    let total_dt = (p2.timestamp.saturating_sub(p1.timestamp) as f64).max(1.0);
                    let r = (dt as f64 / total_dt).clamp(0.0, 1.0);
                    let dist = p1.haversine_distance_km(p2);

                    if dist > GPS_NOISE_THRESHOLD_KM {
                        total_distance_km += dist * r;
                        movement_duration_secs += dt;
                    } else {
                        pause_duration_secs += dt;
                    }
                }
                break;
            }

            let dt = p2.timestamp.saturating_sub(p1.timestamp);
            let dist = p1.haversine_distance_km(p2);

            if dist > GPS_NOISE_THRESHOLD_KM {
                total_distance_km += dist;
                movement_duration_secs += dt;
            } else {
                pause_duration_secs += dt;
            }
        }

        // Account for ongoing elapsed time from last recorded position until 'now' based on snapshot state
        if let Some(last_pos) = positions.last() {
            if last_pos.timestamp >= lower_bound_secs && last_pos.timestamp < now {
                let tail_dt = now.saturating_sub(last_pos.timestamp);
                match state {
                    UserState::Stopped(_) => pause_duration_secs += tail_dt,
                    UserState::Moving(_) => movement_duration_secs += tail_dt,
                    UserState::Disconnected => {}
                }
            }
        }

        let average_speed_kmh = if movement_duration_secs > 0 {
            let movement_hours = movement_duration_secs as f64 / 3600.0;
            total_distance_km / movement_hours
        } else {
            0.0
        };

        Ok(UserStats {
            time_window,
            total_distance_km,
            average_speed_kmh,
            movement_duration_secs,
            pause_duration_secs,
        })
    }

    /// Removes an active client channel sender from the registry upon disconnect.
    pub async fn remove_active_client(&self, username: &str) {
        let username = username.trim();
        let mut reg = self.registry.write().await;
        reg.active_clients.remove(username);

        if let Some(profile) = reg.map.get_mut(username) {
            profile.state = UserState::Disconnected;
            profile.last_session_position = None;
        }
    }

    /// Returns a list of all registered drivers with their current operating state and recorded positions count.
    pub async fn get_driver_list(&self) -> Vec<(String, UserState, usize)> {
        let reg = self.registry.read().await;
        reg.map
            .values()
            .map(|p| (p.username.clone(), p.state.clone(), p.positions.len()))
            .collect()
    }
}

/// Hashes a password using the server's configured bcrypt policy.
pub(crate) fn hash_password(password: &str) -> Result<String, ServerError> {
    bcrypt::hash(password, bcrypt::DEFAULT_COST).map_err(|_| ServerError::PasswordHashingFailed)
}

/// Verifies a password using the server's configured bcrypt policy.
pub(crate) fn verify_password(password: &str, password_hash: &str) -> Result<bool, ServerError> {
    bcrypt::verify(password, password_hash).map_err(|_| ServerError::PasswordVerificationFailed)
}

/// Helper function to validate username and password syntax and constraints.
pub fn validate_credentials(username: &str, password: &str) -> Result<(), ServerError> {
    let username = username.trim();
    if username.len() < 4 || username.len() > 20 {
        return Err(ServerError::InvalidUsernameLength);
    }

    if !username.bytes().all(|b| b.is_ascii_alphanumeric()) {
        return Err(ServerError::InvalidUsernameCharacters);
    }

    if password.len() < 8 {
        return Err(ServerError::PasswordTooShort);
    }

    let mut has_lowercase = false;
    let mut has_uppercase = false;
    let mut has_digit = false;

    for b in password.bytes() {
        match b {
            b'a'..=b'z' => has_lowercase = true,
            b'A'..=b'Z' => has_uppercase = true,
            b'0'..=b'9' => has_digit = true,
            _ => {}
        }
        if has_lowercase && has_uppercase && has_digit {
            break;
        }
    }

    if !has_lowercase || !has_uppercase || !has_digit {
        return Err(ServerError::PasswordMissingRequiredCharacters);
    }

    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::STOPPED_AFTER_SECS;
    use std::path::PathBuf;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::TempDir;
    use tokio::sync::Barrier;
    use tokio::time::{Duration, timeout};

    pub(crate) const VALID_PASSWORD: &str = "Password1";
    const TEST_NOW: u64 = 1_700_000_000;

    #[derive(Debug)]
    struct FixedTimeSource(AtomicU64);

    impl FixedTimeSource {
        fn set(&self, now: u64) {
            self.0.store(now, Ordering::Relaxed);
        }
    }

    impl TimeSource for FixedTimeSource {
        fn now_secs(&self) -> u64 {
            self.0.load(Ordering::Relaxed)
        }
    }

    pub(crate) struct TestDatabase {
        _directory: TempDir,
        path: PathBuf,
    }

    impl TestDatabase {
        pub(crate) fn new() -> Self {
            let directory = TempDir::with_prefix("georuggine-test-")
                .expect("temporary database directory must be created");
            let path = directory.path().join("test.db");
            Self {
                _directory: directory,
                path,
            }
        }

        pub(crate) fn path(&self) -> &std::path::Path {
            &self.path
        }
    }

    fn password_hash() -> &'static str {
        static HASH: OnceLock<String> = OnceLock::new();
        HASH.get_or_init(|| hash_password(VALID_PASSWORD).expect("valid test password must hash"))
    }

    pub(crate) fn state_with_users(usernames: &[&str]) -> (ServerState, TestDatabase) {
        let db = TestDatabase::new();
        let mut registry = UserRegistry::new();
        for username in usernames {
            registry.map.insert(
                (*username).to_string(),
                UserProfile {
                    username: (*username).to_string(),
                    password: password_hash().to_string(),
                    state: UserState::Disconnected,
                    positions: Vec::new(),
                    last_session_position: None,
                },
            );
        }
        let state = ServerState::with_registry(
            registry,
            db.path().to_path_buf(),
            Arc::new(FixedTimeSource(AtomicU64::new(TEST_NOW))),
        );
        (state, db)
    }

    async fn fill_client_mailbox(state: &ServerState, username: &str) {
        for sequence in 0..32 {
            state
                .send_admin_direct(username, &format!("queued-{sequence}"))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn registration_persists_hashed_user() {
        let database = TestDatabase::new();
        let state = ServerState::new(database.path());

        state
            .register_user("driver17", VALID_PASSWORD)
            .await
            .unwrap();

        let registry = state.registry.read().await;
        let profile = &registry.map["driver17"];
        assert_ne!(profile.password, VALID_PASSWORD);
        assert!(bcrypt::verify(VALID_PASSWORD, &profile.password).unwrap());
        drop(registry);
        drop(state);

        let restored_state = ServerState::new(database.path());
        let restored_registry = restored_state.registry.read().await;
        let restored_profile = &restored_registry.map["driver17"];
        assert!(bcrypt::verify(VALID_PASSWORD, &restored_profile.password).unwrap());
    }

    #[tokio::test]
    async fn injected_time_source_controls_session_state_and_stats_tail() {
        let database = TestDatabase::new();
        let time_source = Arc::new(FixedTimeSource(AtomicU64::new(1_000)));
        let state = ServerState::with_time_source(database.path(), time_source.clone());

        let receiver = state
            .register_user("timedDriver", VALID_PASSWORD)
            .await
            .unwrap();
        assert_eq!(
            state.registry.read().await.map["timedDriver"].state,
            UserState::Stopped(1_000)
        );

        state.remove_active_client("timedDriver").await;
        drop(receiver);
        time_source.set(2_000);
        let _receiver = state
            .user_login("timedDriver", VALID_PASSWORD)
            .await
            .unwrap();
        assert_eq!(
            state.registry.read().await.map["timedDriver"].state,
            UserState::Stopped(2_000)
        );

        for position in [
            Position {
                latitude: 45.0,
                longitude: 7.0,
                timestamp: 2_100,
            },
            Position {
                latitude: 45.1,
                longitude: 7.0,
                timestamp: 2_160,
            },
        ] {
            state
                .update_user_position("timedDriver", &position)
                .await
                .unwrap();
        }

        time_source.set(2_220);
        let stats = state
            .calculate_user_stats("timedDriver", TimeWindow::CurrentDay)
            .await
            .unwrap();
        assert_eq!(stats.movement_duration_secs, 120);
    }

    #[tokio::test]
    async fn rejects_duplicate_registration() {
        let database = TestDatabase::new();
        let state = ServerState::new(database.path());
        state
            .register_user("driver17", VALID_PASSWORD)
            .await
            .unwrap();

        assert_eq!(
            state
                .register_user("driver17", VALID_PASSWORD)
                .await
                .unwrap_err(),
            ServerError::UsernameTaken
        );
        assert_eq!(state.registry.read().await.map.len(), 1);
    }

    #[tokio::test]
    async fn registration_rejects_invalid_credentials() {
        let database = TestDatabase::new();
        let state = ServerState::new(database.path());

        assert_eq!(
            state
                .register_user("bad", VALID_PASSWORD)
                .await
                .unwrap_err(),
            ServerError::InvalidUsernameLength
        );
        assert!(state.registry.read().await.map.is_empty());
    }

    #[test]
    fn accepts_credential_boundary_values() {
        let min_username = "user";
        let max_username = "a".repeat(20);
        let minimum_valid_password = "Passw0rd";
        let long_valid_password = format!("A1a{}", "x".repeat(252));

        for (username, password) in [
            (min_username, minimum_valid_password),
            (max_username.as_str(), VALID_PASSWORD),
            ("  driver17  ", VALID_PASSWORD),
            ("driver17", long_valid_password.as_str()),
        ] {
            assert_eq!(
                validate_credentials(username, password),
                Ok(()),
                "{username:?}"
            );
        }
    }

    #[test]
    fn login_rejects_invalid_credentials() {
        let too_long_username = "a".repeat(21);
        for (username, password, expected_error) in [
            ("", VALID_PASSWORD, ServerError::InvalidUsernameLength),
            ("abc", VALID_PASSWORD, ServerError::InvalidUsernameLength),
            ("   ", VALID_PASSWORD, ServerError::InvalidUsernameLength),
            (
                too_long_username.as_str(),
                VALID_PASSWORD,
                ServerError::InvalidUsernameLength,
            ),
            (
                "driver-name",
                VALID_PASSWORD,
                ServerError::InvalidUsernameCharacters,
            ),
            (
                "driver_name",
                VALID_PASSWORD,
                ServerError::InvalidUsernameCharacters,
            ),
            (
                "drivér1",
                VALID_PASSWORD,
                ServerError::InvalidUsernameCharacters,
            ),
            ("driver17", "Passw0r", ServerError::PasswordTooShort),
            (
                "driver17",
                "password1",
                ServerError::PasswordMissingRequiredCharacters,
            ),
            (
                "driver17",
                "PASSWORD1",
                ServerError::PasswordMissingRequiredCharacters,
            ),
            (
                "driver17",
                "Password",
                ServerError::PasswordMissingRequiredCharacters,
            ),
            (
                "driver17",
                "password",
                ServerError::PasswordMissingRequiredCharacters,
            ),
        ] {
            assert_eq!(
                validate_credentials(username, password),
                Err(expected_error),
                "{username:?}"
            );
        }
    }

    #[test]
    fn hash_password_round_trip() {
        let hash = hash_password(VALID_PASSWORD).expect("valid password must hash");

        assert_eq!(verify_password(VALID_PASSWORD, &hash), Ok(true));
        assert_eq!(verify_password("Incorrect1", &hash), Ok(false));
        assert_eq!(
            verify_password(VALID_PASSWORD, "not-a-bcrypt-hash"),
            Err(ServerError::PasswordVerificationFailed)
        );
    }

    #[tokio::test]
    async fn registration_trims_username_before_persisting() {
        let database = TestDatabase::new();
        let state = ServerState::new(database.path());

        state
            .register_user("  driver17  ", VALID_PASSWORD)
            .await
            .unwrap();

        let registry = state.registry.read().await;
        assert!(registry.map.contains_key("driver17"));
        assert!(!registry.map.contains_key("  driver17  "));
        assert!(registry.active_clients.contains_key("driver17"));
    }

    #[tokio::test]
    async fn rejects_second_login() {
        let (state, _db) = state_with_users(&["driver17"]);

        let _ = state.user_login("driver17", VALID_PASSWORD).await.unwrap();
        assert_eq!(
            state
                .user_login("driver17", VALID_PASSWORD)
                .await
                .unwrap_err(),
            ServerError::AlreadyLoggedIn
        );
    }

    #[tokio::test]
    async fn cleanup_allows_relogin() {
        let (state, _db) = state_with_users(&["driver17"]);

        let _ = state.user_login("driver17", VALID_PASSWORD).await.unwrap();
        state.remove_active_client("driver17").await;

        {
            let registry = state.registry.read().await;
            assert_eq!(registry.map["driver17"].state, UserState::Disconnected);
        }

        assert!(state.user_login("driver17", VALID_PASSWORD).await.is_ok());
    }

    #[tokio::test]
    async fn accepted_credentials_can_log_in() {
        let (state, _db) = state_with_users(&["driver17"]);
        let username = "  driver17  ";

        assert!(
            validate_credentials(username, VALID_PASSWORD).is_ok(),
            "invalid credentials"
        );
        let receiver = state.user_login(username, VALID_PASSWORD).await;

        assert!(
            receiver.is_ok(),
            "credentials accepted after trimming must authenticate the same account"
        );
    }

    #[tokio::test]
    async fn concurrent_logins_allow_one_session() {
        let (state, _db) = state_with_users(&["driver17"]);
        let barrier = Arc::new(Barrier::new(2));

        let first_state = state.clone();
        let first_barrier = barrier.clone();
        let first = tokio::spawn(async move {
            first_barrier.wait().await;
            first_state.user_login("driver17", VALID_PASSWORD).await
        });
        let second_state = state.clone();
        let second = tokio::spawn(async move {
            barrier.wait().await;
            second_state.user_login("driver17", VALID_PASSWORD).await
        });

        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert_eq!(
            [first.is_ok(), second.is_ok()]
                .into_iter()
                .filter(|ok| *ok)
                .count(),
            1
        );

        assert_eq!(state.registry.read().await.active_clients.len(), 1);
    }

    #[tokio::test]
    async fn login_rejects_wrong_credentials() {
        let (state, _db) = state_with_users(&["driver17"]);

        for (username, password) in [
            ("driver17", "Incorrect1"),
            ("driver17", "short1A"),
            ("driver17", "password1"),
            ("missing17", VALID_PASSWORD),
            (" DRIVER17 ", VALID_PASSWORD),
        ] {
            assert_eq!(
                state.user_login(username, password).await.unwrap_err(),
                ServerError::InvalidCredentials,
                "{username:?}"
            );
        }
    }

    #[tokio::test]
    async fn direct_message_is_unicast() {
        let (state, _db) = state_with_users(&["driver17", "driver18"]);
        let mut first_rx = state.user_login("driver17", VALID_PASSWORD).await.unwrap();
        let mut second_rx = state.user_login("driver18", VALID_PASSWORD).await.unwrap();

        state
            .send_admin_direct("driver17", "inspection due")
            .await
            .unwrap();
        assert_eq!(
            timeout(Duration::from_millis(100), first_rx.recv())
                .await
                .unwrap(),
            Some(ServerMessage::TextMessage {
                sender: "FleetAdmin".to_string(),
                content: "inspection due".to_string()
            })
        );
        assert!(matches!(
            second_rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn direct_message_rejects_offline_driver() {
        let (state, _db) = state_with_users(&[]);
        assert_eq!(
            state
                .send_admin_direct("missing", "hello")
                .await
                .unwrap_err(),
            ServerError::RecipientOffline {
                recipient: "missing".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn broadcast_reaches_all_live_drivers() {
        let (state, _db) = state_with_users(&["driver17", "driver18"]);
        let mut first_rx = state.user_login("driver17", VALID_PASSWORD).await.unwrap();
        let mut second_rx = state.user_login("driver18", VALID_PASSWORD).await.unwrap();

        assert_eq!(state.send_admin_broadcast("weather alert").await, 2);
        for receiver in [&mut first_rx, &mut second_rx] {
            assert_eq!(
                timeout(Duration::from_millis(100), receiver.recv())
                    .await
                    .unwrap(),
                Some(ServerMessage::TextMessage {
                    sender: "FleetAdmin".to_string(),
                    content: "weather alert".to_string()
                })
            );
        }
    }

    #[tokio::test]
    async fn closed_mailbox_fails_direct_message() {
        let (state, _db) = state_with_users(&["driver17"]);
        let receiver = state.user_login("driver17", VALID_PASSWORD).await.unwrap();
        drop(receiver);

        assert!(
            state
                .send_admin_direct("driver17", "are you there?")
                .await
                .is_err(),
            "a direct send to a closed client channel must not report delivery"
        );
    }

    #[tokio::test]
    async fn full_mailbox_does_not_block_send() {
        let (state, _db) = state_with_users(&["driver17"]);
        let _receiver = state.user_login("driver17", VALID_PASSWORD).await.unwrap();

        fill_client_mailbox(&state, "driver17").await;
        let send_result = timeout(
            Duration::from_millis(100),
            state.send_admin_direct("driver17", "must-not-deadlock"),
        )
        .await;
        assert!(
            send_result.is_ok(),
            "a full client queue must not indefinitely block admin sends"
        );
        assert_eq!(
            send_result.unwrap().unwrap_err(),
            ServerError::MailboxFull {
                recipient: "driver17".to_string()
            }
        );
    }

    #[tokio::test]
    async fn full_mailbox_does_not_block_cleanup() {
        let (state, _db) = state_with_users(&["driver17"]);
        let receiver = state.user_login("driver17", VALID_PASSWORD).await.unwrap();

        fill_client_mailbox(&state, "driver17").await;
        assert_eq!(
            state
                .send_admin_direct("driver17", "must-not-deadlock")
                .await
                .unwrap_err(),
            ServerError::MailboxFull {
                recipient: "driver17".to_string()
            }
        );
        assert!(
            timeout(
                Duration::from_millis(100),
                state.remove_active_client("driver17")
            )
            .await
            .is_ok(),
            "a full client queue must not block disconnect cleanup"
        );
        drop(receiver);
    }

    #[tokio::test]
    async fn rejects_blank_message() {
        let (state, _db) = state_with_users(&["driver17"]);
        assert_eq!(
            state
                .process_message("driver17", " \t\n ")
                .await
                .unwrap_err(),
            ServerError::EmptyMessage
        );
    }

    #[tokio::test]
    async fn accepts_nonempty_message() {
        let (state, _db) = state_with_users(&["driver17"]);
        assert!(
            state
                .process_message("driver17", "ready for dispatch")
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn rejects_outdated_position() {
        let (state, _db) = state_with_users(&["driver17"]);
        state
            .update_user_position(
                "driver17",
                &Position {
                    latitude: 45.0,
                    longitude: 9.0,
                    timestamp: 100,
                },
            )
            .await
            .unwrap();

        assert_eq!(
            state
                .update_user_position(
                    "driver17",
                    &Position {
                        latitude: 45.1,
                        longitude: 9.1,
                        timestamp: 99,
                    }
                )
                .await
                .unwrap_err(),
            ServerError::OutOfOrderPosition
        );
        let registry = state.registry.read().await;
        assert_eq!(registry.map["driver17"].positions.len(), 1);
        assert_eq!(registry.map["driver17"].positions[0].timestamp, 100);
    }

    #[tokio::test]
    async fn position_updates_transition_state() {
        let (state, _db) = state_with_users(&["driver17"]);
        let first = Position {
            latitude: 45.0,
            longitude: 9.0,
            timestamp: 100,
        };
        let moved = Position {
            latitude: 45.01,
            longitude: 9.0,
            timestamp: 120,
        };

        state
            .update_user_position("driver17", &first)
            .await
            .unwrap();
        assert_eq!(
            state.registry.read().await.map["driver17"].state,
            UserState::Stopped(100)
        );

        state
            .update_user_position("driver17", &moved)
            .await
            .unwrap();
        assert_eq!(
            state.registry.read().await.map["driver17"].state,
            UserState::Moving(120)
        );

        let nearly_stopped = Position {
            timestamp: moved.timestamp + STOPPED_AFTER_SECS - 1,
            ..moved
        };
        state
            .update_user_position("driver17", &nearly_stopped)
            .await
            .unwrap();
        assert_eq!(
            state.registry.read().await.map["driver17"].state,
            UserState::Moving(120)
        );

        let stopped = Position {
            timestamp: moved.timestamp + STOPPED_AFTER_SECS,
            ..moved
        };
        state
            .update_user_position("driver17", &stopped)
            .await
            .unwrap();

        let registry = state.registry.read().await;
        let profile = &registry.map["driver17"];
        assert_eq!(profile.state, UserState::Stopped(stopped.timestamp));
        assert_eq!(profile.positions.len(), 4);
        assert_eq!(profile.positions.last(), Some(&stopped));
    }

    #[tokio::test]
    async fn reconnect_starts_a_fresh_movement_baseline() {
        let (state, _db) = state_with_users(&["driver17"]);
        let first_session = state.user_login("driver17", VALID_PASSWORD).await.unwrap();

        let first = Position {
            latitude: 45.0,
            longitude: 9.0,
            timestamp: 100,
        };
        let moved = Position {
            latitude: 45.01,
            timestamp: 120,
            ..first
        };
        state
            .update_user_position("driver17", &first)
            .await
            .unwrap();
        state
            .update_user_position("driver17", &moved)
            .await
            .unwrap();
        assert_eq!(
            state.registry.read().await.map["driver17"].state,
            UserState::Moving(moved.timestamp)
        );

        state.remove_active_client("driver17").await;
        drop(first_session);
        let _second_session = state.user_login("driver17", VALID_PASSWORD).await.unwrap();

        let relocated = Position {
            latitude: 46.0,
            longitude: 10.0,
            timestamp: 200,
        };
        state
            .update_user_position("driver17", &relocated)
            .await
            .unwrap();
        assert_eq!(
            state.registry.read().await.map["driver17"].state,
            UserState::Stopped(relocated.timestamp)
        );

        let moved_again = Position {
            latitude: 46.01,
            timestamp: 220,
            ..relocated
        };
        state
            .update_user_position("driver17", &moved_again)
            .await
            .unwrap();

        let registry = state.registry.read().await;
        let profile = &registry.map["driver17"];
        assert_eq!(profile.state, UserState::Moving(moved_again.timestamp));
        assert_eq!(
            profile.positions,
            vec![first, moved, relocated, moved_again]
        );
    }

    #[tokio::test]
    async fn position_history_persists_across_restart() {
        let (state, database) = state_with_users(&["driver17"]);
        let positions = [
            Position {
                latitude: 45.0,
                longitude: 9.0,
                timestamp: 100,
            },
            Position {
                latitude: 45.01,
                longitude: 9.0,
                timestamp: 120,
            },
        ];

        for position in positions {
            state
                .update_user_position("driver17", &position)
                .await
                .unwrap();
        }
        drop(state);

        let restored_state = ServerState::new(database.path());
        let restored_registry = restored_state.registry.read().await;
        let restored_profile = &restored_registry.map["driver17"];
        assert_eq!(restored_profile.positions, positions.to_vec());
        assert_eq!(restored_profile.state, UserState::Disconnected);
    }

    #[tokio::test]
    async fn position_update_rejects_unknown_user() {
        let (state, _db) = state_with_users(&[]);

        assert_eq!(
            state
                .update_user_position(
                    "missing",
                    &Position {
                        latitude: 45.0,
                        longitude: 9.0,
                        timestamp: 100,
                    }
                )
                .await
                .unwrap_err(),
            ServerError::UserNotFound
        );
    }

    #[tokio::test]
    async fn position_update_accepts_equal_timestamps() {
        let (state, _db) = state_with_users(&["driver17"]);
        let first = Position {
            latitude: 45.0,
            longitude: 9.0,
            timestamp: 100,
        };
        let moved_at_same_time = Position {
            latitude: 45.01,
            ..first
        };

        state
            .update_user_position("driver17", &first)
            .await
            .unwrap();
        state
            .update_user_position("driver17", &moved_at_same_time)
            .await
            .unwrap();

        let registry = state.registry.read().await;
        let profile = &registry.map["driver17"];
        assert_eq!(profile.positions, vec![first, moved_at_same_time]);
        assert_eq!(profile.state, UserState::Moving(first.timestamp));
    }

    #[tokio::test]
    async fn stats_classify_movement_and_pauses() {
        let (state, _db) = state_with_users(&["driver17"]);
        let start = TimeWindow::CurrentDay.lower_bound_timestamp_at(TEST_NOW) + 1;
        let first = Position {
            latitude: 45.0,
            longitude: 9.0,
            timestamp: start,
        };
        let moved = Position {
            latitude: 45.0,
            longitude: 9.01,
            timestamp: start + 120,
        };
        {
            let mut registry = state.registry.write().await;
            let profile = registry.map.get_mut("driver17").unwrap();
            profile.positions = vec![
                first,
                Position {
                    timestamp: start + 60,
                    ..first
                },
                moved,
            ];
        }

        let stats = state
            .calculate_user_stats("driver17", TimeWindow::CurrentDay)
            .await
            .unwrap();
        assert_eq!(stats.pause_duration_secs, 60);
        assert_eq!(stats.movement_duration_secs, 60);
        assert!((stats.total_distance_km - first.haversine_distance_km(&moved)).abs() < 1e-9);
        assert!(stats.average_speed_kmh > 0.0);
    }

    #[tokio::test]
    async fn stats_interpolates_window_boundary_crossing() {
        let (state, _db) = state_with_users(&["driver17"]);
        let lower_bound = TimeWindow::CurrentDay.lower_bound_timestamp_at(TEST_NOW);

        let p1 = Position {
            latitude: 45.0,
            longitude: 9.0,
            timestamp: lower_bound - 30, // 30s before lower_bound (yesterday)
        };
        let p2 = Position {
            latitude: 45.04,
            longitude: 9.0,
            timestamp: lower_bound + 10, // 10s after lower_bound (today) - 40s total segment
        };

        {
            let mut registry = state.registry.write().await;
            let profile = registry.map.get_mut("driver17").unwrap();
            profile.positions = vec![p1, p2];
        }

        let stats = state
            .calculate_user_stats("driver17", TimeWindow::CurrentDay)
            .await
            .unwrap();
        // 10s of duration inside window out of 40s total segment (25%)
        assert_eq!(stats.movement_duration_secs, 10);
        let expected_dist = p1.haversine_distance_km(&p2) * 0.25;
        assert!((stats.total_distance_km - expected_dist).abs() < 1e-6);
    }

    #[tokio::test]
    async fn stats_for_empty_history_are_zero() {
        let (state, _db) = state_with_users(&["driver17"]);

        let stats = state
            .calculate_user_stats("driver17", TimeWindow::CurrentWeek)
            .await
            .unwrap();

        assert_eq!(stats.time_window, TimeWindow::CurrentWeek);
        assert_eq!(stats.total_distance_km, 0.0);
        assert_eq!(stats.average_speed_kmh, 0.0);
        assert_eq!(stats.movement_duration_secs, 0);
        assert_eq!(stats.pause_duration_secs, 0);
    }

    #[tokio::test]
    async fn stats_treat_gps_jitter_as_a_pause() {
        let (state, _db) = state_with_users(&["driver17"]);
        let start = TimeWindow::CurrentDay.lower_bound_timestamp_at(TEST_NOW) + 1;
        let first = Position {
            latitude: 45.0,
            longitude: 9.0,
            timestamp: start,
        };
        let jittered = Position {
            longitude: 9.000_001,
            timestamp: start + 60,
            ..first
        };
        assert!(first.haversine_distance_km(&jittered) < GPS_NOISE_THRESHOLD_KM);

        state
            .update_user_position("driver17", &first)
            .await
            .unwrap();
        state
            .update_user_position("driver17", &jittered)
            .await
            .unwrap();

        //we need this to avoid the elapsed tail being counted as movement, since the last position is within the current time window
        state.remove_active_client("driver17").await;

        let stats = state
            .calculate_user_stats("driver17", TimeWindow::CurrentDay)
            .await
            .unwrap();

        assert_eq!(stats.total_distance_km, 0.0);
        assert_eq!(stats.average_speed_kmh, 0.0);
        assert_eq!(stats.movement_duration_secs, 0);
        assert_eq!(stats.pause_duration_secs, 60);
    }

    #[tokio::test]
    async fn stats_ignore_positions_before_the_selected_window() {
        let (state, _db) = state_with_users(&["driver17"]);
        let lower_bound = TimeWindow::CurrentDay.lower_bound_timestamp_at(TEST_NOW);
        let before_window = Position {
            latitude: 45.0,
            longitude: 9.0,
            timestamp: lower_bound.saturating_sub(120),
        };

        state
            .update_user_position("driver17", &before_window)
            .await
            .unwrap();

        let stats = state
            .calculate_user_stats("driver17", TimeWindow::CurrentDay)
            .await
            .unwrap();

        assert_eq!(stats.total_distance_km, 0.0);
        assert_eq!(stats.average_speed_kmh, 0.0);
        assert_eq!(stats.movement_duration_secs, 0);
        assert_eq!(stats.pause_duration_secs, 0);
    }

    #[tokio::test]
    async fn stats_add_elapsed_tail_to_the_current_state() {
        let (state, _db) = state_with_users(&["driver17"]);
        let time_window = TimeWindow::CurrentDay;
        let timestamp = TEST_NOW - 3;
        let last_position = Position {
            latitude: 45.0,
            longitude: 9.0,
            timestamp,
        };
        state
            .update_user_position("driver17", &last_position)
            .await
            .unwrap();

        let stopped_stats = state
            .calculate_user_stats("driver17", time_window)
            .await
            .unwrap();
        assert_eq!(stopped_stats.pause_duration_secs, 3);
        assert_eq!(stopped_stats.movement_duration_secs, 0);

        state
            .registry
            .write()
            .await
            .map
            .get_mut("driver17")
            .unwrap()
            .state = UserState::Moving(timestamp);
        let moving_stats = state
            .calculate_user_stats("driver17", time_window)
            .await
            .unwrap();
        assert_eq!(moving_stats.movement_duration_secs, 3);
        assert_eq!(moving_stats.pause_duration_secs, 0);
    }

    #[tokio::test]
    async fn driver_list_reports_each_users_position_count_and_state() {
        let (state, _db) = state_with_users(&["driver17", "driver18"]);
        state
            .update_user_position(
                "driver17",
                &Position {
                    latitude: 45.0,
                    longitude: 9.0,
                    timestamp: 100,
                },
            )
            .await
            .unwrap();
        state
            .update_user_position(
                "driver17",
                &Position {
                    latitude: 45.01,
                    longitude: 9.0,
                    timestamp: 101,
                },
            )
            .await
            .unwrap();

        let mut drivers = state.get_driver_list().await;
        drivers.sort_by(|left, right| left.0.cmp(&right.0));

        assert_eq!(
            drivers,
            vec![
                ("driver17".to_string(), UserState::Moving(101), 2),
                ("driver18".to_string(), UserState::Disconnected, 0),
            ]
        );
    }

    #[tokio::test]
    async fn stats_count_only_movement_after_the_window_starts() {
        let (state, _db) = state_with_users(&["driver17"]);
        let window_start = TimeWindow::CurrentDay.lower_bound_timestamp_at(TEST_NOW);
        let before_window = Position {
            latitude: 45.0,
            longitude: 9.0,
            timestamp: window_start.saturating_sub(60),
        };
        let inside_window = Position {
            latitude: 45.01,
            longitude: 9.0,
            timestamp: window_start + 60,
        };

        state
            .update_user_position("driver17", &before_window)
            .await
            .unwrap();
        state
            .update_user_position("driver17", &inside_window)
            .await
            .unwrap();
        state.remove_active_client("driver17").await;

        let stats = state
            .calculate_user_stats("driver17", TimeWindow::CurrentDay)
            .await
            .unwrap();

        assert_eq!(stats.movement_duration_secs, 60);
    }

    #[tokio::test]
    async fn broadcasts_incoming_driver_message_to_subscribers() {
        let (state, _db) = state_with_users(&["driver17"]);
        let mut rx = state.driver_message_sender.subscribe();

        let res = state.process_message("driver17", "Route 66 clear").await;
        assert!(res.is_ok());

        let received = rx
            .recv()
            .await
            .expect("message should be broadcasted to subscriber");
        assert_eq!(received.0, "driver17");
        assert_eq!(received.1, "Route 66 clear");
    }

    #[tokio::test]
    async fn rejects_empty_message_and_does_not_broadcast() {
        let (state, _db) = state_with_users(&["driver17"]);
        let mut rx = state.driver_message_sender.subscribe();

        let res = state.process_message("driver17", "   ").await;
        assert!(matches!(
            res,
            Err(super::super::error::ServerError::EmptyMessage)
        ));

        // Verify that nothing was sent on the broadcast channel
        let recv_result =
            tokio::time::timeout(tokio::time::Duration::from_millis(50), rx.recv()).await;
        assert!(
            recv_result.is_err(),
            "empty message should not emit on broadcast channel"
        );
    }
}
