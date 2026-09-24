mod common;

use common::{
    TEST_TIMESTAMP, TestDatabase, TestSession, TestTimeSource, VALID_PASSWORD, admin_message,
    sample_position_asti, sample_position_torino, start_test_server, wait_for_position_count,
};
use georuggine::server::error::ServerError;
use georuggine::server::runtime::{CpuLoggerConfig, Server, ServerConfig};
use georuggine::server::state::ServerState;
use georuggine::{
    ClientMessage, Position, STOPPED_AFTER_SECS, ServerMessage, TimeWindow, UserState,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio::time::timeout;

const RACE_TIMEOUT: Duration = Duration::from_secs(20);

// =========================================================================
// In-Memory Duplex Test Helpers (Vittorio)
// =========================================================================

async fn auth_response(session: &mut TestSession, message: ClientMessage) -> ServerMessage {
    session.send(&message).await;
    session.next_message().await
}

fn assert_single_auth_winner(responses: &[ServerMessage; 2], loser_error: ServerError) {
    let expected_error = loser_error.to_string();
    let success_count = responses
        .iter()
        .filter(|response| matches!(response, ServerMessage::AuthResult(Ok(()))))
        .count();
    let failure_count = responses
        .iter()
        .filter(|response| {
            matches!(response, ServerMessage::AuthResult(Err(error)) if error == &expected_error)
        })
        .count();

    assert_eq!(success_count, 1, "responses: {responses:?}");
    assert_eq!(failure_count, 1, "responses: {responses:?}");
}

async fn driver_status(state: &ServerState, username: &str) -> (UserState, usize) {
    state
        .get_driver_list()
        .await
        .into_iter()
        .find(|(name, _, _)| name == username)
        .map(|(_, state, positions)| (state, positions))
        .expect("driver must be listed")
}

fn movement_sequence() -> (Position, Position, Position) {
    let first = Position {
        timestamp: TEST_TIMESTAMP.saturating_sub(STOPPED_AFTER_SECS + 2),
        ..sample_position_torino()
    };
    let moved = Position {
        longitude: first.longitude + 0.01,
        timestamp: first.timestamp + 1,
        ..first
    };
    let stopped = Position {
        timestamp: moved.timestamp + STOPPED_AFTER_SECS,
        ..moved
    };

    (first, moved, stopped)
}

fn saved_password(database: &TestDatabase, username: &str) -> String {
    let connection = sqlite::open(database.path()).expect("test database must open");
    let mut statement = connection
        .prepare("SELECT password FROM users WHERE username = ?")
        .expect("password query must prepare");
    statement.bind((1, username)).expect("username must bind");

    assert_eq!(
        statement.next().expect("password query must run"),
        sqlite::State::Row
    );
    statement
        .read::<String, _>("password")
        .expect("stored password must be readable")
}

fn saved_user_count(database: &TestDatabase, username: &str) -> i64 {
    let connection = sqlite::open(database.path()).expect("test database must open");
    let mut statement = connection
        .prepare("SELECT COUNT(*) FROM users WHERE username = ?")
        .expect("user count query must prepare");
    statement.bind((1, username)).expect("username must bind");

    assert_eq!(
        statement.next().expect("user count query must run"),
        sqlite::State::Row
    );
    statement
        .read::<i64, _>(0)
        .expect("user count must be readable")
}

fn load_saved_positions(database: &TestDatabase, username: &str) -> Vec<Position> {
    let connection = sqlite::open(database.path()).expect("test database must open");
    let mut statement = connection
        .prepare(
            "SELECT latitude, longitude, timestamp \
             FROM positions WHERE username = ? ORDER BY id",
        )
        .expect("positions query must prepare");
    statement.bind((1, username)).expect("username must bind");

    let mut positions = Vec::new();
    while statement.next().expect("positions query must run") == sqlite::State::Row {
        positions.push(Position {
            latitude: statement
                .read::<f64, _>("latitude")
                .expect("stored latitude must be readable"),
            longitude: statement
                .read::<f64, _>("longitude")
                .expect("stored longitude must be readable"),
            timestamp: statement
                .read::<i64, _>("timestamp")
                .expect("stored timestamp must be readable") as u64,
        });
    }

    positions
}

// =========================================================================
// Real TCP Loopback Test Helpers (Nicola)
// =========================================================================

async fn connect_client(addr: SocketAddr) -> (Lines<BufReader<OwnedReadHalf>>, OwnedWriteHalf) {
    let stream = TcpStream::connect(addr)
        .await
        .expect("client must connect to test server");
    let (read_half, write_half) = stream.into_split();
    let reader = BufReader::new(read_half).lines();
    (reader, write_half)
}

async fn send_msg(writer: &mut OwnedWriteHalf, msg: &ClientMessage) {
    let mut json_str = serde_json::to_string(msg).expect("serialization failed");
    json_str.push('\n');
    writer
        .write_all(json_str.as_bytes())
        .await
        .expect("write to server failed");
    writer.flush().await.expect("flush to server failed");
}

async fn read_msg(lines: &mut Lines<BufReader<OwnedReadHalf>>) -> ServerMessage {
    let line = timeout(Duration::from_secs(2), lines.next_line())
        .await
        .expect("read_msg timed out")
        .expect("io error reading line")
        .expect("unexpected EOF on server connection");

    serde_json::from_str(&line).expect("server line is not valid ServerMessage JSON")
}

// =========================================================================
// Vittorio Integration Tests (IS-01 to IS-05b + Concurrency)
// =========================================================================

#[tokio::test]
async fn register_restart_login() {
    let database = TestDatabase::new();
    let state = database.state();
    let receiver = state
        .register_user("driver01", VALID_PASSWORD)
        .await
        .expect("valid user must register");

    assert_ne!(saved_password(&database, "driver01"), VALID_PASSWORD);

    state.remove_active_client("driver01").await;
    drop(receiver);
    drop(state);

    let restarted = database.state();
    let _receiver = restarted
        .user_login("driver01", VALID_PASSWORD)
        .await
        .expect("persisted user must log in after restart");
}

#[tokio::test]
async fn positions_update_state() {
    let database = TestDatabase::new();
    let state = database.state();
    let _receiver = state
        .register_user("driver02", VALID_PASSWORD)
        .await
        .expect("driver must register");
    let (first, moved, stopped) = movement_sequence();

    state
        .update_user_position("driver02", &first)
        .await
        .expect("first position must be accepted");
    assert_eq!(
        driver_status(&state, "driver02").await,
        (UserState::Stopped(first.timestamp), 1)
    );

    state
        .update_user_position("driver02", &moved)
        .await
        .expect("changed position must be accepted");
    assert_eq!(
        driver_status(&state, "driver02").await,
        (UserState::Moving(moved.timestamp), 2)
    );

    state
        .update_user_position("driver02", &stopped)
        .await
        .expect("stationary position must be accepted");
    assert_eq!(
        driver_status(&state, "driver02").await,
        (UserState::Stopped(stopped.timestamp), 3)
    );
}

#[tokio::test]
async fn positions_restore_after_restart() {
    let database = TestDatabase::new();
    let state = database.state();
    let receiver = state
        .register_user("driver03", VALID_PASSWORD)
        .await
        .expect("driver must register");
    let (first, moved, _) = movement_sequence();

    state
        .update_user_position("driver03", &first)
        .await
        .expect("first position must be accepted");
    state
        .update_user_position("driver03", &moved)
        .await
        .expect("changed position must be accepted");
    state.remove_active_client("driver03").await;
    drop(receiver);
    drop(state);

    let restarted = database.state();
    assert_eq!(
        driver_status(&restarted, "driver03").await,
        (UserState::Disconnected, 2)
    );
    assert_eq!(
        load_saved_positions(&database, "driver03"),
        vec![first, moved]
    );
}

#[tokio::test]
async fn session_requires_authentication() {
    let database = TestDatabase::new();
    let state = database.state();
    let mut session = TestSession::start(state);
    let position = Position {
        timestamp: TEST_TIMESTAMP,
        ..sample_position_torino()
    };

    session.send(&ClientMessage::UpdatePosition(position)).await;
    assert_eq!(
        session.next_message().await,
        ServerMessage::ErrorMessage(ServerError::Unauthenticated.to_string())
    );

    session.close().await;
}

#[tokio::test]
async fn malformed_payload_recovers() {
    let database = TestDatabase::new();
    let state = database.state();
    let mut session = TestSession::start(state);

    session.send_raw(b"not-json\n").await;
    assert!(matches!(
        session.next_message().await,
        ServerMessage::ErrorMessage(_)
    ));

    session.register("driver04").await;
    session.close().await;
}

#[tokio::test]
async fn registered_session_records_position() {
    let database = TestDatabase::new();
    let state = database.state();
    let mut session = TestSession::start(state.clone());
    let position = Position {
        timestamp: TEST_TIMESTAMP,
        ..sample_position_torino()
    };

    session.register("driver05").await;
    session.send(&ClientMessage::UpdatePosition(position)).await;

    wait_for_position_count(&state, "driver05", 1).await;
    assert_eq!(load_saved_positions(&database, "driver05"), vec![position]);
    session.close().await;
}

#[tokio::test]
async fn broadcast_reaches_sessions() {
    let database = TestDatabase::new();
    let state = database.state();
    let mut first = TestSession::start(state.clone());
    let mut second = TestSession::start(state.clone());
    first.register("driver06").await;
    second.register("driver07").await;

    assert_eq!(state.send_admin_broadcast("Road closed").await, 2);
    let broadcast = admin_message("Road closed");
    assert_eq!(first.next_message().await, broadcast);
    assert_eq!(second.next_message().await, broadcast);

    first.close().await;
    second.close().await;
}

#[tokio::test]
async fn direct_message_isolated() {
    let database = TestDatabase::new();
    let state = database.state();
    let mut recipient = TestSession::start(state.clone());
    let mut other_driver = TestSession::start(state.clone());
    recipient.register("driver08").await;
    other_driver.register("driver09").await;

    state
        .send_admin_direct("driver08", "Use route B")
        .await
        .expect("online driver must receive direct message");
    assert_eq!(recipient.next_message().await, admin_message("Use route B"));

    assert_eq!(state.send_admin_broadcast("Isolation sentinel").await, 2);
    let sentinel = admin_message("Isolation sentinel");
    assert_eq!(recipient.next_message().await, sentinel);
    assert_eq!(
        other_driver.next_message().await,
        sentinel,
        "the other driver's next message must be the broadcast sentinel"
    );

    recipient.close().await;
    other_driver.close().await;
}

#[tokio::test]
async fn disconnect_marks_driver_offline() {
    let database = TestDatabase::new();
    let state = database.state();
    let mut session = TestSession::start(state.clone());
    session.register("driver10").await;
    session.close().await;

    assert_eq!(
        state.send_admin_direct("driver10", "Are you there?").await,
        Err(ServerError::RecipientOffline {
            recipient: "driver10".to_string(),
        })
    );
}

#[tokio::test]
async fn driver_text_reaches_admin() {
    let database = TestDatabase::new();
    let state = database.state();
    let mut notifications = state.driver_message_sender.subscribe();
    let mut session = TestSession::start(state);
    session.register("driver11").await;

    session
        .send(&ClientMessage::SendText {
            content: "Arriving at the depot".to_string(),
        })
        .await;
    assert_eq!(
        timeout(Duration::from_secs(2), notifications.recv())
            .await
            .expect("admin did not receive driver text")
            .expect("driver notification channel failed"),
        ("driver11".to_string(), "Arriving at the depot".to_string())
    );

    session.close().await;
}

#[tokio::test]
async fn blank_driver_text_is_rejected() {
    let database = TestDatabase::new();
    let state = database.state();
    let mut notifications = state.driver_message_sender.subscribe();
    let mut session = TestSession::start(state);
    session.register("driver12").await;

    session
        .send(&ClientMessage::SendText {
            content: "   ".to_string(),
        })
        .await;
    assert_eq!(
        session.next_message().await,
        ServerMessage::ErrorMessage(ServerError::EmptyMessage.to_string())
    );

    session
        .send(&ClientMessage::SendText {
            content: "Valid sentinel".to_string(),
        })
        .await;
    assert_eq!(
        timeout(Duration::from_secs(2), notifications.recv())
            .await
            .expect("admin did not receive the valid sentinel")
            .expect("driver notification channel failed"),
        ("driver12".to_string(), "Valid sentinel".to_string()),
        "the first admin notification after the rejection must be the valid sentinel"
    );

    session.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_duplicate_registration_has_one_winner() {
    let database = TestDatabase::new();
    let state = database.state();
    let barrier = Arc::new(Barrier::new(2));
    let mut first = TestSession::start(state.clone());
    let mut second = TestSession::start(state.clone());

    let first_barrier = barrier.clone();
    let first_attempt = async {
        first_barrier.wait().await;
        auth_response(
            &mut first,
            ClientMessage::Register {
                username: "raceRegister".to_string(),
                password: VALID_PASSWORD.to_string(),
            },
        )
        .await
    };
    let second_attempt = async {
        barrier.wait().await;
        auth_response(
            &mut second,
            ClientMessage::Register {
                username: "raceRegister".to_string(),
                password: VALID_PASSWORD.to_string(),
            },
        )
        .await
    };

    let responses = timeout(RACE_TIMEOUT, async {
        let (first, second) = tokio::join!(first_attempt, second_attempt);
        [first, second]
    })
    .await
    .expect("concurrent registrations must finish");

    assert_single_auth_winner(&responses, ServerError::UsernameTaken);
    assert_eq!(saved_user_count(&database, "raceRegister"), 1);
    assert_eq!(state.registry.read().await.active_clients.len(), 1);

    first.close().await;
    second.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_login_has_one_winner() {
    let database = TestDatabase::new();
    let state = database.state();
    let receiver = state
        .register_user("raceLogin", VALID_PASSWORD)
        .await
        .expect("race test account must be created");
    state.remove_active_client("raceLogin").await;
    drop(receiver);

    let barrier = Arc::new(Barrier::new(2));
    let mut first = TestSession::start(state.clone());
    let mut second = TestSession::start(state.clone());
    let login = || ClientMessage::Login {
        username: "raceLogin".to_string(),
        password: VALID_PASSWORD.to_string(),
    };

    let first_barrier = barrier.clone();
    let first_attempt = async {
        first_barrier.wait().await;
        auth_response(&mut first, login()).await
    };
    let second_attempt = async {
        barrier.wait().await;
        auth_response(&mut second, login()).await
    };

    let responses = timeout(RACE_TIMEOUT, async {
        let (first, second) = tokio::join!(first_attempt, second_attempt);
        [first, second]
    })
    .await
    .expect("concurrent logins must finish");

    assert_single_auth_winner(&responses, ServerError::AlreadyLoggedIn);
    assert_eq!(state.registry.read().await.active_clients.len(), 1);

    first.close().await;
    second.close().await;

    let mut retry = TestSession::start(state);
    assert_eq!(
        auth_response(&mut retry, login()).await,
        ServerMessage::AuthResult(Ok(())),
        "disconnect cleanup must allow a later login"
    );
    retry.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_send_racing_disconnect_leaves_driver_offline() {
    let database = TestDatabase::new();
    let state = database.state();
    let mut session = TestSession::start(state.clone());
    session.register("raceDisconnect").await;

    let send_state = state.clone();
    let (disconnect_result, send_result) = timeout(RACE_TIMEOUT, async {
        tokio::join!(
            session.disconnect(),
            send_state.send_admin_direct("raceDisconnect", "Concurrent message")
        )
    })
    .await
    .expect("disconnect and direct send must finish");

    if let Err(error) = disconnect_result {
        assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::ConnectionReset
            ),
            "unexpected session error: {error}"
        );
    }
    assert!(
        matches!(
            send_result,
            Ok(()) | Err(ServerError::RecipientOffline { .. })
        ),
        "send result must reflect whether disconnect cleanup won the race"
    );
    assert_eq!(
        state
            .send_admin_direct("raceDisconnect", "After disconnect")
            .await,
        Err(ServerError::RecipientOffline {
            recipient: "raceDisconnect".to_string(),
        })
    );

    let mut retry = TestSession::start(state);
    assert_eq!(
        auth_response(
            &mut retry,
            ClientMessage::Login {
                username: "raceDisconnect".to_string(),
                password: VALID_PASSWORD.to_string(),
            },
        )
        .await,
        ServerMessage::AuthResult(Ok(()))
    );
    retry.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clients_persist_independent_positions() {
    const DRIVER_COUNT: usize = 4;

    let database = TestDatabase::new();
    let state = database.state();
    let barrier = Arc::new(Barrier::new(DRIVER_COUNT));
    let timestamp = TEST_TIMESTAMP;
    let mut clients = JoinSet::new();

    for index in 0..DRIVER_COUNT {
        let state = state.clone();
        let barrier = barrier.clone();
        clients.spawn(async move {
            let username = format!("raceDriver{index}");
            let position = Position {
                latitude: 45.0 + index as f64 / 100.0,
                longitude: 7.0 + index as f64 / 100.0,
                timestamp: timestamp + index as u64,
            };
            let mut session = TestSession::start(state);

            barrier.wait().await;
            assert_eq!(
                auth_response(
                    &mut session,
                    ClientMessage::Register {
                        username: username.clone(),
                        password: VALID_PASSWORD.to_string(),
                    },
                )
                .await,
                ServerMessage::AuthResult(Ok(()))
            );
            session.send(&ClientMessage::UpdatePosition(position)).await;
            session.close().await;

            (username, position)
        });
    }

    let mut expected_positions = timeout(RACE_TIMEOUT, async {
        let mut completed = Vec::with_capacity(DRIVER_COUNT);
        while let Some(result) = clients.join_next().await {
            completed.push(result.expect("concurrent client task must not panic"));
        }
        completed
    })
    .await
    .expect("concurrent clients must finish");
    expected_positions.sort_by(|left, right| left.0.cmp(&right.0));

    drop(state);
    let restarted = database.state();
    let mut drivers = restarted.get_driver_list().await;
    drivers.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        drivers,
        expected_positions
            .iter()
            .map(|(username, _)| (username.clone(), UserState::Disconnected, 1))
            .collect::<Vec<_>>()
    );

    for (username, position) in expected_positions {
        assert_eq!(load_saved_positions(&database, &username), vec![position]);
    }
}

// =========================================================================
// Real TCP Loopback Integration Tests (Nicola)
// =========================================================================

#[tokio::test]
async fn tcp_authentication_lifecycle() {
    let test_db = TestDatabase::new();
    let server = Server::bind_with_time_source(
        ServerConfig {
            addr: "127.0.0.1:0".to_string(),
            db_path: test_db.path().to_path_buf(),
            cpu_logger: CpuLoggerConfig {
                log_path: test_db.path().with_extension("cpu.log"),
                ..Default::default()
            },
        },
        TestTimeSource::new(TEST_TIMESTAMP),
    )
    .await
    .expect("server must bind using ServerConfig");
    let addr = server.addr().expect("server address must be available");
    let state = server.state();
    let server_handle = server.start();

    // 1. Client 1 connects and registers
    let (mut client1_lines, mut client1_writer) = connect_client(addr).await;
    send_msg(
        &mut client1_writer,
        &ClientMessage::Register {
            username: "driveralpha".to_string(),
            password: VALID_PASSWORD.to_string(),
        },
    )
    .await;

    let res = read_msg(&mut client1_lines).await;
    assert_eq!(res, ServerMessage::AuthResult(Ok(())));

    // Client 1 is now logged in automatically after registration
    assert!(state.registry.read().await.active_clients.contains_key("driveralpha"));

    // 2. Client 2 connects and tries to log in as driveralpha simultaneously -> denied (AlreadyLoggedIn)
    let (mut client2_lines, mut client2_writer) = connect_client(addr).await;
    send_msg(
        &mut client2_writer,
        &ClientMessage::Login {
            username: "driveralpha".to_string(),
            password: VALID_PASSWORD.to_string(),
        },
    )
    .await;

    let res2 = read_msg(&mut client2_lines).await;
    match res2 {
        ServerMessage::AuthResult(Err(err)) => {
            assert!(err.contains("already logged in"), "Unexpected error: {err}");
        }
        other => panic!("Expected AuthResult(Err), got: {:?}", other),
    }

    // 3. Client 1 disconnects (drop writer and reader)
    drop(client1_writer);
    drop(client1_lines);

    // Give server a moment to process EOF and remove active client
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!state.registry.read().await.active_clients.contains_key("driveralpha"));

    // 4. Now Client 2 can successfully log in as driveralpha
    send_msg(
        &mut client2_writer,
        &ClientMessage::Login {
            username: "driveralpha".to_string(),
            password: VALID_PASSWORD.to_string(),
        },
    )
    .await;

    let res3 = read_msg(&mut client2_lines).await;
    assert_eq!(res3, ServerMessage::AuthResult(Ok(())));

    // Drop Client 2 so driveralpha is offline before testing wrong password
    drop(client2_writer);
    drop(client2_lines);
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 5. Connect Client 3 with invalid password -> denied (InvalidCredentials)
    let (mut client3_lines, mut client3_writer) = connect_client(addr).await;
    send_msg(
        &mut client3_writer,
        &ClientMessage::Login {
            username: "driveralpha".to_string(),
            password: "WrongPassword9!".to_string(),
        },
    )
    .await;

    let res4 = read_msg(&mut client3_lines).await;
    match res4 {
        ServerMessage::AuthResult(Err(err)) => {
            assert!(err.contains("username or password"), "Unexpected error: {err}");
        }
        other => panic!("Expected AuthResult(Err), got: {:?}", other),
    }

    server_handle.stop().await.expect("server must stop gracefully");
}

#[tokio::test]
async fn tcp_positions_drive_state_and_stats() {
    let test_db = TestDatabase::new();
    let (server_handle, addr, state) = start_test_server(test_db.path()).await;

    let (mut lines, mut writer) = connect_client(addr).await;
    send_msg(
        &mut writer,
        &ClientMessage::Register {
            username: "driverfsm".to_string(),
            password: VALID_PASSWORD.to_string(),
        },
    )
    .await;
    assert_eq!(read_msg(&mut lines).await, ServerMessage::AuthResult(Ok(())));

    let base_time = TEST_TIMESTAMP.saturating_sub(500);

    // 1. Initial position in Turin
    let p1 = Position {
        latitude: 45.0703,
        longitude: 7.6869,
        timestamp: base_time,
    };
    send_msg(&mut writer, &ClientMessage::UpdatePosition(p1)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let reg = state.registry.read().await;
    let profile = reg.map.get("driverfsm").expect("user must exist");
    assert_eq!(profile.state, UserState::Stopped(base_time));
    drop(reg);

    // 2. Movement position towards Asti 30 seconds later
    let p2 = Position {
        latitude: 45.0100,
        longitude: 7.8000,
        timestamp: base_time + 30,
    };
    send_msg(&mut writer, &ClientMessage::UpdatePosition(p2)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let reg = state.registry.read().await;
    let profile = reg.map.get("driverfsm").expect("user must exist");
    assert_eq!(profile.state, UserState::Moving(base_time + 30));
    drop(reg);

    // 3. Stationary position at same location after 190s (>= 180s timeout) -> transitions to Stopped
    let p3 = Position {
        latitude: 45.0100,
        longitude: 7.8000,
        timestamp: base_time + 220,
    };
    send_msg(&mut writer, &ClientMessage::UpdatePosition(p3)).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let reg = state.registry.read().await;
    let profile = reg.map.get("driverfsm").expect("user must exist");
    assert_eq!(profile.state, UserState::Stopped(base_time + 220));
    drop(reg);

    // 4. Verify telemetry analytics
    let stats = state
        .calculate_user_stats("driverfsm", TimeWindow::CurrentDay)
        .await
        .expect("stats calculation must succeed");

    assert!(stats.total_distance_km > 5.0, "Distance should be > 5km: {}", stats.total_distance_km);
    assert!(stats.movement_duration_secs >= 30, "Movement duration should be >= 30s: {}", stats.movement_duration_secs);
    assert!(stats.pause_duration_secs >= 180, "Pause duration should be >= 180s: {}", stats.pause_duration_secs);

    server_handle.stop().await.expect("server must stop gracefully");
}

#[tokio::test]
async fn tcp_direct_broadcast_and_driver_messages() {
    let test_db = TestDatabase::new();
    let (server_handle, addr, state) = start_test_server(test_db.path()).await;

    // Connect truck1
    let (mut t1_lines, mut t1_writer) = connect_client(addr).await;
    send_msg(
        &mut t1_writer,
        &ClientMessage::Register {
            username: "truck1".to_string(),
            password: VALID_PASSWORD.to_string(),
        },
    )
    .await;
    assert_eq!(read_msg(&mut t1_lines).await, ServerMessage::AuthResult(Ok(())));

    // Connect truck2
    let (mut t2_lines, mut t2_writer) = connect_client(addr).await;
    send_msg(
        &mut t2_writer,
        &ClientMessage::Register {
            username: "truck2".to_string(),
            password: VALID_PASSWORD.to_string(),
        },
    )
    .await;
    assert_eq!(read_msg(&mut t2_lines).await, ServerMessage::AuthResult(Ok(())));

    // 1. Admin sends broadcast
    let broadcast_content = "Severe storm approaching highway A4";
    let delivered = state.send_admin_broadcast(broadcast_content).await;
    assert_eq!(delivered, 2, "Broadcast must reach both connected trucks");

    let t1_msg = read_msg(&mut t1_lines).await;
    let t2_msg = read_msg(&mut t2_lines).await;

    assert_eq!(
        t1_msg,
        ServerMessage::TextMessage {
            sender: "FleetAdmin".to_string(),
            content: broadcast_content.to_string(),
        }
    );
    assert_eq!(
        t2_msg,
        ServerMessage::TextMessage {
            sender: "FleetAdmin".to_string(),
            content: broadcast_content.to_string(),
        }
    );

    // 2. Admin sends direct message to truck1 only
    let direct_content = "Please refuel at exit 14";
    state
        .send_admin_direct("truck1", direct_content)
        .await
        .expect("direct send to truck1 must succeed");

    let t1_direct = read_msg(&mut t1_lines).await;
    assert_eq!(
        t1_direct,
        ServerMessage::TextMessage {
            sender: "FleetAdmin".to_string(),
            content: direct_content.to_string(),
        }
    );

    // A subsequent broadcast establishes ordering: if the direct message leaked
    // to truck2, it would be read before this sentinel and fail the assertion.
    let isolation_sentinel = "Direct-message isolation verified";
    assert_eq!(state.send_admin_broadcast(isolation_sentinel).await, 2);
    let sentinel = admin_message(isolation_sentinel);
    assert_eq!(read_msg(&mut t1_lines).await, sentinel);
    assert_eq!(
        read_msg(&mut t2_lines).await,
        sentinel,
        "truck2 must receive the broadcast sentinel without a preceding direct message"
    );

    // 3. truck1 sends driver message back to server
    let mut driver_rx = state.driver_message_sender.subscribe();
    send_msg(
        &mut t1_writer,
        &ClientMessage::SendText {
            content: "Acknowledged fuel stop".to_string(),
        },
    )
    .await;

    let (driver_sender, driver_text) = timeout(Duration::from_secs(2), driver_rx.recv())
        .await
        .expect("driver broadcast timed out")
        .expect("driver broadcast recv error");

    assert_eq!(driver_sender, "truck1");
    assert_eq!(driver_text, "Acknowledged fuel stop");

    server_handle.stop().await.expect("server must stop gracefully");
}

#[tokio::test]
async fn sqlite_state_survives_restart() {
    let test_db = TestDatabase::new();

    // Server Phase 1: Register and record positions
    {
        let (server1_handle, addr, _state1) = start_test_server(test_db.path()).await;

        let (mut lines, mut writer) = connect_client(addr).await;
        send_msg(
            &mut writer,
            &ClientMessage::Register {
                username: "persistentdriver".to_string(),
                password: VALID_PASSWORD.to_string(),
            },
        )
        .await;
        assert_eq!(read_msg(&mut lines).await, ServerMessage::AuthResult(Ok(())));

        let pos1 = sample_position_torino();
        let pos2 = sample_position_asti();
        send_msg(&mut writer, &ClientMessage::UpdatePosition(pos1)).await;
        send_msg(&mut writer, &ClientMessage::UpdatePosition(pos2)).await;

        // Ensure asynchronous SQLite writes finish
        tokio::time::sleep(Duration::from_millis(150)).await;

        server1_handle.stop().await.expect("server 1 must stop gracefully");
        // state1 dropped here
    }

    // Server Phase 2: Start a brand-new ServerState on the SAME SQLite file
    {
        let (server2_handle, addr2, state2) = start_test_server(test_db.path()).await;

        // User should already be present in registry restored from SQLite
        {
            let reg = state2.registry.read().await;
            let profile = reg.map.get("persistentdriver").expect("user must be restored from SQLite");
            assert_eq!(profile.state, UserState::Disconnected);
            assert_eq!(profile.positions.len(), 2, "Position history must be restored from SQLite");
        }

        // Connecting client should be able to log in with original credentials
        let (mut lines2, mut writer2) = connect_client(addr2).await;
        send_msg(
            &mut writer2,
            &ClientMessage::Login {
                username: "persistentdriver".to_string(),
                password: VALID_PASSWORD.to_string(),
            },
        )
        .await;
        assert_eq!(read_msg(&mut lines2).await, ServerMessage::AuthResult(Ok(())));

        server2_handle.stop().await.expect("server 2 must stop gracefully");
    }
}

#[tokio::test]
async fn tcp_recovers_after_malformed_input() {
    let test_db = TestDatabase::new();
    let (server_handle, addr, state) = start_test_server(test_db.path()).await;

    let (mut lines, mut writer) = connect_client(addr).await;

    // 1. Send malformed wire data (not valid JSON)
    writer
        .write_all(b"NOT_A_JSON_PAYLOAD_CORRUPT_BYTES\n")
        .await
        .expect("write failed");
    writer.flush().await.expect("flush failed");

    // Server must respond with ErrorMessage without panicking or terminating connection
    let err_response = read_msg(&mut lines).await;
    match err_response {
        ServerMessage::ErrorMessage(msg) => {
            assert!(msg.contains("Invalid JSON payload format"), "Unexpected message: {msg}");
        }
        other => panic!("Expected ErrorMessage, got: {:?}", other),
    }

    // 2. Client continues using the same connection and sends valid registration
    send_msg(
        &mut writer,
        &ClientMessage::Register {
            username: "resilientdriver".to_string(),
            password: VALID_PASSWORD.to_string(),
        },
    )
    .await;

    assert_eq!(read_msg(&mut lines).await, ServerMessage::AuthResult(Ok(())));

    // 3. Abruptly drop client connection (simulates network drop / EOF)
    drop(writer);
    drop(lines);

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!state.registry.read().await.active_clients.contains_key("resilientdriver"));

    server_handle.stop().await.expect("server must stop gracefully");
}
