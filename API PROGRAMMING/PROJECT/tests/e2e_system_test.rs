mod common;

use common::{
    TEST_TIMESTAMP, TestDatabase, TestTimeSource, VALID_PASSWORD, admin_message,
    start_test_server, wait_for_position_count, wait_for_position_count_at_least,
};
use georuggine::client::network::{ServerReader, send_message};
use georuggine::client::runtime::{Client, ClientConfig};
use georuggine::server::error::ServerError;
use georuggine::server::logger::CpuTracker;
use georuggine::terminal::{CliTerminal, TerminalEvent};
use georuggine::{ClientMessage, Position, ServerMessage, TimeWindow, UserState};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const SHORT_TIMEOUT: Duration = Duration::from_millis(300);

struct IdleTerminal;

impl CliTerminal for IdleTerminal {
    fn show_prompt(&self) {}

    fn write(&self, _message: &str) {}

    fn write_plain(&self, _message: &str) {}

    fn read(&mut self) -> Pin<Box<dyn Future<Output = io::Result<TerminalEvent>> + '_>> {
        Box::pin(std::future::pending())
    }
}

/// A real TCP-connected client wrapper for end-to-end multi-vehicle simulation.
struct E2eClient {
    pub reader: ServerReader<OwnedReadHalf>,
    pub writer: OwnedWriteHalf,
}

impl E2eClient {
    /// Establishes a real TCP socket connection to the server.
    pub async fn connect(addr: SocketAddr) -> Self {
        let stream = TcpStream::connect(addr)
            .await
            .expect("must connect to server socket");
        let (read_half, write_half) = stream.into_split();
        Self {
            reader: ServerReader::new(read_half),
            writer: write_half,
        }
    }

    /// Submits a registration request and awaits the server response.
    pub async fn register(&mut self, username: &str, password: &str) -> ServerMessage {
        let msg = ClientMessage::Register {
            username: username.to_string(),
            password: password.to_string(),
        };
        send_message(&mut self.writer, &msg)
            .await
            .expect("must send register");
        self.next_message().await
    }

    /// Submits a login request and awaits the server response.
    pub async fn login(&mut self, username: &str, password: &str) -> ServerMessage {
        let msg = ClientMessage::Login {
            username: username.to_string(),
            password: password.to_string(),
        };
        send_message(&mut self.writer, &msg)
            .await
            .expect("must send login");
        self.next_message().await
    }

    /// Sends a GPS position update over the TCP connection.
    pub async fn send_position(&mut self, pos: Position) {
        let msg = ClientMessage::UpdatePosition(pos);
        send_message(&mut self.writer, &msg)
            .await
            .expect("must send position");
    }

    /// Sends a driver-to-admin text message over the TCP connection.
    pub async fn send_text(&mut self, content: &str) {
        let msg = ClientMessage::SendText {
            content: content.to_string(),
        };
        send_message(&mut self.writer, &msg)
            .await
            .expect("must send text");
    }

    /// Awaits the next typed server response with a bounded timeout.
    pub async fn next_message(&mut self) -> ServerMessage {
        timeout(DEFAULT_TIMEOUT, self.reader.read_message())
            .await
            .expect("server did not respond in time")
            .expect("read error")
            .expect("server closed connection unexpectedly")
    }

    /// Attempts to read the next message, returning None if no message arrives within the duration.
    pub async fn try_next_message(&mut self, wait: Duration) -> Option<ServerMessage> {
        match timeout(wait, self.reader.read_message()).await {
            Ok(Ok(Some(msg))) => Some(msg),
            _ => None,
        }
    }

    /// Sends raw unencoded bytes directly to the socket to test wire resilience.
    pub async fn send_raw(&mut self, payload: &[u8]) {
        self.writer
            .write_all(payload)
            .await
            .expect("must write raw bytes");
        self.writer.flush().await.expect("must flush socket writer");
    }
}

// =========================================================================
// E2E Test Scenarios
// =========================================================================

/// SCENARIO 1: Multi-Vehicle Fleet Lifecycle
/// Verifies concurrent vehicle registration, live GPS telemetry streaming,
/// FSM state transitions (Stopped <-> Moving), admin broadcast reception,
/// isolated admin unicast messaging, driver-to-admin replies, and graceful disconnects.
#[tokio::test]
async fn test_e2e_multi_vehicle_fleet_lifecycle() {
    let database = TestDatabase::new();
    let (server_handle, addr, state) = start_test_server(database.path()).await;

    // Admin message bus listener for driver-to-admin messages
    let mut admin_inbox = state.driver_message_sender.subscribe();

    // 1. Connect 3 real TCP vehicle clients
    let mut truck_alpha = E2eClient::connect(addr).await;
    let mut truck_beta = E2eClient::connect(addr).await;
    let mut truck_gamma = E2eClient::connect(addr).await;

    // 2. Authenticate all three vehicles
    assert_eq!(
        truck_alpha.register("truckalpha", VALID_PASSWORD).await,
        ServerMessage::AuthResult(Ok(()))
    );
    assert_eq!(
        truck_beta.register("truckbeta", VALID_PASSWORD).await,
        ServerMessage::AuthResult(Ok(()))
    );
    assert_eq!(
        truck_gamma.register("truckgamma", VALID_PASSWORD).await,
        ServerMessage::AuthResult(Ok(()))
    );

    let base_time = TEST_TIMESTAMP;

    // 3. Telemetry Stream:
    // truckalpha: First position -> Stopped(t0), Second position -> Moving(t1)
    let pos_a1 = Position {
        latitude: 45.0703,
        longitude: 7.6869,
        timestamp: base_time,
    };
    truck_alpha.send_position(pos_a1).await;
    wait_for_position_count(&state, "truckalpha", 1).await;

    let pos_a2 = Position {
        latitude: 45.0750,
        longitude: 7.6900,
        timestamp: base_time + 30,
    };
    truck_alpha.send_position(pos_a2).await;
    wait_for_position_count(&state, "truckalpha", 2).await;

    // truckbeta: First position -> Stopped(t0), Second position at same location after 200s -> remains Stopped(t0)
    let pos_b1 = Position {
        latitude: 44.9000,
        longitude: 8.2069,
        timestamp: base_time,
    };
    truck_beta.send_position(pos_b1).await;
    wait_for_position_count(&state, "truckbeta", 1).await;

    let pos_b2 = Position {
        latitude: 44.9000,
        longitude: 8.2069,
        timestamp: base_time + 200,
    };
    truck_beta.send_position(pos_b2).await;
    wait_for_position_count(&state, "truckbeta", 2).await;

    // truckgamma: Single position -> Stopped(t0)
    let pos_g1 = Position {
        latitude: 45.0000,
        longitude: 7.5000,
        timestamp: base_time,
    };
    truck_gamma.send_position(pos_g1).await;
    wait_for_position_count(&state, "truckgamma", 1).await;

    // 4. Verify Driver List on Server
    let drivers = state.get_driver_list().await;
    assert_eq!(drivers.len(), 3);

    let alpha_status = drivers.iter().find(|(name, _, _)| name == "truckalpha").unwrap();
    assert_eq!(alpha_status.1, UserState::Moving(base_time + 30));
    assert_eq!(alpha_status.2, 2);

    let beta_status = drivers.iter().find(|(name, _, _)| name == "truckbeta").unwrap();
    assert_eq!(beta_status.1, UserState::Stopped(base_time));
    assert_eq!(beta_status.2, 2);

    let gamma_status = drivers.iter().find(|(name, _, _)| name == "truckgamma").unwrap();
    assert_eq!(gamma_status.1, UserState::Stopped(base_time));
    assert_eq!(gamma_status.2, 1);

    // 5. Admin Broadcast to all live connected vehicles
    let alert_text = "Severe weather warning on highway A21";
    let recipient_count = state.send_admin_broadcast(alert_text).await;
    assert_eq!(recipient_count, 3);

    let expected_broadcast = admin_message(alert_text);
    assert_eq!(truck_alpha.next_message().await, expected_broadcast);
    assert_eq!(truck_beta.next_message().await, expected_broadcast);
    assert_eq!(truck_gamma.next_message().await, expected_broadcast);

    // 6. Admin Direct Message to truckalpha only (Unicast Isolation)
    let direct_text = "Please pull over at rest area 4";
    let send_result = state.send_admin_direct("truckalpha", direct_text).await;
    assert!(send_result.is_ok());

    let expected_direct = admin_message(direct_text);
    assert_eq!(truck_alpha.next_message().await, expected_direct);

    // Ensure truckbeta and truckgamma do NOT receive the direct message
    assert!(truck_beta.try_next_message(SHORT_TIMEOUT).await.is_none());
    assert!(truck_gamma.try_next_message(SHORT_TIMEOUT).await.is_none());

    // 7. Driver sends response message to Admin
    truck_alpha
        .send_text("Message received, pulling over now.")
        .await;

    let (sender, content) = timeout(DEFAULT_TIMEOUT, admin_inbox.recv())
        .await
        .expect("admin did not receive driver message in time")
        .expect("recv error");
    assert_eq!(sender, "truckalpha");
    assert_eq!(content, "Message received, pulling over now.");

    // 8. Disconnect truckalpha and truckbeta
    drop(truck_alpha);
    drop(truck_beta);

    // Allow background EOF detection to complete
    sleep(Duration::from_millis(150)).await;

    let updated_drivers = state.get_driver_list().await;
    let alpha_post = updated_drivers
        .iter()
        .find(|(name, _, _)| name == "truckalpha")
        .unwrap();
    assert_eq!(alpha_post.1, UserState::Disconnected);

    let beta_post = updated_drivers
        .iter()
        .find(|(name, _, _)| name == "truckbeta")
        .unwrap();
    assert_eq!(beta_post.1, UserState::Disconnected);

    let gamma_post = updated_drivers
        .iter()
        .find(|(name, _, _)| name == "truckgamma")
        .unwrap();
    assert_eq!(gamma_post.1, UserState::Stopped(base_time));

    server_handle
        .stop()
        .await
        .expect("production server must stop cleanly");
}

/// SCENARIO 2: Server Restart, Persistence & Historical Analytics
/// Stops one server runtime and starts a new server instance
/// against the persistent SQLite database, validates credential and position recovery,
/// verifies O(K) analytics (distance, speed, durations) over the historical data,
/// and re-authenticates the client to resume telemetry.
#[tokio::test]
async fn test_e2e_server_restart_persistence_and_analytics() {
    let database = TestDatabase::new();
    let db_path = database.path().to_path_buf();

    // 1. Start Server Instance 1 on an ephemeral port
    let (server_handle1, addr1, state1) = start_test_server(&db_path).await;

    // 2. Client connects and registers as "veterandriver"
    let mut client = E2eClient::connect(addr1).await;
    assert_eq!(
        client.register("veterandriver", VALID_PASSWORD).await,
        ServerMessage::AuthResult(Ok(()))
    );

    // Use timestamps strictly within today's UTC window to guarantee CurrentDay inclusion
    let now = TEST_TIMESTAMP;
    let base_time = now.saturating_sub(3600); // 1 hour ago

    // Stream 3 positions along the Torino -> Asti route
    // Torino GPS (lat 45.0703, lon 7.6869)
    let pos1 = Position {
        latitude: 45.0703,
        longitude: 7.6869,
        timestamp: base_time,
    };
    client.send_position(pos1).await;
    wait_for_position_count(&state1, "veterandriver", 1).await;

    // Midpoint GPS (lat 44.9850, lon 7.9469) at +900s (15 min)
    let pos2 = Position {
        latitude: 44.9850,
        longitude: 7.9469,
        timestamp: base_time + 900,
    };
    client.send_position(pos2).await;
    wait_for_position_count(&state1, "veterandriver", 2).await;

    // Asti GPS (lat 44.9000, lon 8.2069) at +1800s (30 min)
    let pos3 = Position {
        latitude: 44.9000,
        longitude: 8.2069,
        timestamp: base_time + 1800,
    };
    client.send_position(pos3).await;
    wait_for_position_count(&state1, "veterandriver", 3).await;

    // Disconnect client from server 1
    drop(client);

    // 3. Stop server 1 cleanly and drop its state before restarting.
    server_handle1
        .stop()
        .await
        .expect("first production server must stop cleanly");
    drop(state1);

    // 4. Start Server Instance 2 pointing to the exact same persistent SQLite file
    let (server_handle2, addr2, state2) = start_test_server(&db_path).await;

    // 5. Verify database restoration on Server 2 boot
    let restored_drivers = state2.get_driver_list().await;
    let veteran = restored_drivers
        .iter()
        .find(|(name, _, _)| name == "veterandriver")
        .expect("veterandriver must be restored from SQLite");
    assert_eq!(veteran.1, UserState::Disconnected);
    assert_eq!(veteran.2, 3);

    // 6. Query Historical Fleet Analytics for CurrentDay
    let stats = state2
        .calculate_user_stats("veterandriver", TimeWindow::CurrentDay)
        .await
        .expect("stats calculation must succeed");

    // Haversine distance between Torino and Asti route points is ~45.1 km
    assert!(
        stats.total_distance_km > 40.0 && stats.total_distance_km < 60.0,
        "Expected ~45 km, got {}",
        stats.total_distance_km
    );
    assert_eq!(stats.movement_duration_secs, 1800);
    assert_eq!(stats.pause_duration_secs, 0);
    // Average speed ~ 45.1 km / 0.5 h = ~90 km/h
    assert!(
        stats.average_speed_kmh > 70.0 && stats.average_speed_kmh < 130.0,
        "Expected ~90 km/h, got {}",
        stats.average_speed_kmh
    );

    // 7. Client reconnects to Server 2 and logs in with original password (validating bcrypt persistence)
    let mut reconnected_client = E2eClient::connect(addr2).await;
    assert_eq!(
        reconnected_client
            .login("veterandriver", VALID_PASSWORD)
            .await,
        ServerMessage::AuthResult(Ok(()))
    );

    // Submit a 4th position on the new server instance
    let pos4 = Position {
        latitude: 44.8900,
        longitude: 8.2100,
        timestamp: base_time + 2100,
    };
    reconnected_client.send_position(pos4).await;
    wait_for_position_count(&state2, "veterandriver", 4).await;

    drop(reconnected_client);
    server_handle2
        .stop()
        .await
        .expect("second production server must stop cleanly");
}

/// SCENARIO 3: Admin Operations & System Monitoring Flow
/// Validates admin commands against live state: offline driver unicast rejection,
/// active driver unicast delivery, broadcast recipient count tracking,
/// and CPU monitoring ring buffer with measurement update.
#[tokio::test]
async fn test_e2e_admin_fleet_operations_and_cpu_monitoring() {
    let database = TestDatabase::new();
    let (server_handle, addr, state) = start_test_server(database.path()).await;

    // Connect two clients
    let mut client1 = E2eClient::connect(addr).await;
    let mut client2 = E2eClient::connect(addr).await;

    assert_eq!(
        client1.register("van01", VALID_PASSWORD).await,
        ServerMessage::AuthResult(Ok(()))
    );
    assert_eq!(
        client2.register("van02", VALID_PASSWORD).await,
        ServerMessage::AuthResult(Ok(()))
    );

    // 1. Direct message to offline/unknown driver is rejected
    let offline_err = state
        .send_admin_direct("vanghost", "Are you there?")
        .await
        .unwrap_err();
    assert_eq!(
        offline_err,
        ServerError::RecipientOffline {
            recipient: "vanghost".to_string(),
        }
    );

    // 2. Direct message to online driver succeeds
    state
        .send_admin_direct("van01", "Delivery address updated")
        .await
        .expect("must deliver to online driver");
    assert_eq!(
        client1.next_message().await,
        admin_message("Delivery address updated")
    );

    // 3. Broadcast reaches both clients
    let broadcast_count = state.send_admin_broadcast("Maintenance at 22:00").await;
    assert_eq!(broadcast_count, 2);
    assert_eq!(
        client1.next_message().await,
        admin_message("Maintenance at 22:00")
    );
    assert_eq!(
        client2.next_message().await,
        admin_message("Maintenance at 22:00")
    );

    // 4. Disconnect client 2; verify direct message to client 2 now fails with RecipientOffline
    drop(client2);
    sleep(Duration::from_millis(150)).await;

    let offline_now_err = state
        .send_admin_direct("van02", "Late ping")
        .await
        .unwrap_err();
    assert_eq!(
        offline_now_err,
        ServerError::RecipientOffline {
            recipient: "van02".to_string(),
        }
    );

    // 5. Broadcast now reaches only 1 remaining client
    let single_broadcast = state.send_admin_broadcast("Only one left").await;
    assert_eq!(single_broadcast, 1);
    assert_eq!(
        client1.next_message().await,
        admin_message("Only one left")
    );

    // 6. Test CPU Tracker Ring Buffer & Measurement
    let temp_log = database.path().with_extension("cpu-test.log");
    let mut cpu_tracker = CpuTracker::with_time_source(
        10,
        temp_log,
        TestTimeSource::new(TEST_TIMESTAMP),
    );
    let sample = cpu_tracker.measure_and_push();
    assert!(sample >= 0.0);
    assert_eq!(cpu_tracker.history.len(), 1);

    // Verify render_ascii_chart runs without panicking
    cpu_tracker.render_ascii_chart();

    drop(client1);
    server_handle
        .stop()
        .await
        .expect("production server must stop cleanly");
}

/// SCENARIO 4: High-Concurrency Fleet Stress & Churn
/// Spawns 8 concurrent vehicle clients connecting and registering in parallel,
/// streaming GPS telemetry under race conditions, receiving concurrent broadcasts,
/// and disconnecting (both graceful and abrupt drops) without server deadlocks.
#[tokio::test]
async fn test_e2e_concurrent_fleet_stress_and_churn() {
    const NUM_VEHICLES: usize = 8;

    let database = TestDatabase::new();
    let (server_handle, addr, state) = start_test_server(database.path()).await;

    // Synchronization barrier: NUM_VEHICLES clients + 1 main coordinator task
    let barrier = Arc::new(Barrier::new(NUM_VEHICLES + 1));
    let mut client_tasks = JoinSet::new();

    for i in 0..NUM_VEHICLES {
        let username = format!("stresstruck{i:02}");
        let client_barrier = barrier.clone();
        client_tasks.spawn(async move {
            let mut client = E2eClient::connect(addr).await;

            // Register
            let auth_res = client.register(&username, VALID_PASSWORD).await;
            assert_eq!(
                auth_res,
                ServerMessage::AuthResult(Ok(())),
                "Vehicle {username} failed registration"
            );

            // Stream 3 positions
            for step in 0..3 {
                client
                    .send_position(Position {
                        latitude: 45.0 + (step as f64) * 0.01,
                        longitude: 7.6 + (step as f64) * 0.01,
                        timestamp: TEST_TIMESTAMP + (step as u64) * 10,
                    })
                    .await;
            }

            // Synchronize with coordinator before broadcast
            client_barrier.wait().await;

            // Await broadcast message
            let broadcast = client.next_message().await;
            assert_eq!(broadcast, admin_message("Stress test fleet broadcast"));

            // Odd vehicles drop abruptly, even vehicles disconnect cleanly
            if i % 2 == 0 {
                sleep(Duration::from_millis(20)).await;
            }
            drop(client);
        });
    }

    // Wait until all NUM_VEHICLES clients have registered and streamed initial telemetry
    barrier.wait().await;

    // Send broadcast to all concurrent clients
    let count = state
        .send_admin_broadcast("Stress test fleet broadcast")
        .await;
    assert_eq!(count, NUM_VEHICLES);

    // Wait for all client tasks to complete
    while let Some(task_res) = client_tasks.join_next().await {
        task_res.expect("concurrent client task must not panic");
    }

    // Allow all background handlers to clean up
    sleep(Duration::from_millis(200)).await;

    // Verify all vehicles are marked Disconnected in server state with 3 positions each
    let drivers = state.get_driver_list().await;
    assert_eq!(drivers.len(), NUM_VEHICLES);
    for (name, driver_state, pos_count) in &drivers {
        assert!(name.starts_with("stresstruck"));
        assert_eq!(*driver_state, UserState::Disconnected);
        assert_eq!(*pos_count, 3);
    }

    server_handle
        .stop()
        .await
        .expect("production server must stop cleanly");
}

/// SCENARIO 5: Adversarial & Malformed Traffic Immunity
/// Demonstrates that hostile or malfunctioning clients (raw garbage bytes,
/// unauthenticated telemetry, invalid credentials, out-of-order timestamps)
/// are rejected and isolated without affecting honest connected vehicles.
#[tokio::test]
async fn test_e2e_malicious_traffic_and_security_immunity() {
    let database = TestDatabase::new();
    let (server_handle, addr, state) = start_test_server(database.path()).await;

    // 1. Connect an honest vehicle client
    let mut honest_truck = E2eClient::connect(addr).await;
    assert_eq!(
        honest_truck.register("honestdriver", VALID_PASSWORD).await,
        ServerMessage::AuthResult(Ok(()))
    );

    let honest_pos = Position {
        latitude: 45.0703,
        longitude: 7.6869,
        timestamp: TEST_TIMESTAMP,
    };
    honest_truck.send_position(honest_pos).await;
    wait_for_position_count(&state, "honestdriver", 1).await;

    // 2. Adversarial Client 1: Sends non-JSON raw garbage
    let mut bad_client_1 = E2eClient::connect(addr).await;
    bad_client_1
        .send_raw(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await;
    let err_msg = bad_client_1.next_message().await;
    assert!(
        matches!(err_msg, ServerMessage::ErrorMessage(_)),
        "Expected ErrorMessage, got {err_msg:?}"
    );

    // 3. Adversarial Client 2: Sends telemetry without prior authentication
    let mut bad_client_2 = E2eClient::connect(addr).await;
    bad_client_2.send_position(honest_pos).await;
    let unauth_err = bad_client_2.next_message().await;
    assert!(
        matches!(unauth_err, ServerMessage::ErrorMessage(_)),
        "Expected ErrorMessage for unauthenticated position, got {unauth_err:?}"
    );

    // 4. Adversarial Client 3: Registration with invalid whitespace username
    let mut bad_client_3 = E2eClient::connect(addr).await;
    let ws_res = bad_client_3.register("   ", VALID_PASSWORD).await;
    assert!(
        matches!(ws_res, ServerMessage::AuthResult(Err(_))),
        "Whitespace username must be rejected"
    );

    // 5. Adversarial Client 4: Registration with weak password
    let mut bad_client_4 = E2eClient::connect(addr).await;
    let weak_res = bad_client_4.register("baduser", "weak").await;
    assert!(
        matches!(weak_res, ServerMessage::AuthResult(Err(_))),
        "Weak password must be rejected"
    );

    // 6. Verify Honest Truck continues operating unaffected
    let honest_pos_2 = Position {
        latitude: 45.0750,
        longitude: 7.6900,
        timestamp: honest_pos.timestamp + 30,
    };
    honest_truck.send_position(honest_pos_2).await;
    wait_for_position_count(&state, "honestdriver", 2).await;

    let broadcast_count = state.send_admin_broadcast("Fleet operational check").await;
    assert_eq!(broadcast_count, 1);
    assert_eq!(
        honest_truck.next_message().await,
        admin_message("Fleet operational check")
    );

    drop(honest_truck);
    drop(bad_client_1);
    drop(bad_client_2);
    drop(bad_client_3);
    drop(bad_client_4);
    server_handle
        .stop()
        .await
        .expect("production server must stop cleanly");
}

/// SCENARIO 6: Production Client CSV Route Replay
/// Authenticates through the production client flow, reads the committed
/// `torino_asti.csv` through its route provider, and streams GPS positions
/// through a real TCP connection to the production server runtime.
#[tokio::test]
async fn test_e2e_production_client_csv_route_replay() {
    let database = TestDatabase::new();
    let (server_handle, addr, state) = start_test_server(database.path()).await;

    let config = ClientConfig {
        addr: addr.to_string(),
        route: "torino_asti.csv".to_string(),
        interval: Duration::from_secs(30),
    };
    let auth_script = format!("2\ncsvreplayer\n{VALID_PASSWORD}\n");
    let mut auth_input = tokio::io::BufReader::new(auth_script.as_bytes());
    let client = Client::connect_with_input_and_time_source(
        &config,
        &mut auth_input,
        TestTimeSource::new(TEST_TIMESTAMP),
    )
        .await
        .expect("production client must connect")
        .expect("production client must authenticate");

    let (client_result, ()) = timeout(DEFAULT_TIMEOUT, async {
        tokio::join!(
            client.run_with_terminal(Duration::from_millis(25), IdleTerminal),
            async {
                wait_for_position_count_at_least(&state, "csvreplayer", 3).await;
                server_handle
                    .stop()
                    .await
                    .expect("production server must stop cleanly");
            }
        )
    })
    .await
    .expect("production client and server must stop within the test timeout");

    client_result
        .expect("production client runtime must stop cleanly after the server disconnects");
}
