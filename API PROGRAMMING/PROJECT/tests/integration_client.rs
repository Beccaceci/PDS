mod common;

use common::{
    TEST_TIMESTAMP, TestDatabase, TestSession, TestTimeSource, admin_message,
    wait_for_position_count,
};
use georuggine::client::movement::{FilePositionProvider, PositionProvider};
use georuggine::client::network::{send_message, ServerReader};
use georuggine::client::state::{ClientState, VehicleStateChange};
use georuggine::{ClientMessage, Position, STOPPED_AFTER_SECS, ServerMessage};
use std::io::Write;
use std::time::Duration;
use tempfile::NamedTempFile;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::time::timeout;

// =========================================================================
// Vittorio Integration Tests (IC-01 to IC-03b)
// =========================================================================

#[tokio::test]
async fn route_positions_encode_in_order() {
    let mut route = NamedTempFile::new().expect("route file must be created");
    writeln!(route, "invalid,row\n\n45.0703,7.6869\n44.9000,8.2069")
        .expect("route rows must be written");
    route.flush().expect("route file must be flushed");
    let mut provider = FilePositionProvider::with_time_source(
        route.path().to_str().expect("route path is UTF-8"),
        TestTimeSource::new(TEST_TIMESTAMP),
    )
    .expect("route provider must open the file");
    let (mut writer, reader) = tokio::io::duplex(1024);
    let mut reader = BufReader::new(reader);

    for expected_coordinates in [(45.0703, 7.6869), (44.9000, 8.2069)] {
        let position = provider
            .next_position()
            .expect("valid route row must yield a position");
        assert_eq!(
            (position.latitude, position.longitude),
            expected_coordinates
        );

        send_message(&mut writer, &ClientMessage::UpdatePosition(position))
            .await
            .expect("position must be encoded");

        let mut line = String::new();
        reader
            .read_line(&mut line)
            .await
            .expect("encoded position must be readable");
        assert_eq!(
            serde_json::from_str::<ClientMessage>(&line).expect("wire message must decode"),
            ClientMessage::UpdatePosition(position)
        );
    }
}

#[tokio::test]
async fn replayed_stop_encodes() {
    let mut route = NamedTempFile::new().expect("route file must be created");
    writeln!(route, "45.0703,7.6869").expect("route row must be written");
    route.flush().expect("route file must be flushed");
    let time_source = TestTimeSource::new(TEST_TIMESTAMP);
    let mut provider = FilePositionProvider::with_time_source(
        route.path().to_str().expect("route path is UTF-8"),
        time_source.clone(),
    )
    .expect("route provider must open the file");
    let first = provider
        .next_position()
        .expect("first route position must exist");

    time_source.set(first.timestamp + STOPPED_AFTER_SECS);
    let replayed = provider
        .next_position()
        .expect("last position must replay after EOF");

    assert_eq!(
        (replayed.latitude, replayed.longitude),
        (first.latitude, first.longitude)
    );
    assert_eq!(replayed.timestamp, first.timestamp + STOPPED_AFTER_SECS);

    let mut movement = ClientState::default();
    assert_eq!(movement.update(first), Some(VehicleStateChange::Stopped));
    let stopped = Position {
        timestamp: first.timestamp + STOPPED_AFTER_SECS,
        ..replayed
    };
    assert_eq!(movement.update(stopped), None);

    let (mut writer, reader) = tokio::io::duplex(256);
    send_message(&mut writer, &ClientMessage::UpdatePosition(replayed))
        .await
        .expect("replayed position must be encoded");
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .await
        .expect("encoded replayed position must be readable");
    assert_eq!(
        serde_json::from_str::<ClientMessage>(&line).expect("wire message must decode"),
        ClientMessage::UpdatePosition(replayed)
    );
}

#[tokio::test]
async fn client_network_records_position() {
    let database = TestDatabase::new();
    let state = database.state();
    let mut session = TestSession::start(state.clone());
    let position = Position {
        latitude: 45.0703,
        longitude: 7.6869,
        timestamp: TEST_TIMESTAMP,
    };

    session.register("driver13").await;
    session.send(&ClientMessage::UpdatePosition(position)).await;
    wait_for_position_count(&state, "driver13", 1).await;

    session.close().await;
}

#[tokio::test]
async fn client_network_decodes_admin_message() {
    let database = TestDatabase::new();
    let state = database.state();
    let mut session = TestSession::start(state.clone());

    session.register("driver14").await;
    state
        .send_admin_direct("driver14", "Continue to Asti")
        .await
        .expect("admin message must be queued");
    assert_eq!(
        session.next_message().await,
        admin_message("Continue to Asti")
    );

    session.close().await;
}

// =========================================================================
// Real Route & Network Stream Integration Tests (Nicola)
// =========================================================================


#[tokio::test]
async fn tcp_framing_roundtrip() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("must bind ephemeral port");
    let addr = listener.local_addr().expect("must get local addr");

    // Server background task
    let server_task = tokio::spawn(async move {
        let (server_stream, _) = listener.accept().await.expect("accept failed");
        let (read_half, mut write_half) = server_stream.into_split();
        let mut lines = BufReader::new(read_half).lines();

        // 1. Receive ClientMessage::Register
        let line1 = lines.next_line().await.unwrap().expect("read line1");
        let client_msg1: ClientMessage = serde_json::from_str(&line1).expect("deserialize 1");
        assert_eq!(
            client_msg1,
            ClientMessage::Register {
                username: "truckdriver1".to_string(),
                password: "Password123!".to_string(),
            }
        );

        // Reply ServerMessage::AuthResult(Ok(()))
        let reply1 = serde_json::to_string(&ServerMessage::AuthResult(Ok(()))).unwrap() + "\n";
        write_half.write_all(reply1.as_bytes()).await.unwrap();
        write_half.flush().await.unwrap();

        // 2. Receive ClientMessage::UpdatePosition
        let line2 = lines.next_line().await.unwrap().expect("read line2");
        let client_msg2: ClientMessage = serde_json::from_str(&line2).expect("deserialize 2");
        match client_msg2 {
            ClientMessage::UpdatePosition(pos) => {
                assert!((pos.latitude - 45.0703).abs() < 1e-4);
            }
            other => panic!("Expected UpdatePosition, got {:?}", other),
        }

        // Reply ServerMessage::TextMessage
        let reply2 = serde_json::to_string(&ServerMessage::TextMessage {
            sender: "FleetAdmin".to_string(),
            content: "Route verified".to_string(),
        }).unwrap() + "\n";
        write_half.write_all(reply2.as_bytes()).await.unwrap();
        write_half.flush().await.unwrap();
    });

    // Client connects using ServerReader and send_message
    let client_stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("client connect failed");
    let (read_half, mut write_half) = client_stream.into_split();
    let mut reader = ServerReader::new(read_half);

    // 1. Send Register
    let reg_msg = ClientMessage::Register {
        username: "truckdriver1".to_string(),
        password: "Password123!".to_string(),
    };
    send_message(&mut write_half, &reg_msg).await.expect("send_message failed");

    let server_resp1 = timeout(Duration::from_secs(2), reader.read_message())
        .await
        .expect("timed out reading reply 1")
        .expect("read_message error")
        .expect("unexpected EOF on reply 1");
    assert_eq!(server_resp1, ServerMessage::AuthResult(Ok(())));

    // 2. Send UpdatePosition
    let pos_msg = ClientMessage::UpdatePosition(Position {
        latitude: 45.0703,
        longitude: 7.6869,
        timestamp: 1700000000,
    });
    send_message(&mut write_half, &pos_msg).await.expect("send position failed");

    let server_resp2 = timeout(Duration::from_secs(2), reader.read_message())
        .await
        .expect("timed out reading reply 2")
        .expect("read_message error")
        .expect("unexpected EOF on reply 2");
    assert_eq!(
        server_resp2,
        ServerMessage::TextMessage {
            sender: "FleetAdmin".to_string(),
            content: "Route verified".to_string(),
        }
    );

    server_task.await.expect("server task failed");
}

#[tokio::test]
async fn tcp_eof_is_clean() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("must bind ephemeral port");
    let addr = listener.local_addr().expect("must get local addr");

    // Server accepts and immediately closes socket
    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept failed");
        // Drop stream immediately to simulate remote close / EOF
        drop(stream);
    });

    let client_stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("client connect failed");
    let (read_half, _write_half) = client_stream.into_split();
    let mut reader = ServerReader::new(read_half);

    // Reading on a closed connection must return Ok(None) cleanly
    let result = timeout(Duration::from_secs(2), reader.read_message())
        .await
        .expect("timed out reading message")
        .expect("read_message should not error on EOF");

    assert_eq!(result, None, "EOF must be indicated by Ok(None)");

    server_task.await.expect("server task failed");
}
