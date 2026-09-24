mod common;

use chrono::{TimeZone, Utc};
use georuggine::{ClientMessage, Position, ServerMessage, TimeWindow};
use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

fn assert_json_contract<T>(value: &T, expected_json: &str)
where
    T: Serialize + DeserializeOwned + Debug + PartialEq,
{
    assert_eq!(
        serde_json::to_string(value).expect("message must serialize"),
        expected_json
    );
    assert_eq!(
        serde_json::from_str::<T>(expected_json).expect("contract JSON must deserialize"),
        *value
    );
}

fn assert_invalid_json<T: DeserializeOwned>(invalid_messages: &[&str]) {
    for message in invalid_messages {
        assert!(
            serde_json::from_str::<T>(message).is_err(),
            "invalid protocol message was accepted: {message}"
        );
    }
}

#[test]
fn test_haversine_distance_calculation() {
    let p1 = common::sample_position_torino();
    let p2 = common::sample_position_asti();

    let distance_km = p1.haversine_distance_km(&p2);

    // Distance between Torino and Asti is approximately 45-55 km
    assert!(
        distance_km > 40.0 && distance_km < 60.0,
        "Calculated distance {distance_km} km out of expected range"
    );
}

#[test]
fn test_euclidean_distance_calculation() {
    let p1 = Position {
        latitude: 0.0,
        longitude: 0.0,
        timestamp: 0,
    };
    let p2 = Position {
        latitude: 3.0,
        longitude: 4.0,
        timestamp: 10,
    };

    let dist = p1.euclidean_distance(&p2);
    assert!((dist - 5.0).abs() < 1e-6, "Euclidean 3-4-5 triangle failed");
}

#[test]
fn test_client_message_json_contract() {
    let position = Position {
        latitude: 45.0703,
        longitude: 7.6869,
        timestamp: 1_700_000_000,
    };

    for (message, expected_json) in [
        (
            ClientMessage::Register {
                username: "test_driver".to_string(),
                password: "Password123!".to_string(),
            },
            r#"{"Register":{"username":"test_driver","password":"Password123!"}}"#,
        ),
        (
            ClientMessage::Login {
                username: "test_driver".to_string(),
                password: "Password123!".to_string(),
            },
            r#"{"Login":{"username":"test_driver","password":"Password123!"}}"#,
        ),
        (
            ClientMessage::UpdatePosition(position),
            r#"{"UpdatePosition":{"latitude":45.0703,"longitude":7.6869,"timestamp":1700000000}}"#,
        ),
        (
            ClientMessage::SendText {
                content: "Route clear 🚚".to_string(),
            },
            r#"{"SendText":{"content":"Route clear 🚚"}}"#,
        ),
    ] {
        assert_json_contract(&message, expected_json);
    }
}

#[test]
fn test_server_message_json_contract() {
    for (message, expected_json) in [
        (
            ServerMessage::AuthResult(Ok(())),
            r#"{"AuthResult":{"Ok":null}}"#,
        ),
        (
            ServerMessage::AuthResult(Err("Incorrect username or password".to_string())),
            r#"{"AuthResult":{"Err":"Incorrect username or password"}}"#,
        ),
        (
            ServerMessage::TextMessage {
                sender: "FleetAdmin".to_string(),
                content: "Caution: heavy traffic on highway A4".to_string(),
            },
            r#"{"TextMessage":{"sender":"FleetAdmin","content":"Caution: heavy traffic on highway A4"}}"#,
        ),
        (
            ServerMessage::ErrorMessage("Invalid payload".to_string()),
            r#"{"ErrorMessage":"Invalid payload"}"#,
        ),
    ] {
        assert_json_contract(&message, expected_json);
    }
}

#[test]
fn malformed_protocol_messages_are_rejected() {
    assert_invalid_json::<ClientMessage>(&[
        r#"{"Unknown":{}}"#,
        r#"{"Register":{"username":"driver01"}}"#,
        r#"{"UpdatePosition":{"latitude":"45.0","longitude":7.0,"timestamp":1}}"#,
        r#"{"SendText":{"content":"hello"}} trailing"#,
    ]);
    assert_invalid_json::<ServerMessage>(&[
        r#"{"Unknown":{}}"#,
        r#"{"TextMessage":{"sender":"FleetAdmin"}}"#,
        r#"{"AuthResult":{"Ok":"unexpected"}}"#,
        r#"{"ErrorMessage":42}"#,
    ]);
}

#[test]
fn test_time_window_lower_bounds() {
    let now = Utc
        .with_ymd_and_hms(2024, 5, 15, 12, 34, 56)
        .unwrap()
        .timestamp() as u64;

    assert_eq!(
        TimeWindow::CurrentDay.lower_bound_timestamp_at(now),
        Utc.with_ymd_and_hms(2024, 5, 15, 0, 0, 0)
            .unwrap()
            .timestamp() as u64
    );
    assert_eq!(
        TimeWindow::CurrentWeek.lower_bound_timestamp_at(now),
        Utc.with_ymd_and_hms(2024, 5, 13, 0, 0, 0)
            .unwrap()
            .timestamp() as u64
    );
    assert_eq!(
        TimeWindow::CurrentMonth.lower_bound_timestamp_at(now),
        Utc.with_ymd_and_hms(2024, 5, 1, 0, 0, 0)
            .unwrap()
            .timestamp() as u64
    );
}
