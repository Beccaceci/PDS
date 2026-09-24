use serde::{Deserialize, Serialize};

pub mod client;
pub mod server;
pub mod terminal;
pub mod time;

pub use time::{SystemTimeSource, TimeSource, current_timestamp_secs};

/// Consecutive stationary seconds required before a moving driver becomes stopped.
pub const STOPPED_AFTER_SECS: u64 = 180;

/// Represents a geographic position with latitude, longitude, and a Unix timestamp in seconds.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Position {
    /// Latitude in decimal degrees.
    pub latitude: f64,
    /// Longitude in decimal degrees.
    pub longitude: f64,
    /// Unix timestamp in seconds since the Epoch.
    pub timestamp: u64,
}

impl Position {
    /// Computes the flat Euclidean distance in degrees.
    pub fn euclidean_distance(&self, other: &Position) -> f64 {
        let delta_lat = (self.latitude - other.latitude).powi(2);
        let delta_long = (self.longitude - other.longitude).powi(2);
        (delta_lat + delta_long).sqrt()
    }

    /// Computes the real-world distance in kilometers between two GPS positions using the Haversine formula.
    pub fn haversine_distance_km(&self, other: &Position) -> f64 {
        if self.latitude == other.latitude && self.longitude == other.longitude {
            return 0.0;
        }

        const EARTH_RADIUS_KM: f64 = 6371.0;

        let d_lat = (other.latitude - self.latitude).to_radians();
        let d_lon = (other.longitude - self.longitude).to_radians();

        let lat1 = self.latitude.to_radians();
        let lat2 = other.latitude.to_radians();

        // Haversine term: a = sin²(Δlat / 2) + cos(lat1) * cos(lat2) * sin²(Δlon / 2)
        let a = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
        let a = a.clamp(0.0, 1.0);

        // Angular distance in radians: c = 2 * atan2(√a, √(1 - a))
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        EARTH_RADIUS_KM * c
    }
}

/// Represents the operating state of a user or vehicle in the fleet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UserState {
    /// Indicates that the user is currently disconnected from the server.
    Disconnected,
    /// Indicates that the user is connected but stationary.
    Stopped(u64),
    /// Indicates that the user is connected and actively moving.
    Moving(u64),
}

/// Holds calculated movement statistics for a specific user and time window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserStats {
    /// Time window applied to calculate these statistics.
    pub time_window: TimeWindow,
    /// Total distance traveled in kilometers.
    pub total_distance_km: f64,
    /// Average speed during moving intervals in km/h.
    pub average_speed_kmh: f64,
    /// Total duration in moving state in seconds.
    pub movement_duration_secs: u64,
    /// Total duration in stopped state in seconds.
    pub pause_duration_secs: u64,
}

/// Represents messages sent from the Client (Truck Driver) to the Server (Fleet Admin) over the network.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClientMessage {
    /// Requests registration of a new user account.
    Register { username: String, password: String },
    /// Requests authentication for an existing user account.
    Login { username: String, password: String },
    /// Submits a periodic GPS position update.
    UpdatePosition(Position),
    /// Sends a text message from the driver to the fleet administrator.
    SendText { content: String },
}

/// Represents messages sent from the Server (Fleet Admin) to the Client (Truck Driver).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ServerMessage {
    /// Delivers the authentication or registration result.
    AuthResult(Result<(), String>),
    /// Delivers a text message sent by the server administrator (unicast or broadcast).
    TextMessage { sender: String, content: String },
    /// Delivers an application-level error notification.
    ErrorMessage(String),
}

/// Represents a time window filter for server-side movement statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeWindow {
    /// Filters analytics for the current calendar day (00:00:00 today).
    CurrentDay,
    /// Filters analytics for the current calendar week (Monday 00:00:00).
    CurrentWeek,
    /// Filters analytics for the current calendar month (1st day 00:00:00).
    CurrentMonth,
}

impl TimeWindow {
    /// Returns the lower bound UNIX timestamp in seconds for this time window.
    /// Computes calendar boundaries for CurrentDay (00:00:00 today), CurrentWeek (Monday 00:00:00),
    /// and CurrentMonth (1st day of month 00:00:00).
    pub fn lower_bound_timestamp(&self) -> u64 {
        let now = current_timestamp_secs();
        self.lower_bound_timestamp_at(now)
    }

    /// Returns the lower bound for this time window relative to `now`.
    ///
    /// This deterministic variant is useful when a caller already obtained its
    /// current time from an injected [`TimeSource`].
    pub fn lower_bound_timestamp_at(&self, now: u64) -> u64 {
        let seconds_in_day = 86_400;

        match self {
            Self::CurrentDay => {
                // Beginning of current calendar day (00:00:00)
                now - (now % seconds_in_day)
            }
            Self::CurrentWeek => {
                // Beginning of current week (aligned to Monday 00:00:00)
                let start_of_day = now - (now % seconds_in_day);
                let days_since_monday = ((now / seconds_in_day) + 3) % 7;
                start_of_day.saturating_sub(days_since_monday * seconds_in_day)
            }
            Self::CurrentMonth => {
                use chrono::{DateTime, Datelike, TimeZone, Utc};
                let now_date = DateTime::<Utc>::from_timestamp(now as i64, 0)
                    .expect("timestamp is outside the supported calendar range");
                let start_of_month = Utc
                    .with_ymd_and_hms(now_date.year(), now_date.month(), 1, 0, 0, 0)
                    .unwrap();
                start_of_month.timestamp() as u64
            }
        }
    }
}
