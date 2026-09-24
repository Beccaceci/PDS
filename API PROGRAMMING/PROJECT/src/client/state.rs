use crate::{Position, STOPPED_AFTER_SECS};

/// A change in the vehicle's movement state detected from GPS updates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VehicleStateChange {
    Moving,
    Stopped,
}

/// Tracks the client's latest moving position and current movement status.
#[derive(Debug, Default)]
pub struct ClientState {
    last_position: Option<Position>,
    is_moving: bool,
}

impl ClientState {
    /// Updates the vehicle movement state for a newly received position.
    ///
    /// Returns a state change only when the vehicle starts moving or has remained
    /// stationary for [`STOPPED_AFTER_SECS`].
    pub fn update(&mut self, position: Position) -> Option<VehicleStateChange> {
        if self.last_position.is_none() {
            self.is_moving = false;
            self.last_position = Some(position);
            return Some(VehicleStateChange::Stopped)
        }

        let last= self.last_position.unwrap();
        
        if position.latitude != last.latitude || position.longitude != last.longitude {
            self.last_position = Some(position);

            if !self.is_moving {
                self.is_moving = true;
                return Some(VehicleStateChange::Moving);
            }
        } else if self.is_moving
            && position.timestamp.saturating_sub(last.timestamp) >= STOPPED_AFTER_SECS
        {
            self.is_moving = false;
            return Some(VehicleStateChange::Stopped);
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientState, VehicleStateChange};
    use crate::{Position, STOPPED_AFTER_SECS};

    #[test]
    fn first_position_starts_the_vehicle_moving() {
        let position = Position {
            latitude: 45.0,
            longitude: 7.0,
            timestamp: 100,
        };
        let mut state = ClientState::default();

        let change = state.update(position);

        assert_eq!(change, Some(VehicleStateChange::Stopped));
        assert!(!state.is_moving);
        assert_eq!(state.last_position, Some(position));
    }

    #[test]
    fn changed_position_keeps_a_moving_vehicle_moving() {
        let previous = Position {
            latitude: 45.0,
            longitude: 7.0,
            timestamp: 100,
        };
        let position = Position {
            latitude: 45.1,
            longitude: 7.0,
            timestamp: 110,
        };
        let mut state = ClientState {
            last_position: Some(previous),
            is_moving: true,
        };

        let change = state.update(position);

        assert_eq!(change, None);
        assert!(state.is_moving);
        assert_eq!(state.last_position, Some(position));
    }

    #[test]
    fn unchanged_position_stops_after_the_threshold() {
        let last_moved = Position {
            latitude: 45.0,
            longitude: 7.0,
            timestamp: 100,
        };
        let position = Position {
            timestamp: last_moved.timestamp + STOPPED_AFTER_SECS,
            ..last_moved
        };
        let mut state = ClientState {
            last_position: Some(last_moved),
            is_moving: true,
        };

        let change = state.update(position);

        assert_eq!(change, Some(VehicleStateChange::Stopped));
        assert!(!state.is_moving);
        assert_eq!(state.last_position, Some(last_moved));
    }

    #[test]
    fn unchanged_position_before_threshold_keeps_the_vehicle_moving() {
        let last_moved = Position {
            latitude: 45.0,
            longitude: 7.0,
            timestamp: 100,
        };
        let position = Position {
            timestamp: last_moved.timestamp + STOPPED_AFTER_SECS - 1,
            ..last_moved
        };
        let mut state = ClientState {
            last_position: Some(last_moved),
            is_moving: true,
        };

        let change = state.update(position);

        assert_eq!(change, None);
        assert!(state.is_moving);
        assert_eq!(state.last_position, Some(last_moved));
    }
}
