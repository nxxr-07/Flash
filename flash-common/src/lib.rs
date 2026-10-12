use serde::{Deserialize, Serialize};

/// Commands sent from Client to Server
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Command {
    /// Publish a message to a topic
    Publish { topic: String, payload: Vec<u8> },
    /// Subscribe to a topic to receive messages
    Subscribe { topic: String },
}

/// Responses sent from Server to Client
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Response {
    /// Operation successful
    Ok,
    /// Operation failed with error message
    Error(String),
    /// A message from a subscribed topic
    Message { topic: String, payload: Vec<u8> },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_round_trips_through_json() {
        let command = Command::Publish {
            topic: "notifications".to_string(),
            payload: b"hello".to_vec(),
        };

        let encoded = serde_json::to_string(&command).unwrap();
        let decoded: Command = serde_json::from_str(&encoded).unwrap();

        match decoded {
            Command::Publish { topic, payload } => {
                assert_eq!(topic, "notifications");
                assert_eq!(payload, b"hello");
            }
            Command::Subscribe { .. } => panic!("decoded a different command variant"),
        }
    }
}
