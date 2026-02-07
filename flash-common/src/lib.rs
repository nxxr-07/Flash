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
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
