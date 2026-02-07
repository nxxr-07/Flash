use flash_client::Client;
use flash_common::Response;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = Client::connect("127.0.0.1:8080").await?;
    println!("Connected to server");

    let topic = "notifications".to_string();
    client.subscribe(topic.clone()).await?;
    println!("Subscribed to {}", topic);

    // Read the OK from subscribe
    if let Some(resp) = client.next_response().await {
        println!("Subscription response: {:?}", resp);
    }

    println!("Waiting for messages...");
    while let Some(resp) = client.next_response().await {
        match resp {
            Response::Message { topic, payload } => {
                let msg = String::from_utf8_lossy(&payload);
                println!("Received on {}: {}", topic, msg);
            }
            other => {
                println!("Received other response: {:?}", other);
            }
        }
    }

    Ok(())
}
