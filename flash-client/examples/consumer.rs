use flash_client::Client;
use flash_common::Response;
use flash_common::logging::{init_tracing, shutdown_tracing};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let logger_provider = init_tracing("flash-client-consumer")?;

    let mut client = Client::connect("127.0.0.1:8080").await?;
    println!("Connected to server");

    let topic = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "notifications".to_string());
    tracing::info!(%topic, "consumer listening to topic");
    client.subscribe(topic.clone()).await?;
    println!("Subscribed to {}", topic);

    // Read the OK from subscribe
    if let Some(resp) = client.next_response().await {
        println!("Subscription response: {:?}", resp);
    }

    println!("Waiting for messages...");
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);
    loop {
        let response = tokio::select! {
            response = client.next_response() => response,
            signal = &mut shutdown_signal => {
                signal?;
                tracing::info!("consumer shutdown requested");
                break;
            }
        };
        let Some(resp) = response else {
            break;
        };

        match resp {
            Response::Message { topic, payload } => {
                let msg = String::from_utf8_lossy(&payload);
                tracing::debug!(%topic, payload_len = payload.len(), "received published message");
                println!("Received on {}: {}", topic, msg);
            }
            other => {
                println!("Received other response: {:?}", other);
            }
        }
    }

    tracing::info!("consumer closed");
    shutdown_tracing(logger_provider)?;
    Ok(())
}
