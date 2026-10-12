use flash_client::Client;
use flash_common::Response;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = match EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()
    {
        Ok(filter) => filter,
        Err(error) => return Err(anyhow::anyhow!("invalid RUST_LOG filter: {error}")),
    };

    if let Err(error) = tracing_subscriber::fmt().with_env_filter(filter).try_init() {
        return Err(anyhow::anyhow!(
            "failed to initialize tracing subscriber: {error}"
        ));
    }

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
                tracing::debug!(%topic, payload_len = payload.len(), "received published message");
                println!("Received on {}: {}", topic, msg);
            }
            other => {
                println!("Received other response: {:?}", other);
            }
        }
    }

    Ok(())
}
