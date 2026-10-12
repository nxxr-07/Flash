use flash_client::Client;
use flash_common::logging::{init_tracing, shutdown_tracing};
use tokio::io::{self, AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let logger_provider = init_tracing("flash-client-producer")?;

    let mut client = Client::connect("127.0.0.1:8080").await?;
    println!("Connected to Flash server");

    let topic = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "notifications".to_string());
    tracing::info!(%topic, "producer publishing to topic");
    println!("Publishing to topic: {topic}");
    println!("Type messages to publish (Ctrl+C to exit):");

    // Read from stdin
    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();
    let shutdown_signal = tokio::signal::ctrl_c();
    tokio::pin!(shutdown_signal);

    loop {
        line.clear();
        print!("> ");

        // Flush stdout so the prompt appears immediately
        use std::io::Write;
        std::io::stdout().flush()?;

        let bytes_read = tokio::select! {
            result = reader.read_line(&mut line) => result?,
            signal = &mut shutdown_signal => {
                signal?;
                tracing::info!("producer shutdown requested");
                break;
            }
        };

        if bytes_read == 0 {
            // EOF (Ctrl+D on Unix, Ctrl+Z on Windows)
            println!("\nDisconnecting...");
            break;
        }

        // Remove trailing newline
        let message = line.trim();

        if message.is_empty() {
            continue;
        }

        // Publish the message
        client
            .publish(topic.clone(), message.as_bytes().to_vec())
            .await?;

        // Wait for response
        if let Some(resp) = client.next_response().await {
            match resp {
                flash_common::Response::Ok => {
                    println!("✓ Published: {}", message);
                }
                flash_common::Response::Error(e) => {
                    println!("✗ Error: {}", e);
                }
                _ => {
                    println!("Unexpected response: {:?}", resp);
                }
            }
        }
    }

    tracing::info!("producer closed");
    shutdown_tracing(logger_provider)?;
    Ok(())
}
