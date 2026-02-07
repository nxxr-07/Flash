use flash_client::Client;
use tokio::io::{self, AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut client = Client::connect("127.0.0.1:8080").await?;
    println!("Connected to Flash server");
    println!("Type messages to publish (Ctrl+C to exit):");

    let topic = "notifications".to_string();
    
    // Read from stdin
    let stdin = io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        line.clear();
        print!("> ");
        
        // Flush stdout so the prompt appears immediately
        use std::io::Write;
        std::io::stdout().flush()?;
        
        let bytes_read = reader.read_line(&mut line).await?;
        
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
        client.publish(topic.clone(), message.as_bytes().to_vec()).await?;
        
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

    Ok(())
}
