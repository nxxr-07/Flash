use flash_common::{Command, Response};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tracing::Instrument;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

/// Shared state to hold topics and their broadcast channels
/// In a real world scenario, we'd use DashMap for better concurrency,
/// but Mutex<HashMap> is fine for learning.
type Db = Arc<Mutex<HashMap<String, broadcast::Sender<Vec<u8>>>>>;

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

    // Bind to a port
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    tracing::info!(address = %listener.local_addr()?, "server listening");

    let db: Db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        // Accept a new connection
        let (socket, addr) = listener.accept().await?;
        tracing::info!(%addr, "accepted connection");

        let db = db.clone();
        let span = tracing::info_span!("connection", %addr);

        // Spawn a task for each connection so we can handle them concurrently
        tokio::spawn(
            async move {
                if let Err(e) = process(socket, db).await {
                    tracing::error!(error = %e, "failed to process connection");
                }
            }
            .instrument(span),
        );
    }
}

async fn process(socket: TcpStream, db: Db) -> anyhow::Result<()> {
    tracing::debug!("connection processing started");

    // Split the socket into read and write halves so we can operate on them independently
    let (read_half, mut write_half) = socket.into_split();

    // Create a channel effectively acting as the "write queue" for this socket
    // This allows multiple parts of our logic (the reader loop, subscription tasks)
    // to send messages to the client without fighting over the socket lock.
    let (tx, mut rx) = mpsc::channel::<Response>(32);

    // Spawn the "Writer Task"
    // It simply takes Responses from the channel and writes them to the TCP socket
    tokio::spawn(
        async move {
            while let Some(resp) = rx.recv().await {
                let mut bytes = match serde_json::to_vec(&resp) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to serialize response");
                        continue;
                    }
                };
                bytes.push(b'\n'); // Newline delimiter for this simple protocol

                if let Err(e) = write_half.write_all(&bytes).await {
                    tracing::error!(error = %e, "failed to write to socket");
                    break;
                }
            }
        }
        .instrument(tracing::Span::current()),
    );

    // Main "Reader Loop"
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    loop {
        line.clear();
        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            break; // Connection closed
        }

        let cmd: Command = match serde_json::from_str(&line) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "received invalid command");
                let _ = tx.send(Response::Error(e.to_string())).await;
                continue;
            }
        };

        match cmd {
            Command::Publish { topic, payload } => {
                tracing::debug!(%topic, payload_len = payload.len(), "publishing message");
                // Get the broadcast sender for the topic
                let sender = {
                    let mut lock = db.lock().unwrap();
                    lock.entry(topic.clone())
                        .or_insert_with(|| {
                            // Create a new broadcast channel if it doesn't exist
                            // Capacity 100 messages backlog
                            let (s, _) = broadcast::channel(100);
                            s
                        })
                        .clone()
                };

                // Send the payload to all subscribers
                // We ignore the error if there are no subscribers
                match sender.send(payload) {
                    Ok(subscriber_count) => {
                        tracing::debug!(%topic, subscriber_count, "message broadcast");
                    }
                    Err(_) => {
                        tracing::debug!(%topic, "published message with no subscribers");
                    }
                }

                // Acknowledge the publish
                let _ = tx.send(Response::Ok).await;
            }
            Command::Subscribe { topic } => {
                tracing::info!(%topic, "subscribed client to topic");
                // Get a receiver for the topic
                let mut rx = {
                    let mut lock = db.lock().unwrap();
                    let sender = lock.entry(topic.clone()).or_insert_with(|| {
                        let (s, _) = broadcast::channel(100);
                        s
                    });
                    sender.subscribe()
                };

                // Clone tx for the task, so we keep the original tx for sending Ok
                let tx_for_task = tx.clone();
                let topic_name = topic.clone();

                // Spawn a task to forward messages from this topic to the client
                tokio::spawn(async move {
                    let tx = tx_for_task;
                    loop {
                        match rx.recv().await {
                            Ok(payload) => {
                                let msg = Response::Message {
                                    topic: topic_name.clone(),
                                    payload,
                                };
                                if tx.send(msg).await.is_err() {
                                    break; // Client disconnected (receiver dropped)
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                tracing::warn!(%topic_name, skipped_messages = n, "subscriber lagged");
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                break; // Channel closed
                            }
                        }
                    }
                }.instrument(tracing::Span::current()));

                let _ = tx.send(Response::Ok).await;
            }
        }
    }

    tracing::debug!("connection closed");
    Ok(())
}
