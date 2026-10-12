use flash_common::logging::{init_tracing, shutdown_tracing};
use flash_common::{Command, Response};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

/// Shared state to hold topics and their broadcast channels
/// In a real world scenario, we'd use DashMap for better concurrency,
/// but Mutex<HashMap> is fine for learning.
type Db = Arc<Mutex<HashMap<String, broadcast::Sender<Vec<u8>>>>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let logger_provider = init_tracing("flash-server")?;

    // Bind to a port
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    tracing::info!(address = %listener.local_addr()?, "server started");

    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let shutdown = CancellationToken::new();
    let mut connections = JoinSet::new();
    let shutdown_signal = shutdown_signal();
    tokio::pin!(shutdown_signal);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (socket, addr) = accepted?;
                tracing::info!(%addr, "new connection started");

                let db = db.clone();
                let span = tracing::info_span!("connection", %addr);
                let shutdown = shutdown.clone();

                connections.spawn(
                    async move {
                        if let Err(e) = process(socket, db, shutdown).await {
                            tracing::error!(error = %e, "failed to process connection");
                        }
                        tracing::info!("connection closed");
                    }
                    .instrument(span),
                );
            }
            Some(result) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = result {
                    tracing::error!(%error, "connection task failed");
                }
            }
            signal = &mut shutdown_signal => {
                signal?;
                tracing::info!("server shutdown requested");
                shutdown.cancel();
                break;
            }
        }
    }

    while let Some(result) = connections.join_next().await {
        if let Err(error) = result {
            tracing::error!(%error, "connection task failed during shutdown");
        }
    }

    tracing::info!("server closed");
    shutdown_tracing(logger_provider)?;
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result,
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}

async fn process(
    socket: TcpStream,
    db: Db,
    server_shutdown: CancellationToken,
) -> anyhow::Result<()> {
    tracing::debug!("connection processing started");

    // Split the socket into read and write halves so we can operate on them independently
    let (read_half, mut write_half) = socket.into_split();

    // Create a channel effectively acting as the "write queue" for this socket
    // This allows multiple parts of our logic (the reader loop, subscription tasks)
    // to send messages to the client without fighting over the socket lock.
    let (tx, mut rx) = mpsc::channel::<Response>(32);
    let connection_shutdown = CancellationToken::new();
    let mut tasks = JoinSet::new();

    // Spawn the "Writer Task"
    // It simply takes Responses from the channel and writes them to the TCP socket
    let writer_shutdown = connection_shutdown.clone();
    tasks.spawn(
        async move {
            loop {
                let resp = tokio::select! {
                    _ = writer_shutdown.cancelled() => break,
                    response = rx.recv() => match response {
                        Some(response) => response,
                        None => break,
                    }
                };
                let mut bytes = match serde_json::to_vec(&resp) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to serialize response");
                        continue;
                    }
                };
                bytes.push(b'\n'); // Newline delimiter for this simple protocol

                let write_result = tokio::select! {
                    _ = writer_shutdown.cancelled() => break,
                    result = write_half.write_all(&bytes) => result,
                };
                if let Err(e) = write_result {
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

    let result = 'reader: loop {
        line.clear();
        let bytes_read = tokio::select! {
            _ = server_shutdown.cancelled() => break 'reader Ok(()),
            _ = connection_shutdown.cancelled() => break 'reader Ok(()),
            result = reader.read_line(&mut line) => match result {
                Ok(bytes_read) => bytes_read,
                Err(error) => break 'reader Err(error.into()),
            },
        };
        if bytes_read == 0 {
            break 'reader Ok(()); // Connection closed
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
                tracing::info!(%topic, payload_len = payload.len(), "message produced");
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
                let subscription_shutdown = connection_shutdown.clone();

                // Spawn a task to forward messages from this topic to the client
                tasks.spawn(async move {
                    let tx = tx_for_task;
                    loop {
                        let received = tokio::select! {
                            _ = subscription_shutdown.cancelled() => break,
                            received = rx.recv() => received,
                        };
                        match received {
                            Ok(payload) => {
                                tracing::info!(
                                    topic = %topic_name,
                                    payload_len = payload.len(),
                                    "message received by subscriber"
                                );
                                let msg = Response::Message {
                                    topic: topic_name.clone(),
                                    payload,
                                };
                                let sent = tokio::select! {
                                    _ = subscription_shutdown.cancelled() => break,
                                    sent = tx.send(msg) => sent,
                                };
                                if sent.is_err() {
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
    };

    connection_shutdown.cancel();
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            tracing::error!(%error, "connection child task failed");
        }
    }

    result
}
