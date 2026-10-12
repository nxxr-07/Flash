use flash_common::{Command, Response};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

pub struct Client {
    writer: OwnedWriteHalf,
    lines_rx: mpsc::Receiver<String>,
}

impl Client {
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        debug!(%addr, "connecting to server");
        let stream = TcpStream::connect(addr).await?;
        let (read_half, write_half) = stream.into_split();
        info!(%addr, "connected to server");

        let (tx, rx) = mpsc::channel(32);

        // Background reader task
        tokio::spawn(async move {
            let mut reader = BufReader::new(read_half);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => {
                        debug!("server closed the connection");
                        break;
                    }
                    Ok(_) => {
                        if tx.send(line.clone()).await.is_err() {
                            debug!("response receiver was dropped");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "failed to read response from server");
                        break;
                    }
                }
            }
        });

        Ok(Self {
            writer: write_half,
            lines_rx: rx,
        })
    }

    pub async fn publish(&mut self, topic: String, payload: Vec<u8>) -> anyhow::Result<()> {
        debug!(%topic, payload_len = payload.len(), "sending publish command");
        let cmd = Command::Publish { topic, payload };
        let bytes = serde_json::to_vec(&cmd)?;
        self.writer.write_all(&bytes).await?;
        self.writer.write_all(b"\n").await?;
        Ok(())
    }

    pub async fn subscribe(&mut self, topic: String) -> anyhow::Result<()> {
        debug!(%topic, "sending subscribe command");
        let cmd = Command::Subscribe { topic };
        let bytes = serde_json::to_vec(&cmd)?;
        self.writer.write_all(&bytes).await?;
        self.writer.write_all(b"\n").await?;
        Ok(())
    }

    pub async fn next_response(&mut self) -> Option<Response> {
        // Read from the background reader
        if let Some(line) = self.lines_rx.recv().await {
            match serde_json::from_str(&line) {
                Ok(response) => Some(response),
                Err(e) => {
                    warn!(error = %e, "received invalid response from server");
                    None
                }
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn next_response_decodes_server_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut response = serde_json::to_vec(&Response::Ok).unwrap();
            response.push(b'\n');
            socket.write_all(&response).await.unwrap();
        });

        let mut client = Client::connect(&addr.to_string()).await.unwrap();
        assert!(matches!(client.next_response().await, Some(Response::Ok)));
        server.await.unwrap();
    }
}
