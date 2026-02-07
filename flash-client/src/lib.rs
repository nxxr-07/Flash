use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use flash_common::{Command, Response};
use tokio::sync::mpsc;

pub struct Client {
    writer: OwnedWriteHalf,
    lines_rx: mpsc::Receiver<String>,
}

impl Client {
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (read_half, write_half) = stream.into_split();
        
        let (tx, rx) = mpsc::channel(32);
        
        // Background reader task
        tokio::spawn(async move {
            let mut reader = BufReader::new(read_half);
            let mut line = String::new();
            loop {
                line.clear();
                match reader.read_line(&mut line).await {
                    Ok(0) => break, // EOF
                    Ok(_) => {
                        if tx.send(line.clone()).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(Self {
            writer: write_half,
            lines_rx: rx,
        })
    }

    pub async fn publish(&mut self, topic: String, payload: Vec<u8>) -> anyhow::Result<()> {
        let cmd = Command::Publish { topic, payload };
        let bytes = serde_json::to_vec(&cmd)?;
        self.writer.write_all(&bytes).await?;
        self.writer.write_all(b"\n").await?;
        Ok(())
    }
    
    pub async fn subscribe(&mut self, topic: String) -> anyhow::Result<()> {
        let cmd = Command::Subscribe { topic };
        let bytes = serde_json::to_vec(&cmd)?;
        self.writer.write_all(&bytes).await?;
        self.writer.write_all(b"\n").await?;
        Ok(())
    }
    
    pub async fn next_response(&mut self) -> Option<Response> {
        // Read from the background reader
        if let Some(line) = self.lines_rx.recv().await {
             serde_json::from_str(&line).ok()
        } else {
            None
        }
    }
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
