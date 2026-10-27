//! A JSON Lines file sink, for the standalone binary.

use std::io;
use std::path::Path;

use async_trait::async_trait;
use mansio_core::{Envelope, Record, Sink, SinkError};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// Appends each envelope to a file as one line of CloudEvents JSON. It makes
/// no record, so nothing is set on the envelope.
#[derive(Debug)]
pub struct FileSink {
    file: Mutex<File>,
}

impl FileSink {
    /// Open the file for appending, creating it if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened.
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn record(&self, envelope: &Envelope) -> Result<Option<Record>, SinkError> {
        let mut line = envelope
            .to_json_vec()
            .map_err(|e| SinkError::Failed(e.to_string()))?;
        line.push(b'\n');
        let mut file = self.file.lock().await;
        file.write_all(&line)
            .await
            .map_err(|e| SinkError::Failed(e.to_string()))?;
        file.flush()
            .await
            .map_err(|e| SinkError::Failed(e.to_string()))?;
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use mansio_core::EventId;

    use super::*;

    fn envelope(kind: &str) -> Envelope {
        Envelope::new(
            EventId::generate(),
            "test".parse().unwrap(),
            kind.parse().unwrap(),
            "acme".parse().unwrap(),
            "facts.build".parse().unwrap(),
            "service:test".parse().unwrap(),
        )
    }

    #[tokio::test]
    async fn each_record_is_one_line_and_reopening_appends() {
        let dir = std::env::temp_dir().join(format!("mansio-sink-{}", EventId::generate()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("sink.jsonl");

        let first = envelope("one");
        let second = envelope("two");
        let sink = FileSink::open(&path).await.unwrap();
        assert_eq!(sink.record(&first).await.unwrap(), None);
        drop(sink);
        let sink = FileSink::open(&path).await.unwrap();
        sink.record(&second).await.unwrap();

        let text = tokio::fs::read_to_string(&path).await.unwrap();
        let read: Vec<Envelope> = text
            .lines()
            .map(|line| Envelope::from_json_slice(line.as_bytes()).unwrap())
            .collect();
        assert_eq!(read, vec![first, second]);
        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }
}
