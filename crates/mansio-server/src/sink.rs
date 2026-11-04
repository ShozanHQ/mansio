//! A JSON Lines file sink, for the standalone binary.

use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use mansio_core::{Envelope, Record, Sink, SinkError};
use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;

/// Why the sink file could not be opened.
#[derive(Debug, Error)]
pub enum SinkOpenError {
    /// The file could not be opened or read.
    #[error("{}: {source}", path.display())]
    Io {
        /// The sink file.
        path: PathBuf,
        /// What the operating system said.
        source: io::Error,
    },
    /// The file ends in a line that has no newline: a write was cut short.
    /// The server will not append after it, because the next record would
    /// join the torn one.
    #[error(
        "the sink file {} ends in a torn line at byte offset {offset}; \
         repair or remove that line (truncate the file to {offset} bytes) and start again",
        path.display()
    )]
    TornLastLine {
        /// The sink file.
        path: PathBuf,
        /// Offset of the first byte of the torn line.
        offset: u64,
    },
}

/// Appends each envelope to a file as one line of CloudEvents JSON. It makes
/// no record, so nothing is set on the envelope.
///
/// Each record is written in one call as a complete line, and the file is
/// synced to disk before `record` returns, unless the sync is turned off
/// (`MANSIO_SINK_SYNC=false`).
#[derive(Debug)]
pub struct FileSink {
    path: PathBuf,
    state: Mutex<State<File>>,
    sync: bool,
}

/// The open file and, once a failed write could not be cut back, the offset
/// of the half line it left. A broken sink refuses every record.
#[derive(Debug)]
struct State<T> {
    target: T,
    broken_at: Option<u64>,
}

/// What a record needs from the file, so a test can make a write fail.
#[async_trait]
trait Target: Send {
    async fn len(&mut self) -> io::Result<u64>;
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>;
    async fn sync_data(&mut self) -> io::Result<()>;
    async fn set_len(&mut self, len: u64) -> io::Result<()>;
}

#[async_trait]
impl Target for File {
    async fn len(&mut self) -> io::Result<u64> {
        Ok(self.metadata().await?.len())
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(self, bytes).await?;
        self.flush().await
    }
    async fn sync_data(&mut self) -> io::Result<()> {
        File::sync_data(self).await
    }
    async fn set_len(&mut self, len: u64) -> io::Result<()> {
        File::set_len(self, len).await
    }
}

impl FileSink {
    /// Open the file for appending, creating it if it does not exist. With
    /// `sync`, every record is synced to disk before `record` returns.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, or if it ends in a
    /// torn line (one with no newline), naming the file and the offset.
    pub async fn open(path: impl AsRef<Path>, sync: bool) -> Result<Self, SinkOpenError> {
        let path = path.as_ref();
        let io_err = |source| SinkOpenError::Io {
            path: path.to_owned(),
            source,
        };
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .await
            .map_err(io_err)?;
        if let Some(offset) = torn_line_offset(&mut file).await.map_err(io_err)? {
            return Err(SinkOpenError::TornLastLine {
                path: path.to_owned(),
                offset,
            });
        }
        Ok(Self {
            path: path.to_owned(),
            state: Mutex::new(State {
                target: file,
                broken_at: None,
            }),
            sync,
        })
    }
}

/// The offset where the last line starts, if the file does not end in a
/// newline.
async fn torn_line_offset(file: &mut File) -> io::Result<Option<u64>> {
    const BLOCK: u64 = 8192;
    let len = file.metadata().await?.len();
    let mut end = len;
    while end > 0 {
        let start = end.saturating_sub(BLOCK);
        file.seek(SeekFrom::Start(start)).await?;
        let mut buf = vec![0_u8; usize::try_from(end - start).unwrap_or(0)];
        file.read_exact(&mut buf).await?;
        if end == len && buf.last() == Some(&b'\n') {
            return Ok(None);
        }
        if let Some(i) = buf.iter().rposition(|b| *b == b'\n') {
            return Ok(Some(start + u64::try_from(i).unwrap_or(0) + 1));
        }
        end = start;
    }
    Ok(if len == 0 { None } else { Some(0) })
}

#[async_trait]
impl Sink for FileSink {
    async fn record(&self, envelope: &Envelope) -> Result<Option<Record>, SinkError> {
        let mut line = envelope
            .to_json_vec()
            .map_err(|e| SinkError::Failed(e.to_string()))?;
        line.push(b'\n');
        let mut state = self.state.lock().await;
        append(&mut state, &line, self.sync, &self.path).await?;
        Ok(None)
    }
}

/// Write one whole line and sync it. If the write fails, cut the file back to
/// where it was; if that fails too, the sink is broken until a restart, so the
/// next record cannot join a half line.
async fn append<T: Target>(
    state: &mut State<T>,
    line: &[u8],
    sync: bool,
    path: &Path,
) -> Result<(), SinkError> {
    if let Some(offset) = state.broken_at {
        return Err(broken(path, offset));
    }
    let before = state
        .target
        .len()
        .await
        .map_err(|e| SinkError::Failed(format!("{}: {e}", path.display())))?;
    let written = async {
        state.target.write_all(line).await?;
        if sync {
            state.target.sync_data().await?;
        }
        Ok::<(), io::Error>(())
    }
    .await;
    let Err(error) = written else {
        return Ok(());
    };
    if let Err(cut) = state.target.set_len(before).await {
        state.broken_at = Some(before);
        tracing::error!(
            file = %path.display(),
            offset = before,
            %error,
            %cut,
            "the sink could not cut a failed write back; refusing records until restart"
        );
        return Err(broken(path, before));
    }
    Err(SinkError::Failed(format!("{}: {error}", path.display())))
}

fn broken(path: &Path, offset: u64) -> SinkError {
    SinkError::Failed(format!(
        "the sink file {} may end in a torn line at byte offset {offset}; \
         refusing records until the server restarts and the line is repaired",
        path.display()
    ))
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
        let sink = FileSink::open(&path, true).await.unwrap();
        assert_eq!(sink.record(&first).await.unwrap(), None);
        drop(sink);
        let sink = FileSink::open(&path, true).await.unwrap();
        sink.record(&second).await.unwrap();

        let text = tokio::fs::read_to_string(&path).await.unwrap();
        let read: Vec<Envelope> = text
            .lines()
            .map(|line| Envelope::from_json_slice(line.as_bytes()).unwrap())
            .collect();
        assert_eq!(read, vec![first, second]);
        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn a_torn_last_line_is_refused_with_the_file_and_the_offset() {
        let dir = std::env::temp_dir().join(format!("mansio-sink-{}", EventId::generate()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("sink.jsonl");
        let sink = FileSink::open(&path, true).await.unwrap();
        sink.record(&envelope("one")).await.unwrap();
        drop(sink);
        let whole = tokio::fs::metadata(&path).await.unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&path).await.unwrap();
        AsyncWriteExt::write_all(&mut file, b"{\"specversion\":\"1.0\",\"id\"")
            .await
            .unwrap();
        file.flush().await.unwrap();
        drop(file);

        let error = FileSink::open(&path, true).await.unwrap_err();
        assert!(matches!(
            &error,
            SinkOpenError::TornLastLine { offset, .. } if *offset == whole
        ));
        let message = error.to_string();
        assert!(message.contains(&path.display().to_string()), "{message}");
        assert!(message.contains(&format!("offset {whole}")), "{message}");
        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn a_file_with_no_newline_at_all_is_torn_at_offset_zero() {
        let dir = std::env::temp_dir().join(format!("mansio-sink-{}", EventId::generate()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("sink.jsonl");
        tokio::fs::write(&path, b"{\"half").await.unwrap();
        let error = FileSink::open(&path, false).await.unwrap_err();
        assert!(matches!(
            error,
            SinkOpenError::TornLastLine { offset: 0, .. }
        ));
        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    #[tokio::test]
    async fn the_sync_can_be_turned_off_and_lines_still_land() {
        let dir = std::env::temp_dir().join(format!("mansio-sink-{}", EventId::generate()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        let path = dir.join("sink.jsonl");
        let sink = FileSink::open(&path, false).await.unwrap();
        sink.record(&envelope("one")).await.unwrap();
        let text = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(text.ends_with('\n'));
        assert_eq!(text.lines().count(), 1);
        tokio::fs::remove_dir_all(&dir).await.unwrap();
    }

    /// An in-memory file whose writes and truncation can be made to fail.
    #[derive(Default)]
    struct Fake {
        bytes: Vec<u8>,
        fail_write_after: Option<usize>,
        fail_set_len: bool,
        syncs: usize,
    }

    #[async_trait]
    impl Target for Fake {
        async fn len(&mut self) -> io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }
        async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
            if let Some(n) = self.fail_write_after {
                self.bytes.extend_from_slice(&bytes[..n.min(bytes.len())]);
                return Err(io::Error::other("disk full"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(())
        }
        async fn sync_data(&mut self) -> io::Result<()> {
            self.syncs += 1;
            Ok(())
        }
        async fn set_len(&mut self, len: u64) -> io::Result<()> {
            if self.fail_set_len {
                return Err(io::Error::other("cannot truncate"));
            }
            self.bytes.truncate(usize::try_from(len).unwrap());
            Ok(())
        }
    }

    fn state(fake: Fake) -> State<Fake> {
        State {
            target: fake,
            broken_at: None,
        }
    }

    #[tokio::test]
    async fn sync_data_runs_per_record_when_on_and_not_when_off() {
        let path = Path::new("sink.jsonl");
        let mut on = state(Fake::default());
        append(&mut on, b"a\n", true, path).await.unwrap();
        append(&mut on, b"b\n", true, path).await.unwrap();
        assert_eq!(on.target.syncs, 2);
        let mut off = state(Fake::default());
        append(&mut off, b"a\n", false, path).await.unwrap();
        assert_eq!(off.target.syncs, 0);
    }

    #[tokio::test]
    async fn a_failed_write_is_cut_back_and_the_next_record_reads_cleanly() {
        let path = Path::new("sink.jsonl");
        let mut s = state(Fake::default());
        append(&mut s, b"one\n", true, path).await.unwrap();
        s.target.fail_write_after = Some(2);
        assert!(append(&mut s, b"two\n", true, path).await.is_err());
        assert_eq!(s.target.bytes, b"one\n");
        s.target.fail_write_after = None;
        append(&mut s, b"three\n", true, path).await.unwrap();
        assert_eq!(s.target.bytes, b"one\nthree\n");
    }

    #[tokio::test]
    async fn when_the_cut_back_fails_the_sink_refuses_every_record() {
        let path = Path::new("sink.jsonl");
        let mut s = state(Fake::default());
        append(&mut s, b"one\n", true, path).await.unwrap();
        s.target.fail_write_after = Some(2);
        s.target.fail_set_len = true;
        assert!(append(&mut s, b"two\n", true, path).await.is_err());
        s.target.fail_write_after = None;
        s.target.fail_set_len = false;
        let SinkError::Failed(message) = append(&mut s, b"three\n", true, path).await.unwrap_err();
        assert!(message.contains("sink.jsonl"), "{message}");
        assert!(message.contains("offset 4"), "{message}");
        assert_eq!(s.target.bytes, b"one\ntw");
    }
}
