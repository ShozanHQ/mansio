//! A webhook receiver for the tests: an HTTP server that records what is
//! POSTed to it and answers as it is told to.
//!
//! In process the server reaches it on loopback. Against a live server, which
//! runs in a container, the receiver listens on every interface and the server
//! reaches it by the name in `MANSIO_E2E_WEBHOOK_HOST` (`host.docker.internal`
//! for `compose.yaml`); without it those tests fail, as a run that cannot be
//! delivered to has shown nothing.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context, ensure};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use crate::harness::{SLACK, Target};

/// How the receiver answers one request.
#[derive(Debug, Clone, Copy)]
pub enum Reply {
    /// With this status and nothing else.
    Status(u16),
    /// Not at all: it reads the request and holds the connection open longer
    /// than any timeout.
    Hang,
}

/// One request the receiver was sent.
#[derive(Debug, Clone)]
pub struct Received {
    /// The headers, names in lower case.
    pub headers: HashMap<String, String>,
    /// The body.
    pub body: Vec<u8>,
    /// When it arrived.
    pub at: Instant,
}

impl Received {
    pub fn header(&self, name: &str) -> anyhow::Result<&str> {
        self.headers
            .get(name)
            .map(String::as_str)
            .with_context(|| format!("the request has no {name} header"))
    }
}

struct Script {
    replies: VecDeque<Reply>,
    otherwise: Reply,
}

/// A receiver that is listening. It stops when dropped.
pub struct Receiver {
    /// The address to register a webhook for.
    pub url: String,
    received: Arc<Mutex<Vec<Received>>>,
    task: JoinHandle<()>,
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> anyhow::Result<Received> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    let end = loop {
        if let Some(at) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break at + 4;
        }
        let n = socket.read(&mut chunk).await?;
        ensure!(
            n > 0,
            "the connection closed before the request was complete"
        );
        buffer.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buffer[..end]).into_owned();
    let headers: HashMap<String, String> = head
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let length: usize = headers
        .get("content-length")
        .map_or(Ok(0), |value| value.parse())
        .context("content-length")?;
    while buffer.len() < end + length {
        let n = socket.read(&mut chunk).await?;
        ensure!(n > 0, "the connection closed before the body was complete");
        buffer.extend_from_slice(&chunk[..n]);
    }
    Ok(Received {
        headers,
        body: buffer[end..end + length].to_vec(),
        at: Instant::now(),
    })
}

impl Receiver {
    /// Start a receiver that answers the first requests as `replies` says, and
    /// every one after with a 204.
    pub async fn start(
        target: &Target,
        replies: impl IntoIterator<Item = Reply>,
    ) -> anyhow::Result<Self> {
        let (bind, host) = if target.is_local() {
            ("127.0.0.1:0", "127.0.0.1".to_owned())
        } else {
            let host = std::env::var("MANSIO_E2E_WEBHOOK_HOST")
                .ok()
                .filter(|host| !host.is_empty())
                .context(
                    "MANSIO_E2E_WEBHOOK_HOST must be set when MANSIO_E2E_URL is: the server has to reach the receiver",
                )?;
            ("0.0.0.0:0", host)
        };
        let listener = TcpListener::bind(bind).await?;
        let url = format!("http://{host}:{}/hook", listener.local_addr()?.port());
        let received = Arc::new(Mutex::new(Vec::new()));
        let script = Arc::new(Mutex::new(Script {
            replies: replies.into_iter().collect(),
            otherwise: Reply::Status(204),
        }));
        let (log, answers) = (Arc::clone(&received), Arc::clone(&script));
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let (log, answers) = (Arc::clone(&log), Arc::clone(&answers));
                tokio::spawn(async move {
                    let Ok(request) = read_request(&mut socket).await else {
                        return;
                    };
                    locked(&log).push(request);
                    let reply = {
                        let mut script = locked(&answers);
                        let otherwise = script.otherwise;
                        script.replies.pop_front().unwrap_or(otherwise)
                    };
                    match reply {
                        Reply::Status(status) => {
                            let head = format!(
                                "HTTP/1.1 {status} Answer\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                            );
                            let _ = socket.write_all(head.as_bytes()).await;
                        }
                        Reply::Hang => tokio::time::sleep(Duration::from_secs(3600)).await,
                    }
                });
            }
        });
        Ok(Self {
            url,
            received,
            task,
        })
    }

    /// Everything received so far, in the order it arrived.
    pub fn requests(&self) -> Vec<Received> {
        locked(&self.received).clone()
    }

    /// Wait until at least `count` requests have arrived.
    pub async fn wait_for(
        &self,
        target: &Target,
        count: usize,
        timeout: Duration,
    ) -> anyhow::Result<Vec<Received>> {
        target
            .until(&format!("{count} deliveries"), timeout, || async {
                let all = self.requests();
                Ok((all.len() >= count).then_some(all))
            })
            .await
    }
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The signature of a body, worked out here and not by mansio: HMAC-SHA256 of
/// `<t>.<body>` under the secret's text, in hex.
#[expect(
    clippy::expect_used,
    reason = "HMAC takes a key of any length, so this cannot fail"
)]
pub fn signature(secret: &str, timestamp: i64, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("any key length");
    mac.update(format!("{timestamp}.").as_bytes());
    mac.update(body);
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Check the delivery as a receiver would: the content type, a signature that
/// is the one worked out here over the timestamp it names and the body, and a
/// timestamp that is recent. Returns the timestamp.
pub fn check_signed(received: &Received, secret: &str) -> anyhow::Result<i64> {
    ensure!(
        received.header("content-type")? == "application/cloudevents+json",
        "content-type is {:?}",
        received.header("content-type")
    );
    let header = received.header("mansio-signature")?;
    let mut parts = header.split(',');
    let timestamp: i64 = parts
        .next()
        .and_then(|part| part.strip_prefix("t="))
        .and_then(|value| value.parse().ok())
        .with_context(|| format!("no t= in {header:?}"))?;
    let given = parts
        .next()
        .and_then(|part| part.strip_prefix("v1="))
        .with_context(|| format!("no v1= in {header:?}"))?;
    ensure!(parts.next().is_none(), "more than t and v1 in {header:?}");
    ensure!(
        given == signature(secret, timestamp, &received.body),
        "the signature in {header:?} is not the HMAC-SHA256 of the timestamp and the body"
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let age = now.abs_diff(u64::try_from(timestamp)?);
    ensure!(
        age <= SLACK.as_secs() * 4,
        "the timestamp in {header:?} is {age}s from now"
    );
    Ok(timestamp)
}
