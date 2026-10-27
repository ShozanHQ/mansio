//! A client for the mansio HTTP API.
//!
//! It retries a request that fails on the network: always when the connection
//! could not be made, because nothing was sent, and for a request that is safe
//! to repeat also when the answer may have been lost on the way. A request is
//! safe to repeat if doing it twice leaves the same state as doing it once, as
//! a read, a `work_pull` (which returns the lease a consumer already holds), a
//! `work_extend` (which leaves the lease running to the later of the two ends)
//! and a `consumers_commit` do.

use std::time::Duration;

use anyhow::{Context, bail};
use reqwest::{Method, StatusCode, Url};
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;

/// The default server address: a local `mansio-server`.
pub const DEFAULT_URL: &str = "http://127.0.0.1:7070";
/// The default request timeout, in seconds (`MANSIO_TIMEOUT_SECONDS`).
pub const DEFAULT_TIMEOUT_SECONDS: u64 = 30;
/// The default number of times a failed request is tried again
/// (`MANSIO_RETRIES`).
pub const DEFAULT_RETRIES: u32 = 3;
/// The default wait before the first retry, in milliseconds
/// (`MANSIO_RETRY_BACKOFF_MILLIS`). It doubles with each retry.
pub const DEFAULT_RETRY_BACKOFF_MILLIS: u64 = 250;
/// The default wait between looks for something that is not there yet, in
/// milliseconds (`MANSIO_POLL_INTERVAL_MILLIS`).
pub const DEFAULT_POLL_INTERVAL_MILLIS: u64 = 500;

/// Whether a request can be sent again after its answer may have been lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replay {
    /// Doing it twice leaves the state it would after once.
    Safe,
    /// Doing it twice may do it twice. It is sent again only if the
    /// connection could not be made.
    Unsafe,
}

/// Where the server is and who is asking.
#[derive(Debug, Clone)]
pub struct Client {
    http: reqwest::Client,
    base: Url,
    token: SecretString,
    retries: u32,
    retry_backoff: Duration,
    poll_interval: Duration,
}

/// What the caller said on the command line, before the environment fills in
/// the rest.
#[derive(Debug, Clone, Default)]
pub struct Connection {
    /// `--url`.
    pub url: Option<String>,
    /// `--token`.
    pub token: Option<String>,
}

impl Client {
    /// Build a client from the command line and the variables `MANSIO_URL`,
    /// `MANSIO_TOKEN`, `MANSIO_TIMEOUT_SECONDS`, `MANSIO_RETRIES`,
    /// `MANSIO_RETRY_BACKOFF_MILLIS` and `MANSIO_POLL_INTERVAL_MILLIS`, read
    /// through `lookup`. The command line wins.
    ///
    /// # Errors
    ///
    /// Fails, before anything is sent, if there is no token or a setting is
    /// not valid.
    pub fn from_lookup(
        connection: Connection,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> anyhow::Result<Self> {
        let set = |name: &str| lookup(name).filter(|value| !value.is_empty());
        let token = connection
            .token
            .filter(|token| !token.is_empty())
            .or_else(|| set("MANSIO_TOKEN"))
            .context("there is no token: set MANSIO_TOKEN or pass --token")?;
        let base = connection
            .url
            .filter(|url| !url.is_empty())
            .or_else(|| set("MANSIO_URL"))
            .unwrap_or_else(|| DEFAULT_URL.to_owned());
        let number = |name: &str, default: u64| -> anyhow::Result<u64> {
            match set(name) {
                Some(text) => text
                    .parse::<u64>()
                    .with_context(|| format!("{name} must be a whole number")),
                None => Ok(default),
            }
        };
        let timeout = number("MANSIO_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)?;
        let retries = number("MANSIO_RETRIES", u64::from(DEFAULT_RETRIES))?;
        let retry_backoff = number("MANSIO_RETRY_BACKOFF_MILLIS", DEFAULT_RETRY_BACKOFF_MILLIS)?;
        let poll_interval = number("MANSIO_POLL_INTERVAL_MILLIS", DEFAULT_POLL_INTERVAL_MILLIS)?;
        let base = Url::parse(&base).with_context(|| format!("the server address {base:?}"))?;
        if !matches!(base.scheme(), "http" | "https") || base.cannot_be_a_base() {
            bail!("the server address {base} must be an http or https address");
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout))
            .build()
            .context("building the HTTP client")?;
        Ok(Self {
            http,
            base,
            token: SecretString::from(token),
            retries: u32::try_from(retries).unwrap_or(u32::MAX),
            retry_backoff: Duration::from_millis(retry_backoff),
            poll_interval: Duration::from_millis(poll_interval.max(1)),
        })
    }

    /// How long to wait between looks for something that is not there yet.
    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    fn url(&self, segments: &[&str]) -> anyhow::Result<Url> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|()| anyhow::anyhow!("the server address cannot take a path"))?
            .pop_if_empty()
            .extend(segments);
        Ok(url)
    }

    /// Send a request and return the JSON the server answered with, or `None`
    /// for a 204.
    ///
    /// # Errors
    ///
    /// Fails with the server's reason if it refuses, and with the network's if
    /// the request could not be made even after the retries.
    pub async fn call(
        &self,
        method: Method,
        segments: &[&str],
        query: &[(&str, String)],
        body: Option<&Value>,
        replay: Replay,
    ) -> anyhow::Result<Option<Value>> {
        let url = self.url(segments)?;
        let mut attempt = 0_u32;
        loop {
            let mut request = self
                .http
                .request(method.clone(), url.clone())
                .bearer_auth(self.token.expose_secret())
                .query(query);
            if let Some(body) = body {
                request = request.json(body);
            }
            match send(request).await {
                Ok((status, bytes)) => return answer(status, &bytes),
                Err(error) if attempt < self.retries && may_retry(&error, replay) => {
                    let wait = self.retry_backoff.saturating_mul(1_u32 << attempt.min(16));
                    tokio::time::sleep(wait).await;
                    attempt += 1;
                }
                Err(error) => {
                    return Err(anyhow::Error::new(error.without_url())
                        .context(format!("sending to {}", self.base)));
                }
            }
        }
    }
}

/// Send, and read the whole answer: an answer cut off while it is read is as
/// lost as one that never came.
async fn send(request: reqwest::RequestBuilder) -> Result<(StatusCode, Vec<u8>), reqwest::Error> {
    let response = request.send().await?;
    let status = response.status();
    Ok((status, response.bytes().await?.to_vec()))
}

fn may_retry(error: &reqwest::Error, replay: Replay) -> bool {
    error.is_connect()
        || (replay == Replay::Safe && (error.is_timeout() || error.is_request() || error.is_body()))
}

/// The body of a successful answer, or the server's reason for refusing.
fn answer(status: StatusCode, bytes: &[u8]) -> anyhow::Result<Option<Value>> {
    if status == StatusCode::NO_CONTENT {
        return Ok(None);
    }
    if status.is_success() {
        let value =
            serde_json::from_slice(bytes).context("the server's answer was not understood")?;
        return Ok(Some(value));
    }
    bail!("{}", refusal(status, bytes))
}

fn refusal(status: StatusCode, body: &[u8]) -> String {
    match serde_json::from_slice::<mansio_core::api::ErrorBody>(body) {
        Ok(mansio_core::api::ErrorBody { error }) => {
            format!("refused ({status}): {}: {}", error.code, error.message)
        }
        Err(_) => format!("refused ({status})"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let map: std::collections::HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn the_token_is_required_and_nothing_is_built_without_it() {
        let error = Client::from_lookup(Connection::default(), lookup(&[])).unwrap_err();
        assert!(error.to_string().contains("MANSIO_TOKEN"), "{error}");
        let empty = Connection {
            url: None,
            token: Some(String::new()),
        };
        assert!(Client::from_lookup(empty, lookup(&[("MANSIO_TOKEN", "")])).is_err());
    }

    #[test]
    fn the_command_line_wins_over_the_environment() {
        let connection = Connection {
            url: Some("http://flag:1".to_owned()),
            token: Some("from-flag".to_owned()),
        };
        let client = Client::from_lookup(
            connection,
            lookup(&[("MANSIO_TOKEN", "from-env"), ("MANSIO_URL", "http://env:2")]),
        )
        .unwrap();
        assert_eq!(client.base.as_str(), "http://flag:1/");
        assert_eq!(client.token.expose_secret(), "from-flag");
        let client = Client::from_lookup(
            Connection::default(),
            lookup(&[("MANSIO_TOKEN", "from-env"), ("MANSIO_URL", "http://env:2")]),
        )
        .unwrap();
        assert_eq!(client.base.as_str(), "http://env:2/");
    }

    #[test]
    fn the_url_defaults_and_each_segment_is_encoded() {
        let client =
            Client::from_lookup(Connection::default(), lookup(&[("MANSIO_TOKEN", "t")])).unwrap();
        assert_eq!(client.base.as_str(), "http://127.0.0.1:7070/");
        let url = client
            .url(&[
                "v1", "tenants", "acme", "work", "builds", "items", "a/b c?", "ack",
            ])
            .unwrap();
        assert_eq!(
            url.as_str(),
            "http://127.0.0.1:7070/v1/tenants/acme/work/builds/items/a%2Fb%20c%3F/ack"
        );
        // A server behind a path keeps it.
        let client = Client::from_lookup(
            Connection::default(),
            lookup(&[
                ("MANSIO_TOKEN", "t"),
                ("MANSIO_URL", "https://h.example/mansio/"),
            ]),
        )
        .unwrap();
        assert_eq!(
            client.url(&["v1", "principals"]).unwrap().as_str(),
            "https://h.example/mansio/v1/principals"
        );
    }

    #[test]
    fn settings_that_are_not_numbers_or_addresses_are_refused() {
        for pairs in [
            &[("MANSIO_TOKEN", "t"), ("MANSIO_TIMEOUT_SECONDS", "soon")][..],
            &[("MANSIO_TOKEN", "t"), ("MANSIO_RETRIES", "-1")][..],
            &[("MANSIO_TOKEN", "t"), ("MANSIO_RETRY_BACKOFF_MILLIS", "x")][..],
            &[("MANSIO_TOKEN", "t"), ("MANSIO_POLL_INTERVAL_MILLIS", "x")][..],
            &[("MANSIO_TOKEN", "t"), ("MANSIO_URL", "not a url")][..],
            &[("MANSIO_TOKEN", "t"), ("MANSIO_URL", "ftp://h/")][..],
        ] {
            assert!(
                Client::from_lookup(Connection::default(), lookup(pairs)).is_err(),
                "{pairs:?}"
            );
        }
    }

    #[test]
    fn refusals_carry_the_servers_reason() {
        let body = br#"{"error":{"code":"bad_request","message":"count must be between 1 and 5"}}"#;
        assert_eq!(
            refusal(StatusCode::BAD_REQUEST, body),
            "refused (400 Bad Request): bad_request: count must be between 1 and 5"
        );
        assert_eq!(
            refusal(StatusCode::BAD_GATEWAY, b"<html>"),
            "refused (502 Bad Gateway)"
        );
        assert_eq!(answer(StatusCode::NO_CONTENT, b"").unwrap(), None);
        assert_eq!(
            answer(StatusCode::OK, b"{\"a\":1}").unwrap(),
            Some(json!({"a": 1}))
        );
        assert!(answer(StatusCode::OK, b"nope").is_err());
        assert!(answer(StatusCode::NOT_FOUND, b"").is_err());
    }

    /// A server that drops the first `drops` connections after reading the
    /// request, as a connection that fails with the answer on its way, and
    /// answers the rest. It counts the requests it read.
    async fn flaky_server(drops: u32) -> (String, Arc<AtomicU32>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(AtomicU32::new(0));
        let count = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let n = count.fetch_add(1, Ordering::SeqCst);
                let mut buffer = vec![0_u8; 4096];
                let _ = socket.read(&mut buffer).await;
                if n >= drops {
                    let body = r#"{"ok":true}"#;
                    let reply = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(reply.as_bytes()).await;
                }
                drop(socket);
            }
        });
        (base, seen)
    }

    fn client_for(base: &str, retries: &str) -> Client {
        let pairs = [
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_RETRY_BACKOFF_MILLIS", "1"),
            ("MANSIO_URL", base),
            ("MANSIO_RETRIES", retries),
        ];
        Client::from_lookup(Connection::default(), lookup(&pairs)).unwrap()
    }

    #[tokio::test]
    async fn a_request_that_is_safe_to_repeat_is_sent_again_when_its_answer_is_lost() {
        let (base, seen) = flaky_server(2).await;
        let client = client_for(&base, "3");
        let answer = client
            .call(Method::GET, &["v1", "x"], &[], None, Replay::Safe)
            .await
            .unwrap();
        assert_eq!(answer, Some(json!({"ok": true})));
        assert_eq!(seen.load(Ordering::SeqCst), 3, "two lost, one answered");
    }

    #[tokio::test]
    async fn a_request_that_is_not_safe_to_repeat_is_not_sent_again_once_it_was_sent() {
        let (base, seen) = flaky_server(1).await;
        let client = client_for(&base, "3");
        let error = client
            .call(
                Method::POST,
                &["v1", "x"],
                &[],
                Some(&json!({})),
                Replay::Unsafe,
            )
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("sending to"), "{error:#}");
        assert_eq!(seen.load(Ordering::SeqCst), 1, "it reached the server once");
    }

    #[tokio::test]
    async fn the_retries_are_a_setting_and_run_out() {
        let (base, seen) = flaky_server(100).await;
        let client = client_for(&base, "2");
        assert!(
            client
                .call(Method::GET, &["v1", "x"], &[], None, Replay::Safe)
                .await
                .is_err()
        );
        assert_eq!(seen.load(Ordering::SeqCst), 3, "the first try and two more");
    }

    #[tokio::test]
    async fn a_connection_that_cannot_be_made_is_tried_again_for_any_request() {
        // Nothing listens here: the port is taken and freed.
        let port = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap().port()
        };
        let base = format!("http://127.0.0.1:{port}");
        let client = client_for(&base, "1");
        let started = std::time::Instant::now();
        let error = client
            .call(
                Method::POST,
                &["v1", "x"],
                &[],
                Some(&json!({})),
                Replay::Unsafe,
            )
            .await
            .unwrap_err();
        let text = format!("{error:#}");
        assert!(text.contains("sending to"), "{text}");
        assert!(started.elapsed() < Duration::from_secs(10));

        // And when the server comes up in time, the request is answered.
        let listener = TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = vec![0_u8; 4096];
            let _ = socket.read(&mut buffer).await;
            let body = "{}";
            let reply = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(reply.as_bytes()).await;
        });
        let answered = client
            .call(
                Method::POST,
                &["v1", "x"],
                &[],
                Some(&json!({})),
                Replay::Unsafe,
            )
            .await
            .unwrap();
        assert_eq!(answered, Some(json!({})));
    }
}
