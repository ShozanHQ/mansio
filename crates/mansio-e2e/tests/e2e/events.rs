//! Server-sent events: `GET /v1/events` serves a topic, and resumes from
//! `Last-Event-ID`.

use std::time::Duration;

use anyhow::{Context, bail, ensure};
use mansio_core::Envelope;
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::harness::{SLACK, Target};

/// One frame of an event stream.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Frame {
    /// A comment, such as a keep-alive.
    Comment(String),
    /// An event.
    Event {
        id: Option<String>,
        event: Option<String>,
        data: String,
    },
}

/// An event stream that is open, read frame by frame.
struct Stream {
    response: reqwest::Response,
    pending: String,
}

impl Stream {
    /// Open the stream, which must be accepted.
    async fn open(
        target: &Target,
        topic: &str,
        query: &str,
        last_event_id: Option<&str>,
    ) -> anyhow::Result<Self> {
        let response = Self::request(target, &target.token, topic, query, last_event_id).await?;
        ensure!(
            response.status() == StatusCode::OK,
            "the stream answered {}",
            response.status()
        );
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        ensure!(
            content_type.starts_with("text/event-stream"),
            "content-type is {content_type:?}"
        );
        Ok(Self {
            response,
            pending: String::new(),
        })
    }

    async fn request(
        target: &Target,
        token: &str,
        topic: &str,
        query: &str,
        last_event_id: Option<&str>,
    ) -> anyhow::Result<reqwest::Response> {
        let mut uri = format!(
            "{}/v1/events?tenant={}&topic={topic}",
            target.base, target.tenant
        );
        if !query.is_empty() {
            uri.push('&');
            uri.push_str(query);
        }
        let mut request = target.http.get(uri).bearer_auth(token);
        if let Some(id) = last_event_id {
            request = request.header("Last-Event-ID", id);
        }
        Ok(request.send().await?)
    }

    /// The next frame, or `None` if nothing comes within `timeout`.
    async fn next(&mut self, timeout: Duration) -> anyhow::Result<Option<Frame>> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(end) = self.pending.find("\n\n") {
                let block: String = self.pending.drain(..end + 2).collect();
                return Ok(Some(parse(block.trim_end_matches('\n'))?));
            }
            let Ok(chunk) = tokio::time::timeout_at(deadline, self.response.chunk()).await else {
                return Ok(None);
            };
            match chunk? {
                Some(bytes) => self.pending.push_str(&String::from_utf8(bytes.to_vec())?),
                None => bail!("the stream ended"),
            }
        }
    }

    /// The next event, skipping comments.
    async fn event(&mut self, timeout: Duration) -> anyhow::Result<Event> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.next(left).await? {
                Some(Frame::Comment(_)) => {}
                Some(Frame::Event { id, event, data }) => {
                    return Ok(Event {
                        id: id.context("an event has an id")?.parse()?,
                        event: event.context("an event has a name")?,
                        envelope: Envelope::from_json_slice(data.as_bytes())?,
                        data: serde_json::from_str(&data)?,
                    });
                }
                None => bail!("no event within {timeout:?}"),
            }
        }
    }

    /// `count` events, in order.
    async fn events(&mut self, count: usize) -> anyhow::Result<Vec<Event>> {
        let mut events = Vec::new();
        for _ in 0..count {
            events.push(self.event(SLACK).await?);
        }
        Ok(events)
    }

    /// Whether any event arrives within `wait`. Comments do not count.
    async fn quiet_for(&mut self, wait: Duration) -> anyhow::Result<bool> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.next(left).await? {
                None => return Ok(true),
                Some(Frame::Comment(_)) => {}
                Some(Frame::Event { .. }) => return Ok(false),
            }
        }
    }
}

/// An event, read.
struct Event {
    id: u64,
    event: String,
    envelope: Envelope,
    data: Value,
}

fn parse(block: &str) -> anyhow::Result<Frame> {
    let mut id = None;
    let mut event = None;
    let mut data: Vec<&str> = Vec::new();
    let mut comment = None;
    for line in block.lines() {
        if let Some(text) = line.strip_prefix(':') {
            comment = Some(text.trim().to_owned());
        } else if let Some(value) = line.strip_prefix("id:") {
            id = Some(value.trim_start().to_owned());
        } else if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim_start().to_owned());
        } else if let Some(value) = line.strip_prefix("data:") {
            data.push(value.strip_prefix(' ').unwrap_or(value));
        } else {
            bail!("a line of an event stream that is none of these: {line:?}");
        }
    }
    match (comment, data.is_empty()) {
        (Some(comment), true) if id.is_none() && event.is_none() => Ok(Frame::Comment(comment)),
        (_, false) => Ok(Frame::Event {
            id,
            event,
            data: data.join("\n"),
        }),
        _ => bail!("a frame with no data: {block:?}"),
    }
}

async fn publish_n(
    target: &Target,
    topic: &str,
    from: u32,
    to: u32,
) -> anyhow::Result<Vec<Envelope>> {
    let mut published = Vec::new();
    for n in from..to {
        let body = json!({"source": "e2e", "type": format!("e2e.n{n}"), "data": {"n": n}});
        published.push(target.publish(topic, &body).await?.envelope);
    }
    Ok(published)
}

/// Done criterion 4: `GET /events` serves server-sent events. Each event's id
/// is the offset, its name the envelope's type, its data the envelope.
#[tokio::test]
async fn events_carry_the_offset_as_id_the_type_as_name_and_the_envelope_as_data()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let sent = publish_n(&target, "facts.build", 0, 3).await?;

    let mut stream = Stream::open(&target, "facts.build", "", None).await?;
    let events = stream.events(3).await?;
    for (n, (event, sent)) in events.iter().zip(&sent).enumerate() {
        assert_eq!(event.id, n as u64);
        assert_eq!(event.event, format!("e2e.n{n}"));
        assert_eq!(&event.envelope, sent);
        assert_eq!(event.data, serde_json::to_value(sent)?);
    }

    // What is published while the stream is open arrives on it.
    let live = publish_n(&target, "facts.build", 3, 4).await?;
    let event = stream.event(SLACK).await?;
    assert_eq!((event.id, &event.envelope), (3, &live[0]));
    assert!(stream.quiet_for(target.settle()).await?);
    Ok(())
}

/// Done criterion 4: it resumes from `Last-Event-ID`, with nothing missed and
/// nothing twice.
#[tokio::test]
async fn a_stream_resumes_after_last_event_id_with_nothing_missed_and_nothing_twice()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let mut sent = publish_n(&target, "facts.build", 0, 5).await?;

    let mut stream = Stream::open(&target, "facts.build", "", None).await?;
    let first = stream.events(2).await?;
    let last = first.last().context("two events")?.id;
    drop(stream);

    sent.extend(publish_n(&target, "facts.build", 5, 8).await?);
    let mut stream = Stream::open(&target, "facts.build", "", Some(&last.to_string())).await?;
    let rest = stream.events(6).await?;
    assert_eq!(
        rest.iter().map(|event| event.id).collect::<Vec<_>>(),
        [2, 3, 4, 5, 6, 7]
    );
    let seen: Vec<Envelope> = first
        .into_iter()
        .chain(rest)
        .map(|event| event.envelope)
        .collect();
    assert_eq!(seen, sent, "every envelope, once, in order");
    assert!(stream.quiet_for(target.settle()).await?, "nothing twice");
    Ok(())
}

/// `?from=` starts a first connection at an offset, and `Last-Event-ID`, which
/// a reconnecting client sends, wins over it.
#[tokio::test]
async fn from_starts_at_an_offset_and_last_event_id_wins() -> anyhow::Result<()> {
    let target = Target::start().await?;
    publish_n(&target, "facts.build", 0, 5).await?;

    let mut stream = Stream::open(&target, "facts.build", "from=3", None).await?;
    let ids: Vec<u64> = stream.events(2).await?.iter().map(|e| e.id).collect();
    assert_eq!(ids, [3, 4]);

    let mut stream = Stream::open(&target, "facts.build", "from=3", Some("0")).await?;
    assert_eq!(stream.event(SLACK).await?.id, 1);

    // A start past the end waits for what comes.
    let mut stream = Stream::open(&target, "facts.build", "from=5", None).await?;
    assert!(stream.quiet_for(target.settle()).await?);
    publish_n(&target, "facts.build", 5, 6).await?;
    assert_eq!(stream.event(SLACK).await?.id, 5);
    Ok(())
}

/// A stream with nothing to send says it is alive.
#[tokio::test]
async fn a_quiet_stream_sends_a_comment_every_keepalive_interval() -> anyhow::Result<()> {
    let target = Target::start().await?;
    publish_n(&target, "facts.build", 0, 1).await?;
    let mut stream = Stream::open(&target, "facts.build", "", None).await?;
    stream.event(SLACK).await?;

    let keepalive = Duration::from_secs(target.settings.sse_keepalive_seconds);
    let started = std::time::Instant::now();
    let frame = stream
        .next(keepalive + SLACK)
        .await?
        .context("a comment within the keep-alive interval")?;
    assert!(matches!(frame, Frame::Comment(_)), "{frame:?}");
    assert!(started.elapsed() <= keepalive + SLACK);
    Ok(())
}

/// The stream is authorised like `poll`: with the caller's credential.
#[tokio::test]
async fn a_stream_needs_the_right_to_read_the_topic() -> anyhow::Result<()> {
    let target = Target::start().await?;
    publish_n(&target, "facts.build", 0, 1).await?;
    let reader = target
        .issue(
            "person",
            "reader",
            &json!([{"topic": "facts.build", "verbs": ["subscribe"]}]),
            None,
        )
        .await?;
    let writer = target
        .issue(
            "person",
            "writer",
            &json!([{"topic": "facts.build", "verbs": ["publish"]}]),
            None,
        )
        .await?;

    let status = |token: &str, topic: &str, query: &str, id: Option<&str>| {
        let token = token.to_owned();
        let (topic, query) = (topic.to_owned(), query.to_owned());
        let id = id.map(str::to_owned);
        let target = &target;
        async move {
            Stream::request(target, &token, &topic, &query, id.as_deref())
                .await
                .map(|response| response.status())
        }
    };

    assert_eq!(
        status(&reader.token, "facts.build", "", None).await?,
        StatusCode::OK
    );
    assert_eq!(
        status(&target.token, "facts.build", "", None).await?,
        StatusCode::OK
    );
    assert_eq!(
        status(&writer.token, "facts.build", "", None).await?,
        StatusCode::FORBIDDEN,
        "publish is not the right to read"
    );
    assert_eq!(
        status(&reader.token, "facts.other", "", None).await?,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        status("not-a-token", "facts.build", "", None).await?,
        StatusCode::UNAUTHORIZED
    );
    // A topic nobody has published to is not found, and an event stream is
    // not made of it.
    assert_eq!(
        status(&target.token, "facts.nobody", "", None).await?,
        StatusCode::NOT_FOUND
    );
    for (query, id) in [
        ("from=-1", None),
        ("from=x", None),
        ("other=1", None),
        ("", Some("not-an-offset")),
        ("", Some("-1")),
    ] {
        assert_eq!(
            status(&target.token, "facts.build", query, id).await?,
            StatusCode::BAD_REQUEST,
            "{query} {id:?}"
        );
    }
    let (status_without_tenant, _) = target
        .authed(
            reqwest::Method::GET,
            &format!("{}/v1/events?topic=facts.build", target.base),
            None,
        )
        .await?;
    assert_eq!(status_without_tenant, StatusCode::BAD_REQUEST);
    let (without_token, _) = target
        .call(
            reqwest::Method::GET,
            &format!(
                "{}/v1/events?tenant={}&topic=facts.build",
                target.base, target.tenant
            ),
            None,
            None,
        )
        .await?;
    assert_eq!(without_token, StatusCode::UNAUTHORIZED);
    Ok(())
}

/// A credential that is revoked while its stream is open does not keep
/// reading.
#[tokio::test]
async fn a_stream_ends_when_its_credential_is_revoked() -> anyhow::Result<()> {
    let target = Target::start().await?;
    publish_n(&target, "facts.build", 0, 1).await?;
    let reader = target
        .issue(
            "person",
            "reader",
            &json!([{"topic": "facts.build", "verbs": ["subscribe"]}]),
            None,
        )
        .await?;
    let response = Stream::request(&target, &reader.token, "facts.build", "", None).await?;
    let mut stream = Stream {
        response,
        pending: String::new(),
    };
    stream.event(SLACK).await?;

    let (status, _) = target.revoke(&reader.id).await?;
    assert_eq!(status, StatusCode::OK);
    publish_n(&target, "facts.build", 1, 2).await?;

    // The stream ends, and the event published after the revocation is not
    // sent on it.
    let deadline = std::time::Instant::now() + SLACK;
    loop {
        match stream.next(Duration::from_millis(200)).await {
            Err(_) => break,
            Ok(Some(Frame::Event { .. })) => bail!("an event after the revocation"),
            Ok(_) => {}
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "the stream did not end after the revocation"
        );
    }
    Ok(())
}

/// An event stream that is open does not hold a shutdown up: the server ends
/// it, and the client sees the stream end.
#[tokio::test]
async fn a_server_stops_at_once_with_an_event_stream_open() -> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        return Ok(());
    }
    publish_n(&target, "facts.build", 0, 1).await?;
    let mut stream = Stream::open(&target, "facts.build", "", None).await?;
    stream.event(SLACK).await?;

    let started = std::time::Instant::now();
    target.stop_server().await?;
    let took = started.elapsed();
    // Not aborted after the time a stop is given, and not after the grace.
    assert!(
        took < Duration::from_secs(target.settings.shutdown_grace_seconds),
        "the server took {took:?} to stop with a stream open"
    );
    // The stream ended: the client reads to its end.
    let ended = tokio::time::timeout(SLACK, async {
        loop {
            if stream.next(Duration::from_secs(1)).await.is_err() {
                return;
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "the stream did not end");
    Ok(())
}

/// A request that never finishes does not hold it up for ever either: the
/// shutdown waits `MANSIO_SHUTDOWN_GRACE_SECONDS` and stops.
#[tokio::test]
async fn a_server_stops_after_the_grace_with_a_request_that_never_ends() -> anyhow::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut target = Target::start().await?;
    if !target.is_local() {
        return Ok(());
    }
    // The head of a request that promises a body, and not the body.
    let address = target.base.trim_start_matches("http://").to_owned();
    let mut socket = tokio::net::TcpStream::connect(&address).await?;
    socket
        .write_all(
            format!(
                "POST /v1/tenants/{}/topics/facts.build/messages HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{{\"source\"",
                target.tenant, target.token
            )
            .as_bytes(),
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;

    let grace = Duration::from_secs(target.settings.shutdown_grace_seconds);
    let started = std::time::Instant::now();
    target.stop_server().await?;
    let took = started.elapsed();
    assert!(took >= grace, "it did not wait the grace: {took:?}");
    assert!(
        took < crate::harness::STOP_TIME,
        "it was aborted, not stopped: {took:?}"
    );
    drop(socket);
    Ok(())
}
