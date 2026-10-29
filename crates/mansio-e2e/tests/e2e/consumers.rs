//! Named durable consumers: mansio keeps an offset per tenant, topic, principal
//! and name, in its own log in the broker.

use anyhow::{Context, ensure};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::harness::{Outcome, Target};

/// Publish `count` messages to the topic.
async fn publish(target: &Target, topic: &str, count: u32) -> anyhow::Result<()> {
    for n in 0..count {
        let body = json!({"source": "e2e", "type": format!("e2e.n{n}")});
        target.publish(topic, &body).await?;
    }
    Ok(())
}

async fn get_as(target: &Target, token: &str, topic: &str, name: &str) -> anyhow::Result<Value> {
    let (status, body) = target
        .call(
            Method::GET,
            &target.consumer_uri(topic, name),
            Some(token),
            None,
        )
        .await?;
    ensure!(
        status == StatusCode::OK,
        "consumers_get answered {status}: {body}"
    );
    Ok(body)
}

async fn commit_as(
    target: &Target,
    token: &str,
    topic: &str,
    name: &str,
    offset: u64,
) -> anyhow::Result<(StatusCode, Value)> {
    let uri = format!("{}/commit", target.consumer_uri(topic, name));
    target
        .call(
            Method::POST,
            &uri,
            Some(token),
            Some(&json!({ "offset": offset })),
        )
        .await
}

async fn get(target: &Target, topic: &str, name: &str) -> anyhow::Result<Value> {
    get_as(target, &target.token, topic, name).await
}

async fn commit(target: &Target, topic: &str, name: &str, offset: u64) -> anyhow::Result<Value> {
    let (status, body) = commit_as(target, &target.token, topic, name, offset).await?;
    ensure!(
        status == StatusCode::OK,
        "consumers_commit answered {status}: {body}"
    );
    Ok(body)
}

/// A consumer reads from 0 until it commits, then from one after what it
/// committed; names and topics are separate consumers; and a commit may move
/// a consumer back.
#[tokio::test]
async fn a_consumer_keeps_the_offset_it_committed_per_topic_and_name() -> anyhow::Result<()> {
    let target = Target::start().await?;
    publish(&target, "facts.build", 5).await?;
    publish(&target, "facts.other", 3).await?;

    assert_eq!(
        get(&target, "facts.build", "dash").await?,
        json!({"consumer": "dash", "next": 0})
    );
    assert_eq!(
        commit(&target, "facts.build", "dash", 4).await?,
        json!({"consumer": "dash", "offset": 4, "next": 5})
    );
    assert_eq!(
        get(&target, "facts.build", "dash").await?,
        json!({"consumer": "dash", "offset": 4, "next": 5})
    );
    // Zero is an offset, not "nothing".
    commit(&target, "facts.build", "first", 0).await?;
    assert_eq!(get(&target, "facts.build", "first").await?["next"], 1);
    // Another name and another topic are other consumers.
    assert_eq!(get(&target, "facts.build", "other").await?["next"], 0);
    assert_eq!(get(&target, "facts.other", "dash").await?["next"], 0);
    // A commit replaces what was there, even with less.
    commit(&target, "facts.build", "dash", 1).await?;
    assert_eq!(get(&target, "facts.build", "dash").await?["offset"], 1);
    Ok(())
}

/// Only a message that exists can be committed: an offset past the last one
/// would make the consumer skip what is published next. It is a 409 on every
/// backend, and a refused commit changes nothing.
#[tokio::test]
async fn an_offset_past_the_last_message_is_refused_and_changes_nothing() -> anyhow::Result<()> {
    let target = Target::start().await?;
    publish(&target, "facts.build", 3).await?;

    commit(&target, "facts.build", "dash", 1).await?;
    for offset in [3, 4, 1000] {
        let (status, body) =
            commit_as(&target, &target.token, "facts.build", "dash", offset).await?;
        assert_eq!(status, StatusCode::CONFLICT, "{offset}: {body}");
        assert_eq!(body["error"]["code"], "conflict");
    }
    assert_eq!(get(&target, "facts.build", "dash").await?["offset"], 1);
    // The last message is the highest that can be committed, and what is
    // published after it can then be.
    commit(&target, "facts.build", "dash", 2).await?;
    publish(&target, "facts.build", 1).await?;
    commit(&target, "facts.build", "dash", 3).await?;
    assert_eq!(get(&target, "facts.build", "dash").await?["next"], 4);

    // A topic nobody has published to has no consumers, and no message to
    // commit.
    let (status, _) = commit_as(&target, &target.token, "facts.nobody", "dash", 0).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = target
        .authed(
            Method::GET,
            &target.consumer_uri("facts.nobody", "dash"),
            None,
        )
        .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

/// A consumer belongs to the principal that uses it: another principal that
/// may read the topic and uses the same name has a position of its own, and
/// cannot read or move the first's.
#[tokio::test]
async fn a_consumer_belongs_to_its_principal_and_another_cannot_read_or_move_it()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    publish(&target, "facts.build", 5).await?;
    let scopes = json!([{"topic": "facts.build", "verbs": ["subscribe"]}]);
    let first = target.issue("person", "first", &scopes, None).await?;
    let second = target.issue("person", "second", &scopes, None).await?;

    let (status, _) = commit_as(&target, &first.token, "facts.build", "dash", 3).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        get_as(&target, &first.token, "facts.build", "dash").await?["next"],
        4
    );

    // The second sees a consumer that has committed nothing, not the first's.
    assert_eq!(
        get_as(&target, &second.token, "facts.build", "dash").await?,
        json!({"consumer": "dash", "next": 0})
    );
    // And what it commits is its own.
    let (status, _) = commit_as(&target, &second.token, "facts.build", "dash", 0).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        get_as(&target, &first.token, "facts.build", "dash").await?["offset"],
        3,
        "the first consumer did not move"
    );
    assert_eq!(
        get_as(&target, &second.token, "facts.build", "dash").await?["offset"],
        0
    );
    // The admin has consumers of its own too.
    assert_eq!(get(&target, "facts.build", "dash").await?["next"], 0);

    // Where the positions are kept, no credential reaches.
    if let Some(outcome) = target
        .broker_read_system(&first.token, mansio_iggy::SYSTEM_CURSORS_TOPIC)
        .await?
    {
        assert_eq!(outcome, Outcome::Forbidden);
    }
    Ok(())
}

/// Reading the topic is the right a consumer takes; what is refused is
/// refused before anything changes.
#[tokio::test]
async fn a_consumer_needs_the_right_to_read_its_topic() -> anyhow::Result<()> {
    let target = Target::start().await?;
    publish(&target, "facts.build", 3).await?;
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

    let (status, _) = commit_as(&target, &reader.token, "facts.build", "dash", 1).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        get_as(&target, &reader.token, "facts.build", "dash").await?["next"],
        2
    );

    // Publishing is not reading.
    let uri = target.consumer_uri("facts.build", "dash");
    let (status, _) = target
        .call(Method::GET, &uri, Some(&writer.token), None)
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = commit_as(&target, &writer.token, "facts.build", "dash", 2).await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Not a topic the credential's scopes cover, and not signed in.
    let (status, _) = commit_as(&target, &reader.token, "facts.other", "dash", 0).await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = target.call(Method::GET, &uri, None, None).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Malformed requests.
    let commit_uri = format!("{uri}/commit");
    for (method, address, body) in [
        (Method::POST, commit_uri.clone(), json!({})),
        (Method::POST, commit_uri.clone(), json!({"offset": -1})),
        (
            Method::POST,
            commit_uri.clone(),
            json!({"offset": 1, "x": 1}),
        ),
        (
            Method::GET,
            target.consumer_uri("facts.build", "a b"),
            json!(null),
        ),
        (
            Method::GET,
            target.consumer_uri("nope", "dash"),
            json!(null),
        ),
    ] {
        let body = (!body.is_null()).then_some(&body);
        let (status, _) = target.authed(method, &address, body).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{address}");
    }
    Ok(())
}

/// The position is kept in the broker, so a restarted server finds it.
#[tokio::test]
async fn a_restarted_server_finds_the_position_it_kept() -> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        return Ok(());
    }
    publish(&target, "facts.build", 8).await?;
    commit(&target, "facts.build", "dash", 3).await?;
    commit(&target, "facts.build", "dash", 7).await?;
    commit(&target, "facts.build", "other", 0).await?;
    target.stop_server().await?;
    target.start_server().await?;
    assert_eq!(
        get(&target, "facts.build", "dash").await?,
        json!({"consumer": "dash", "offset": 7, "next": 8})
    );
    assert_eq!(get(&target, "facts.build", "other").await?["next"], 1);
    commit(&target, "facts.build", "dash", 2).await?;
    target.stop_server().await?;
    target.start_server().await?;
    assert_eq!(
        get(&target, "facts.build", "dash")
            .await?
            .get("offset")
            .and_then(Value::as_u64)
            .context("an offset")?,
        2,
        "the last commit is the position, wherever it points"
    );
    Ok(())
}
