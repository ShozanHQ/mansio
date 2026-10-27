//! Named durable consumers: the broker keeps an offset per tenant, topic and
//! name.

use reqwest::{Method, StatusCode};
use serde_json::json;

use crate::harness::Target;

async fn get(target: &Target, topic: &str, name: &str) -> anyhow::Result<serde_json::Value> {
    let (status, body) = target
        .authed(Method::GET, &target.consumer_uri(topic, name), None)
        .await?;
    anyhow::ensure!(
        status == StatusCode::OK,
        "consumers_get answered {status}: {body}"
    );
    Ok(body)
}

async fn commit(
    target: &Target,
    topic: &str,
    name: &str,
    offset: u64,
) -> anyhow::Result<serde_json::Value> {
    let uri = format!("{}/commit", target.consumer_uri(topic, name));
    let (status, body) = target
        .authed(Method::POST, &uri, Some(&json!({ "offset": offset })))
        .await?;
    anyhow::ensure!(
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
    target.seed(&target.tenant, "facts.build").await?;
    target.seed(&target.tenant, "facts.other").await?;

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

/// What a consumer is for is reading the topic, so reading it is the right it
/// takes; and what is refused is refused before anything changes.
#[tokio::test]
async fn a_consumer_needs_the_right_to_read_its_topic() -> anyhow::Result<()> {
    let target = Target::start().await?;
    target.seed(&target.tenant, "facts.build").await?;
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
    let uri = target.consumer_uri("facts.build", "dash");
    let commit_uri = format!("{uri}/commit");
    let one = json!({"offset": 1});

    let (status, _) = target
        .call(Method::POST, &commit_uri, Some(&reader.token), Some(&one))
        .await?;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = target
        .call(Method::GET, &uri, Some(&reader.token), None)
        .await?;
    assert_eq!((status, &body["next"]), (StatusCode::OK, &json!(2)));

    for (method, address, body) in [
        (Method::GET, &uri, None),
        (Method::POST, &commit_uri, Some(&json!({"offset": 9}))),
    ] {
        let (status, _) = target
            .call(method, address, Some(&writer.token), body)
            .await?;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
    let (status, _) = target.call(Method::GET, &uri, None, None).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        get(&target, "facts.build", "dash").await?["offset"],
        1,
        "the refused commit changed nothing"
    );

    // A topic nobody has published to has no consumers.
    let (status, _) = target
        .authed(
            Method::GET,
            &target.consumer_uri("facts.nobody", "dash"),
            None,
        )
        .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Malformed requests.
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

/// The offset is the broker's, so a restarted server finds it.
#[tokio::test]
async fn a_restarted_server_finds_the_offset_the_broker_kept() -> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        return Ok(());
    }
    target.seed(&target.tenant, "facts.build").await?;
    commit(&target, "facts.build", "dash", 7).await?;
    target.stop_server().await?;
    target.start_server().await?;
    assert_eq!(
        get(&target, "facts.build", "dash").await?,
        json!({"consumer": "dash", "offset": 7, "next": 8})
    );
    Ok(())
}
