//! Webhooks: signed CloudEvents deliveries, in offset order, retried with a
//! backoff, resumed by a restarted server.

use std::time::Duration;

use anyhow::{Context, ensure};
use mansio_core::Envelope;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::harness::{Outcome, SLACK, Target};
use crate::receiver::{Receiver, Reply, check_signed};

fn event(n: u32) -> Value {
    json!({"source": "e2e", "type": format!("e2e.n{n}"), "data": {"n": n}})
}

/// The id of the envelope a delivery carried.
fn delivered_id(body: &[u8]) -> anyhow::Result<String> {
    Ok(Envelope::from_json_slice(body)?.id.to_string())
}

/// Done criterion 5: a delivery that fails twice, once with an error and once
/// with no answer, arrives on the third attempt as CloudEvents JSON with a
/// valid signature, and `webhooks list` shows how it went.
#[tokio::test]
async fn a_delivery_that_fails_twice_arrives_on_the_third_attempt_with_a_valid_signature()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    ensure!(
        target.settings.webhook_max_attempts >= 3,
        "the test needs three attempts"
    );
    let receiver = Receiver::start(&target, [Reply::Status(500), Reply::Hang]).await?;
    let created = target.webhook("facts.build", &receiver.url).await?;
    let secret = created["secret"].as_str().context("the secret")?.to_owned();
    assert!(secret.starts_with("whsec_"), "{secret}");
    let id = created["webhook"]["id"]
        .as_str()
        .context("the id")?
        .to_owned();
    assert_eq!(created["webhook"]["topic"], "facts.build");
    assert_eq!(created["webhook"]["tenant"], json!(target.tenant));
    assert_eq!(created["webhook"]["url"], json!(receiver.url));

    let published = target.publish("facts.build", &event(0)).await?;
    let requests = receiver
        .wait_for(
            &target,
            3,
            target.webhook_give_up() + target.webhook_backoff(2) + SLACK,
        )
        .await?;

    // The same envelope each time, signed each time for the moment it was sent.
    let mut stamps = Vec::new();
    for request in &requests[..3] {
        stamps.push(check_signed(request, &secret)?);
        let carried = Envelope::from_json_slice(&request.body)?;
        assert_eq!(carried, published.envelope);
    }
    assert!(stamps.windows(2).all(|pair| pair[0] <= pair[1]));
    // It waited between attempts, and for longer after the second.
    let gaps: Vec<Duration> = requests
        .windows(2)
        .map(|pair| pair[1].at.duration_since(pair[0].at))
        .collect();
    assert!(gaps[0] >= target.webhook_backoff(1), "{gaps:?}");
    assert!(gaps[1] >= target.webhook_backoff(2), "{gaps:?}");

    // The listing shows it, and holds no secret.
    let listed = target
        .until("the delivery to be listed", SLACK, || async {
            let all = target.webhooks().await?;
            Ok(all
                .iter()
                .any(|webhook| webhook["last_delivered_offset"] == 0)
                .then_some(all))
        })
        .await?;
    assert_eq!(listed.len(), 1);
    let webhook = &listed[0];
    assert_eq!(webhook["id"], json!(id));
    assert_eq!(webhook["last_delivered_offset"], 0);
    let error = &webhook["last_error"];
    assert_eq!(error["offset"], 0);
    let message = error["message"].as_str().context("an error message")?;
    assert!(
        !message.is_empty() && !message.contains(&receiver.url),
        "{message}"
    );
    assert!(!webhook.to_string().contains(&secret));
    assert!(!webhook.to_string().contains("whsec_"));
    assert!(webhook.get("secret").is_none());

    // Nothing more is sent for it.
    tokio::time::sleep(target.settle()).await;
    assert_eq!(receiver.requests().len(), 3);
    target.delete_webhooks().await?;
    Ok(())
}

/// A webhook delivers each envelope of its topic once, in offset order, and a
/// new one starts at the first message the topic holds.
#[tokio::test]
async fn envelopes_arrive_in_offset_order_once_each_from_the_first_message() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let receiver = Receiver::start(&target, []).await?;
    let mut sent = Vec::new();
    for n in 0..2 {
        sent.push(target.publish("facts.build", &event(n)).await?.envelope.id);
    }
    // The webhook is made after two messages; they are delivered too.
    let created = target.webhook("facts.build", &receiver.url).await?;
    let secret = created["secret"].as_str().context("the secret")?.to_owned();
    for n in 2..6 {
        sent.push(target.publish("facts.build", &event(n)).await?.envelope.id);
    }

    receiver.wait_for(&target, 6, SLACK).await?;
    tokio::time::sleep(target.settle()).await;
    let requests = receiver.requests();
    let got: Vec<String> = requests
        .iter()
        .map(|request| delivered_id(&request.body))
        .collect::<anyhow::Result<_>>()?;
    let expected: Vec<String> = sent.iter().map(ToString::to_string).collect();
    assert_eq!(got, expected, "every envelope once, in offset order");
    for request in &requests {
        check_signed(request, &secret)?;
    }
    target.delete_webhooks().await?;
    Ok(())
}

/// Past the attempts, the delivery is recorded as failed on `facts.webhook`
/// and the webhook moves on to the next envelope.
#[tokio::test]
async fn a_delivery_that_fails_every_attempt_is_recorded_and_the_webhook_moves_on()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let attempts = target.settings.webhook_max_attempts;
    let receiver = Receiver::start(&target, (0..attempts).map(|_| Reply::Status(500))).await?;
    let first = target.publish("facts.build", &event(0)).await?.envelope;
    let second = target.publish("facts.build", &event(1)).await?.envelope;
    let created = target.webhook("facts.build", &receiver.url).await?;
    let id = created["webhook"]["id"]
        .as_str()
        .context("the id")?
        .to_owned();

    let wait = target.webhook_give_up() + SLACK;
    let requests = receiver
        .wait_for(&target, attempts as usize + 1, wait)
        .await?;
    let ids: Vec<String> = requests
        .iter()
        .map(|request| delivered_id(&request.body))
        .collect::<anyhow::Result<_>>()?;
    let mut expected = vec![first.id.to_string(); attempts as usize];
    expected.push(second.id.to_string());
    assert_eq!(
        ids, expected,
        "the first envelope is tried, then the second"
    );

    // The give-up is a fact, made by the engine, naming what and why.
    let facts = target
        .until("the failure to be recorded", SLACK, || async {
            let facts = target.read("facts.webhook").await?;
            Ok((!facts.is_empty()).then_some(facts))
        })
        .await?;
    assert_eq!(facts.len(), 1, "one delivery was given up on");
    let fact = &facts[0];
    assert_eq!(fact.event_type.as_str(), "mansio.webhook.failed");
    assert_eq!(
        fact.subject.as_ref().map(ToString::to_string),
        Some(id.clone())
    );
    assert_eq!(fact.principal.to_string(), "service:mansio-engine");
    let data = fact.data.as_ref().context("data")?;
    assert_eq!(data["webhook_id"], json!(id));
    assert_eq!(data["topic"], "facts.build");
    assert_eq!(data["offset"], 0);
    assert_eq!(data["message_id"], json!(first.id.to_string()));
    assert_eq!(data["attempts"], attempts);
    assert_eq!(data["error"], "HTTP 500");
    assert!(
        !data.to_string().contains(&receiver.url),
        "no address in a fact"
    );

    // The webhook moved on: the second envelope was delivered.
    let listed = target
        .until("the second delivery to be listed", SLACK, || async {
            let all = target.webhooks().await?;
            Ok(all
                .iter()
                .any(|webhook| webhook["last_delivered_offset"] == 1)
                .then_some(all))
        })
        .await?;
    assert_eq!(listed[0]["last_error"]["message"], "HTTP 500");
    target.delete_webhooks().await?;
    Ok(())
}

/// Only the admin makes, lists and deletes webhooks; an address that cannot
/// be delivered to is refused; a deleted webhook delivers nothing more.
#[tokio::test]
async fn webhooks_are_made_listed_and_deleted_by_the_admin_alone() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let receiver = Receiver::start(&target, []).await?;
    let agent = target
        .issue(
            "agent",
            "builder",
            &json!([{"topic": "facts.*", "verbs": ["publish", "subscribe"]}]),
            None,
        )
        .await?;

    // Not the admin.
    let uri = format!("{}/v1/webhooks", target.base);
    let body = json!({"tenant": target.tenant, "topic": "facts.build", "url": receiver.url});
    let (status, _) = target
        .call(Method::POST, &uri, Some(&agent.token), Some(&body))
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = target
        .call(Method::GET, &uri, Some(&agent.token), None)
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = target.call(Method::POST, &uri, None, Some(&body)).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // An address or a request that is refused.
    for url in ["ftp://example.com/hook", "not a url", "", "http://"] {
        let (status, response) = target.create_webhook("facts.build", url).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{url:?}: {response}");
    }
    let (status, response) = target
        .create_webhook("facts.webhook", &receiver.url)
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    for bad in [
        json!({"tenant": target.tenant, "topic": "facts.build"}),
        json!({"tenant": target.tenant, "topic": "nope", "url": receiver.url}),
        json!({"tenant": "Acme", "topic": "facts.build", "url": receiver.url}),
        json!({"tenant": target.tenant, "topic": "facts.build", "url": receiver.url, "x": 1}),
    ] {
        let (status, _) = target.authed(Method::POST, &uri, Some(&bad)).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
    }
    assert!(target.webhooks().await?.is_empty(), "nothing was made");

    // The admin makes one, and the secret is in that answer and no other.
    let created = target.webhook("facts.build", &receiver.url).await?;
    let id = created["webhook"]["id"]
        .as_str()
        .context("the id")?
        .to_owned();
    let secret = created["secret"].as_str().context("the secret")?.to_owned();
    let listed = target.webhooks().await?;
    assert_eq!(listed.len(), 1);
    assert!(!serde_json::to_string(&listed)?.contains(&secret));
    // Another tenant's listing does not have it.
    let (status, other) = target
        .authed(Method::GET, &format!("{uri}?tenant=someone-else"), None)
        .await?;
    assert_eq!(
        (status, other["webhooks"].as_array().map(Vec::len)),
        (StatusCode::OK, Some(0))
    );

    // Where the broker keeps the secrets, no credential reaches.
    if let Some(outcome) = target
        .broker_read_system(&agent.token, mansio_iggy::SYSTEM_WEBHOOKS_TOPIC)
        .await?
    {
        assert_eq!(outcome, Outcome::Forbidden);
    }

    // It delivers while it exists.
    target.publish("facts.build", &event(0)).await?;
    receiver.wait_for(&target, 1, SLACK).await?;

    // Deleted, it delivers nothing more, and a second delete finds nothing.
    let delete_uri = format!("{uri}/{id}");
    let (status, _) = target
        .call(Method::DELETE, &delete_uri, Some(&agent.token), None)
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, deleted) = target.authed(Method::DELETE, &delete_uri, None).await?;
    assert_eq!(status, StatusCode::OK, "{deleted}");
    assert_eq!(deleted["id"], json!(id));
    assert!(target.webhooks().await?.is_empty());
    let (status, _) = target.authed(Method::DELETE, &delete_uri, None).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = target
        .authed(Method::DELETE, &format!("{uri}/not-an-id"), None)
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    target.publish("facts.build", &event(1)).await?;
    tokio::time::sleep(target.settle() + Duration::from_secs(1)).await;
    assert_eq!(receiver.requests().len(), 1, "nothing after the delete");
    Ok(())
}

/// A webhook is made before its topic exists, and delivers once there is one.
#[tokio::test]
async fn a_webhook_for_a_topic_nobody_has_published_to_waits_for_it() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let receiver = Receiver::start(&target, []).await?;
    target.webhook("facts.later", &receiver.url).await?;
    tokio::time::sleep(target.settle()).await;
    assert!(receiver.requests().is_empty());
    target.publish("facts.later", &event(0)).await?;
    receiver.wait_for(&target, 1, SLACK).await?;
    target.delete_webhooks().await?;
    Ok(())
}

/// A server that stops and starts again keeps the webhook, its secret and its
/// place: what was delivered is not delivered again, and what came after is.
#[tokio::test]
async fn a_restarted_server_resumes_a_webhook_where_it_stopped() -> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        eprintln!("MANSIO_E2E_URL is set: a live server cannot be restarted from here");
        return Ok(());
    }
    let receiver = Receiver::start(&target, []).await?;
    let created = target.webhook("facts.build", &receiver.url).await?;
    let secret = created["secret"].as_str().context("the secret")?.to_owned();
    let mut sent = Vec::new();
    for n in 0..2 {
        sent.push(
            target
                .publish("facts.build", &event(n))
                .await?
                .envelope
                .id
                .to_string(),
        );
    }
    receiver.wait_for(&target, 2, SLACK).await?;
    // Wait until the place is kept, so that stopping now loses none.
    target
        .until("both deliveries to be kept", SLACK, || async {
            Ok(target
                .webhooks()
                .await?
                .iter()
                .any(|webhook| webhook["last_delivered_offset"] == 1)
                .then_some(()))
        })
        .await?;

    target.stop_server().await?;
    target.start_server().await?;
    for n in 2..4 {
        sent.push(
            target
                .publish("facts.build", &event(n))
                .await?
                .envelope
                .id
                .to_string(),
        );
    }
    receiver.wait_for(&target, 4, SLACK).await?;
    tokio::time::sleep(target.settle()).await;
    let requests = receiver.requests();
    let got: Vec<String> = requests
        .iter()
        .map(|request| delivered_id(&request.body))
        .collect::<anyhow::Result<_>>()?;
    assert_eq!(got, sent, "the first two once, and the next two");
    for request in &requests {
        check_signed(request, &secret)?;
    }
    // The restarted server still lists it.
    assert_eq!(target.webhooks().await?.len(), 1);
    target.delete_webhooks().await?;
    Ok(())
}
