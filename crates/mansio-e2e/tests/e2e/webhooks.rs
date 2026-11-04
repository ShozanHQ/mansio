//! Webhooks: signed CloudEvents deliveries, in offset order, retried with a
//! backoff, resumed by a restarted server.

use std::time::Duration;

use anyhow::{Context, ensure};
use mansio_core::Envelope;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::harness::{DataValue, LocalSettings, Outcome, SLACK, Target, type_names};
use crate::receiver::{Receiver, Reply, check_signed, check_signed_under};

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
            let facts: Vec<_> = target
                .read("facts.webhook")
                .await?
                .into_iter()
                .filter(|fact| fact.event_type.as_str() == "mansio.webhook.failed")
                .collect();
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
    let data = fact.data.as_ref().context("data")?.value();
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

/// A server that stops and starts again keeps the webhooks, their secrets and
/// their places: what was delivered is not delivered again, and what came
/// after is. There are two webhooks, and the server's broker gives way to the
/// other tasks on every call as Iggy does, because the server starts a task
/// for each webhook that reads its place at once: a server that read the
/// places while it was still reading them would send the second webhook its
/// whole topic from the first message.
#[tokio::test]
async fn a_restarted_server_resumes_every_webhook_where_it_stopped() -> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        eprintln!("MANSIO_E2E_URL is set: a live server cannot be restarted from here");
        return Ok(());
    }
    let first = Receiver::start(&target, []).await?;
    let second = Receiver::start(&target, []).await?;
    let first_secret = secret_of(&target.webhook("facts.build", &first.url).await?)?;
    let second_secret = secret_of(&target.webhook("facts.build", &second.url).await?)?;
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
    first.wait_for(&target, 2, SLACK).await?;
    second.wait_for(&target, 2, SLACK).await?;
    // Wait until both places are kept, so that stopping now loses none.
    target
        .until("both deliveries to be kept for both", SLACK, || async {
            Ok((target
                .webhooks()
                .await?
                .iter()
                .filter(|webhook| webhook["last_delivered_offset"] == 1)
                .count()
                == 2)
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
    first.wait_for(&target, 4, SLACK).await?;
    second.wait_for(&target, 4, SLACK).await?;
    tokio::time::sleep(target.settle()).await;
    for (receiver, secret) in [(&first, &first_secret), (&second, &second_secret)] {
        let requests = receiver.requests();
        let got: Vec<String> = requests
            .iter()
            .map(|request| delivered_id(&request.body))
            .collect::<anyhow::Result<_>>()?;
        assert_eq!(got, sent, "the first two once, and the next two");
        for request in &requests {
            check_signed(request, secret)?;
        }
    }
    // The restarted server still lists both.
    assert_eq!(target.webhooks().await?.len(), 2);
    target.delete_webhooks().await?;
    Ok(())
}

/// The signing secret in the answer to creating a webhook.
fn secret_of(created: &Value) -> anyhow::Result<String> {
    Ok(created["secret"].as_str().context("the secret")?.to_owned())
}

/// A webhook's position is the server's own: it is not reachable through the
/// consumers API, so a credential that reads the topic cannot move a webhook
/// by committing a consumer named after it, and what an admin commits under
/// that name is a consumer of the admin's and nothing more.
#[tokio::test]
async fn a_consumer_named_after_a_webhook_does_not_move_it() -> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        return Ok(());
    }
    let receiver = Receiver::start(&target, []).await?;
    for n in 0..3 {
        target.publish("facts.build", &event(n)).await?;
    }
    let created = target.webhook("facts.build", &receiver.url).await?;
    let id = created["webhook"]["id"]
        .as_str()
        .context("the id")?
        .to_owned();
    receiver.wait_for(&target, 3, SLACK).await?;
    target
        .until("all three to be kept", SLACK, || async {
            Ok(target
                .webhooks()
                .await?
                .iter()
                .any(|webhook| webhook["last_delivered_offset"] == 2)
                .then_some(()))
        })
        .await?;

    // The consumer a webhook is named after is any other consumer's name.
    let reader = target
        .issue(
            "person",
            "reader",
            &json!([{"topic": "facts.build", "verbs": ["subscribe"]}]),
            None,
        )
        .await?;
    let name = format!("webhook-{id}");
    for token in [&reader.token, &target.token] {
        let uri = format!("{}/commit", target.consumer_uri("facts.build", &name));
        let (status, _) = target
            .call(Method::POST, &uri, Some(token), Some(&json!({"offset": 0})))
            .await?;
        assert_eq!(status, StatusCode::OK);
    }

    // A restarted server resumes the webhook after the third, not the first.
    target.stop_server().await?;
    target.start_server().await?;
    target.publish("facts.build", &event(3)).await?;
    receiver.wait_for(&target, 4, SLACK).await?;
    tokio::time::sleep(target.settle()).await;
    let got: Vec<String> = receiver
        .requests()
        .iter()
        .map(|request| delivered_id(&request.body))
        .collect::<anyhow::Result<_>>()?;
    assert_eq!(got.len(), 4, "{got:?}: nothing was delivered twice");
    target.delete_webhooks().await?;
    Ok(())
}

/// The longest address a webhook may have is a setting.
#[tokio::test]
async fn the_longest_address_is_the_setting() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let max = usize::try_from(target.settings.webhook_url_max_bytes)?;
    let base = "http://127.0.0.1:9/";
    let at_the_limit = format!("{base}{}", "a".repeat(max - base.len()));
    let (status, response) = target.create_webhook("facts.build", &at_the_limit).await?;
    assert_eq!(status, StatusCode::CREATED, "{response}");
    let too_long = format!("{at_the_limit}a");
    let (status, response) = target.create_webhook("facts.build", &too_long).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
    assert!(
        response["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains(&format!("longer than {max} bytes"))),
        "{response}"
    );
    target.delete_webhooks().await?;
    Ok(())
}

/// A new webhook starts at the topic's first message unless it asks for the
/// next: with no start, or `first`, it is sent the whole history; with `next`,
/// only what is published after it is made. Anything else is refused.
#[tokio::test]
async fn a_webhook_starts_at_the_first_message_by_default_or_at_the_next_when_asked()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let by_default = Receiver::start(&target, []).await?;
    let first = Receiver::start(&target, []).await?;
    let next = Receiver::start(&target, []).await?;
    let mut sent = Vec::new();
    for n in 0..2 {
        sent.push(target.publish("facts.build", &event(n)).await?.envelope.id);
    }
    for (receiver, start) in [
        (&by_default, None),
        (&first, Some("first")),
        (&next, Some("next")),
    ] {
        let (status, made) = target
            .create_webhook_at("facts.build", &receiver.url, start)
            .await?;
        assert_eq!(status, StatusCode::CREATED, "{start:?}: {made}");
    }
    for n in 2..4 {
        sent.push(target.publish("facts.build", &event(n)).await?.envelope.id);
    }

    let ids = |receiver: &Receiver| -> anyhow::Result<Vec<String>> {
        receiver
            .requests()
            .iter()
            .map(|request| delivered_id(&request.body))
            .collect()
    };
    let all: Vec<String> = sent.iter().map(ToString::to_string).collect();
    by_default.wait_for(&target, 4, SLACK).await?;
    first.wait_for(&target, 4, SLACK).await?;
    next.wait_for(&target, 2, SLACK).await?;
    tokio::time::sleep(target.settle()).await;
    assert_eq!(ids(&by_default)?, all, "no start is first");
    assert_eq!(ids(&first)?, all);
    assert_eq!(ids(&next)?, all[2..], "next skips what the topic held");

    for bad in ["last", "", "NEXT", "0"] {
        let (status, response) = target
            .create_webhook_at("facts.build", &next.url, Some(bad))
            .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad:?}: {response}");
    }
    assert_eq!(target.webhooks().await?.len(), 3, "the refused made none");
    target.delete_webhooks().await?;
    Ok(())
}

/// Rotating returns a new secret once. Deliveries are signed with the old
/// secret and the new for the grace period, so a receiver that holds either
/// verifies them, and with the new one only after it. The creation, the
/// rotation and the deletion are facts that name no secret.
#[tokio::test]
async fn a_rotated_secret_signs_beside_the_old_for_the_grace_and_alone_after() -> anyhow::Result<()>
{
    let target = Target::start().await?;
    let grace = Duration::from_secs(target.settings.webhook_rotation_grace_seconds);
    ensure!(
        grace <= Duration::from_secs(60),
        "the test needs MANSIO_WEBHOOK_ROTATION_GRACE_SECONDS of a minute or less"
    );
    let receiver = Receiver::start(&target, []).await?;
    let created = target.webhook("facts.build", &receiver.url).await?;
    let id = created["webhook"]["id"]
        .as_str()
        .context("the id")?
        .to_owned();
    let old = created["secret"].as_str().context("the secret")?.to_owned();

    // Before the rotation: the old secret alone.
    target.publish("facts.build", &event(0)).await?;
    let requests = receiver.wait_for(&target, 1, SLACK).await?;
    check_signed(&requests[0], &old)?;

    // The rotation: a new secret, shown in this answer only.
    let rotated = target.rotate_webhook(&id).await?;
    let rotated_at = std::time::Instant::now();
    let new = rotated["secret"].as_str().context("the secret")?.to_owned();
    assert!(new.starts_with("whsec_"), "{new}");
    assert_ne!(new, old);
    assert_eq!(rotated["webhook"]["id"], json!(id));
    assert!(rotated["webhook"].get("secret").is_none());
    let valid_until = serde_json::from_value(rotated["previous_secret_valid_until"].clone())?;
    let valid_until = crate::harness::millis(&valid_until)?;
    let listed = serde_json::to_string(&target.webhooks().await?)?;
    assert!(!listed.contains(&new) && !listed.contains(&old), "{listed}");

    // Inside the grace: both signatures, the new one first, and each is the
    // HMAC of the body under its own secret.
    target.publish("facts.build", &event(1)).await?;
    let requests = receiver.wait_for(&target, 2, SLACK).await?;
    let stamp = check_signed_under(&requests[1], &[&new, &old])?;
    assert!(stamp * 1000 < valid_until, "sent inside the grace");

    // After it: the new secret alone, and the old one no longer signs.
    tokio::time::sleep((grace + target.settle()).saturating_sub(rotated_at.elapsed())).await;
    target.publish("facts.build", &event(2)).await?;
    let requests = receiver.wait_for(&target, 3, SLACK).await?;
    check_signed(&requests[2], &new)?;
    assert!(check_signed_under(&requests[2], &[&old]).is_err());
    assert!(check_signed_under(&requests[2], &[&new, &old]).is_err());

    // The facts, on the topic and in the sink, name no secret.
    target.delete_webhooks().await?;
    let facts = target.read("facts.webhook").await?;
    assert_eq!(
        type_names(&facts, "mansio.webhook."),
        ["created", "rotated", "deleted"]
    );
    for fact in &facts {
        assert_eq!(
            fact.subject.as_ref().map(ToString::to_string),
            Some(id.clone())
        );
        let data = fact.data.as_ref().context("data")?.value();
        assert_eq!(data["webhook_id"], json!(id));
        assert_eq!(data["topic"], "facts.build");
        let text = serde_json::to_string(fact)?;
        for secret in [old.as_str(), new.as_str(), "whsec_", receiver.url.as_str()] {
            assert!(!text.contains(secret), "{secret} is in {text}");
        }
    }
    let data = |n: usize| -> anyhow::Result<Value> {
        Ok(facts[n].data.as_ref().context("data")?.value().clone())
    };
    assert_eq!(data(0)?["start"], "first");
    assert_eq!(
        data(1)?["previous_secret_valid_until"],
        rotated["previous_secret_valid_until"]
    );
    if let Some(records) = target.sink_records()? {
        let recorded = type_names(&records, "mansio.webhook.");
        for kind in ["created", "rotated", "deleted"] {
            assert!(
                recorded.iter().any(|recorded| recorded == kind),
                "{kind} is not in the sink: {recorded:?}"
            );
        }
        let sunk = records
            .iter()
            .filter(|record| record.event_type.as_str().starts_with("mansio.webhook."))
            .map(serde_json::to_string)
            .collect::<Result<String, _>>()?;
        assert!(!sunk.contains(&old) && !sunk.contains(&new), "{sunk}");
    }
    Ok(())
}

/// Only the admin rotates, and only a webhook that exists.
#[tokio::test]
async fn only_the_admin_rotates_and_only_a_webhook_that_exists() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let receiver = Receiver::start(&target, []).await?;
    let agent = target
        .issue(
            "agent",
            "rotator",
            &json!([{"topic": "facts.*", "verbs": ["publish", "subscribe"]}]),
            None,
        )
        .await?;
    let created = target.webhook("facts.build", &receiver.url).await?;
    let id = created["webhook"]["id"]
        .as_str()
        .context("the id")?
        .to_owned();
    let uri = format!("{}/v1/webhooks/{id}/rotate", target.base);
    let (status, _) = target
        .call(Method::POST, &uri, Some(&agent.token), None)
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = target.call(Method::POST, &uri, None, None).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let missing = format!(
        "{}/v1/webhooks/0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3/rotate",
        target.base
    );
    let (status, _) = target.authed(Method::POST, &missing, None).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let bad = format!("{}/v1/webhooks/not-an-id/rotate", target.base);
    let (status, _) = target.authed(Method::POST, &bad, None).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = target.authed(Method::GET, &uri, None).await?;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    // None of that changed the secret.
    let old = created["secret"].as_str().context("the secret")?.to_owned();
    target.publish("facts.build", &event(0)).await?;
    let requests = receiver.wait_for(&target, 1, SLACK).await?;
    check_signed(&requests[0], &old)?;
    target.delete_webhooks().await?;
    Ok(())
}

/// With the setting off a delivery to the loopback is refused, and the
/// listing says so without naming the address; the receiver is never sent
/// anything.
#[tokio::test]
async fn a_delivery_to_a_private_address_is_refused_unless_the_setting_allows_it()
-> anyhow::Result<()> {
    let mut settings = LocalSettings::default();
    settings.webhooks.allow_private_destinations = false;
    let target = Target::start_with(settings).await?;
    if !target.is_local() {
        // A live server runs with what it was started with, and the tests'
        // receivers are on a private address.
        return Ok(());
    }
    let receiver = Receiver::start(&target, []).await?;
    target.webhook("facts.build", &receiver.url).await?;
    target.publish("facts.build", &event(0)).await?;
    let wait = target.webhook_give_up() + SLACK;
    let listed = target
        .until("the refusal to be listed", wait, || async {
            let all = target.webhooks().await?;
            Ok(all
                .iter()
                .any(|webhook| webhook["last_error"]["offset"] == 0)
                .then_some(all))
        })
        .await?;
    let message = listed[0]["last_error"]["message"]
        .as_str()
        .context("an error message")?;
    assert!(message.contains("loopback"), "{message}");
    assert!(!message.contains(&receiver.url), "{message}");
    assert!(receiver.requests().is_empty(), "nothing was sent");
    target.delete_webhooks().await?;
    Ok(())
}
