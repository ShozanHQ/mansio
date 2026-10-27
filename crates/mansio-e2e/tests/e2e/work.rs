//! Work items: claim, lease, ack, fail, retry with backoff, the dead topic.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use anyhow::{Context, ensure};
use mansio_core::EventId;
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use tokio::task::JoinSet;

use crate::harness::{PRINCIPAL, SLACK, Target, extended_to, item_of, job, millis};
use crate::{only_line, run_cli};

const QUEUE: &str = "builds";

/// Done criterion 1: a claimed item is invisible to other consumers until its
/// lease expires.
#[tokio::test]
async fn a_claimed_item_is_invisible_to_other_consumers_until_its_lease_expires()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let item = target.work_publish(QUEUE, &job(1)).await?.envelope;

    let before = Instant::now();
    let claim = target.claim(QUEUE, "first", SLACK).await?;
    assert_eq!(claim.item.id, item.id);
    assert_eq!(item_of(&claim)?, item.id.to_string());

    // Nobody else sees it, however they ask. The consumer that holds the lease
    // is given that lease again, and not another item.
    for consumer in ["second", "third"] {
        assert!(target.pull(QUEUE, consumer).await?.is_none(), "{consumer}");
    }
    let again = target.pull(QUEUE, "first").await?;
    assert_eq!(
        again.map(|claim| claim.lease_id),
        Some(claim.lease_id),
        "the holder is given its lease again"
    );

    // It reappears only after the lease, and then as a new attempt.
    let wait = target.lease() + target.backoff(1) + SLACK;
    let again = target
        .until("the item to be claimable again", wait, || async {
            Ok(target.pull(QUEUE, "second").await?.map(|claim| {
                // Measured before the first claim was asked for, so the lease
                // had not run out if this is shorter.
                (claim, before.elapsed())
            }))
        })
        .await?;
    let (again, elapsed) = again;
    assert!(
        elapsed >= target.lease(),
        "claimed again after {elapsed:?}, inside the lease of {:?}",
        target.lease()
    );
    assert_eq!(item_of(&again)?, item_of(&claim)?);
    assert_ne!(again.lease_id, claim.lease_id);
    Ok(())
}

/// Done criterion 2: an unacked claim expires and the item is claimed again
/// with the attempt count incremented.
#[tokio::test]
async fn an_unacked_claim_expires_and_the_item_is_claimed_again_with_the_attempt_incremented()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    target.work_publish(QUEUE, &job(1)).await?;

    let first = target.claim(QUEUE, "worker", SLACK).await?;
    assert_eq!(first.item.attempt, Some(1));
    let expires_at = millis(&first.expires_at)?;

    // By another consumer: "worker" is given its lease again until it ends.
    let wait = target.lease() + target.backoff(1) + SLACK;
    let second = target.claim(QUEUE, "worker-2", wait).await?;
    assert_eq!(second.item.attempt, Some(2));
    assert_eq!(second.item.item_id, first.item.item_id);
    assert_ne!(second.item.id, first.item.id, "a retry is a new message");
    assert_eq!(
        second
            .item
            .causation_id
            .as_ref()
            .map(|c| c.as_str().to_owned()),
        Some(first.item.id.to_string())
    );
    assert!(millis(&second.expires_at)? > expires_at);

    // The transitions say so, in order.
    assert_eq!(
        target.work_transitions().await?,
        ["claimed", "expired", "retried", "claimed"]
    );
    let facts = target.read("facts.work").await?;
    let expired = &facts[1];
    assert_eq!(expired.item_id, first.item.item_id);
    assert_eq!(expired.attempt, Some(1));
    // The fact says which lease it was and not what its id is.
    let data = expired.data.as_ref().context("data")?;
    assert_eq!(data["lease"], first.lease_id.fingerprint().to_string());
    assert!(data.get("lease_id").is_none(), "{data}");
    for fact in &facts {
        let text = serde_json::to_string(fact)?;
        for claim in [&first, &second] {
            assert!(!text.contains(&claim.lease_id.to_string()), "{text}");
        }
    }
    assert_eq!(
        facts[2].data.as_ref().context("data")?["message_id"],
        second.item.id.to_string()
    );
    // The queue holds both messages; the first is not claimable again.
    assert_eq!(target.read("work.builds").await?.len(), 2);
    Ok(())
}

/// Done criterion 3: a fail republishes after the backoff; after the cap the
/// item is on `dead` only.
#[tokio::test]
async fn fail_republishes_after_the_backoff_and_after_the_cap_the_item_is_on_dead_only()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let published = target.work_publish(QUEUE, &job(1)).await?.envelope;
    let max = target.settings.max_attempts;
    ensure!(max >= 2, "the test needs at least two attempts");

    let mut claim = target.claim(QUEUE, "worker", SLACK).await?;
    for attempt in 1..max {
        assert_eq!(claim.item.attempt, Some(attempt));
        let failed_at = Instant::now();
        let failed = target
            .fail_claim(QUEUE, &claim, &format!("attempt {attempt} broke"))
            .await?;
        assert_eq!(failed.envelope.event_type.as_str(), "mansio.work.failed");
        assert_eq!(failed.envelope.principal.to_string(), PRINCIPAL);
        let data = failed.envelope.data.as_ref().context("data")?;
        assert_eq!(data["reason"], format!("attempt {attempt} broke"));
        assert_eq!(data["next"], "retry");

        // Not before the backoff.
        assert!(target.pull(QUEUE, "worker").await?.is_none());
        let next = target
            .claim(QUEUE, "worker", target.backoff(attempt) + SLACK)
            .await?;
        assert!(
            failed_at.elapsed() >= target.backoff(attempt),
            "attempt {} came after {:?}, inside the backoff of {:?}",
            attempt + 1,
            failed_at.elapsed(),
            target.backoff(attempt)
        );
        assert_eq!(next.item.item_id, claim.item.item_id);
        assert_eq!(
            next.item
                .causation_id
                .as_ref()
                .map(|c| c.as_str().to_owned()),
            Some(claim.item.id.to_string())
        );
        claim = next;
    }

    // The last attempt: its failure ends the item.
    assert_eq!(claim.item.attempt, Some(max));
    let failed = target.fail_claim(QUEUE, &claim, "the last straw").await?;
    assert_eq!(
        failed.envelope.data.as_ref().context("data")?["next"],
        "dead"
    );

    let dead = target.read("dead").await?;
    assert_eq!(dead.len(), 1, "exactly one message on dead");
    assert_eq!(dead[0].item_id, Some(published.id.to_string().parse()?));
    assert_eq!(dead[0].attempt, Some(max));
    assert_eq!(dead[0].data, published.data);
    assert_eq!(
        dead[0].causation_id.as_ref().map(|c| c.as_str().to_owned()),
        Some(claim.item.id.to_string())
    );

    // And nowhere else: not claimable, and not published to its queue again.
    tokio::time::sleep(target.settle()).await;
    assert!(target.pull(QUEUE, "worker").await?.is_none());
    let queue = target.read("work.builds").await?;
    assert_eq!(
        queue.len(),
        max as usize,
        "one message per attempt, no more"
    );
    assert_eq!(
        queue.last().map(|m| m.attempt),
        Some(Some(max)),
        "the last message on the queue is the last attempt"
    );

    let mut expected = ["claimed", "failed", "retried"].repeat(max as usize - 1);
    expected.extend(["claimed", "failed", "dead"]);
    assert_eq!(target.work_transitions().await?, expected);
    Ok(())
}

/// Only the holder, presenting the lease id, can ack or fail; anything else
/// is refused and changes nothing.
#[tokio::test]
async fn only_the_holder_presenting_the_lease_id_can_ack_or_fail() -> anyhow::Result<()> {
    let target = Target::start().await?;
    target.work_publish(QUEUE, &job(1)).await?;
    let claim = target.claim(QUEUE, "worker", SLACK).await?;
    let item = item_of(&claim)?;
    let lease = claim.lease_id.to_string();
    let stranger = EventId::generate().to_string();

    // A lease id that is not the one given out.
    let (status, body) = target.ack(QUEUE, &item, "worker", &stranger).await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["code"], "forbidden");
    let (status, _) = target.fail(QUEUE, &item, "worker", &stranger, "no").await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Another consumer of the same principal, with the right lease id, is not
    // the holder either.
    let (status, body) = target.ack(QUEUE, &item, "another", &lease).await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, _) = target.fail(QUEUE, &item, "another", &lease, "no").await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Not a lease id at all, or no body, or a reason that says nothing.
    let (status, _) = target.ack(QUEUE, &item, "worker", "nope").await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let uri = target.tenant_uri(&format!("work/{QUEUE}/items/{item}/ack"));
    let (status, _) = target.authed(Method::POST, &uri, None).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = target.fail(QUEUE, &item, "worker", &lease, "  ").await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // An item that is not there.
    let (status, body) = target.ack(QUEUE, "no-such-item", "worker", &lease).await?;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    // Not signed in.
    let (status, _) = target
        .call(
            Method::POST,
            &uri,
            None,
            Some(&json!({"lease_id": lease, "consumer": "worker"})),
        )
        .await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // None of it changed anything.
    assert_eq!(target.work_transitions().await?, ["claimed"]);

    // Once the lease has ended even the holder is refused.
    tokio::time::sleep(target.lease()).await;
    let (status, body) = target.ack(QUEUE, &item, "worker", &lease).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "conflict");
    let (status, _) = target.fail(QUEUE, &item, "worker", &lease, "late").await?;
    assert_eq!(status, StatusCode::CONFLICT);
    let transitions = target.work_transitions().await?;
    assert!(
        !transitions.iter().any(|t| t == "acked" || t == "failed"),
        "{transitions:?}"
    );
    Ok(())
}

/// An acked item is done: it is not claimed again, and is not acked twice.
#[tokio::test]
async fn an_acked_item_is_done() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let mut body = job(1);
    let id = EventId::generate().to_string();
    body["id"] = json!(id);
    target.work_publish(QUEUE, &body).await?;
    let claim = target.claim(QUEUE, "worker", SLACK).await?;
    let acked = target.ack_claim(QUEUE, &claim).await?;
    assert_eq!(acked.envelope.event_type.as_str(), "mansio.work.acked");
    assert_eq!(acked.envelope.principal.to_string(), PRINCIPAL);
    assert_eq!(acked.envelope.item_id, claim.item.item_id);
    assert_eq!(acked.envelope.attempt, Some(1));

    let (status, _) = target
        .ack(
            QUEUE,
            &item_of(&claim)?,
            claim.consumer.as_str(),
            &claim.lease_id.to_string(),
        )
        .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Its id is not used again, so sending the same work twice does not run
    // it twice.
    let uri = target.tenant_uri(&format!("work/{QUEUE}/items"));
    let (status, response) = target.authed(Method::POST, &uri, Some(&body)).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{response}");

    // Past where the lease would have ended, and the backoff after it.
    tokio::time::sleep(target.lease() + target.backoff(1) + target.settle()).await;
    assert!(target.pull(QUEUE, "worker").await?.is_none());
    assert_eq!(target.work_transitions().await?, ["claimed", "acked"]);
    assert_eq!(target.read("work.builds").await?.len(), 1);
    Ok(())
}

/// Items are claimed oldest first, and a pull with nothing to claim is 204.
#[tokio::test]
async fn items_are_claimed_oldest_first_and_an_empty_queue_answers_204() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let uri = target.tenant_uri(&format!("work/{QUEUE}/pull"));
    let (status, _) = target
        .authed(Method::POST, &uri, Some(&json!({"consumer": "worker"})))
        .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let mut published = Vec::new();
    for n in 0..3 {
        published.push(target.work_publish(QUEUE, &job(n)).await?.envelope.id);
    }
    // Another queue's items are not this queue's.
    target.work_publish("reviews", &job(9)).await?;
    for (n, expected) in published.iter().enumerate() {
        // A consumer of its own for each, so that none is given its lease
        // again.
        let claim = target.claim(QUEUE, &format!("worker-{n}"), SLACK).await?;
        assert_eq!(&claim.item.id, expected);
    }
    assert!(target.pull(QUEUE, "worker-last").await?.is_none());

    // A pull must say who is pulling.
    for body in [
        json!({}),
        json!({"consumer": ""}),
        json!({"consumer": "a b"}),
        json!({"consumer": "w", "x": 1}),
    ] {
        let (status, _) = target.authed(Method::POST, &uri, Some(&body)).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }
    Ok(())
}

/// A claim is atomic: concurrent pulls never get the same item.
#[tokio::test]
async fn concurrent_pulls_never_get_the_same_item() -> anyhow::Result<()> {
    let target = std::sync::Arc::new(Target::start().await?);
    let items = 8;
    let mut published = HashSet::new();
    for n in 0..items {
        published.insert(target.work_publish(QUEUE, &job(n)).await?.envelope.id);
    }

    let mut pulls = JoinSet::new();
    for n in 0..items * 3 {
        let target = std::sync::Arc::clone(&target);
        pulls.spawn(async move { target.pull(QUEUE, &format!("worker-{n}")).await });
    }
    let mut claimed = Vec::new();
    while let Some(pull) = pulls.join_next().await {
        if let Some(claim) = pull?? {
            claimed.push(claim.item.id);
        }
    }
    assert_eq!(claimed.len(), items as usize, "every item, and no more");
    assert_eq!(
        claimed.iter().copied().collect::<HashSet<_>>(),
        published,
        "each item exactly once"
    );
    Ok(())
}

/// Work goes through `work_publish`: the generic route refuses it, the
/// server names the item and sets its attempt, and a caller may not.
#[tokio::test]
async fn work_is_published_through_work_publish_only() -> anyhow::Result<()> {
    let target = Target::start().await?;

    // The generic route refuses work topics and the topics mansio writes.
    for topic in [
        "work.builds",
        "facts.work",
        "facts.alert",
        "facts.principal",
        "dead",
    ] {
        let (status, body) = target
            .authed(Method::POST, &target.messages_uri(topic), Some(&job(1)))
            .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{topic}: {body}");
    }
    assert!(target.read("work.builds").await?.is_empty());

    // A caller cannot set the item id or the attempt.
    for (name, value) in [("itemid", json!("mine")), ("attempt", json!(1))] {
        let mut body = job(1);
        body[name] = value;
        let uri = target.tenant_uri(&format!("work/{QUEUE}/items"));
        let (status, response) = target.authed(Method::POST, &uri, Some(&body)).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{name}: {response}");
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap_or("")
                .contains(name)
        );
    }
    assert!(target.read("work.builds").await?.is_empty());

    // The server names the item after the message, and starts at attempt 1.
    let published = target.work_publish(QUEUE, &job(1)).await?;
    let envelope = &published.envelope;
    assert_eq!(
        envelope.item_id.as_ref().map(|i| i.to_string()),
        Some(envelope.id.to_string())
    );
    assert_eq!(envelope.attempt, Some(1));
    assert_eq!(envelope.topic.to_string(), "work.builds");
    assert_eq!(envelope.principal.to_string(), PRINCIPAL);
    assert_eq!(target.read("work.builds").await?, vec![envelope.clone()]);

    // A client that names the message names the item too, and cannot open
    // the same item twice.
    let mut named = job(2);
    let id = EventId::generate().to_string();
    named["id"] = json!(id);
    let published = target.work_publish(QUEUE, &named).await?;
    assert_eq!(published.envelope.item_id.map(|i| i.to_string()), Some(id));
    let uri = target.tenant_uri(&format!("work/{QUEUE}/items"));
    let (status, _) = target.authed(Method::POST, &uri, Some(&named)).await?;
    assert_eq!(status, StatusCode::CONFLICT);
    Ok(())
}

/// The API tells a client the settings the server runs with.
#[tokio::test]
async fn the_settings_are_served_and_need_the_token() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let settings = &target.settings;
    assert!(settings.lease_seconds >= 1);
    assert!(settings.max_attempts >= 1);
    assert!(settings.backoff_base_seconds >= 1);
    assert!(settings.backoff_cap_seconds >= settings.backoff_base_seconds);
    assert!(settings.alert_resurface_seconds >= 1);
    assert!(settings.tick_millis >= 1);
    assert!(settings.poll_max_count >= settings.poll_default_count);
    assert!(settings.agent_credential_ttl_seconds >= 1);
    assert!(settings.credential_recheck_seconds >= 1);
    assert!(settings.credential_sweep_seconds >= 1);

    let uri = format!("{}/v1/settings", target.base);
    let (status, body) = target.call(Method::GET, &uri, None, None).await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["error"]["code"], "unauthorized");
    let (status, body) = target.authed(Method::GET, &uri, None).await?;
    assert_eq!(status, StatusCode::OK);
    let keys: HashSet<&str> = body
        .as_object()
        .context("an object")?
        .keys()
        .map(String::as_str)
        .collect();
    for key in [
        "lease_seconds",
        "max_attempts",
        "backoff_base_seconds",
        "backoff_cap_seconds",
        "alert_resurface_seconds",
        "tick_millis",
        "agent_credential_ttl_seconds",
        "credential_recheck_seconds",
        "credential_sweep_seconds",
    ] {
        assert!(keys.contains(key), "{key} in {body}");
    }
    assert!(
        !Value::Object(body.as_object().context("an object")?.clone())
            .to_string()
            .to_lowercase()
            .contains("token")
    );
    Ok(())
}

// ---- extending a lease -----------------------------------------------------

/// What a comparison of a time the server read with one the test read may be
/// out by, in milliseconds.
const ROUNDING: i64 = 50;

/// The lease setting in milliseconds.
fn lease_millis(target: &Target) -> i64 {
    i64::try_from(target.lease().as_millis()).unwrap_or(i64::MAX)
}

/// Where the longest lease from a claim ends: the claim's own expiry is the
/// claim plus the lease setting.
fn longest_end(target: &Target, claim: &mansio_core::api::Claimed) -> anyhow::Result<i64> {
    let max = i64::try_from(target.lease_max().as_millis()).unwrap_or(i64::MAX);
    Ok(millis(&claim.expires_at)? - lease_millis(target) + max)
}

/// Done criterion 1: the holder extends its lease, to now plus the lease
/// setting or to a duration it asks for, and the `extended` fact says where it
/// ends. Anyone else is refused as for an ack, and nothing changes.
#[tokio::test]
async fn the_holder_extends_its_lease_and_anyone_else_is_refused_as_for_an_ack()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let lease = target.lease().as_secs();
    ensure!(
        target.lease_max() >= target.lease() * 3,
        "the test asks for three leases"
    );
    target.work_publish(QUEUE, &job(1)).await?;
    let started = Instant::now();
    let claim = target.claim(QUEUE, "worker", SLACK).await?;
    let item = item_of(&claim)?;
    let id = claim.lease_id.to_string();
    let first_end = millis(&claim.expires_at)?;

    // Refused, and none of it changes anything.
    let stranger = EventId::generate().to_string();
    let (status, body) = target
        .extend(QUEUE, &item, "worker", &stranger, None)
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_eq!(body["error"]["code"], "forbidden");
    let (status, body) = target.extend(QUEUE, &item, "another", &id, None).await?;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    let (status, _) = target.extend(QUEUE, &item, "worker", "nope", None).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, body) = target.extend(QUEUE, &item, "worker", &id, Some(0)).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let uri = target.tenant_uri(&format!("work/{QUEUE}/items/{item}/extend"));
    let (status, _) = target.authed(Method::POST, &uri, None).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    for bad in [
        json!({"lease_id": id, "consumer": "worker", "seconds": "soon"}),
        json!({"lease_id": id, "consumer": "worker", "seconds": -5}),
        json!({"lease_id": id, "consumer": "worker", "until": "tomorrow"}),
    ] {
        let (status, _) = target.authed(Method::POST, &uri, Some(&bad)).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
    }
    let (status, body) = target
        .extend(QUEUE, "no-such-item", "worker", &id, None)
        .await?;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    let (status, _) = target
        .call(
            Method::POST,
            &uri,
            None,
            Some(&json!({"lease_id": id, "consumer": "worker"})),
        )
        .await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(target.work_transitions().await?, ["claimed"]);

    // By the lease setting from now: the end moves by how long the holder
    // waited, no more.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let by_default = target.extend_claim(QUEUE, &claim, None).await?;
    assert_eq!(
        by_default.envelope.event_type.as_str(),
        "mansio.work.extended"
    );
    assert_eq!(by_default.envelope.principal.to_string(), PRINCIPAL);
    assert_eq!(by_default.envelope.item_id, claim.item.item_id);
    assert_eq!(by_default.envelope.attempt, Some(1));
    let moved = extended_to(&by_default)? - first_end;
    assert!(moved >= 100, "the end moved {moved} ms");
    let waited = i64::try_from(started.elapsed().as_millis())? + ROUNDING;
    assert!(moved <= waited, "the end moved {moved} ms in {waited} ms");

    // To a duration of its own, from now.
    let long = target.extend_claim(QUEUE, &claim, Some(lease * 3)).await?;
    let long_end = extended_to(&long)?;
    let moved = long_end - first_end;
    let waited = i64::try_from(started.elapsed().as_millis())? + ROUNDING;
    assert!(
        moved >= 2 * lease_millis(&target) && moved <= 2 * lease_millis(&target) + waited,
        "the end moved {moved} ms"
    );
    // Never to an earlier end than the lease already has.
    let shorter = target.extend_claim(QUEUE, &claim, Some(1)).await?;
    assert_eq!(extended_to(&shorter)?, long_end);

    // The fact says which lease, whose and where it ends, and not its id.
    let facts = target.read("facts.work").await?;
    assert_eq!(
        target.work_transitions().await?,
        ["claimed", "extended", "extended", "extended"]
    );
    let fact = &facts[1];
    assert_eq!(fact.item_id, claim.item.item_id);
    let data = fact.data.as_ref().context("data")?;
    assert_eq!(data["lease"], claim.lease_id.fingerprint().to_string());
    assert_eq!(data["holder"], PRINCIPAL);
    assert_eq!(data["consumer"], "worker");
    assert_eq!(data["queue"], QUEUE);
    assert_eq!(data["message_id"], claim.item.id.to_string());
    for fact in &facts {
        assert!(!serde_json::to_string(fact)?.contains(&id));
    }
    if let Some(recorded) = target.sink_records()? {
        let in_sink: Vec<_> = recorded
            .iter()
            .filter(|e| e.event_type.as_str() == "mansio.work.extended")
            .map(|e| e.id)
            .collect();
        let on_topic: Vec<_> = facts
            .iter()
            .filter(|e| e.event_type.as_str() == "mansio.work.extended")
            .map(|e| e.id)
            .collect();
        assert_eq!(in_sink, on_topic, "the sink recorded each extension");
    }

    // Past where the first lease ended it still holds: nobody else is given
    // the item, nothing expired, and the holder is told the end it has now.
    tokio::time::sleep(target.lease() + target.settle()).await;
    assert!(target.pull(QUEUE, "other").await?.is_none());
    let held = target
        .pull(QUEUE, "worker")
        .await?
        .context("the lease it holds")?;
    assert_eq!(held.lease_id, claim.lease_id);
    assert_eq!(millis(&held.expires_at)?, long_end);
    assert!(
        !target
            .work_transitions()
            .await?
            .iter()
            .any(|t| t == "expired")
    );

    // It acks once, and a done item is not extended.
    target.ack_claim(QUEUE, &claim).await?;
    let (status, _) = target.extend(QUEUE, &item, "worker", &id, None).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

/// An extension after the lease has ended is refused with 409 and changes
/// nothing. Once another consumer holds the item, the old lease is not the
/// holder's.
#[tokio::test]
async fn an_extension_after_the_lease_has_ended_is_a_409_and_changes_nothing() -> anyhow::Result<()>
{
    let target = Target::start().await?;
    target.work_publish(QUEUE, &job(1)).await?;
    let first = target.claim(QUEUE, "worker", SLACK).await?;
    let item = item_of(&first)?;

    tokio::time::sleep(target.lease() + target.settle()).await;
    let (status, body) = target
        .extend(
            QUEUE,
            &item,
            "worker",
            &first.lease_id.to_string(),
            Some(60),
        )
        .await?;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"]["code"], "conflict");
    let transitions = target.work_transitions().await?;
    assert!(
        !transitions.iter().any(|t| t == "extended"),
        "{transitions:?}"
    );

    // The item comes back as the next attempt, and the old lease is not the
    // new holder's.
    let wait = target.backoff(1) + SLACK;
    let second = target.claim(QUEUE, "other", wait).await?;
    assert_eq!(second.item.attempt, Some(2));
    let (status, _) = target
        .extend(
            QUEUE,
            &item,
            "worker",
            &first.lease_id.to_string(),
            Some(60),
        )
        .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let extended = target.extend_claim(QUEUE, &second, None).await?;
    assert_eq!(extended.envelope.attempt, Some(2));
    target.ack_claim(QUEUE, &second).await?;
    let transitions = target.work_transitions().await?;
    assert_eq!(
        transitions.iter().filter(|t| *t == "extended").count(),
        1,
        "{transitions:?}"
    );
    Ok(())
}

/// Done criterion 2: a consumer that extends every half lease keeps an item
/// for many lease lengths, and acks it once. Nothing else claims it.
#[tokio::test]
async fn a_consumer_that_extends_every_half_lease_keeps_its_item_for_many_leases_and_acks_it_once()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    target.work_publish(QUEUE, &job(1)).await?;
    let claim = target.claim(QUEUE, "worker", SLACK).await?;
    let lease = target.lease();
    ensure!(
        target.lease_max() >= lease * 6,
        "the test holds the item for five leases"
    );

    // Extend on a schedule of half leases from the start, so that a slow
    // answer does not push the next extension later. Hold it for five leases
    // of wall time, however many extensions that takes on this machine.
    let started = Instant::now();
    let half = lease / 2;
    let mut end = millis(&claim.expires_at)?;
    let mut extensions = 0_u32;
    let mut held = Duration::ZERO;
    while held < lease * 5 {
        extensions += 1;
        tokio::time::sleep_until(tokio::time::Instant::from_std(started + half * extensions)).await;
        let extended = extended_to(&target.extend_claim(QUEUE, &claim, None).await?)?;
        assert!(extended > end, "the end moved from {end} to {extended}");
        end = extended;
        held = started.elapsed();
        // Nobody else is given it, whatever they ask.
        assert!(target.pull(QUEUE, "other").await?.is_none());
    }
    // Each extension reaches a lease from the time it is made, so holding the
    // item for a stretch of time takes a count of them that the stretch
    // fixes: the claim covers the first lease and the rest are extensions.
    let leases_held = held.as_millis() / lease.as_millis();
    assert!(
        u128::from(extensions) + 1 >= leases_held,
        "{extensions} extensions held it for {held:?}, which is {leases_held} leases"
    );

    target.ack_claim(QUEUE, &claim).await?;
    let mut expected = vec!["claimed"];
    expected.extend(std::iter::repeat_n(
        "extended",
        usize::try_from(extensions)?,
    ));
    expected.push("acked");
    assert_eq!(target.work_transitions().await?, expected);
    assert_eq!(
        target.read("work.builds").await?.len(),
        1,
        "the item was never republished"
    );
    Ok(())
}

/// The longest a lease may run is counted from the claim. An extension that
/// asks for more is capped there and not refused, and none ever shortens the
/// lease.
#[tokio::test]
async fn an_extension_is_capped_at_the_longest_lease_counted_from_the_claim() -> anyhow::Result<()>
{
    let target = Target::start().await?;
    target.work_publish(QUEUE, &job(1)).await?;
    let claim = target.claim(QUEUE, "worker", SLACK).await?;
    let cap = longest_end(&target, &claim)?;

    for seconds in [
        Some(u64::MAX),
        Some(target.lease_max().as_secs() * 10),
        None,
    ] {
        let extended = target.extend_claim(QUEUE, &claim, seconds).await?;
        assert_eq!(extended_to(&extended)?, cap, "asked for {seconds:?}");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // And a short one does not bring it back.
    let shorter = target.extend_claim(QUEUE, &claim, Some(1)).await?;
    assert_eq!(extended_to(&shorter)?, cap);
    target.ack_claim(QUEUE, &claim).await?;
    Ok(())
}

/// Done criterion 3: a restarted server keeps the extended lease, and the
/// longest lease still counts from the claim and not from the restart.
#[tokio::test]
async fn a_restarted_server_keeps_the_extended_lease_and_the_longest_lease_from_the_claim()
-> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        eprintln!("MANSIO_E2E_URL is set: a live server cannot be restarted from here");
        return Ok(());
    }
    target.work_publish(QUEUE, &job(1)).await?;
    let first = target.claim(QUEUE, "worker", SLACK).await?;
    let extended = target
        .extend_claim(QUEUE, &first, Some(target.lease().as_secs() * 4))
        .await?;
    let end = extended_to(&extended)?;
    assert!(end > millis(&first.expires_at)?);

    target.stop_server().await?;
    target.start_server().await?;

    // Past where the first lease ended, the extended one holds.
    tokio::time::sleep(target.lease() + target.settle()).await;
    assert!(target.pull(QUEUE, "other").await?.is_none());
    let held = target
        .pull(QUEUE, "worker")
        .await?
        .context("the lease it holds")?;
    assert_eq!(held.item, first.item);
    assert_eq!(millis(&held.expires_at)?, end, "the extended end");
    assert_ne!(held.lease_id, first.lease_id, "a restart cannot say the id");

    // The longest lease counts from the claim.
    let capped = target.extend_claim(QUEUE, &held, Some(u64::MAX)).await?;
    assert_eq!(extended_to(&capped)?, longest_end(&target, &first)?);
    target.ack_claim(QUEUE, &held).await?;
    assert_eq!(
        target.work_transitions().await?,
        ["claimed", "extended", "claimed", "extended", "acked"]
    );
    Ok(())
}

/// Done criterion 4: the command line extends a lease and prints the fact.
#[tokio::test]
async fn the_cli_extends_a_lease_and_prints_the_fact() -> anyhow::Result<()> {
    let target = Target::start().await?;
    target.work_publish(QUEUE, &job(1)).await?;
    let claim = target.claim(QUEUE, "worker", SLACK).await?;
    let item = item_of(&claim)?;
    let id = claim.lease_id.to_string();
    let seconds = (target.lease().as_secs() * 3).to_string();
    let words = |lease: &str, more: &[&str]| -> Vec<String> {
        [
            "work",
            "extend",
            "--tenant",
            &target.tenant,
            "--queue",
            QUEUE,
            "--item",
            &item,
            "--consumer",
            "worker",
            "--lease-id",
            lease,
        ]
        .into_iter()
        .chain(more.iter().copied())
        .map(str::to_owned)
        .collect()
    };

    let output = run_cli(&target, words(&id, &["--seconds", &seconds])).await?;
    let fact = only_line(&output)?;
    assert_eq!(fact["type"], "mansio.work.extended");
    assert_eq!(fact["itemid"], json!(item));
    assert_eq!(fact["attempt"], 1);
    let ends = millis(&serde_json::from_value(fact["data"]["expires_at"].clone())?)?;
    assert!(ends > millis(&claim.expires_at)?);

    // Without --seconds it is the lease setting, and not earlier than it was.
    let output = run_cli(&target, words(&id, &[])).await?;
    let fact = only_line(&output)?;
    let again = millis(&serde_json::from_value(fact["data"]["expires_at"].clone())?)?;
    assert_eq!(again, ends, "the lease is not shortened");

    // A lease that is not the holder's is refused, with the reason on stderr.
    let stranger = EventId::generate().to_string();
    let output = run_cli(&target, words(&stranger, &[])).await?;
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("403") && stderr.contains("not yours"),
        "{stderr}"
    );
    assert_eq!(
        target.work_transitions().await?,
        ["claimed", "extended", "extended"]
    );
    target.ack_claim(QUEUE, &claim).await?;
    Ok(())
}
