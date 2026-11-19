//! `work_list` and `work_show`: what a queue holds, read from the engine's
//! own state. Each case builds the state it needs on a tenant of its own, by
//! the steps of the cases before it.

use anyhow::{Context, ensure};
use mansio_core::Timestamp;
use mansio_core::api::{Claimed, Published};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::harness::{DataValue, PRINCIPAL, SLACK, Target, item_of, job, millis};

const QUEUE: &str = "listing";
const ENGINE: &str = "service:mansio-engine";

/// A tenant with items published to `listing`, named in publish order.
struct World {
    target: Target,
    /// The ids of the items published so far: `ids[0]` is `i1`.
    ids: Vec<String>,
    published: Vec<Published>,
    /// The claim of `i1` by `w1`.
    claim: Claimed,
    /// Where its lease ends after the extension, as the `extended` fact says.
    expires_at: Value,
}

async fn get(target: &Target, token: &str, rest: &str) -> anyhow::Result<(StatusCode, Value)> {
    target
        .call(Method::GET, &target.tenant_uri(rest), Some(token), None)
        .await
}

/// `L(query)`: the admin's `work_list`, which must be accepted.
async fn list(target: &Target, query: &str) -> anyhow::Result<Value> {
    let (status, body) = get(
        target,
        &target.token,
        &format!("work/{QUEUE}/items?{query}"),
    )
    .await?;
    ensure!(
        status == StatusCode::OK,
        "work_list answered {status}: {body}"
    );
    Ok(body)
}

/// `work_show` by the admin, which must be accepted.
async fn show(target: &Target, queue: &str, item: &str) -> anyhow::Result<Value> {
    let (status, body) = get(target, &target.token, &format!("work/{queue}/items/{item}")).await?;
    ensure!(
        status == StatusCode::OK,
        "work_show answered {status}: {body}"
    );
    Ok(body)
}

fn ids(page: &Value) -> Vec<String> {
    page["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item["item_id"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn states(page: &Value) -> Vec<String> {
    page["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item["state"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// Milliseconds of a timestamp member of an answer.
fn ms(value: &Value) -> anyhow::Result<i64> {
    let timestamp: Timestamp = serde_json::from_value(value.clone())?;
    millis(&timestamp)
}

/// The text a fact's data member holds.
fn member(fact: &Published, name: &str) -> anyhow::Result<Value> {
    Ok(fact
        .envelope
        .data
        .as_ref()
        .context("the fact has data")?
        .value()[name]
        .clone())
}

/// A tenant where `i1`, `i2` and `i3` are published, `w1` holds `i1`, and its
/// lease is extended to 30 seconds.
async fn three_items_one_leased() -> anyhow::Result<World> {
    let target = Target::start().await?;
    let mut ids = Vec::new();
    let mut published = Vec::new();
    for n in 1..=3 {
        let item = target.work_publish(QUEUE, &job(n)).await?;
        ids.push(item.envelope.id.to_string());
        published.push(item);
    }
    let claim = target.claim(QUEUE, "w1", SLACK).await?;
    assert_eq!(item_of(&claim)?, ids[0]);
    let extended = target.extend_claim(QUEUE, &claim, Some(30)).await?;
    let expires_at = member(&extended, "expires_at")?;
    Ok(World {
        target,
        ids,
        published,
        claim,
        expires_at,
    })
}

/// Then `i1` is failed and waited for until it is claimable again: its
/// attempt 2, at the end of the queue. Returns the `failed` fact.
async fn i1_failed_and_back(world: &World) -> anyhow::Result<Published> {
    let failed = world.target.fail_claim(QUEUE, &world.claim, "e2e").await?;
    let wait = world.target.backoff(1) + SLACK;
    world
        .target
        .until("i1 to be claimable again", wait, || async {
            let shown = show(&world.target, QUEUE, &world.ids[0]).await?;
            Ok((shown["state"] == "claimable").then_some(shown))
        })
        .await?;
    Ok(failed)
}

/// Then `w2` claims `i2` and acks it.
async fn i2_acked(world: &World) -> anyhow::Result<()> {
    let claim = world.target.claim(QUEUE, "w2", SLACK).await?;
    assert_eq!(item_of(&claim)?, world.ids[1]);
    world.target.ack_claim(QUEUE, &claim).await?;
    Ok(())
}

/// Then `i5` and `i6` are published and `w3` holds `i3`, its lease extended.
/// Returns the ids of `i5` and `i6`.
async fn i5_i6_and_i3_leased(world: &World) -> anyhow::Result<(String, String)> {
    let i5 = world.target.work_publish(QUEUE, &job(5)).await?;
    let i6 = world.target.work_publish(QUEUE, &job(6)).await?;
    let claim = world.target.claim(QUEUE, "w3", SLACK).await?;
    assert_eq!(item_of(&claim)?, world.ids[2]);
    world.target.extend_claim(QUEUE, &claim, Some(30)).await?;
    Ok((i5.envelope.id.to_string(), i6.envelope.id.to_string()))
}

/// The pages of `work_list`, `count` items at a time, as one list of items.
async fn all_items(target: &Target, count: u32) -> anyhow::Result<Vec<Value>> {
    let mut items = Vec::new();
    let mut after = String::new();
    loop {
        let page = list(target, &format!("count={count}{after}")).await?;
        items.extend(page["items"].as_array().cloned().unwrap_or_default());
        match page["next"].as_u64() {
            Some(next) => after = format!("&after={next}"),
            None => return Ok(items),
        }
    }
}

async fn error_code(target: &Target, token: &str, rest: &str) -> anyhow::Result<(u16, Value)> {
    let (status, body) = get(target, token, rest).await?;
    Ok((status.as_u16(), body["error"]["code"].clone()))
}

#[tokio::test]
async fn work_list_shows_one_leased_and_two_claimable_in_publish_order() -> anyhow::Result<()> {
    let world = three_items_one_leased().await?;
    let target = &world.target;
    let reader = target
        .issue(
            "agent",
            "reader",
            &json!([{"topic": "work.listing", "verbs": ["subscribe"]}]),
            None,
        )
        .await?;

    let page = list(target, "").await?;
    let (status, as_reader) = get(target, &reader.token, &format!("work/{QUEUE}/items")).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(as_reader, page, "the admin and a subscriber read the same");

    assert_eq!(ids(&page), world.ids);
    assert_eq!(states(&page), ["leased", "claimable", "claimable"]);
    let items = page["items"].as_array().context("items")?;
    assert_eq!(
        items
            .iter()
            .map(|i| i["attempt"].clone())
            .collect::<Vec<_>>(),
        [json!(1), json!(1), json!(1)]
    );
    assert_eq!(items[0]["holder"], PRINCIPAL);
    assert_eq!(items[0]["consumer"], "w1");
    assert_eq!(items[0]["expires_at"], world.expires_at);
    for lacks in ["holder", "consumer", "expires_at", "claimable_at"] {
        for open in &items[1..] {
            assert!(open.get(lacks).is_none(), "{lacks} on a claimable item");
        }
    }
    for (n, item) in items.iter().enumerate() {
        assert_eq!(item["type"], "job.requested");
        assert_eq!(item["subject"], format!("job-{}", n + 1));
        assert_eq!(item["publisher"], PRINCIPAL);
        assert_eq!(
            item["published_at"],
            serde_json::to_value(&world.published[n].envelope.time)?
        );
        assert!(item.get("envelope").is_none());
        assert!(item.get("data").is_none());
    }
    assert!(page.get("next").is_none());
    assert!(!page.to_string().contains(&world.claim.lease_id.to_string()));
    Ok(())
}

#[tokio::test]
async fn work_show_answers_a_live_item_with_its_envelope_and_no_lease_id() -> anyhow::Result<()> {
    let world = three_items_one_leased().await?;
    let target = &world.target;
    let page = list(target, "").await?;

    let leased = show(target, QUEUE, &world.ids[0]).await?;
    let mut expected = page["items"][0].clone();
    expected["envelope"] = serde_json::to_value(&world.published[0].envelope)?;
    assert_eq!(leased, expected);
    assert!(
        !leased
            .to_string()
            .contains(&world.claim.lease_id.to_string())
    );

    let open = show(target, QUEUE, &world.ids[1]).await?;
    assert_eq!(open["state"], "claimable");
    for lacks in ["holder", "consumer", "expires_at", "claimable_at"] {
        assert!(open.get(lacks).is_none(), "{lacks}");
    }
    Ok(())
}

#[tokio::test]
async fn a_failed_item_shows_backoff_then_claimable_at_attempt_2_from_the_engine()
-> anyhow::Result<()> {
    let world = three_items_one_leased().await?;
    let target = &world.target;
    let failed = target.fail_claim(QUEUE, &world.claim, "e2e").await?;
    let retry_at = member(&failed, "retry_at")?;

    let waiting = show(target, QUEUE, &world.ids[0]).await?;
    assert_eq!(waiting["state"], "backoff");
    assert_eq!(waiting["attempt"], 1);
    assert_eq!(ms(&waiting["claimable_at"])?, ms(&retry_at)?);
    assert!(waiting.get("holder").is_none());

    let wait = target.backoff(1) + SLACK;
    let back = target
        .until("i1 to be claimable again", wait, || async {
            let shown = show(target, QUEUE, &world.ids[0]).await?;
            Ok((shown["state"] == "claimable").then_some(shown))
        })
        .await?;
    assert_eq!(back["attempt"], 2);
    assert_eq!(back["publisher"], ENGINE);
    assert!(ms(&back["published_at"])? >= ms(&retry_at)?);
    let page = list(target, "").await?;
    assert_eq!(
        ids(&page),
        [&world.ids[1], &world.ids[2], &world.ids[0]].map(String::clone)
    );
    Ok(())
}

#[tokio::test]
async fn an_acked_item_leaves_the_list_and_shows_only_as_finished() -> anyhow::Result<()> {
    let world = three_items_one_leased().await?;
    i1_failed_and_back(&world).await?;
    i2_acked(&world).await?;
    let target = &world.target;

    let (status, body) = get(
        target,
        &target.token,
        &format!("work/{QUEUE}/items/{}", world.ids[1]),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"item_id": world.ids[1], "state": "finished"}));
    assert_eq!(
        ids(&list(target, "").await?),
        [world.ids[2].clone(), world.ids[0].clone()]
    );
    Ok(())
}

#[tokio::test]
async fn a_dead_item_shows_only_as_finished_and_is_on_dead() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let queue = "endings";
    let item = target.work_publish(queue, &job(1)).await?;
    let id = item.envelope.id.to_string();
    let attempts = target.settings.max_attempts;
    for attempt in 1..=attempts {
        let claim = target
            .claim(queue, "w1", target.backoff(attempt) + SLACK)
            .await?;
        assert_eq!(item_of(&claim)?, id);
        target.fail_claim(queue, &claim, "e2e").await?;
    }

    assert_eq!(
        show(&target, queue, &id).await?,
        json!({"item_id": id, "state": "finished"})
    );
    let (status, body) = get(&target, &target.token, &format!("work/{queue}/items")).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"items": []}));
    let dead = target
        .until("the message on dead", SLACK, || async {
            let read = target.read("dead").await?;
            Ok(read
                .iter()
                .any(|m| m.item_id.as_ref().map(|i| i.as_str()) == Some(id.as_str()))
                .then_some(()))
        })
        .await;
    dead.context("the dead topic holds the item")?;
    Ok(())
}

#[tokio::test]
async fn an_id_the_queue_never_held_is_not_found() -> anyhow::Result<()> {
    let world = three_items_one_leased().await?;
    let target = &world.target;
    let fresh = mansio_core::EventId::generate().to_string();
    assert_eq!(
        error_code(
            target,
            &target.token,
            &format!("work/{QUEUE}/items/{fresh}")
        )
        .await?,
        (404, json!("not_found"))
    );

    let (status, body) = get(target, &target.token, "work/nobody/items").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!({"items": []}));
    assert_eq!(
        error_code(target, &target.token, &format!("work/nobody/items/{fresh}")).await?,
        (404, json!("not_found"))
    );
    Ok(())
}

#[tokio::test]
async fn the_state_filter_and_paging_return_every_item_once() -> anyhow::Result<()> {
    let world = three_items_one_leased().await?;
    i1_failed_and_back(&world).await?;
    i2_acked(&world).await?;
    let (i5, i6) = i5_i6_and_i3_leased(&world).await?;
    let target = &world.target;
    let (i1, i3) = (world.ids[0].clone(), world.ids[2].clone());

    let claimable = list(target, "state=claimable").await?;
    assert_eq!(ids(&claimable), [i1.clone(), i5.clone(), i6.clone()]);
    assert!(claimable.get("next").is_none());
    assert_eq!(ids(&list(target, "state=leased").await?), vec![i3.clone()]);
    assert!(ids(&list(target, "state=backoff").await?).is_empty());

    let mut seen = Vec::new();
    let mut pages = 0;
    let mut after = String::new();
    loop {
        let page = list(target, &format!("count=1{after}")).await?;
        seen.extend(ids(&page));
        pages += 1;
        match page["next"].as_u64() {
            Some(next) => after = format!("&after={next}"),
            None => break,
        }
    }
    assert_eq!(pages, 4);
    assert_eq!(seen, [i3, i1, i5.clone(), i6.clone()]);

    let first = list(target, "state=claimable&count=2").await?;
    assert_eq!(ids(&first), [world.ids[0].clone(), i5]);
    let next = first["next"].as_u64().context("a next")?;
    let second = list(target, &format!("state=claimable&count=2&after={next}")).await?;
    assert_eq!(ids(&second), [i6]);
    assert!(second.get("next").is_none());
    Ok(())
}

#[tokio::test]
async fn work_list_and_work_show_refuse_who_may_not_read_and_bad_queries() -> anyhow::Result<()> {
    let world = three_items_one_leased().await?;
    i1_failed_and_back(&world).await?;
    i2_acked(&world).await?;
    let (i5, _) = i5_i6_and_i3_leased(&world).await?;
    let target = &world.target;

    let writer = target
        .issue(
            "agent",
            "writer",
            &json!([{"topic": "work.listing", "verbs": ["publish"]}]),
            None,
        )
        .await?;
    let other = format!("{}b", target.tenant);
    let (status, body) = target
        .create_principal(&json!({
            "kind": "agent",
            "name": "stranger",
            "tenant": other,
            "scopes": [{"topic": "work.listing", "verbs": ["subscribe"]}],
            "ttl_seconds": 600,
        }))
        .await?;
    ensure!(
        status == StatusCode::CREATED,
        "principals_create: {status} {body}"
    );
    let stranger = body["credential"].as_str().context("a credential")?;

    let list_uri = format!("work/{QUEUE}/items");
    let show_uri = format!("work/{QUEUE}/items/{i5}");
    for (who, token) in [("writer", writer.token.as_str()), ("stranger", stranger)] {
        for uri in [&list_uri, &show_uri] {
            let (status, body) = get(target, token, uri).await?;
            assert_eq!(status, StatusCode::FORBIDDEN, "{who} {uri}");
            assert_eq!(body["error"]["code"], "forbidden");
            assert!(body.get("items").is_none() && body.get("item_id").is_none());
        }
    }

    let too_many = target.settings.work_list_max_count + 1;
    for query in [
        "count=0".to_owned(),
        format!("count={too_many}"),
        "state=finished".to_owned(),
        "state=held".to_owned(),
        "after=x".to_owned(),
        "limit=1".to_owned(),
    ] {
        assert_eq!(
            error_code(target, &target.token, &format!("{list_uri}?{query}")).await?,
            (400, json!("bad_request")),
            "{query}"
        );
    }

    assert!(target.settings.work_list_default_count >= 1);
    assert!(target.settings.work_list_default_count <= target.settings.work_list_max_count);
    if target.is_local() {
        assert_eq!(target.settings.work_list_default_count, 100);
        assert_eq!(target.settings.work_list_max_count, 1000);
    }
    let shown = target
        .authed(Method::GET, &format!("{}/v1/settings", target.base), None)
        .await?;
    assert_eq!(
        shown.1["work_list_default_count"],
        target.settings.work_list_default_count
    );
    assert_eq!(
        shown.1["work_list_max_count"],
        target.settings.work_list_max_count
    );
    Ok(())
}

#[tokio::test]
async fn work_list_after_a_restart_is_the_same() -> anyhow::Result<()> {
    let mut world = three_items_one_leased().await?;
    if !world.target.is_local() {
        eprintln!("MANSIO_E2E_URL is set: a live server cannot be restarted from here");
        return Ok(());
    }
    i1_failed_and_back(&world).await?;
    i2_acked(&world).await?;
    i5_i6_and_i3_leased(&world).await?;
    let finished = json!({"item_id": world.ids[1], "state": "finished"});

    let before = all_items(&world.target, 2).await?;
    assert_eq!(before.len(), 4);

    // A start that may use the snapshot.
    world.target.stop_server().await?;
    world.target.start_server().await?;
    assert_eq!(all_items(&world.target, 2).await?, before);
    assert_eq!(show(&world.target, QUEUE, &world.ids[1]).await?, finished);

    // A rebuild from offset 0.
    world.target.forget_snapshots()?;
    world.target.stop_server().await?;
    world.target.start_server().await?;
    assert_eq!(all_items(&world.target, 2).await?, before);
    assert_eq!(show(&world.target, QUEUE, &world.ids[1]).await?, finished);
    Ok(())
}
