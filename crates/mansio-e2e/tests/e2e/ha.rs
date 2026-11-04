//! Two servers on one broker: a tenant has one owner at a time.
//!
//! The servers run with `MANSIO_OWNER_TTL_SECONDS`, so each tenant is served
//! by one of them and the other answers 503 for it. These check that:
//!
//! - however the pulls are spread over the two servers, no item is leased
//!   twice, and every item is leased;
//! - when the owner crashes, the other takes the tenant within the ttl (and a
//!   quarter of it, for the interval between renewals), and no item is lost:
//!   the one that was leased comes back after its lease, and the ones that were
//!   waiting are claimed.
//!
//! In process, both servers share a `MemoryBroker`. Against compose, the
//! servers are `MANSIO_E2E_URL` and `MANSIO_E2E_URL_B`; without the second the
//! tests that need two do nothing. The handover cannot crash a container from
//! here, so against compose it is two passes (`ha_first_pass`,
//! `ha_second_pass`, `#[ignore]`d and selected by name) with the CI job
//! killing the owner between them.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail, ensure};
use mansio_core::api::{Claimed, Published};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::harness::{LocalSettings, SLACK, Target, item_of, job};

const QUEUE: &str = "builds";
/// The compose services, by the URL that reaches them.
const SERVICE_A: &str = "mansio-server";
const SERVICE_B: &str = "mansio-server-b";
/// The ttl of the in-process servers.
const TTL: Duration = Duration::from_secs(4);

/// What a handover may take beyond its bound: a probe every 50 ms, a request,
/// and the round that claims the tenant, with room for a loaded runner. The
/// bound itself is the setting: the ttl and a quarter of it.
const HANDOVER_MARGIN: Duration = Duration::from_secs(2);

/// Now, in milliseconds since the epoch: a time one process writes and another
/// reads, as the job does between the kill and the second pass.
fn unix_millis() -> anyhow::Result<u128> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())
}
/// How many items the servers share out between consumers.
const ITEMS: u32 = 24;
/// Consumers on each server in the race for items.
const WORKERS_PER_SERVER: u32 = 3;

async fn publish(target: &Target, n: u32) -> anyhow::Result<(StatusCode, Value)> {
    let uri = target.tenant_uri(&format!("work/{QUEUE}/items"));
    target.authed(Method::POST, &uri, Some(&job(n))).await
}

async fn pull(target: &Target, consumer: &str) -> anyhow::Result<(StatusCode, Value)> {
    let uri = target.tenant_uri(&format!("work/{QUEUE}/pull"));
    target
        .authed(Method::POST, &uri, Some(&json!({ "consumer": consumer })))
        .await
}

fn item_id_of(published: Value) -> anyhow::Result<String> {
    let published: Published = serde_json::from_value(published)?;
    Ok(published
        .envelope
        .item_id
        .context("a published item has an id")?
        .to_string())
}

/// The ttl the servers run with, which must be set.
fn ttl_of(target: &Target) -> anyhow::Result<Duration> {
    let ttl = Duration::from_secs(target.settings.owner_ttl_seconds);
    ensure!(
        !ttl.is_zero(),
        "the servers must run with MANSIO_OWNER_TTL_SECONDS to share tenants"
    );
    Ok(ttl)
}

/// A consumer that pulls from one server, acknowledges what it gets, and
/// stops when `want` items have been got by all the consumers. Servers that
/// do not own the tenant answer 503, which it counts and goes on.
async fn worker(
    target: &Target,
    name: &str,
    got: &Mutex<Vec<String>>,
    refused: &AtomicU32,
    want: usize,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + SLACK * 2;
    loop {
        if got.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.len() >= want {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "{name} gave up waiting for items"
        );
        let (status, body) = pull(target, name).await?;
        match status {
            StatusCode::OK => {
                let claim: Claimed = serde_json::from_value(body)?;
                got.lock()
                    .map_err(|_| anyhow::anyhow!("poisoned"))?
                    .push(item_of(&claim)?);
                target.ack_claim(QUEUE, &claim).await?;
            }
            StatusCode::NO_CONTENT => tokio::time::sleep(Duration::from_millis(20)).await,
            StatusCode::SERVICE_UNAVAILABLE => {
                refused.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            other => bail!("{name}: work_pull answered {other}: {body}"),
        }
    }
}

/// What the first half leaves for the second.
struct Handover {
    owner_is_a: bool,
    /// Every item published for the handover.
    items: Vec<String>,
    /// The one acknowledged before the owner stopped.
    acked: String,
    /// The one leased, never acknowledged.
    leased: String,
    /// An agent credential the owner issued, and its principal's id: the
    /// server that takes over never issued it.
    agent_id: String,
    agent_token: String,
}

const AGENT_SCOPES: &str = "work.builds";

/// Both servers publish to a tenant nobody owns, at the same moment: one
/// wins it. Then consumers on both servers take every item, and none is
/// leased twice. Then work is left in the state a crash finds it: one item
/// acknowledged, one leased, the rest waiting. Returns who owns the tenant
/// and what was left.
async fn first_half(a: &Target, b: &Target) -> anyhow::Result<Handover> {
    ttl_of(a)?;

    let (from_a, from_b) = tokio::join!(publish(a, 0), publish(b, 0));
    let ((status_a, body_a), (status_b, body_b)) = (from_a?, from_b?);
    let owner_is_a = match (status_a, status_b) {
        (StatusCode::CREATED, StatusCode::SERVICE_UNAVAILABLE) => true,
        (StatusCode::SERVICE_UNAVAILABLE, StatusCode::CREATED) => false,
        other => bail!("exactly one server should own the tenant: {other:?} {body_a} {body_b}"),
    };
    let (owner, standby) = if owner_is_a { (a, b) } else { (b, a) };
    let mut published = vec![item_id_of(if owner_is_a { body_a } else { body_b })?];
    // The other keeps refusing for as long as the owner renews.
    let (status, body) = publish(standby, 1).await?;
    ensure!(
        status == StatusCode::SERVICE_UNAVAILABLE && body["error"]["code"] == "unavailable",
        "the standby answered {status}: {body}"
    );

    for n in 1..ITEMS {
        let (status, body) = publish(owner, n).await?;
        ensure!(
            status == StatusCode::CREATED,
            "publish answered {status}: {body}"
        );
        published.push(item_id_of(body)?);
    }

    // Consumers on both servers at once.
    let got = Mutex::new(Vec::new());
    let refused = AtomicU32::new(0);
    let want = usize::try_from(ITEMS)?;
    let mut workers = Vec::new();
    for (server_number, server) in [a, b].into_iter().enumerate() {
        for n in 0..WORKERS_PER_SERVER {
            workers.push((server, format!("race-{server_number}-{n}")));
        }
    }
    for result in race(&workers, &got, &refused, want).await {
        result?;
    }
    let got = got.into_inner().map_err(|_| anyhow::anyhow!("poisoned"))?;
    let leased_once: BTreeSet<&String> = got.iter().collect();
    ensure!(
        leased_once.len() == got.len(),
        "an item was leased twice: {got:?}"
    );
    ensure!(
        got.len() == usize::try_from(ITEMS)?,
        "{} of {ITEMS} items were leased",
        got.len()
    );
    ensure!(
        published.iter().all(|id| leased_once.contains(id)),
        "an item was never leased"
    );
    ensure!(
        refused.load(Ordering::SeqCst) > 0,
        "the server that does not own the tenant never refused a pull"
    );
    let claimed = owner.read(&format!("work.{QUEUE}")).await?;
    ensure!(
        claimed.len() == usize::try_from(ITEMS)?,
        "the work topic holds {} items",
        claimed.len()
    );
    let transitions = owner.work_transitions().await?;
    ensure!(
        transitions.iter().filter(|name| *name == "claimed").count() == got.len(),
        "the facts hold a claim that is not an item: {transitions:?}"
    );

    // Work for the handover.
    let mut items = Vec::new();
    for n in 100..105 {
        let (status, body) = publish(owner, n).await?;
        ensure!(
            status == StatusCode::CREATED,
            "publish answered {status}: {body}"
        );
        items.push(item_id_of(body)?);
    }
    let first = owner.claim(QUEUE, "before-crash-1", SLACK).await?;
    owner.ack_claim(QUEUE, &first).await?;
    let second = owner.claim(QUEUE, "before-crash-2", SLACK).await?;
    // Agent credentials are issued per build, after the other server started.
    let agent = owner
        .issue(
            "agent",
            "builder-ha",
            &json!([{ "topic": AGENT_SCOPES, "verbs": ["claim"] }]),
            None,
        )
        .await?;
    Ok(Handover {
        owner_is_a,
        items,
        acked: item_of(&first)?,
        leased: item_of(&second)?,
        agent_id: agent.id.clone(),
        agent_token: agent.token.clone(),
    })
}

/// Run the consumers of both servers at once, to the end. `tokio::join!`
/// takes a fixed number of futures, so the consumers are joined as six, which
/// is `WORKERS_PER_SERVER` on each of the two servers.
async fn race(
    workers: &[(&Target, String)],
    got: &Mutex<Vec<String>>,
    refused: &AtomicU32,
    want: usize,
) -> Vec<anyhow::Result<()>> {
    let one = |n: usize| async move {
        match workers.get(n) {
            Some((target, name)) => worker(target, name, got, refused, want).await,
            None => Ok(()),
        }
    };
    let (a, b, c, d, e, f) = tokio::join!(one(0), one(1), one(2), one(3), one(4), one(5));
    vec![a, b, c, d, e, f]
}

/// The owner was killed at `killed_at`, in milliseconds since the epoch. The
/// other server takes the tenant within the ttl and a quarter of it, plus
/// [`HANDOVER_MARGIN`], of that time, and the items the owner left are all
/// done, each once.
async fn second_half(standby: &Target, left: &Handover, killed_at: u128) -> anyhow::Result<()> {
    let ttl = ttl_of(standby)?;
    let bound = ttl + ttl / 4 + HANDOVER_MARGIN;

    let first = standby
        .until(
            "the other server to take the tenant over",
            bound,
            || async {
                let (status, body) = pull(standby, "after-crash").await?;
                match status {
                    StatusCode::SERVICE_UNAVAILABLE => Ok(None),
                    StatusCode::OK => Ok(Some(Some(serde_json::from_value::<Claimed>(body)?))),
                    StatusCode::NO_CONTENT => Ok(Some(None)),
                    other => bail!("work_pull answered {other}: {body}"),
                }
            },
        )
        .await?;
    let handover = Duration::from_millis(u64::try_from(unix_millis()?.saturating_sub(killed_at))?);
    eprintln!("handover took {handover:?} with a ttl of {ttl:?}");
    ensure!(
        handover <= bound,
        "the tenant was taken over {handover:?} after the kill, more than the ttl of {ttl:?} \
         and a quarter of it, plus {HANDOVER_MARGIN:?}"
    );

    // Everything not acknowledged is claimed once more: the item that was
    // leased when the owner stopped, after its lease ran out and its backoff
    // passed, and the ones that were waiting.
    let mut again: Vec<String> = Vec::new();
    // The credential the dead owner issued works on the server that took over,
    // which has never seen it: the agent claims an item with it.
    let agent = standby.adopt(&left.agent_id, &left.agent_token);
    let (status, claim) = agent.pull(QUEUE).await?;
    ensure!(
        status == StatusCode::OK,
        "the credential the dead owner issued was answered {status}: {claim}"
    );
    again.push(
        claim["item"]["itemid"]
            .as_str()
            .context("a claim names its item")?
            .to_owned(),
    );
    let (status, body) = agent.ack(QUEUE, &claim).await?;
    ensure!(
        status == StatusCode::OK,
        "the agent's ack answered {status}: {body}"
    );
    let mut next = first;
    let wait = Duration::from_secs(standby.settings.lease_seconds) + standby.backoff(1) + SLACK;
    let deadline = Instant::now() + wait;
    let want = left.items.len() - 1;
    while again.len() < want {
        ensure!(
            Instant::now() < deadline,
            "after the handover only {again:?} of {want} items came back"
        );
        match next.take() {
            Some(claim) => {
                again.push(item_of(&claim)?);
                standby.ack_claim(QUEUE, &claim).await?;
            }
            None => {
                let (status, body) = pull(standby, "after-crash").await?;
                match status {
                    StatusCode::OK => next = Some(serde_json::from_value(body)?),
                    StatusCode::NO_CONTENT => tokio::time::sleep(Duration::from_millis(50)).await,
                    other => bail!("work_pull answered {other}: {body}"),
                }
            }
        }
    }
    let once: BTreeSet<&String> = again.iter().collect();
    ensure!(
        once.len() == again.len(),
        "an item came back twice: {again:?}"
    );
    ensure!(
        !once.contains(&left.acked),
        "the acknowledged item came back"
    );
    ensure!(once.contains(&left.leased), "the leased item was dropped");
    let mut all: BTreeSet<&String> = once;
    all.insert(&left.acked);
    ensure!(
        left.items.iter().all(|id| all.contains(id)),
        "an item was dropped: {all:?} of {:?}",
        left.items
    );
    // Nothing is left to claim.
    tokio::time::sleep(standby.settle()).await;
    let (status, _) = pull(standby, "after-crash").await?;
    ensure!(status == StatusCode::NO_CONTENT, "pull answered {status}");
    Ok(())
}

/// The two servers of an in-process test, or `None` where there is a live one
/// that has no second.
async fn pair() -> anyhow::Result<Option<(Target, Target)>> {
    let a = Target::start_with(LocalSettings::sharing(TTL)).await?;
    match a.peer().await? {
        Some(b) => Ok(Some((a, b))),
        None => {
            eprintln!("MANSIO_E2E_URL_B is not set: there is no second server to share with");
            Ok(None)
        }
    }
}

#[tokio::test]
async fn two_servers_never_lease_the_same_item_twice_and_lease_every_item() -> anyhow::Result<()> {
    let Some((a, b)) = pair().await? else {
        return Ok(());
    };
    first_half(&a, &b).await?;
    Ok(())
}

#[tokio::test]
async fn a_crashed_server_hands_its_tenants_to_the_other_and_no_item_is_dropped()
-> anyhow::Result<()> {
    let Some((mut a, mut b)) = pair().await? else {
        return Ok(());
    };
    if !a.is_local() {
        return Ok(());
    }
    let left = first_half(&a, &b).await?;
    if left.owner_is_a {
        a.crash_server().await?;
        second_half(&b, &left, unix_millis()?).await
    } else {
        b.crash_server().await?;
        second_half(&a, &left, unix_millis()?).await
    }
}

#[tokio::test]
async fn a_server_that_shuts_down_gives_its_tenants_up_at_once() -> anyhow::Result<()> {
    let Some((mut a, b)) = pair().await? else {
        return Ok(());
    };
    if !a.is_local() {
        return Ok(());
    }
    // `a` is first to ask, so it owns the tenant.
    let (status, body) = publish(&a, 0).await?;
    ensure!(
        status == StatusCode::CREATED,
        "publish answered {status}: {body}"
    );
    let (status, _) = pull(&b, "waiting").await?;
    ensure!(
        status == StatusCode::SERVICE_UNAVAILABLE,
        "pull answered {status}"
    );

    a.stop_server().await?;
    let stopped = Instant::now();
    let claim = b
        .until(
            "the other server to take the tenant",
            TTL + SLACK,
            || async {
                let (status, body) = pull(&b, "waiting").await?;
                match status {
                    StatusCode::OK => Ok(Some(serde_json::from_value::<Claimed>(body)?)),
                    StatusCode::SERVICE_UNAVAILABLE | StatusCode::NO_CONTENT => Ok(None),
                    other => bail!("work_pull answered {other}: {body}"),
                }
            },
        )
        .await?;
    ensure!(
        stopped.elapsed() <= HANDOVER_MARGIN,
        "taking over after a shutdown took {:?}, and is meant to be at once, not after the ttl",
        stopped.elapsed()
    );
    b.ack_claim(QUEUE, &claim).await?;
    Ok(())
}

// ---- against compose ----------------------------------------------------------

/// The first pass: share the tenant between the two servers, and leave work
/// behind. The job then kills whichever service owns the tenant, named in the
/// state of the pass.
#[tokio::test]
#[ignore = "runs against compose with two servers, and the job kills one between the passes"]
async fn ha_first_pass() -> anyhow::Result<()> {
    let a = Target::live(None).await?;
    let b = a
        .peer()
        .await?
        .context("MANSIO_E2E_URL_B must name the second server")?;
    let left = first_half(&a, &b).await?;
    crate::passes::save(
        "ha",
        &json!({
            "tenant": a.tenant,
            "owner_service": if left.owner_is_a { SERVICE_A } else { SERVICE_B },
            "items": left.items,
            "acked": left.acked,
            "leased": left.leased,
            "agent_id": left.agent_id,
            "agent_token": left.agent_token,
        }),
    )
}

/// The second pass: the owner has been killed. The other server takes the
/// tenant within the ttl and the work is done.
#[tokio::test]
#[ignore = "runs against compose after ha_first_pass and a kill of the owner"]
async fn ha_second_pass() -> anyhow::Result<()> {
    let state = crate::passes::load("ha")?;
    let text = |key: &str| -> anyhow::Result<String> {
        Ok(state[key]
            .as_str()
            .with_context(|| format!("{key} in the state of the first pass"))?
            .to_owned())
    };
    let tenant = text("tenant")?;
    let owner_service = text("owner_service")?;
    // The owner is gone: only the other server is asked.
    let (url_a, url_b) = (
        std::env::var("MANSIO_E2E_URL").context("MANSIO_E2E_URL")?,
        std::env::var("MANSIO_E2E_URL_B").context("MANSIO_E2E_URL_B")?,
    );
    let standby_url = if owner_service == SERVICE_A {
        url_b
    } else {
        url_a
    };
    let standby = Target::live_at(&standby_url, &tenant).await?;
    let items = state["items"]
        .as_array()
        .context("items in the state of the first pass")?
        .iter()
        .map(|id| id.as_str().map(str::to_owned).context("an item id"))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let left = Handover {
        owner_is_a: owner_service == SERVICE_A,
        items,
        acked: text("acked")?,
        leased: text("leased")?,
        agent_id: text("agent_id")?,
        agent_token: text("agent_token")?,
    };
    // The job writes the time of the kill when it has killed the owner.
    let killed_at: u128 = std::env::var("MANSIO_E2E_KILLED_AT_MS")
        .context("MANSIO_E2E_KILLED_AT_MS, the time of the kill in milliseconds since the epoch")?
        .parse()
        .context("MANSIO_E2E_KILLED_AT_MS is not a number")?;
    second_half(&standby, &left, killed_at).await
}
