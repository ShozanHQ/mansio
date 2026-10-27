//! Credentials: a principal becomes a broker user with permissions on exactly
//! its tenant's stream and topics, with an expiry and a revocation.
//!
//! The broker enforces the scopes, so these tests ask it directly as well as
//! through the server: against Iggy in CI, and against the memory broker,
//! which holds users and permissions the same way, in process.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use anyhow::Context;
use mansio_core::{Envelope, EventId, Timestamp};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};

use crate::harness::{Outcome, SLACK, Target, job, millis};

const ONE_MINUTE: Duration = Duration::from_secs(60);

/// Scopes from `(topic pattern, verbs)` pairs.
fn scopes(spec: &[(&str, &[&str])]) -> Value {
    Value::Array(
        spec.iter()
            .map(|(topic, verbs)| json!({"topic": topic, "verbs": verbs}))
            .collect(),
    )
}

fn alert() -> Value {
    json!({"source": "e2e", "type": "disk.full", "data": {"free_gb": 0}})
}

/// `(type, subject, reason)` of what is on `facts.principal`.
fn principal_facts(facts: &[Envelope]) -> Vec<(String, String, Option<String>)> {
    facts
        .iter()
        .map(|fact| {
            (
                fact.event_type.as_str().replace("mansio.principal.", ""),
                fact.subject
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
                fact.data
                    .as_ref()
                    .and_then(|data| data["reason"].as_str())
                    .map(str::to_owned),
            )
        })
        .collect()
}

/// Done criterion 1: an agent credential can publish to its build's topics and
/// claim from its queue and nothing else.
#[tokio::test]
async fn an_agent_credential_publishes_to_its_build_and_claims_from_its_queue_and_nothing_else()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    // The topics are there, so that what is refused is refused to the
    // credential and not for being missing.
    target.seed(&target.tenant, "facts.other").await?;
    target.seed(&target.tenant, "alerts").await?;
    target.seed_queue("builds").await?;
    target.seed_queue("reviews").await?;

    let agent = target
        .issue(
            "agent",
            "builder-1",
            &scopes(&[("facts.build", &["publish"]), ("work.builds", &["claim"])]),
            None,
        )
        .await?;

    // What it was given: publish to its build's topic, claim from its queue,
    // and finish what it claimed. Who it is comes from the credential.
    let (status, published) = agent.publish("facts.build").await?;
    assert_eq!(status, StatusCode::CREATED, "{published}");
    assert_eq!(published["envelope"]["principalkind"], "agent");
    assert_eq!(published["envelope"]["principalname"], "builder-1");
    assert_eq!(published["envelope"]["tenant"], json!(target.tenant));
    let (status, claim) = agent.pull("builds").await?;
    assert_eq!(status, StatusCode::OK, "{claim}");
    assert_eq!(claim["item"]["topic"], "work.builds");
    let (status, extended) = agent.extend("builds", &claim).await?;
    assert_eq!(status, StatusCode::OK, "{extended}");
    assert_eq!(extended["envelope"]["type"], "mansio.work.extended");
    assert_eq!(extended["envelope"]["principalname"], "builder-1");
    let (status, acked) = agent.ack("builds", &claim).await?;
    assert_eq!(status, StatusCode::OK, "{acked}");
    assert_eq!(agent.pull("builds").await?.0, StatusCode::NO_CONTENT);
    // Claiming from a queue reads it.
    assert_eq!(agent.poll("work.builds").await?.0, StatusCode::OK);

    // Nothing else.
    let refused = [
        ("read its build", agent.poll("facts.build").await?),
        ("publish elsewhere", agent.publish("facts.other").await?),
        ("publish an alert", agent.publish("alerts").await?),
        ("publish to its queue", agent.publish("work.builds").await?),
        ("work_publish", agent.work_publish("builds").await?),
        ("claim elsewhere", agent.pull("reviews").await?),
        ("extend elsewhere", agent.extend("reviews", &claim).await?),
        ("read elsewhere", agent.poll("work.reviews").await?),
        ("read facts", agent.poll("facts.work").await?),
        (
            "acknowledge an alert",
            agent.alert_ack(&EventId::generate().to_string()).await?,
        ),
        (
            "work_publish elsewhere",
            agent.work_publish("reviews").await?,
        ),
        (
            "publish in another tenant",
            agent.publish_to("someone-else", "facts.build").await?,
        ),
        ("principals_list", agent.list_principals().await?),
        ("principals_create", agent.create_principal().await?),
        (
            "principals_revoke",
            agent.revoke_principal(&agent.id).await?,
        ),
    ];
    for (what, answer) in &refused {
        assert_eq!(answer.0, StatusCode::FORBIDDEN, "{what}: {}", answer.1);
        assert_eq!(answer.1["error"]["code"], "forbidden", "{what}");
    }
    // Any valid credential may ask what the server runs with.
    assert_eq!(agent.settings().await?.0, StatusCode::OK);
    // A refusal makes nothing: the topic the agent was refused is not there.
    assert_eq!(agent.publish("facts.ghost").await?.0, StatusCode::FORBIDDEN);
    let (status, _) = target
        .authed(
            Method::GET,
            &format!("{}?offset=0", target.messages_uri("facts.ghost")),
            None,
        )
        .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // The refusals changed nothing: one message on its topic, nothing on the
    // others, and the claim, its extension and its ack in the facts under the agent's name.
    assert_eq!(target.read("facts.build").await?.len(), 1);
    assert_eq!(target.read("facts.other").await?.len(), 1);
    assert_eq!(target.read("alerts").await?.len(), 1);
    assert_eq!(target.read("work.reviews").await?.len(), 1);
    let facts = target.read("facts.work").await?;
    assert_eq!(
        facts
            .iter()
            .map(|fact| fact.principal.to_string())
            .collect::<Vec<_>>(),
        ["agent:builder-1", "agent:builder-1", "agent:builder-1"]
    );
    // Identity is the credential's alone: a body cannot say otherwise.
    let uri = target.messages_uri("facts.build");
    let forged = json!({"source": "e2e", "type": "e2e.forged", "principalname": "root"});
    let (status, _) = target
        .call(Method::POST, &uri, Some(&agent.token), Some(&forged))
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    Ok(())
}

/// Done criterion 2: publishing to another tenant's stream is refused by the
/// broker, not by mansio. The test asks the broker with the credential, and
/// fails if it cannot. A credential's user may read what its scopes cover and
/// send to nothing at all: every write goes through the server.
#[tokio::test]
async fn the_broker_refuses_a_credential_that_sends_to_another_tenants_stream() -> anyhow::Result<()>
{
    let target = Target::start().await?;
    let other = format!("{}-other", target.tenant);
    target.seed(&other, "facts.build").await?;
    target.seed(&target.tenant, "facts.build").await?;
    target.seed(&target.tenant, "facts.other").await?;
    target.seed(&target.tenant, "alerts").await?;

    let agent = target
        .issue(
            "agent",
            "builder-1",
            &scopes(&[("facts.build", &["publish", "subscribe"])]),
            None,
        )
        .await?;
    let own = target.tenant.as_str();

    // The credential works at the broker for what it was given, a read of its
    // own topic, so a refusal below is the permission and not a login that
    // failed.
    assert_eq!(
        target.broker_poll(&agent.token, own, "facts.build").await?,
        Outcome::Accepted
    );
    // It may not send to the topic it may publish through the server to, to
    // another tenant's stream, to a topic of its own tenant it was not given,
    // or to a topic of another kind.
    for (tenant, topic) in [
        (own, "facts.build"),
        (other.as_str(), "facts.build"),
        (own, "facts.other"),
        (own, "alerts"),
    ] {
        assert_eq!(
            target.broker_send(&agent.token, tenant, topic).await?,
            Outcome::Forbidden,
            "send to {tenant} {topic}"
        );
    }
    // And it may not read what it was not given.
    for (tenant, topic) in [
        (other.as_str(), "facts.build"),
        (own, "facts.other"),
        (own, "alerts"),
    ] {
        assert_eq!(
            target.broker_poll(&agent.token, tenant, topic).await?,
            Outcome::Forbidden,
            "poll {tenant} {topic}"
        );
    }
    // Nothing the broker refused is there.
    assert_eq!(target.read("facts.build").await?.len(), 1);
    assert_eq!(target.read("facts.other").await?.len(), 1);
    assert_eq!(target.read("alerts").await?.len(), 1);
    assert_eq!(
        target
            .authed(
                Method::GET,
                &format!(
                    "{}/v1/tenants/{other}/topics/facts.build/messages?offset=0",
                    target.base
                ),
                None
            )
            .await?
            .1["messages"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );

    // The registry of principals is where no credential reaches.
    if let Some(outcome) = target.broker_read_registry(&agent.token).await? {
        assert_eq!(outcome, Outcome::Forbidden);
    }
    // And a token that was never issued signs in nowhere.
    assert_eq!(
        target.broker_login("never-issued").await?,
        Outcome::Unauthenticated
    );
    Ok(())
}

/// Done criterion 3: a person credential can acknowledge alerts and claim
/// approval items; a connector credential can publish to `facts.signal` only.
#[tokio::test]
async fn a_person_acknowledges_alerts_and_claims_approvals_and_a_connector_publishes_signals_only()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let first = target
        .alert_publish(&alert())
        .await?
        .envelope
        .id
        .to_string();
    let second = target
        .alert_publish(&alert())
        .await?
        .envelope
        .id
        .to_string();
    target.work_publish("approvals", &job(1)).await?;
    target.seed(&target.tenant, "facts.signal").await?;
    target.seed(&target.tenant, "facts.other").await?;
    target.seed_queue("builds").await?;

    let person = target
        .issue(
            "person",
            "paul",
            &scopes(&[
                ("alerts", &["subscribe", "ack"]),
                ("work.approvals", &["approve"]),
            ]),
            None,
        )
        .await?;
    // A person's credential does not expire unless it is asked to.
    assert!(
        person.principal.get("expires_at").is_none(),
        "{}",
        person.principal
    );

    // Acknowledge: the alert is read, acknowledged once, and the ack says by
    // whom.
    let (status, polled) = person.poll("alerts").await?;
    assert_eq!(status, StatusCode::OK, "{polled}");
    assert_eq!(polled["messages"].as_array().map(Vec::len), Some(2));
    let (status, acked) = person.alert_ack(&first).await?;
    assert_eq!(status, StatusCode::OK, "{acked}");
    assert_eq!(acked["envelope"]["type"], "mansio.alert.acknowledged");
    assert_eq!(acked["envelope"]["principalkind"], "person");
    assert_eq!(acked["envelope"]["principalname"], "paul");
    assert_eq!(
        person.alert_ack(&first).await?.0,
        StatusCode::CONFLICT,
        "once"
    );

    // Claim an approval, and finish it.
    let (status, claim) = person.pull("approvals").await?;
    assert_eq!(status, StatusCode::OK, "{claim}");
    assert_eq!(person.ack("approvals", &claim).await?.0, StatusCode::OK);
    assert_eq!(person.pull("approvals").await?.0, StatusCode::NO_CONTENT);

    // Nothing else.
    for (what, answer) in [
        ("publish an alert", person.alert_publish().await?),
        ("publish a signal", person.publish("facts.signal").await?),
        ("claim a build", person.pull("builds").await?),
        (
            "publish to approvals",
            person.work_publish("approvals").await?,
        ),
        ("read facts", person.poll("facts.other").await?),
    ] {
        assert_eq!(answer.0, StatusCode::FORBIDDEN, "{what}: {}", answer.1);
    }

    // Reading alerts is not acknowledging them.
    let viewer = target
        .issue(
            "person",
            "viewer",
            &scopes(&[("alerts", &["subscribe"])]),
            None,
        )
        .await?;
    assert_eq!(viewer.poll("alerts").await?.0, StatusCode::OK);
    assert_eq!(viewer.alert_ack(&second).await?.0, StatusCode::FORBIDDEN);
    // Acknowledging does not need reading: the id is enough.
    let clerk = target
        .issue("person", "clerk", &scopes(&[("alerts", &["ack"])]), None)
        .await?;
    assert_eq!(clerk.poll("alerts").await?.0, StatusCode::FORBIDDEN);
    assert_eq!(clerk.alert_ack(&second).await?.0, StatusCode::OK);

    // A connector publishes to facts.signal and to nothing else.
    let connector = target
        .issue(
            "connector",
            "github",
            &scopes(&[("facts.signal", &["publish"])]),
            None,
        )
        .await?;
    let (status, signal) = connector.publish("facts.signal").await?;
    assert_eq!(status, StatusCode::CREATED, "{signal}");
    assert_eq!(signal["envelope"]["principalkind"], "connector");
    assert_eq!(signal["envelope"]["principalname"], "github");
    for (what, answer) in [
        ("publish elsewhere", connector.publish("facts.other").await?),
        ("publish an alert", connector.publish("alerts").await?),
        ("alert", connector.alert_publish().await?),
        ("read its own topic", connector.poll("facts.signal").await?),
        ("claim approvals", connector.pull("approvals").await?),
        ("claim builds", connector.pull("builds").await?),
        ("work_publish", connector.work_publish("builds").await?),
        ("acknowledge", connector.alert_ack(&first).await?),
    ] {
        assert_eq!(answer.0, StatusCode::FORBIDDEN, "{what}: {}", answer.1);
    }
    // And at the broker: it publishes through the server and cannot read, send
    // or do anything else there. The person reads the alerts and nothing it
    // may not.
    for topic in ["facts.signal", "facts.other", "alerts"] {
        for outcome in [
            target
                .broker_send(&connector.token, &target.tenant, topic)
                .await?,
            target
                .broker_poll(&connector.token, &target.tenant, topic)
                .await?,
            target
                .broker_send(&person.token, &target.tenant, topic)
                .await?,
        ] {
            assert_eq!(outcome, Outcome::Forbidden, "{topic}");
        }
    }
    assert_eq!(
        target
            .broker_poll(&person.token, &target.tenant, "alerts")
            .await?,
        Outcome::Accepted
    );
    assert_eq!(
        target
            .broker_poll(&person.token, &target.tenant, "facts.other")
            .await?,
        Outcome::Forbidden
    );
    // The seed and the connector's publish.
    assert_eq!(target.read("facts.signal").await?.len(), 2);
    let types: Vec<String> = target
        .alert_transitions()
        .await?
        .into_iter()
        .filter(|kind| kind == "acknowledged")
        .collect();
    assert_eq!(types.len(), 2, "the person's and the clerk's");
    Ok(())
}

/// Done criterion 4: a revocation takes effect within one minute, at the
/// server and at the broker.
#[tokio::test]
async fn a_revoked_credential_is_refused_within_a_minute_and_the_broker_forgets_it()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    target.seed(&target.tenant, "facts.build").await?;
    let agent = target
        .issue(
            "agent",
            "builder-1",
            &scopes(&[("facts.build", &["publish"])]),
            None,
        )
        .await?;
    assert_eq!(agent.publish("facts.build").await?.0, StatusCode::CREATED);
    assert_eq!(target.broker_login(&agent.token).await?, Outcome::Accepted);
    assert_eq!(target.principals().await?.len(), 1);

    let (status, revoked) = target.revoke(&agent.id).await?;
    assert_eq!(status, StatusCode::OK, "{revoked}");
    assert_eq!(revoked["id"], json!(agent.id));
    assert_eq!(revoked["name"], "builder-1");
    let revoked_at = Instant::now();

    let wait =
        (Duration::from_secs(target.settings.credential_recheck_seconds) + SLACK).min(ONE_MINUTE);
    target
        .until("the revoked credential to be refused", wait, || async {
            Ok((agent.publish("facts.build").await?.0 == StatusCode::UNAUTHORIZED).then_some(()))
        })
        .await?;
    assert!(revoked_at.elapsed() < ONE_MINUTE);
    let (status, body) = agent.poll("facts.build").await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["error"]["code"], "unauthorized");

    // The broker deleted the user, so the credential signs in nowhere.
    assert_eq!(
        target.broker_login(&agent.token).await?,
        Outcome::Unauthenticated
    );
    assert_eq!(
        target
            .broker_send(&agent.token, &target.tenant, "facts.build")
            .await?,
        Outcome::Unauthenticated
    );
    assert!(target.principals().await?.is_empty());
    assert_eq!(target.revoke(&agent.id).await?.0, StatusCode::NOT_FOUND);

    // Both changes are on facts.principal.
    let facts = target.read("facts.principal").await?;
    assert_eq!(
        principal_facts(&facts),
        [
            ("created".to_owned(), agent.id.clone(), None),
            (
                "revoked".to_owned(),
                agent.id.clone(),
                Some("requested".to_owned())
            ),
        ]
    );
    assert_eq!(facts[1].principal.to_string(), "service:mansio");
    Ok(())
}

/// Done criterion 4: an expired credential is refused, and a sweep removes it.
#[tokio::test]
async fn an_expired_credential_is_refused_and_the_sweep_revokes_it() -> anyhow::Result<()> {
    let target = Target::start().await?;
    target.seed(&target.tenant, "facts.build").await?;
    let ttl = target.settings.agent_credential_ttl_seconds;
    // No ttl in the request: an agent gets the setting.
    let agent = target
        .issue_with(
            "agent",
            "builder-1",
            &scopes(&[("facts.build", &["publish"])]),
            None,
        )
        .await?;
    let created: Timestamp = agent.principal["created_at"]
        .as_str()
        .context("created_at")?
        .parse()?;
    let expires: Timestamp = agent.principal["expires_at"]
        .as_str()
        .context("an agent credential has an expiry")?
        .parse()?;
    assert_eq!(
        millis(&expires)? - millis(&created)?,
        i64::try_from(ttl * 1000)?
    );

    // Before the expiry it works.
    assert_eq!(agent.publish("facts.build").await?.0, StatusCode::CREATED);
    assert_eq!(target.broker_login(&agent.token).await?, Outcome::Accepted);

    // After it, the server refuses it.
    let until_expiry = millis(&expires)? - now_millis()?;
    tokio::time::sleep(Duration::from_millis(
        u64::try_from(until_expiry.max(0))? + 100,
    ))
    .await;
    let (status, body) = agent.publish("facts.build").await?;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(agent.poll("facts.build").await?.0, StatusCode::UNAUTHORIZED);
    // And so does the broker.
    target
        .until(
            "the broker to refuse the expired credential",
            SLACK,
            || async {
                Ok(
                    (target.broker_login(&agent.token).await? == Outcome::Unauthenticated)
                        .then_some(()),
                )
            },
        )
        .await?;
    assert_eq!(
        target
            .broker_send(&agent.token, &target.tenant, "facts.build")
            .await?,
        Outcome::Unauthenticated
    );

    // The sweep revokes it, and says why.
    let sweep = Duration::from_secs(target.settings.credential_sweep_seconds);
    target
        .until(
            "the sweep to revoke the credential",
            sweep * 2 + SLACK,
            || async { Ok(target.principals().await?.is_empty().then_some(())) },
        )
        .await?;
    let facts = target.read("facts.principal").await?;
    assert_eq!(
        principal_facts(&facts),
        [
            ("created".to_owned(), agent.id.clone(), None),
            (
                "revoked".to_owned(),
                agent.id.clone(),
                Some("expired".to_owned())
            ),
        ]
    );
    assert_eq!(facts[1].principal.to_string(), "service:mansio-engine");

    // A credential that is asked to expire does, whatever its kind.
    let person = target
        .issue_with(
            "person",
            "paul",
            &scopes(&[("alerts", &["subscribe"])]),
            Some(600),
        )
        .await?;
    assert!(person.principal["expires_at"].is_string());
    Ok(())
}

/// Only the admin credential may use the admin API, and a request that is not
/// well formed is refused before anything is made.
#[tokio::test]
async fn the_admin_api_is_the_admins_and_refuses_what_is_not_well_formed() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let create = format!("{}/v1/principals", target.base);
    let body = json!({
        "kind": "agent", "name": "builder-1", "tenant": target.tenant,
        "scopes": scopes(&[("facts.build", &["publish"])])
    });

    // No credential, or one that is not valid: 401. A principal's: 403.
    for token in [None, Some("not-a-credential"), Some("")] {
        let (status, answer) = target
            .call(Method::POST, &create, token, Some(&body))
            .await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{token:?}: {answer}");
        let (status, _) = target.call(Method::GET, &create, token, None).await?;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{token:?}");
    }
    let person = target
        .issue(
            "person",
            "paul",
            &scopes(&[("alerts", &["subscribe", "ack"])]),
            None,
        )
        .await?;
    let others = target
        .issue(
            "person",
            "ana",
            &scopes(&[("alerts", &["subscribe"])]),
            None,
        )
        .await?;
    assert_eq!(person.list_principals().await?.0, StatusCode::FORBIDDEN);
    assert_eq!(person.create_principal().await?.0, StatusCode::FORBIDDEN);
    assert_eq!(
        person.revoke_principal(&others.id).await?.0,
        StatusCode::FORBIDDEN
    );
    // Neither the create nor the revoke did anything.
    assert_eq!(target.principals().await?.len(), 2);
    assert_eq!(others.alert_publish().await?.0, StatusCode::FORBIDDEN);
    assert_eq!(others.poll("alerts").await?.0, StatusCode::NOT_FOUND);

    // Not well formed: 400, and nothing is issued.
    let mut bad: Vec<(&str, Value)> = Vec::new();
    let with = |key: &str, value: Value| {
        let mut changed = body.clone();
        changed[key] = value;
        changed
    };
    let without = |key: &str| {
        let mut changed = body.clone();
        if let Some(object) = changed.as_object_mut() {
            object.remove(key);
        }
        changed
    };
    bad.push(("no scopes", with("scopes", json!([]))));
    bad.push(("scopes missing", without("scopes")));
    bad.push(("an unknown field", with("role", json!("admin"))));
    bad.push((
        "claim on facts",
        with("scopes", scopes(&[("facts.build", &["claim"])])),
    ));
    bad.push((
        "approve on a queue",
        with("scopes", scopes(&[("work.builds", &["approve"])])),
    ));
    bad.push((
        "ack on facts",
        with("scopes", scopes(&[("facts.build", &["ack"])])),
    ));
    bad.push(("no verbs", with("scopes", scopes(&[("facts.build", &[])]))));
    bad.push((
        "an unknown verb",
        with("scopes", scopes(&[("facts.build", &["root"])])),
    ));
    bad.push((
        "the whole stream",
        with("scopes", scopes(&[("*", &["publish"])])),
    ));
    bad.push((
        "a bad pattern",
        with("scopes", scopes(&[("facts.b*", &["publish"])])),
    ));
    bad.push(("a bad kind", with("kind", json!("Agent"))));
    bad.push(("a bad name", with("name", json!("-x"))));
    bad.push(("a bad tenant", with("tenant", json!("ACME"))));
    bad.push(("no ttl at all", with("ttl_seconds", json!(0))));
    bad.push(("a ttl too far off", with("ttl_seconds", json!(u64::MAX))));
    for name in ["mansio", "mansio-engine"] {
        let mut taken = with("kind", json!("service"));
        taken["name"] = json!(name);
        bad.push(("a name mansio uses itself", taken));
    }
    for (what, request) in bad {
        let (status, answer) = target.create_principal(&request).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{what}: {answer}");
        assert_eq!(answer["error"]["code"], "bad_request", "{what}");
    }
    let (status, _) = target
        .call(Method::POST, &create, Some(&target.token), None)
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST, "no body");

    // A name is one principal's within its kind and tenant.
    let (status, issued) = target.create_principal(&body).await?;
    assert_eq!(status, StatusCode::CREATED, "{issued}");
    let (status, again) = target.create_principal(&body).await?;
    assert_eq!(status, StatusCode::CONFLICT, "{again}");
    assert_eq!(again["error"]["code"], "conflict");
    let elsewhere = with("tenant", json!(format!("{}-elsewhere", target.tenant)));
    assert_eq!(
        target.create_principal(&elsewhere).await?.0,
        StatusCode::CREATED
    );

    // The list is the tenant's, without any credential in it.
    let listed = target.principals().await?;
    assert_eq!(listed.len(), 3);
    let uri = format!("{}/v1/principals", target.base);
    let (status, all) = target.authed(Method::GET, &uri, None).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(all["principals"].as_array().map_or(0, Vec::len) >= 4);
    assert!(!listed.iter().any(|p| p.get("credential").is_some()));
    let token = issued["credential"].as_str().context("a credential")?;
    assert!(!serde_json::to_string(&listed)?.contains(token));
    let (status, _) = target
        .authed(Method::GET, &format!("{uri}?tenant=ACME"), None)
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = target
        .authed(Method::GET, &format!("{uri}?other=1"), None)
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = target.revoke("nope").await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = target.revoke(&EventId::generate().to_string()).await?;
    assert_eq!(status, StatusCode::NOT_FOUND, "well formed, and nobody's");
    Ok(())
}

/// A topic that is made after the credential is open to it when a scope
/// matches, and to nobody else.
#[tokio::test]
async fn a_topic_made_after_the_credential_is_open_to_it_when_a_scope_matches() -> anyhow::Result<()>
{
    let target = Target::start().await?;
    let agent = target
        .issue(
            "agent",
            "builder-1",
            &scopes(&[
                ("facts.*", &["publish", "subscribe"]),
                ("work.*", &["publish", "claim"]),
            ]),
            None,
        )
        .await?;
    let narrow = target
        .issue(
            "agent",
            "narrow",
            &scopes(&[("facts.build", &["publish"])]),
            None,
        )
        .await?;

    // Neither topic exists. The agent makes the first by publishing to it, and
    // can read it at once; the admin makes the second.
    let (status, body) = agent.publish("facts.later").await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(agent.poll("facts.later").await?.0, StatusCode::OK);
    target.seed_queue("later").await?;
    let (status, body) = agent.work_publish("later").await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(agent.pull("later").await?.0, StatusCode::OK);

    // At the broker as well: the agent may read the topic made after it, and
    // the one whose scope does not cover it may not.
    assert_eq!(
        target
            .broker_poll(&agent.token, &target.tenant, "facts.later")
            .await?,
        Outcome::Accepted
    );
    assert_eq!(
        target
            .broker_poll(&narrow.token, &target.tenant, "facts.later")
            .await?,
        Outcome::Forbidden
    );
    assert_eq!(
        target
            .broker_send(&agent.token, &target.tenant, "facts.later")
            .await?,
        Outcome::Forbidden
    );
    assert_eq!(
        narrow.publish("facts.later").await?.0,
        StatusCode::FORBIDDEN
    );
    Ok(())
}

/// What only mansio writes stays only mansio's: a credential that may publish
/// to every `facts.*` topic still cannot write a transition, a dead letter or a
/// credential fact, and so cannot make a work item vanish or run twice.
#[tokio::test]
async fn a_facts_star_credential_cannot_write_what_only_mansio_writes() -> anyhow::Result<()> {
    let mut target = Target::start().await?;
    target.seed(&target.tenant, "facts.build").await?;
    // Make the topics mansio writes, so that what is refused is refused for
    // the credential and not for being missing: a claim and its ack make
    // facts.work, and an acknowledged alert facts.alert.
    target.seed_queue("builds").await?;
    let claim = target.claim("builds", "worker", SLACK).await?;
    target.ack_claim("builds", &claim).await?;
    let alert = target.alert_publish(&alert()).await?;
    let (status, _) = target.alert_ack(&alert.envelope.id.to_string()).await?;
    assert_eq!(status, StatusCode::OK);
    // The item the forger would like to make vanish.
    target.seed_queue("builds").await?;
    let forger = target
        .issue(
            "agent",
            "forger",
            &scopes(&[("facts.*", &["publish", "subscribe"])]),
            None,
        )
        .await?;

    // What it may write is written, and what mansio writes is refused.
    let (status, body) = forger.publish("facts.build").await?;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    for topic in ["facts.work", "facts.alert", "facts.principal", "dead"] {
        let (status, body) = forger.publish(topic).await?;
        assert_eq!(status, StatusCode::FORBIDDEN, "{topic}: {body}");
        // Reading the facts is another matter; `dead` is not under `facts.*`.
        let (status, _) = forger.poll(topic).await?;
        if topic == "dead" {
            assert_eq!(status, StatusCode::FORBIDDEN, "{topic}");
        } else {
            assert_ne!(status, StatusCode::FORBIDDEN, "{topic} is readable");
        }
    }
    // Not at the broker either: a credential writes nothing there.
    for topic in [
        "facts.work",
        "facts.alert",
        "facts.principal",
        "facts.build",
    ] {
        assert_eq!(
            target
                .broker_send(&forger.token, &target.tenant, topic)
                .await?,
            Outcome::Forbidden,
            "{topic}"
        );
    }
    // The admin's generic route refuses them too.
    for topic in ["facts.work", "facts.alert", "facts.principal", "dead"] {
        let (status, _) = target
            .authed(
                Method::POST,
                &target.messages_uri(topic),
                Some(&json!({"source": "e2e", "type": "mansio.work.acked"})),
            )
            .await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{topic}");
    }
    assert!(target.read("dead").await?.is_empty());
    assert_eq!(target.work_transitions().await?, ["claimed", "acked"]);
    assert_eq!(target.alert_transitions().await?, ["acknowledged"]);

    // The item is still there to claim, after a restart too.
    if target.is_local() {
        target.stop_server().await?;
        target.start_server().await?;
    }
    let claim = target.claim("builds", "worker", SLACK).await?;
    target.ack_claim("builds", &claim).await?;
    assert_eq!(
        target.work_transitions().await?,
        ["claimed", "acked", "claimed", "acked"]
    );
    Ok(())
}

/// Iggy checks a token when it signs in and not after, so a session opened
/// before a revocation is a session an Iggy user holds: the revocation deletes
/// the user, which takes away what it may read, and any token it made for
/// itself goes with it.
#[tokio::test]
async fn a_session_opened_before_a_revocation_is_refused_after_it() -> anyhow::Result<()> {
    let target = Target::start().await?;
    target.seed(&target.tenant, "facts.build").await?;
    let agent = target
        .issue(
            "agent",
            "builder-1",
            &scopes(&[("facts.build", &["subscribe"])]),
            None,
        )
        .await?;
    let tenant = target.tenant.as_str();
    let session = target
        .broker_session(&agent.token)
        .await?
        .context("the credential signs in")?;
    assert_eq!(
        session.poll(tenant, "facts.build").await?,
        Outcome::Accepted
    );
    // A user signed in can make tokens for itself, with any expiry.
    let own = session.mint_token().await;

    let (status, _) = target.revoke(&agent.id).await?;
    assert_eq!(status, StatusCode::OK);

    // The session that was open is refused: the user is gone.
    let outcome = session.poll(tenant, "facts.build").await?;
    assert!(
        matches!(outcome, Outcome::Forbidden | Outcome::Unauthenticated),
        "{outcome:?}"
    );
    // So is everything made for that user.
    if let Some(token) = own {
        assert_eq!(target.broker_login(&token).await?, Outcome::Unauthenticated);
    }
    session.close().await;
    Ok(())
}

/// A session opened before a credential expires reads until the sweep deletes
/// the user: the bound is the sweep interval, and only while mansio-server
/// runs. The server itself refuses the credential at the expiry.
#[tokio::test]
async fn a_session_opened_before_an_expiry_is_ended_by_the_sweep() -> anyhow::Result<()> {
    let target = Target::start().await?;
    target.seed(&target.tenant, "facts.build").await?;
    let agent = target
        .issue_with(
            "agent",
            "builder-1",
            &scopes(&[("facts.build", &["subscribe"])]),
            None,
        )
        .await?;
    let tenant = target.tenant.as_str();
    let session = target
        .broker_session(&agent.token)
        .await?
        .context("the credential signs in")?;
    assert_eq!(
        session.poll(tenant, "facts.build").await?,
        Outcome::Accepted
    );

    let sweep = Duration::from_secs(target.settings.credential_sweep_seconds);
    let ttl = Duration::from_secs(target.settings.agent_credential_ttl_seconds);
    target
        .until(
            "the sweep to end the session",
            ttl + sweep * 2 + SLACK,
            || async {
                let outcome = session.poll(tenant, "facts.build").await?;
                Ok(matches!(outcome, Outcome::Forbidden | Outcome::Unauthenticated).then_some(()))
            },
        )
        .await?;
    session.close().await;
    Ok(())
}

/// A read does not make a stream or a topic: a topic nobody has published to
/// is not found, whoever asks, and stays not found.
#[tokio::test]
async fn reading_a_topic_nobody_published_to_is_a_404_and_makes_nothing() -> anyhow::Result<()> {
    let target = Target::start().await?;
    let reader = target
        .issue(
            "agent",
            "reader",
            &scopes(&[("facts.*", &["subscribe"])]),
            None,
        )
        .await?;
    for _ in 0..2 {
        let (status, body) = reader.poll("facts.nothing").await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        let uri = format!("{}?offset=0", target.messages_uri("facts.nothing"));
        let (status, body) = target.authed(Method::GET, &uri, None).await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        assert_eq!(body["error"]["code"], "not_found");
    }
    // A pull from a queue that is not there has nothing to claim.
    let worker = target
        .issue(
            "agent",
            "worker",
            &scopes(&[("work.nothing", &["claim"])]),
            None,
        )
        .await?;
    assert_eq!(worker.pull("nothing").await?.0, StatusCode::NO_CONTENT);
    // Still not there.
    let uri = format!("{}?offset=0", target.messages_uri("work.nothing"));
    assert_eq!(
        target.authed(Method::GET, &uri, None).await?.0,
        StatusCode::NOT_FOUND
    );
    Ok(())
}

/// What mansio records about a credential holds no secret, and the sink saw it
/// first.
#[tokio::test]
async fn the_facts_about_a_credential_name_no_secret_and_the_sink_has_them_first()
-> anyhow::Result<()> {
    let target = Target::start().await?;
    let agent = target
        .issue(
            "agent",
            "builder-1",
            &scopes(&[("facts.build", &["publish"]), ("work.builds", &["claim"])]),
            None,
        )
        .await?;
    target.revoke(&agent.id).await?;

    let facts = target.read("facts.principal").await?;
    assert_eq!(facts.len(), 2);
    let created = &facts[0];
    assert_eq!(created.event_type.as_str(), "mansio.principal.created");
    assert_eq!(created.principal.to_string(), "service:mansio");
    assert_eq!(created.tenant.as_str(), target.tenant);
    let data = created.data.as_ref().context("data")?;
    assert_eq!(data["principal"]["id"], json!(agent.id));
    assert_eq!(data["principal"]["kind"], "agent");
    assert_eq!(data["principal"]["name"], "builder-1");
    assert_eq!(data["principal"]["scopes"], agent.principal["scopes"]);
    assert!(data["principal"]["expires_at"].is_string());

    // The credential is in the answer that issued it and nowhere else.
    for text in [
        serde_json::to_string(&facts)?,
        serde_json::to_string(&target.principals().await?)?,
    ] {
        assert!(!text.contains(&agent.token));
    }
    // Neither does the credential reach the generic route's refusal of the
    // topic: it is written by mansio alone.
    let (status, _) = target
        .authed(
            Method::POST,
            &target.messages_uri("facts.principal"),
            Some(&json!({"source": "e2e", "type": "mansio.principal.created"})),
        )
        .await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    if let Some(records) = target.sink_records()? {
        let recorded: Vec<&Envelope> = records
            .iter()
            .filter(|record| record.topic.to_string() == "facts.principal")
            .collect();
        assert_eq!(
            recorded.iter().map(|r| r.id).collect::<Vec<_>>(),
            facts.iter().map(|f| f.id).collect::<Vec<_>>(),
            "the sink holds the same facts in the same order"
        );
        assert!(!serde_json::to_string(&records)?.contains(&agent.token));
    }
    Ok(())
}

/// The registry of principals is kept in the broker, so a restarted server
/// knows who has a credential and who does not. Only the in-process server can
/// be restarted from here.
#[tokio::test]
async fn a_restarted_server_knows_who_has_a_credential() -> anyhow::Result<()> {
    let mut target = Target::start().await?;
    if !target.is_local() {
        return Ok(());
    }
    target.seed(&target.tenant, "facts.build").await?;
    let kept = target
        .issue(
            "agent",
            "kept",
            &scopes(&[("facts.build", &["publish"])]),
            None,
        )
        .await?;
    let dropped = target
        .issue(
            "agent",
            "dropped",
            &scopes(&[("facts.build", &["publish"])]),
            None,
        )
        .await?;
    target.revoke(&dropped.id).await?;

    target.stop_server().await?;
    target.start_server().await?;
    let (kept, dropped) = (kept.at(&target), dropped.at(&target));

    assert_eq!(kept.publish("facts.build").await?.0, StatusCode::CREATED);
    assert_eq!(
        dropped.publish("facts.build").await?.0,
        StatusCode::UNAUTHORIZED
    );
    let listed = target.principals().await?;
    assert_eq!(
        listed.iter().map(|p| p["id"].clone()).collect::<Vec<_>>(),
        [json!(kept.id)]
    );
    // It is still the same name, so it is still taken; the revoked one is free.
    let (status, _) = target
        .create_principal(&json!({
            "kind": "agent", "name": "kept", "tenant": target.tenant,
            "scopes": scopes(&[("facts.build", &["publish"])]),
        }))
        .await?;
    assert_eq!(status, StatusCode::CONFLICT);
    target
        .issue(
            "agent",
            "dropped",
            &scopes(&[("facts.build", &["publish"])]),
            None,
        )
        .await?;
    // One of each in the facts, and each exactly once.
    let facts = principal_facts(&target.read("facts.principal").await?);
    let kinds: HashSet<&str> = facts.iter().map(|(kind, _, _)| kind.as_str()).collect();
    assert_eq!(kinds, HashSet::from(["created", "revoked"]));
    assert_eq!(
        facts
            .iter()
            .filter(|(kind, _, _)| kind == "revoked")
            .count(),
        1
    );
    assert_eq!(
        facts
            .iter()
            .filter(|(kind, _, _)| kind == "created")
            .count(),
        3
    );
    assert_eq!(target.revoke(&dropped.id).await?.0, StatusCode::NOT_FOUND);
    Ok(())
}

fn now_millis() -> anyhow::Result<i64> {
    let since = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
    Ok(i64::try_from(since.as_millis())?)
}
