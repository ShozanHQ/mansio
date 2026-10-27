//! The operations of the bus: one description of each, which the command line
//! and the MCP server both read, and one implementation of each, which both
//! call. They cannot drift apart, because there is only one of each.
//!
//! An operation is named as its tool is, `work_pull`, and as its command is,
//! `work pull`. Its parameters are named as its flags are, with `_` for `-`:
//! `--lease-id` is `lease_id`. What an operation prints is JSON, one object
//! per line.

use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use mansio_core::api::{CreatePrincipal, CreateWebhook};
use mansio_core::{
    CausationId, ConsumerName, CorrelationId, EventId, EventType, Grant, IdempotencyKey,
    PrincipalId, PrincipalKind, PrincipalName, Segment, Source, Subject, TenantId, Topic,
    WebhookId,
};
use reqwest::Method;
use serde_json::{Map, Value, json};

use crate::client::{Client, Replay};

/// What an operation does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// Publish an envelope to a topic.
    Publish,
    /// Read the envelopes in a topic.
    Poll,
    /// Follow a topic, resuming from a durable consumer.
    Watch,
    /// Publish a work item.
    WorkPublish,
    /// Claim the oldest item of a queue.
    WorkPull,
    /// Move the end of a claimed item's lease later.
    WorkExtend,
    /// Complete a claimed item.
    WorkAck,
    /// Give a claimed item back as failed.
    WorkFail,
    /// Publish an alert.
    AlertsPublish,
    /// Acknowledge an alert.
    AlertsAck,
    /// Issue a credential.
    PrincipalsCreate,
    /// List the principals that have a credential.
    PrincipalsList,
    /// Withdraw a credential.
    PrincipalsRevoke,
    /// Register a webhook.
    WebhooksCreate,
    /// List the webhooks.
    WebhooksList,
    /// Delete a webhook.
    WebhooksDelete,
    /// Show where a named consumer stands.
    ConsumersGet,
    /// Move a named consumer.
    ConsumersCommit,
}

/// What kind of value a parameter takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Text.
    Text,
    /// A whole number, zero or more.
    Int,
    /// Any JSON value. On the command line it is written as JSON.
    Json,
    /// The scopes of a principal: a list of `{"topic", "verbs"}`. On the
    /// command line it is written as JSON.
    Scopes,
}

/// One parameter of an operation: a flag on the command line and a property
/// of the tool.
#[derive(Debug, Clone, Copy)]
pub struct Param {
    /// The name, with `_`. The flag is the name with `-`.
    pub name: &'static str,
    /// What it takes.
    pub kind: Kind,
    /// Whether it must be given.
    pub required: bool,
    /// What it is for.
    pub help: &'static str,
}

impl Param {
    /// The flag, without the dashes.
    pub fn flag(&self) -> String {
        self.name.replace('_', "-")
    }
}

const fn required(name: &'static str, kind: Kind, help: &'static str) -> Param {
    Param {
        name,
        kind,
        required: true,
        help,
    }
}

const fn optional(name: &'static str, kind: Kind, help: &'static str) -> Param {
    Param {
        name,
        kind,
        required: false,
        help,
    }
}

const TENANT: Param = required("tenant", Kind::Text, "The tenant.");
const TOPIC: Param = required(
    "topic",
    Kind::Text,
    "The topic: facts.<kind>, work.<queue>, alerts, presence or dead.",
);
const QUEUE: Param = required(
    "queue",
    Kind::Text,
    "The work queue: the <queue> of work.<queue>.",
);
const CONSUMER_PULL: Param = required(
    "consumer",
    Kind::Text,
    "The name this consumer pulls under. A consumer that holds a live lease is given it again.",
);
const CONSUMER_HELD: Param = required(
    "consumer",
    Kind::Text,
    "The consumer name the lease was taken under: the consumer of the work_pull answer.",
);
const CONSUMER_NAMED: Param = required(
    "consumer",
    Kind::Text,
    "The consumer's name. The broker keeps its offset.",
);
const ENVELOPE: [Param; 8] = [
    required("type", Kind::Text, "What happened (CloudEvents type)."),
    required(
        "source",
        Kind::Text,
        "Where it happened (CloudEvents source).",
    ),
    optional("subject", Kind::Text, "The subject within the source."),
    optional("data", Kind::Json, "The payload, as JSON."),
    optional(
        "correlation_id",
        Kind::Text,
        "The chain of work this belongs to.",
    ),
    optional(
        "causation_id",
        Kind::Text,
        "The event that caused this one.",
    ),
    optional(
        "idempotency_key",
        Kind::Text,
        "A key consumers can deduplicate on.",
    ),
    optional(
        "id",
        Kind::Text,
        "The id to give it, a lowercase UUID. Sending the same id again is how a lost answer is safely retried.",
    ),
];

impl Op {
    /// Every operation, in the order they are listed.
    pub const ALL: [Op; 18] = [
        Op::Publish,
        Op::Poll,
        Op::Watch,
        Op::WorkPublish,
        Op::WorkPull,
        Op::WorkExtend,
        Op::WorkAck,
        Op::WorkFail,
        Op::AlertsPublish,
        Op::AlertsAck,
        Op::PrincipalsCreate,
        Op::PrincipalsList,
        Op::PrincipalsRevoke,
        Op::WebhooksCreate,
        Op::WebhooksList,
        Op::WebhooksDelete,
        Op::ConsumersGet,
        Op::ConsumersCommit,
    ];

    /// The words of the command: `["work", "pull"]`.
    pub fn path(self) -> &'static [&'static str] {
        match self {
            Op::Publish => &["publish"],
            Op::Poll => &["poll"],
            Op::Watch => &["watch"],
            Op::WorkPublish => &["work", "publish"],
            Op::WorkPull => &["work", "pull"],
            Op::WorkExtend => &["work", "extend"],
            Op::WorkAck => &["work", "ack"],
            Op::WorkFail => &["work", "fail"],
            Op::AlertsPublish => &["alerts", "publish"],
            Op::AlertsAck => &["alerts", "ack"],
            Op::PrincipalsCreate => &["principals", "create"],
            Op::PrincipalsList => &["principals", "list"],
            Op::PrincipalsRevoke => &["principals", "revoke"],
            Op::WebhooksCreate => &["webhooks", "create"],
            Op::WebhooksList => &["webhooks", "list"],
            Op::WebhooksDelete => &["webhooks", "delete"],
            Op::ConsumersGet => &["consumers", "get"],
            Op::ConsumersCommit => &["consumers", "commit"],
        }
    }

    /// The operation's name, as its tool is called: `work_pull`.
    pub fn name(self) -> String {
        self.path().join("_")
    }

    /// The operation whose command is `path`.
    pub fn from_path(path: &[&str]) -> Option<Op> {
        Op::ALL.into_iter().find(|op| op.path() == path)
    }

    /// Whether the MCP server offers it as a tool. `watch` streams, which a
    /// tool call cannot.
    pub fn is_tool(self) -> bool {
        self != Op::Watch
    }

    /// What the operation does, in a sentence.
    pub fn about(self) -> &'static str {
        match self {
            Op::Publish => "Publish an envelope to a topic and print it as stored.",
            Op::Poll => "Print the envelopes in a topic from an offset, one JSON object per line.",
            Op::Watch => {
                "Stream the envelopes of a topic as they arrive, one JSON object per line. With --consumer the position is kept by the broker, so a restart resumes at the next envelope; stop with SIGINT or SIGTERM."
            }
            Op::WorkPublish => {
                "Publish a work item to a queue and print it as stored. The item is named by its id."
            }
            Op::WorkPull => {
                "Claim the oldest claimable item of a queue under a lease and print the lease id, the consumer, the expiry and the item. Prints nothing if there is none. A consumer that holds a live lease is given it again, so asking again after a lost answer is safe."
            }
            Op::WorkExtend => {
                "Move the end of the lease of an item you claimed later, so that work that takes longer than the lease keeps its item. Prints the extended fact: its data.expires_at is the new end. A lease is never shortened and never runs past the server's longest lease from the claim. Safe to repeat."
            }
            Op::WorkAck => {
                "Complete an item you claimed, naming the consumer and presenting the lease."
            }
            Op::WorkFail => {
                "Give an item you claimed back as failed. It is tried again after a backoff, or goes to the dead topic when its attempts are used."
            }
            Op::AlertsPublish => {
                "Publish an alert and print it as stored. Its id is the alert's id."
            }
            Op::AlertsAck => "Acknowledge an alert, once.",
            Op::PrincipalsCreate => {
                "Issue a credential to a principal. The credential is printed this once. Admin only."
            }
            Op::PrincipalsList => {
                "List the principals that have a credential, one JSON object per line. Admin only."
            }
            Op::PrincipalsRevoke => "Withdraw a principal's credential. Admin only.",
            Op::WebhooksCreate => {
                "Deliver every envelope of a topic, signed, to a URL. The signing secret is printed this once. Admin only."
            }
            Op::WebhooksList => {
                "List the webhooks with their last delivered offset and last error, one JSON object per line. Admin only."
            }
            Op::WebhooksDelete => "Delete a webhook. Admin only.",
            Op::ConsumersGet => {
                "Show the offset a named consumer last committed on a topic, and the one it reads next."
            }
            Op::ConsumersCommit => {
                "Set the offset of the last message a named consumer has handled. Its next read starts one after it."
            }
        }
    }

    /// The parameters.
    pub fn params(self) -> Vec<Param> {
        let envelope =
            |first: &[Param]| -> Vec<Param> { first.iter().copied().chain(ENVELOPE).collect() };
        match self {
            Op::Publish => envelope(&[TENANT, TOPIC]),
            Op::Poll => vec![
                TENANT,
                TOPIC,
                optional(
                    "offset",
                    Kind::Int,
                    "Where to start; the first message is offset 0.",
                ),
                optional(
                    "count",
                    Kind::Int,
                    "How many to read at most. The server decides if not given.",
                ),
            ],
            Op::Watch => vec![
                TENANT,
                TOPIC,
                optional(
                    "consumer",
                    Kind::Text,
                    "Resume from this named consumer, and commit each offset after its line is written. Not with --from.",
                ),
                optional(
                    "from",
                    Kind::Int,
                    "Start at this offset, without keeping a position. Not with --consumer. Default 0.",
                ),
                optional("limit", Kind::Int, "Stop after this many envelopes."),
            ],
            Op::WorkPublish => envelope(&[TENANT, QUEUE]),
            Op::WorkPull => vec![
                TENANT,
                QUEUE,
                CONSUMER_PULL,
                optional(
                    "wait",
                    Kind::Int,
                    "Keep looking for this many seconds until an item can be claimed.",
                ),
            ],
            Op::WorkExtend => vec![
                TENANT,
                QUEUE,
                required("item", Kind::Text, "The item id."),
                CONSUMER_HELD,
                required(
                    "lease_id",
                    Kind::Text,
                    "The lease the item was claimed under.",
                ),
                optional(
                    "seconds",
                    Kind::Int,
                    "End the lease this many seconds from now, at least 1. The server's lease setting if not given.",
                ),
            ],
            Op::WorkAck => vec![
                TENANT,
                QUEUE,
                required("item", Kind::Text, "The item id."),
                CONSUMER_HELD,
                required(
                    "lease_id",
                    Kind::Text,
                    "The lease the item was claimed under.",
                ),
            ],
            Op::WorkFail => vec![
                TENANT,
                QUEUE,
                required("item", Kind::Text, "The item id."),
                CONSUMER_HELD,
                required(
                    "lease_id",
                    Kind::Text,
                    "The lease the item was claimed under.",
                ),
                required("reason", Kind::Text, "Why it failed."),
            ],
            Op::AlertsPublish => envelope(&[TENANT]),
            Op::AlertsAck => vec![TENANT, required("alert", Kind::Text, "The alert's id.")],
            Op::PrincipalsCreate => vec![
                required(
                    "kind",
                    Kind::Text,
                    "The kind: person, agent, connector, service or another lowercase token. An agent credential always expires.",
                ),
                required(
                    "name",
                    Kind::Text,
                    "The name, unique within its kind and tenant.",
                ),
                TENANT,
                required(
                    "scopes",
                    Kind::Scopes,
                    "What the credential may do: a list of {\"topic\": ..., \"verbs\": [...]}. Verbs are publish, subscribe, claim, ack and approve.",
                ),
                optional(
                    "ttl_seconds",
                    Kind::Int,
                    "Seconds until the credential stops working.",
                ),
            ],
            Op::PrincipalsList => vec![optional(
                "tenant",
                Kind::Text,
                "Only this tenant's principals.",
            )],
            Op::PrincipalsRevoke => vec![required("id", Kind::Text, "The principal's id.")],
            Op::WebhooksCreate => vec![
                TENANT,
                TOPIC,
                required(
                    "endpoint",
                    Kind::Text,
                    "The http or https address each envelope is POSTed to.",
                ),
            ],
            Op::WebhooksList => vec![optional(
                "tenant",
                Kind::Text,
                "Only this tenant's webhooks.",
            )],
            Op::WebhooksDelete => vec![required("id", Kind::Text, "The webhook's id.")],
            Op::ConsumersGet => vec![TENANT, TOPIC, CONSUMER_NAMED],
            Op::ConsumersCommit => vec![
                TENANT,
                TOPIC,
                CONSUMER_NAMED,
                required(
                    "offset",
                    Kind::Int,
                    "The offset of the last message handled.",
                ),
            ],
        }
    }
}

/// The arguments of one call, checked against the operation's parameters.
#[derive(Debug)]
pub struct Args<'a> {
    values: &'a Map<String, Value>,
}

impl<'a> Args<'a> {
    /// Check the arguments: none that the operation does not take, every
    /// required one present, and each of its kind. A `null` is the same as
    /// absent.
    ///
    /// # Errors
    ///
    /// Fails naming the argument.
    pub fn check(op: Op, values: &'a Map<String, Value>) -> anyhow::Result<Self> {
        let params = op.params();
        for name in values.keys() {
            if !params.iter().any(|param| param.name == name) {
                bail!("{name} is not an argument of {}", op.name());
            }
        }
        for param in &params {
            match values.get(param.name) {
                None | Some(Value::Null) if param.required => {
                    bail!("{} is required", param.name)
                }
                None | Some(Value::Null) => {}
                Some(value) => kind_ok(param, value)?,
            }
        }
        Ok(Self { values })
    }

    fn get(&self, name: &str) -> Option<&Value> {
        self.values.get(name).filter(|value| !value.is_null())
    }

    fn text(&self, name: &str) -> Option<&str> {
        self.get(name).and_then(Value::as_str)
    }

    /// A parameter, parsed as the type that checks it.
    fn parse<T>(&self, name: &str) -> anyhow::Result<T>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        self.parse_opt(name)?
            .with_context(|| format!("{name} is required"))
    }

    fn parse_opt<T>(&self, name: &str) -> anyhow::Result<Option<T>>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        self.text(name)
            .map(|text| {
                text.parse()
                    .map_err(|reason| anyhow::anyhow!("{name}: {reason}"))
            })
            .transpose()
    }

    /// A whole number. It may be written as text, as a tool's caller
    /// sometimes does.
    fn int(&self, name: &str) -> Option<u64> {
        match self.get(name)? {
            Value::Number(number) => number.as_u64(),
            Value::String(text) => text.trim().parse().ok(),
            _ => None,
        }
    }

    fn require_int(&self, name: &str) -> anyhow::Result<u64> {
        self.int(name)
            .with_context(|| format!("{name} is required"))
    }

    /// The `watch` arguments, which the streaming command reads.
    pub fn watch(&self) -> anyhow::Result<WatchArgs> {
        let consumer = self.parse_opt::<ConsumerName>("consumer")?;
        let from = self.int("from");
        if consumer.is_some() && from.is_some() {
            bail!(
                "--consumer and --from cannot be used together: a consumer resumes where it stopped"
            );
        }
        Ok(WatchArgs {
            tenant: self.parse("tenant")?,
            topic: self.parse("topic")?,
            consumer,
            from: from.unwrap_or(0),
            limit: self.int("limit"),
        })
    }
}

fn kind_ok(param: &Param, value: &Value) -> anyhow::Result<()> {
    let name = param.name;
    match param.kind {
        Kind::Text if value.is_string() => Ok(()),
        Kind::Text => bail!("{name} must be text"),
        Kind::Int => match value {
            Value::Number(number) if number.as_u64().is_some() => Ok(()),
            Value::String(text) if text.trim().parse::<u64>().is_ok() => Ok(()),
            _ => bail!("{name} must be a whole number, zero or more"),
        },
        Kind::Json => Ok(()),
        Kind::Scopes => match serde_json::from_value::<Vec<Grant>>(value.clone()) {
            Ok(_) => Ok(()),
            Err(reason) => bail!("{name} must be a list of {{\"topic\", \"verbs\"}}: {reason}"),
        },
    }
}

/// What `watch` is told.
#[derive(Debug)]
pub struct WatchArgs {
    /// The tenant to follow.
    pub tenant: TenantId,
    /// The topic to follow.
    pub topic: Topic,
    /// The named consumer whose position the broker keeps, if any.
    pub consumer: Option<ConsumerName>,
    /// Where to start without a consumer.
    pub from: u64,
    /// Stop after this many envelopes.
    pub limit: Option<u64>,
}

/// The attributes of an envelope that a client sets.
fn envelope_body(args: &Args<'_>) -> anyhow::Result<Value> {
    let mut body = Map::new();
    let mut put = |name: &str, value: String| {
        body.insert(name.to_owned(), Value::String(value));
    };
    put("source", args.parse::<Source>("source")?.to_string());
    put("type", args.parse::<EventType>("type")?.to_string());
    if let Some(subject) = args.parse_opt::<Subject>("subject")? {
        put("subject", subject.to_string());
    }
    if let Some(id) = args.parse_opt::<CorrelationId>("correlation_id")? {
        put("correlationid", id.to_string());
    }
    if let Some(id) = args.parse_opt::<CausationId>("causation_id")? {
        put("causationid", id.to_string());
    }
    if let Some(key) = args.parse_opt::<IdempotencyKey>("idempotency_key")? {
        put("idempotencykey", key.to_string());
    }
    if let Some(id) = args.parse_opt::<EventId>("id")? {
        put("id", id.to_string());
    }
    if let Some(data) = args.get("data") {
        body.insert(
            "datacontenttype".to_owned(),
            Value::String("application/json".to_owned()),
        );
        body.insert("data".to_owned(), data.clone());
    }
    Ok(Value::Object(body))
}

/// What the server answered, or the failure of an answer with nothing in it.
fn answered(reply: Option<Value>, what: &str) -> anyhow::Result<Value> {
    reply.with_context(|| format!("the server answered {what} with nothing"))
}

/// A member of an answer.
fn member(mut answer: Value, name: &str) -> anyhow::Result<Value> {
    answer
        .get_mut(name)
        .map(Value::take)
        .with_context(|| format!("the server's answer has no {name}"))
}

/// The items of a list in an answer.
fn items(answer: Value, name: &str) -> anyhow::Result<Vec<Value>> {
    match member(answer, name)? {
        Value::Array(items) => Ok(items),
        _ => bail!("the server's {name} is not a list"),
    }
}

/// Read a topic from `offset`, with each message's offset. `watch` needs them
/// and `poll` leaves them out.
///
/// # Errors
///
/// Fails if the server refuses or cannot be reached.
pub async fn read_messages(
    client: &Client,
    tenant: &TenantId,
    topic: &Topic,
    offset: u64,
    count: Option<u64>,
) -> anyhow::Result<Vec<(u64, Value)>> {
    let mut query = vec![("offset", offset.to_string())];
    if let Some(count) = count {
        query.push(("count", count.to_string()));
    }
    let reply = client
        .call(
            Method::GET,
            &[
                "v1",
                "tenants",
                tenant.as_str(),
                "topics",
                &topic.to_string(),
                "messages",
            ],
            &query,
            None,
            Replay::Safe,
        )
        .await?;
    items(answered(reply, "poll")?, "messages")?
        .into_iter()
        .map(|message| {
            let at = message
                .get("offset")
                .and_then(Value::as_u64)
                .context("a message in the answer has no offset")?;
            Ok((at, member(message, "envelope")?))
        })
        .collect()
}

/// Where a named consumer stands: the answer of `consumers_get`.
///
/// # Errors
///
/// Fails if the server refuses or cannot be reached.
pub async fn consumer_state(
    client: &Client,
    tenant: &TenantId,
    topic: &Topic,
    consumer: &ConsumerName,
) -> anyhow::Result<Value> {
    let reply = client
        .call(
            Method::GET,
            &[
                "v1",
                "tenants",
                tenant.as_str(),
                "topics",
                &topic.to_string(),
                "consumers",
                consumer.as_str(),
            ],
            &[],
            None,
            Replay::Safe,
        )
        .await?;
    answered(reply, "consumers_get")
}

/// Commit a named consumer's offset: `consumers_commit`.
///
/// # Errors
///
/// Fails if the server refuses or cannot be reached.
pub async fn consumer_commit(
    client: &Client,
    tenant: &TenantId,
    topic: &Topic,
    consumer: &ConsumerName,
    offset: u64,
) -> anyhow::Result<Value> {
    let reply = client
        .call(
            Method::POST,
            &[
                "v1",
                "tenants",
                tenant.as_str(),
                "topics",
                &topic.to_string(),
                "consumers",
                consumer.as_str(),
                "commit",
            ],
            &[],
            Some(&json!({ "offset": offset })),
            Replay::Safe,
        )
        .await?;
    answered(reply, "consumers_commit")
}

/// Do an operation, and return what it prints, one JSON value per line.
/// `watch` streams and is run by [`crate::watch`] instead.
///
/// # Errors
///
/// Fails, before anything is sent, if an argument is not valid. Fails with the
/// server's reason if it refuses, and with the network's if it cannot be
/// reached.
pub async fn run(
    client: &Client,
    op: Op,
    values: &Map<String, Value>,
) -> anyhow::Result<Vec<Value>> {
    let args = Args::check(op, values)?;
    match op {
        Op::Watch => bail!("watch streams, and is not run as a single call"),
        Op::Publish => {
            let tenant: TenantId = args.parse("tenant")?;
            let topic: Topic = args.parse("topic")?;
            let reply = client
                .call(
                    Method::POST,
                    &[
                        "v1",
                        "tenants",
                        tenant.as_str(),
                        "topics",
                        &topic.to_string(),
                        "messages",
                    ],
                    &[],
                    Some(&envelope_body(&args)?),
                    Replay::Unsafe,
                )
                .await?;
            Ok(vec![member(answered(reply, "publish")?, "envelope")?])
        }
        Op::Poll => {
            let tenant: TenantId = args.parse("tenant")?;
            let topic: Topic = args.parse("topic")?;
            let read = read_messages(
                client,
                &tenant,
                &topic,
                args.int("offset").unwrap_or(0),
                args.int("count"),
            )
            .await?;
            Ok(read.into_iter().map(|(_, envelope)| envelope).collect())
        }
        Op::WorkPublish => {
            let tenant: TenantId = args.parse("tenant")?;
            let queue: Segment = args.parse("queue")?;
            let reply = client
                .call(
                    Method::POST,
                    &[
                        "v1",
                        "tenants",
                        tenant.as_str(),
                        "work",
                        queue.as_str(),
                        "items",
                    ],
                    &[],
                    Some(&envelope_body(&args)?),
                    Replay::Unsafe,
                )
                .await?;
            Ok(vec![member(answered(reply, "work_publish")?, "envelope")?])
        }
        Op::WorkPull => work_pull(client, &args).await,
        Op::WorkExtend | Op::WorkAck | Op::WorkFail => {
            let tenant: TenantId = args.parse("tenant")?;
            let queue: Segment = args.parse("queue")?;
            let item = args.text("item").context("item is required")?;
            let lease: mansio_core::LeaseId = args.parse("lease_id")?;
            let consumer: ConsumerName = args.parse("consumer")?;
            let mut body = json!({ "lease_id": lease, "consumer": consumer });
            let (verb, replay) = match op {
                // An extension done twice leaves the lease running to the
                // later of the two ends, as once would: safe to send again.
                Op::WorkExtend => {
                    if let Some(seconds) = args.int("seconds") {
                        body["seconds"] = json!(seconds);
                    }
                    ("extend", Replay::Safe)
                }
                Op::WorkAck => ("ack", Replay::Unsafe),
                _ => {
                    body["reason"] = json!(args.text("reason").context("reason is required")?);
                    ("fail", Replay::Unsafe)
                }
            };
            let reply = client
                .call(
                    Method::POST,
                    &[
                        "v1",
                        "tenants",
                        tenant.as_str(),
                        "work",
                        queue.as_str(),
                        "items",
                        item,
                        verb,
                    ],
                    &[],
                    Some(&body),
                    replay,
                )
                .await?;
            Ok(vec![member(answered(reply, &op.name())?, "envelope")?])
        }
        Op::AlertsPublish => {
            let tenant: TenantId = args.parse("tenant")?;
            let reply = client
                .call(
                    Method::POST,
                    &["v1", "tenants", tenant.as_str(), "alerts"],
                    &[],
                    Some(&envelope_body(&args)?),
                    Replay::Unsafe,
                )
                .await?;
            Ok(vec![member(
                answered(reply, "alerts_publish")?,
                "envelope",
            )?])
        }
        Op::AlertsAck => {
            let tenant: TenantId = args.parse("tenant")?;
            let alert: EventId = args.parse("alert")?;
            let reply = client
                .call(
                    Method::POST,
                    &[
                        "v1",
                        "tenants",
                        tenant.as_str(),
                        "alerts",
                        &alert.to_string(),
                        "ack",
                    ],
                    &[],
                    None,
                    Replay::Unsafe,
                )
                .await?;
            Ok(vec![member(answered(reply, "alerts_ack")?, "envelope")?])
        }
        Op::PrincipalsCreate => {
            let request = CreatePrincipal {
                kind: args.parse::<PrincipalKind>("kind")?,
                name: args.parse::<PrincipalName>("name")?,
                tenant: args.parse("tenant")?,
                scopes: serde_json::from_value(
                    args.get("scopes").context("scopes is required")?.clone(),
                )
                .context("scopes")?,
                ttl_seconds: args.int("ttl_seconds"),
            };
            let reply = client
                .call(
                    Method::POST,
                    &["v1", "principals"],
                    &[],
                    Some(&serde_json::to_value(&request)?),
                    Replay::Unsafe,
                )
                .await?;
            Ok(vec![answered(reply, "principals_create")?])
        }
        Op::PrincipalsList => {
            let tenant = args.parse_opt::<TenantId>("tenant")?;
            let query: Vec<(&str, String)> = tenant
                .iter()
                .map(|tenant| ("tenant", tenant.to_string()))
                .collect();
            let reply = client
                .call(
                    Method::GET,
                    &["v1", "principals"],
                    &query,
                    None,
                    Replay::Safe,
                )
                .await?;
            items(answered(reply, "principals_list")?, "principals")
        }
        Op::PrincipalsRevoke => {
            let id: PrincipalId = args.parse("id")?;
            let reply = client
                .call(
                    Method::DELETE,
                    &["v1", "principals", &id.to_string()],
                    &[],
                    None,
                    Replay::Unsafe,
                )
                .await?;
            Ok(vec![answered(reply, "principals_revoke")?])
        }
        Op::WebhooksCreate => {
            let request = CreateWebhook {
                tenant: args.parse("tenant")?,
                topic: args.parse("topic")?,
                url: args
                    .text("endpoint")
                    .context("endpoint is required")?
                    .to_owned(),
            };
            let reply = client
                .call(
                    Method::POST,
                    &["v1", "webhooks"],
                    &[],
                    Some(&serde_json::to_value(&request)?),
                    Replay::Unsafe,
                )
                .await?;
            Ok(vec![answered(reply, "webhooks_create")?])
        }
        Op::WebhooksList => {
            let tenant = args.parse_opt::<TenantId>("tenant")?;
            let query: Vec<(&str, String)> = tenant
                .iter()
                .map(|tenant| ("tenant", tenant.to_string()))
                .collect();
            let reply = client
                .call(Method::GET, &["v1", "webhooks"], &query, None, Replay::Safe)
                .await?;
            items(answered(reply, "webhooks_list")?, "webhooks")
        }
        Op::WebhooksDelete => {
            let id: WebhookId = args.parse("id")?;
            let reply = client
                .call(
                    Method::DELETE,
                    &["v1", "webhooks", &id.to_string()],
                    &[],
                    None,
                    Replay::Unsafe,
                )
                .await?;
            Ok(vec![answered(reply, "webhooks_delete")?])
        }
        Op::ConsumersGet => Ok(vec![
            consumer_state(
                client,
                &args.parse("tenant")?,
                &args.parse("topic")?,
                &args.parse("consumer")?,
            )
            .await?,
        ]),
        Op::ConsumersCommit => Ok(vec![
            consumer_commit(
                client,
                &args.parse("tenant")?,
                &args.parse("topic")?,
                &args.parse("consumer")?,
                args.require_int("offset")?,
            )
            .await?,
        ]),
    }
}

/// `work_pull`, looking again every poll interval until an item is claimed or
/// `wait` seconds have passed. A pull is safe to repeat: a consumer that holds
/// a lease is given it again, so a lost answer costs nothing.
async fn work_pull(client: &Client, args: &Args<'_>) -> anyhow::Result<Vec<Value>> {
    let tenant: TenantId = args.parse("tenant")?;
    let queue: Segment = args.parse("queue")?;
    let consumer: ConsumerName = args.parse("consumer")?;
    let deadline = args
        .int("wait")
        .map(|seconds| Instant::now() + Duration::from_secs(seconds));
    loop {
        let reply = client
            .call(
                Method::POST,
                &[
                    "v1",
                    "tenants",
                    tenant.as_str(),
                    "work",
                    queue.as_str(),
                    "pull",
                ],
                &[],
                Some(&json!({ "consumer": consumer })),
                Replay::Safe,
            )
            .await?;
        if let Some(claim) = reply {
            return Ok(vec![claim]);
        }
        let Some(deadline) = deadline else {
            return Ok(Vec::new());
        };
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Ok(Vec::new());
        }
        tokio::time::sleep(client.poll_interval().min(left)).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    fn object(value: Value) -> Map<String, Value> {
        let Value::Object(map) = value else {
            panic!("not an object");
        };
        map
    }

    #[test]
    fn names_are_the_command_with_underscores() {
        let names: Vec<String> = Op::ALL.iter().map(|op| op.name()).collect();
        assert_eq!(
            names,
            [
                "publish",
                "poll",
                "watch",
                "work_publish",
                "work_pull",
                "work_extend",
                "work_ack",
                "work_fail",
                "alerts_publish",
                "alerts_ack",
                "principals_create",
                "principals_list",
                "principals_revoke",
                "webhooks_create",
                "webhooks_list",
                "webhooks_delete",
                "consumers_get",
                "consumers_commit",
            ]
        );
        for op in Op::ALL {
            assert_eq!(Op::from_path(op.path()), Some(op));
        }
        assert_eq!(Op::from_path(&["work"]), None);
    }

    #[test]
    fn every_operation_is_described_and_its_parameters_are_distinct() {
        for op in Op::ALL {
            assert!(op.about().ends_with('.'), "{}", op.name());
            let params = op.params();
            let mut names: Vec<&str> = params.iter().map(|p| p.name).collect();
            names.sort_unstable();
            names.dedup();
            assert_eq!(names.len(), params.len(), "{}", op.name());
            for param in params {
                assert!(param.help.ends_with('.'), "{}: {}", op.name(), param.name);
            }
        }
        assert!(!Op::Watch.is_tool());
        assert_eq!(Op::ALL.iter().filter(|op| !op.is_tool()).count(), 1);
    }

    #[test]
    fn arguments_are_checked_before_anything_is_sent() {
        let ok = object(json!({"tenant": "acme", "queue": "builds", "consumer": "w1"}));
        assert!(Args::check(Op::WorkPull, &ok).is_ok());

        for (op, bad, why) in [
            (
                Op::WorkPull,
                json!({"tenant": "acme", "queue": "builds"}),
                "consumer is required",
            ),
            (
                Op::WorkPull,
                json!({"tenant": "acme", "queue": "builds", "consumer": "w", "extra": 1}),
                "extra is not an argument",
            ),
            (
                Op::WorkPull,
                json!({"tenant": 7, "queue": "builds", "consumer": "w"}),
                "tenant must be text",
            ),
            (
                Op::WorkPull,
                json!({"tenant": "acme", "queue": "builds", "consumer": "w", "wait": -1}),
                "wait must be a whole number",
            ),
            (
                Op::WorkPull,
                json!({"tenant": "acme", "queue": "builds", "consumer": "w", "wait": "soon"}),
                "wait must be a whole number",
            ),
            (
                Op::PrincipalsCreate,
                json!({"kind": "agent", "name": "a", "tenant": "acme", "scopes": [{"topic": "facts.build", "verbs": ["claim"]}]}),
                "scopes must be a list",
            ),
            (
                Op::PrincipalsCreate,
                json!({"kind": "agent", "name": "a", "tenant": "acme", "scopes": "all"}),
                "scopes must be a list",
            ),
        ] {
            let error = Args::check(op, &object(bad)).unwrap_err().to_string();
            assert!(error.contains(why), "{op:?}: {error}");
        }
    }

    #[test]
    fn a_null_is_the_same_as_leaving_it_out_and_a_number_may_be_written_as_text() {
        let args = object(json!({
            "tenant": "acme", "queue": "builds", "consumer": "w", "wait": null
        }));
        let checked = Args::check(Op::WorkPull, &args).unwrap();
        assert_eq!(checked.int("wait"), None);
        let args = object(json!({
            "tenant": "acme", "queue": "builds", "consumer": "w", "wait": "5"
        }));
        let checked = Args::check(Op::WorkPull, &args).unwrap();
        assert_eq!(checked.int("wait"), Some(5));
    }

    #[test]
    fn work_extend_takes_the_lease_and_an_optional_number_of_seconds() {
        let lease = "5b0c0b51-4a53-4a3c-9d3a-0d6e0e2f7a11";
        let held = json!({
            "tenant": "acme", "queue": "builds", "item": "item-1",
            "consumer": "w", "lease_id": lease
        });
        assert!(Args::check(Op::WorkExtend, &object(held.clone())).is_ok());
        let mut with_seconds = held.clone();
        with_seconds["seconds"] = json!("30");
        let values = object(with_seconds);
        let checked = Args::check(Op::WorkExtend, &values).unwrap();
        assert_eq!(checked.int("seconds"), Some(30));

        for (field, bad, why) in [
            ("seconds", json!(-1), "seconds must be a whole number"),
            ("seconds", json!("soon"), "seconds must be a whole number"),
            ("reason", json!("no"), "reason is not an argument"),
        ] {
            let mut values = held.clone();
            values[field] = bad;
            let error = Args::check(Op::WorkExtend, &object(values))
                .unwrap_err()
                .to_string();
            assert!(error.contains(why), "{error}");
        }
        for missing in ["tenant", "queue", "item", "consumer", "lease_id"] {
            let mut values = held.clone();
            values.as_object_mut().unwrap().remove(missing);
            let error = Args::check(Op::WorkExtend, &object(values))
                .unwrap_err()
                .to_string();
            assert!(error.contains(&format!("{missing} is required")), "{error}");
        }
    }

    /// A server that reads each request and drops the first `drops` of them
    /// without answering, as a connection that fails with the answer on its
    /// way, and answers the rest. It keeps every request it read.
    async fn lossy_server(drops: usize) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let kept = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let mut request = Vec::new();
                let mut chunk = [0_u8; 1024];
                while let Ok(read) = socket.read(&mut chunk).await {
                    if read == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..read]);
                    let text = String::from_utf8_lossy(&request).into_owned();
                    let Some((head, body)) = text.split_once("\r\n\r\n") else {
                        continue;
                    };
                    let wanted = head
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|n| n.trim().parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    if body.len() >= wanted {
                        break;
                    }
                }
                let n = {
                    let mut kept = kept.lock().unwrap();
                    kept.push(String::from_utf8_lossy(&request).into_owned());
                    kept.len()
                };
                if n > drops {
                    let body = r#"{"envelope":{"type":"mansio.work.extended"}}"#;
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

    fn client_for(base: &str) -> Client {
        let base = base.to_owned();
        Client::from_lookup(
            crate::client::Connection::default(),
            move |name| match name {
                "MANSIO_TOKEN" => Some("t".to_owned()),
                "MANSIO_URL" => Some(base.clone()),
                "MANSIO_RETRY_BACKOFF_MILLIS" => Some("1".to_owned()),
                _ => None,
            },
        )
        .unwrap()
    }

    #[tokio::test]
    async fn a_work_extend_whose_answer_was_lost_is_sent_again_and_an_ack_is_not() {
        let lease = "5b0c0b51-4a53-4a3c-9d3a-0d6e0e2f7a11";
        let values = |extra: Value| {
            let mut values = json!({
                "tenant": "acme", "queue": "builds", "item": "item-1",
                "consumer": "w", "lease_id": lease
            });
            values
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            object(values)
        };

        // An extension done twice leaves the lease where once would.
        let (base, seen) = lossy_server(1).await;
        let printed = run(
            &client_for(&base),
            Op::WorkExtend,
            &values(json!({"seconds": 7})),
        )
        .await
        .unwrap();
        assert_eq!(printed, [json!({"type": "mansio.work.extended"})]);
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 2, "one lost, one answered");
        for request in &seen {
            assert!(
                request.starts_with("POST /v1/tenants/acme/work/builds/items/item-1/extend "),
                "{request}"
            );
            let body: Value =
                serde_json::from_str(request.split_once("\r\n\r\n").unwrap().1).unwrap();
            assert_eq!(
                body,
                json!({"lease_id": lease, "consumer": "w", "seconds": 7})
            );
        }

        // An extension without --seconds sends none, and an ack is not sent
        // again once it was sent.
        let (base, seen) = lossy_server(0).await;
        run(&client_for(&base), Op::WorkExtend, &values(json!({})))
            .await
            .unwrap();
        let sent = seen.lock().unwrap().clone();
        let body: Value = serde_json::from_str(sent[0].split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(body, json!({"lease_id": lease, "consumer": "w"}));

        let (base, seen) = lossy_server(1).await;
        let failed = run(&client_for(&base), Op::WorkAck, &values(json!({}))).await;
        assert!(failed.is_err());
        assert_eq!(seen.lock().unwrap().len(), 1, "an ack is sent once");
    }

    #[test]
    fn publish_sends_only_what_was_given() {
        let values = object(json!({
            "tenant": "acme",
            "topic": "facts.build",
            "type": "build.done",
            "source": "ci",
            "data": {"ok": true},
            "correlation_id": "c1",
            "id": "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3",
        }));
        let args = Args::check(Op::Publish, &values).unwrap();
        assert_eq!(
            envelope_body(&args).unwrap(),
            json!({
                "source": "ci",
                "type": "build.done",
                "correlationid": "c1",
                "id": "0192f1c2-7d3e-7a4b-8c5d-6e7f8091a2b3",
                "datacontenttype": "application/json",
                "data": {"ok": true}
            })
        );
    }

    #[test]
    fn what_the_server_would_refuse_is_refused_here() {
        for (op, values) in [
            (
                Op::Publish,
                json!({"tenant": "Acme", "topic": "alerts", "type": "t", "source": "s"}),
            ),
            (
                Op::Publish,
                json!({"tenant": "acme", "topic": "nope", "type": "t", "source": "s"}),
            ),
            (
                Op::Publish,
                json!({"tenant": "acme", "topic": "alerts", "type": "t", "source": "s", "id": "x"}),
            ),
        ] {
            let values = object(values);
            let args = Args::check(op, &values).unwrap();
            let failed = args.parse::<TenantId>("tenant").is_err()
                || args.parse::<Topic>("topic").is_err()
                || envelope_body(&args).is_err();
            assert!(failed, "{values:?}");
        }
    }

    #[test]
    fn watch_takes_a_consumer_or_a_start_and_not_both() {
        let consumer = object(json!({"tenant": "acme", "topic": "alerts", "consumer": "w"}));
        let watch = Args::check(Op::Watch, &consumer).unwrap().watch().unwrap();
        assert_eq!(watch.consumer.unwrap().as_str(), "w");
        assert_eq!((watch.from, watch.limit), (0, None));

        let from = object(json!({"tenant": "acme", "topic": "alerts", "from": 4, "limit": 2}));
        let watch = Args::check(Op::Watch, &from).unwrap().watch().unwrap();
        assert!(watch.consumer.is_none());
        assert_eq!((watch.from, watch.limit), (4, Some(2)));

        let both = object(json!({"tenant": "acme", "topic": "alerts", "consumer": "w", "from": 1}));
        assert!(Args::check(Op::Watch, &both).unwrap().watch().is_err());
    }
}
