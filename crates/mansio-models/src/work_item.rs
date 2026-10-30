//! One work item's life in the engine, from `work_publish` to acked or dead
//! (ARCHITECTURE.md, "Work items" and "State"), with two consumers, a
//! producer whose publish may answer 503 having stored the message, and a
//! server that may restart.
//!
//! The state holds three things apart. The topics: the newest attempt on
//! `work.<queue>`, the last fact recorded on `facts.work` for it, and the
//! copies on `dead`; with the clock, they are what is true. The engine's
//! memory: where it thinks the item stands, which `crates/mansio-server`
//! rebuilds from the topics on start and after a publish whose outcome it
//! does not know (`recover.rs`, `replay`). And the consumers: the lease id
//! each was last given. The rules read what is true; the steps go through
//! what the engine has in memory, as the server does.
//!
//! Every step is taken by a function of `crates/mansio-server/src/engine.rs`,
//! named in its label, or by the clock. A call the engine refuses (403, 404,
//! 409) changes nothing and is no step. The loops are bounded: attempts by
//! the cap, restarts, lost answers and extensions by one each, and the clock
//! by the leases that were given.

use std::num::NonZeroU8;

use stateright::{Model, Property};

use crate::Readable;

/// One of the two consumers, each a principal and a consumer name.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Consumer {
    One,
    Two,
}

impl Consumer {
    pub const ALL: [Consumer; 2] = [Consumer::One, Consumer::Two];

    fn index(self) -> usize {
        match self {
            Consumer::One => 0,
            Consumer::Two => 1,
        }
    }
}

/// Where the producer stands with its publish.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Producer {
    /// It has not sent the item yet.
    Sending,
    /// Its publish answered 503, so it sends the item again with the same id.
    Resending,
    /// It was answered 201 or 409.
    Done,
}

/// A lease id, numbered in the order the engine gave them out.
pub type LeaseId = u8;

/// The last fact on `facts.work` for the newest attempt.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Fact {
    Claimed {
        lease: LeaseId,
        by: Consumer,
    },
    Extended {
        lease: LeaseId,
        by: Consumer,
    },
    /// Failed by its holder, with a retry next.
    Failed,
    /// Its lease expired, with a retry next.
    Expired,
    /// The `retried` fact of the newest attempt.
    Retried,
    Acked,
    Dead,
}

/// Where the engine has the item in memory (`state.rs`, `Phase`).
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Phase {
    /// Not in the queue: nothing was published.
    Absent,
    Ready,
    /// `remembered` is whether the server still has the lease id, which it
    /// does not after a rebuild: the facts keep only a fingerprint of it.
    Leased {
        lease: LeaseId,
        by: Consumer,
        remembered: bool,
    },
    Backoff,
    /// Acked or dead: the engine keeps only the item's id.
    Finished,
}

/// One item: the topics, the clock, the engine's memory and the consumers.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Item {
    pub producer: Producer,
    /// The newest attempt on `work.<queue>`; 0 while nothing is stored.
    pub attempt: u8,
    /// The last fact recorded for that attempt.
    pub last: Option<Fact>,
    /// Copies on `dead`.
    pub dead: u8,
    /// `acked` facts, of every attempt.
    pub acks: u8,
    /// Whether the engine decided the item had used its last attempt.
    pub spent: bool,
    /// The leases a `claimed` fact gave and no `acked`, `failed` or `expired`
    /// fact closed, with their holders. Sorted.
    pub open: Vec<(LeaseId, Consumer)>,
    /// The leases whose end has passed. Sorted.
    pub ended: Vec<LeaseId>,
    /// The engine's memory of the item; `None` once a publish answered 503,
    /// which the next use rebuilds from the topics.
    pub memory: Option<Phase>,
    /// The lease id each consumer was last given.
    pub held: [Option<LeaseId>; 2],
    /// Lease ids given so far.
    pub issued: u8,
    pub restarts: u8,
    /// Answers lost: publishes, claims and acks that answered 503.
    pub lost: u8,
    pub extensions: u8,
    /// Whether the engine accepted an ack or an extension with a lease that
    /// was not live.
    pub acted_without_lease: bool,
}

/// A step from one state to the next.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Step {
    Publish,
    /// The publish answers 503; the message was stored all the same.
    PublishLostStored,
    /// The publish answers 503 and nothing was stored.
    PublishLostNothing,
    Resend,
    Pull(Consumer),
    /// The claim is recorded and its answer is lost.
    PullLost(Consumer),
    Extend(Consumer),
    Ack(Consumer),
    /// The ack is recorded and its answer is lost.
    AckLost(Consumer),
    Fail(Consumer),
    LeaseRunsOut(LeaseId),
    Expire,
    Retry,
    Restart,
}

pub const ONE_LEASE: &str = "an item has at most one live lease";
pub const REFUSED: &str = "an ack or extend with a lease that is not live is refused";
pub const ONCE: &str = "an item is acked at most once";
pub const DEAD: &str = "an item past the attempt cap is in dead and nowhere else";
pub const ENDS: &str = "every item ends acked or dead";
pub const RETRY_ACKED: &str = "an item is acked after a retry";
pub const REACHES_DEAD: &str = "an item reaches dead";

/// The bounds that keep the check finite.
const RESTARTS: u8 = 1;
const LOST: u8 = 1;
const EXTENSIONS: u8 = 1;

/// One work item under an attempt cap (`MANSIO_MAX_ATTEMPTS`).
#[derive(Clone, Debug)]
pub struct WorkItem {
    max_attempts: NonZeroU8,
    ack_checks_expiry: bool,
}

impl WorkItem {
    /// The item as the engine runs it.
    pub fn new(max_attempts: NonZeroU8) -> Self {
        Self {
            max_attempts,
            ack_checks_expiry: true,
        }
    }

    /// A seeded bug: `work_ack` accepts a lease whose end has passed, as long
    /// as the timer has not yet expired it.
    pub fn ack_on_expired_lease(max_attempts: NonZeroU8) -> Self {
        Self {
            max_attempts,
            ack_checks_expiry: false,
        }
    }

    pub fn max_attempts(&self) -> u8 {
        self.max_attempts.get()
    }

    /// The engine's view: its memory, or what a rebuild from the topics
    /// gives.
    pub fn view(&self, s: &Item) -> Phase {
        s.memory.unwrap_or_else(|| replay(s))
    }

    fn last_attempt(&self, s: &Item) -> bool {
        s.attempt >= self.max_attempts.get()
    }

    /// `held_lease` in `state.rs`: whether `c`, presenting its lease id, may
    /// act on the item, and the lease if so.
    fn held_lease(&self, s: &Item, c: Consumer, ack: bool) -> Option<LeaseId> {
        let presented = s.held[c.index()]?;
        let Phase::Leased { lease, by, .. } = self.view(s) else {
            return None; // 404 or 409
        };
        if lease != presented || by != c {
            return None; // 403
        }
        let checks_expiry = !ack || self.ack_checks_expiry;
        if checks_expiry && s.ended.contains(&lease) {
            return None; // 409: the lease has expired
        }
        Some(lease)
    }

    /// What `work_pull` by `c` records, if anything: the lease id it gives
    /// and whether it is the same lease given a new id.
    fn pull(&self, s: &Item, c: Consumer) -> Option<(LeaseId, Option<LeaseId>)> {
        let fresh = s.issued.checked_add(1)?;
        match self.view(s) {
            // `held_by`: `c` holds a live lease and is given it again; a lease
            // read back from the facts is given a new id (`give_again`).
            Phase::Leased {
                lease,
                by,
                remembered,
            } if by == c && !s.ended.contains(&lease) => {
                (!remembered).then_some((fresh, Some(lease)))
            }
            Phase::Ready => Some((fresh, None)),
            _ => None,
        }
    }

    /// A failure or an expiry: a retry after the backoff, or past the cap
    /// `dead` at once (`decide`, then `advance`).
    fn give_up(&self, n: &mut Item, lease: LeaseId, failed: bool) {
        n.open.retain(|(l, _)| *l != lease);
        if self.last_attempt(n) {
            n.spent = true;
            n.dead += 1;
            n.last = Some(Fact::Dead);
            n.memory = Some(Phase::Finished);
        } else {
            n.last = Some(if failed { Fact::Failed } else { Fact::Expired });
            n.memory = Some(Phase::Backoff);
        }
    }
}

/// `replay` in `recover.rs`: the newest message says what the item is, and
/// the facts of that attempt say where it stands.
pub fn replay(s: &Item) -> Phase {
    if s.attempt == 0 {
        return Phase::Absent;
    }
    match s.last {
        None | Some(Fact::Retried) => Phase::Ready,
        Some(Fact::Claimed { lease, by } | Fact::Extended { lease, by }) => Phase::Leased {
            lease,
            by,
            remembered: false,
        },
        Some(Fact::Failed | Fact::Expired) => Phase::Backoff,
        Some(Fact::Acked | Fact::Dead) => Phase::Finished,
    }
}

/// Whether `lease`, held by `c`, is live: given, not closed by a fact, and
/// not past its end.
pub fn live(s: &Item, lease: LeaseId, c: Consumer) -> bool {
    s.open.contains(&(lease, c)) && !s.ended.contains(&lease)
}

impl Item {
    fn start() -> Self {
        Item {
            producer: Producer::Sending,
            attempt: 0,
            last: None,
            dead: 0,
            acks: 0,
            spent: false,
            open: Vec::new(),
            ended: Vec::new(),
            memory: Some(Phase::Absent),
            held: [None, None],
            issued: 0,
            restarts: 0,
            lost: 0,
            extensions: 0,
            acted_without_lease: false,
        }
    }

    /// Leases that are live.
    pub fn live_leases(&self) -> usize {
        self.open
            .iter()
            .filter(|(l, _)| !self.ended.contains(l))
            .count()
    }

    /// Stored: the first message of the item, attempt 1.
    fn store(&mut self) {
        self.attempt = 1;
        self.last = None;
    }
}

impl Model for WorkItem {
    type State = Item;
    type Action = Step;

    fn init_states(&self) -> Vec<Item> {
        vec![Item::start()]
    }

    fn actions(&self, s: &Item, out: &mut Vec<Step>) {
        match s.producer {
            Producer::Sending => {
                out.push(Step::Publish);
                if s.lost < LOST {
                    out.extend([Step::PublishLostStored, Step::PublishLostNothing]);
                }
            }
            Producer::Resending => out.push(Step::Resend),
            Producer::Done => {}
        }
        for c in Consumer::ALL {
            out.push(Step::Pull(c));
            if s.lost < LOST {
                out.push(Step::PullLost(c));
            }
            if s.extensions < EXTENSIONS {
                out.push(Step::Extend(c));
            }
            out.push(Step::Ack(c));
            if s.lost < LOST {
                out.push(Step::AckLost(c));
            }
            out.push(Step::Fail(c));
        }
        for (lease, _) in &s.open {
            if !s.ended.contains(lease) {
                out.push(Step::LeaseRunsOut(*lease));
            }
        }
        out.extend([Step::Expire, Step::Retry]);
        if s.restarts < RESTARTS && s.attempt > 0 {
            out.push(Step::Restart);
        }
    }

    fn next_state(&self, s: &Item, step: Step) -> Option<Item> {
        let mut n = s.clone();
        match step {
            // work_publish: an id the queue has never had is stored as
            // attempt 1 and is claimable.
            Step::Publish => {
                n.store();
                n.memory = Some(Phase::Ready);
                n.producer = Producer::Done;
            }
            // work_publish answers 503 (`append`, `outcome_unknown`): the
            // engine throws its memory away, and the producer sends again.
            Step::PublishLostStored | Step::PublishLostNothing => {
                if step == Step::PublishLostStored {
                    n.store();
                }
                n.memory = None;
                n.producer = Producer::Resending;
                n.lost += 1;
            }
            // work_publish again, with the same id, after a rebuild: 409 if
            // the queue has had the id, open or done; else stored.
            Step::Resend => {
                let view = self.view(s);
                if view == Phase::Absent {
                    n.store();
                    n.memory = Some(Phase::Ready);
                } else {
                    n.memory = Some(view);
                }
                n.producer = Producer::Done;
            }
            // work_pull: a fresh claim, or the lease `c` holds given again.
            Step::Pull(c) | Step::PullLost(c) => {
                let (lease, renews) = self.pull(s, c)?;
                n.issued = lease;
                n.last = Some(Fact::Claimed { lease, by: c });
                if let Some(old) = renews {
                    n.open.retain(|(l, _)| *l != old);
                }
                n.open.push((lease, c));
                n.open.sort();
                if step == Step::PullLost(c) {
                    n.memory = None;
                    n.lost += 1;
                } else {
                    n.memory = Some(Phase::Leased {
                        lease,
                        by: c,
                        remembered: true,
                    });
                    n.held[c.index()] = Some(lease);
                }
            }
            // work_extend, by the holder with a live lease: the lease ends
            // later, so its end has not passed.
            Step::Extend(c) => {
                let lease = self.held_lease(s, c, false)?;
                n.acted_without_lease |= !live(s, lease, c);
                n.last = Some(Fact::Extended { lease, by: c });
                n.memory = Some(match self.view(s) {
                    Phase::Leased { remembered, .. } => Phase::Leased {
                        lease,
                        by: c,
                        remembered,
                    },
                    other => other,
                });
                n.extensions += 1;
            }
            // work_ack, by the holder with a live lease: done.
            Step::Ack(c) | Step::AckLost(c) => {
                let lease = self.held_lease(s, c, true)?;
                n.acted_without_lease |= !live(s, lease, c);
                n.open.retain(|(l, _)| *l != lease);
                n.acks += 1;
                n.last = Some(Fact::Acked);
                if step == Step::AckLost(c) {
                    n.memory = None;
                    n.lost += 1;
                } else {
                    n.memory = Some(Phase::Finished);
                    n.held[c.index()] = None;
                }
            }
            // work_fail, by the holder with a live lease.
            Step::Fail(c) => {
                let lease = self.held_lease(s, c, false)?;
                n.held[c.index()] = None;
                self.give_up(&mut n, lease, true);
            }
            // The clock passes the end of a lease.
            Step::LeaseRunsOut(lease) => {
                if !s.open.iter().any(|(l, _)| *l == lease) || s.ended.contains(&lease) {
                    return None;
                }
                n.ended.push(lease);
                n.ended.sort();
            }
            // tick, then `expire`: a lease whose end has passed.
            Step::Expire => {
                let Phase::Leased { lease, .. } = self.view(s) else {
                    return None;
                };
                if !s.ended.contains(&lease) {
                    return None;
                }
                self.give_up(&mut n, lease, false);
            }
            // tick, then `advance`: the backoff is over; a new message with
            // the same item id and the attempt plus one, and its `retried`.
            Step::Retry => {
                if self.view(s) != Phase::Backoff {
                    return None;
                }
                n.attempt += 1;
                n.last = Some(Fact::Retried);
                n.memory = Some(Phase::Ready);
            }
            // The server starts again: `recover` rebuilds the state from the
            // topics.
            Step::Restart => {
                n.memory = Some(replay(s));
                n.restarts += 1;
            }
        }
        Some(n)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(ONE_LEASE, |_, s: &Item| s.live_leases() <= 1),
            Property::always(REFUSED, |_, s: &Item| !s.acted_without_lease),
            Property::always(ONCE, |_, s: &Item| s.acks <= 1),
            Property::always(DEAD, |m: &WorkItem, s: &Item| {
                let within = s.attempt <= m.max_attempts.get();
                let dead_only_when_spent = (s.dead > 0) == s.spent;
                let nowhere_else = !s.spent
                    || (s.dead == 1
                        && s.attempt == m.max_attempts.get()
                        && m.view(s) == Phase::Finished
                        && s.live_leases() == 0);
                within && dead_only_when_spent && nowhere_else
            }),
            Property::eventually(ENDS, |_, s: &Item| s.acks == 1 || s.dead == 1),
            Property::sometimes(RETRY_ACKED, |_, s: &Item| s.acks == 1 && s.attempt >= 2),
            Property::sometimes(REACHES_DEAD, |_, s: &Item| s.dead == 1),
        ]
    }
}

impl Readable for WorkItem {
    fn title(&self) -> String {
        let max = self.max_attempts.get();
        let mut t = format!(
            "One work item, two consumers, up to {max} attempt{}",
            if max == 1 { "" } else { "s" }
        );
        if !self.ack_checks_expiry {
            t.push_str(", with an ack accepted on an expired lease (a seeded bug)");
        }
        t
    }

    fn state_label(&self, s: &Item) -> String {
        if s.attempt == 0 {
            return match s.producer {
                Producer::Resending => {
                    "The publish answered 503 and stored nothing, the producer sends it again"
                }
                Producer::Sending | Producer::Done => "Not published yet",
            }
            .to_string();
        }
        match s.last {
            None | Some(Fact::Retried) => "Waiting to be claimed",
            Some(Fact::Claimed { lease, .. } | Fact::Extended { lease, .. })
                if s.ended.contains(&lease) =>
            {
                "Its lease has run out, and the timer has not yet expired it"
            }
            Some(Fact::Claimed { .. } | Fact::Extended { .. }) => "Leased to a consumer",
            Some(Fact::Failed | Fact::Expired) => "Waiting out its backoff before a retry",
            Some(Fact::Acked) => "Acked, done",
            Some(Fact::Dead) => "On dead, done",
        }
        .to_string()
    }

    fn step_label(&self, s: &Item, step: &Step) -> String {
        let attempts = if self.last_attempt(s) {
            "last attempt"
        } else {
            "attempts left"
        };
        let renews = |c: Consumer| matches!(self.pull(s, c), Some((_, Some(_))));
        match step {
            Step::Publish => "the producer publishes it (work_publish)".to_string(),
            Step::PublishLostStored => {
                "the publish answers 503 though the message was stored (work_publish)".to_string()
            }
            Step::PublishLostNothing => {
                "the publish answers 503 and nothing was stored (work_publish)".to_string()
            }
            Step::Resend if self.view(s) == Phase::Absent => {
                "the producer sends it again (work_publish)".to_string()
            }
            Step::Resend => {
                "the producer sends it again and is answered 409, it was stored (work_publish)"
                    .to_string()
            }
            Step::Pull(c) if renews(*c) => "its holder pulls again after a rebuild and is given \
                 a new lease id for the same lease (work_pull)"
                .to_string(),
            Step::Pull(_) => "a consumer claims it with a lease (work_pull)".to_string(),
            Step::PullLost(c) if renews(*c) => "its holder pulls again after a rebuild, and the \
                 answer with the new lease id is lost to a 503 (work_pull)"
                .to_string(),
            Step::PullLost(_) => {
                "a consumer claims it and the answer is lost to a 503 (work_pull)".to_string()
            }
            Step::Extend(_) => "its holder extends the lease (work_extend)".to_string(),
            Step::Ack(_) => "its holder acks it (work_ack)".to_string(),
            Step::AckLost(_) => {
                "its holder acks it and the answer is lost to a 503 (work_ack)".to_string()
            }
            Step::Fail(_) if self.last_attempt(s) => {
                format!("its holder fails it and it goes to dead [{attempts}] (work_fail)")
            }
            Step::Fail(_) => format!("its holder fails it [{attempts}] (work_fail)"),
            Step::LeaseRunsOut(_) => "the lease runs out (the clock)".to_string(),
            Step::Expire if self.last_attempt(s) => format!(
                "the timer expires the lease and it goes to dead [{attempts}] (tick, expire)"
            ),
            Step::Expire => format!("the timer expires the lease [{attempts}] (tick, expire)"),
            Step::Retry => "the timer republishes it as a new message, the attempt plus one \
                 (tick, advance)"
                .to_string(),
            Step::Restart => {
                "the server restarts and rebuilds its state from the topics (recover, replay)"
                    .to_string()
            }
        }
    }

    fn cites(&self, rule: &str) -> Option<&'static str> {
        match rule {
            ONE_LEASE => Some("ARCHITECTURE.md, Work items, Claim"),
            REFUSED => Some("ARCHITECTURE.md, Work items, Extend and Ack"),
            ONCE | ENDS => Some("README, The guarantee"),
            DEAD | REACHES_DEAD => Some("ARCHITECTURE.md, Work items, Fail"),
            RETRY_ACKED => Some("ARCHITECTURE.md, Work items, Fail, and State"),
            _ => None,
        }
    }
}
