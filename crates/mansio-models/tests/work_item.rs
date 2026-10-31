//! Every rule of one work item, checked over every reachable state, and a
//! seeded bug that pins the shortest-path helper. A failure prints the
//! numbered shortest path in the reader's words.

use mansio_models::work_item::{
    Consumer, DEAD, ENDS, ONCE, ONE_LEASE, Phase, REACHES_DEAD, REFUSED, RETRY_ACKED, Step,
    WorkItem, live, replay,
};
use mansio_models::{Kind, Outcome, Readable, check};
use stateright::Model;

use helpers::{assert_holds, max, reachable};

/// The tests' helpers, under `#[cfg(test)]` (always set in a test target) so
/// that, like a `#[test]` function, they may unwrap (clippy.toml).
#[cfg(test)]
mod helpers {
    use std::num::NonZeroU8;

    use mansio_models::work_item::{Item, WorkItem};
    use mansio_models::{Outcome, Report};
    use stateright::Model;

    /// An attempt cap (`MANSIO_MAX_ATTEMPTS`).
    pub fn max(n: u8) -> NonZeroU8 {
        NonZeroU8::new(n).unwrap()
    }

    pub fn assert_holds(report: &Report<WorkItem>, rule: &str) {
        let r = report.rule(rule).unwrap();
        assert_eq!(r.outcome, Outcome::Holds, "\n{}\n", report.explain(r));
    }

    /// Every state the model reaches.
    pub fn reachable(model: &WorkItem) -> Vec<Item> {
        let mut seen: Vec<Item> = model.init_states();
        let mut known: std::collections::HashSet<Item> = seen.iter().cloned().collect();
        let mut i = 0;
        while let Some(s) = seen.get(i).cloned() {
            let mut steps = Vec::new();
            model.actions(&s, &mut steps);
            for step in steps {
                if let Some(n) = model.next_state(&s, step)
                    && known.insert(n.clone())
                {
                    seen.push(n);
                }
            }
            i += 1;
        }
        seen
    }
}

const ALL: [&str; 7] = [
    ONE_LEASE,
    REFUSED,
    ONCE,
    DEAD,
    ENDS,
    RETRY_ACKED,
    REACHES_DEAD,
];

#[test]
fn every_rule_holds_for_the_engine_as_it_is() {
    for cap in [2, 3] {
        let report = check(WorkItem::new(max(cap))).unwrap();
        assert_eq!(report.rules().len(), ALL.len());
        for rule in ALL {
            assert_holds(&report, rule);
        }
        assert!(
            report.failures().is_empty(),
            "cap {cap}:\n{}",
            report.failures()
        );
    }
}

#[test]
fn with_one_attempt_nothing_is_retried_and_every_other_rule_holds() {
    let report = check(WorkItem::new(max(1))).unwrap();
    for rule in [ONE_LEASE, REFUSED, ONCE, DEAD, ENDS, REACHES_DEAD] {
        assert_holds(&report, rule);
    }
    let r = report.rule(RETRY_ACKED).unwrap();
    assert_eq!((r.kind, r.outcome), (Kind::Sometimes, Outcome::Broken));
}

#[test]
fn the_shortest_way_to_dead_fails_every_attempt() {
    let report = check(WorkItem::new(max(2))).unwrap();
    let r = report.rule(REACHES_DEAD).unwrap();
    let path = r.path.as_ref().unwrap();
    let steps: Vec<Step> = path.iter().filter_map(|(_, s)| *s).collect();
    assert_eq!(
        steps,
        [
            Step::Publish,
            Step::Pull(Consumer::One),
            Step::Fail(Consumer::One),
            Step::Retry,
            Step::Pull(Consumer::One),
            Step::Fail(Consumer::One),
        ],
        "\n{}\n",
        report.explain(r)
    );
}

#[test]
fn an_ack_accepted_on_an_expired_lease_is_found_in_four_steps() {
    let report = check(WorkItem::ack_on_expired_lease(max(2))).unwrap();
    let r = report.rule(REFUSED).unwrap();
    assert_eq!((r.kind, r.outcome), (Kind::Always, Outcome::Broken));
    let path = r.path.as_ref().unwrap();
    let steps: Vec<Step> = path.iter().filter_map(|(_, s)| *s).collect();
    assert_eq!(
        steps,
        [
            Step::Publish,
            Step::Pull(Consumer::One),
            Step::LeaseRunsOut(1),
            Step::Ack(Consumer::One),
        ],
        "\n{}\n",
        report.explain(r)
    );
    assert_eq!(
        report.explain(r),
        "always: an ack or extend with a lease that is not live is refused: broken \
         (ARCHITECTURE.md, Work items, Extend and Ack)\n\
         Shortest path that breaks it, 4 steps:\n  \
         start: Not published yet\n  \
         1. the producer publishes it (work_publish) -> Waiting to be claimed\n  \
         2. a consumer claims it with a lease (work_pull) -> Leased to a consumer\n  \
         3. the lease runs out (the clock) -> Its lease has run out, and the timer has not yet expired it\n  \
         4. its holder acks it (work_ack) -> Acked, done"
    );
    // The seeded bug breaks that rule and no other.
    for rule in [ONE_LEASE, ONCE, DEAD, ENDS, RETRY_ACKED, REACHES_DEAD] {
        assert_holds(&report, rule);
    }
}

#[test]
fn a_call_on_a_lease_that_is_not_live_changes_nothing() {
    let model = WorkItem::new(max(2));
    let mut refused = 0;
    for s in reachable(&model) {
        for c in Consumer::ALL {
            let Some(lease) = s.held[match c {
                Consumer::One => 0,
                Consumer::Two => 1,
            }] else {
                continue;
            };
            if live(&s, lease, c) {
                continue;
            }
            for step in [
                Step::Ack(c),
                Step::AckLost(c),
                Step::Extend(c),
                Step::Fail(c),
            ] {
                assert!(
                    model.next_state(&s, step).is_none(),
                    "{step:?} accepted on lease {lease} in {s:?}"
                );
                refused += 1;
            }
        }
    }
    // The check above is not empty: stale leases are presented.
    assert!(refused > 0);
}

#[test]
fn the_engine_remembers_what_a_rebuild_from_the_topics_gives() {
    let model = WorkItem::new(max(3));
    for s in reachable(&model) {
        let Some(memory) = s.memory else { continue };
        let forget = |p: Phase| match p {
            Phase::Leased { lease, by, .. } => Phase::Leased {
                lease,
                by,
                remembered: false,
            },
            other => other,
        };
        assert_eq!(forget(memory), replay(&s), "{s:?}");
    }
}

#[test]
fn every_loop_is_bounded() {
    // The check ends, and every path does: no state can be reached from
    // itself.
    let model = WorkItem::new(max(3));
    let states = reachable(&model);
    let report = check(model.clone()).unwrap();
    assert_eq!(report.states(), states.len());
    for s in &states {
        let mut steps = Vec::new();
        model.actions(s, &mut steps);
        for step in steps {
            if let Some(n) = model.next_state(s, step) {
                assert_ne!(&n, s, "{step:?} leaves {s:?} as it was");
            }
        }
    }
}

#[test]
fn every_rule_cites_a_section() {
    let model = WorkItem::new(max(2));
    for p in model.properties() {
        let cites = model.cites(p.name).unwrap();
        assert!(
            cites.starts_with("ARCHITECTURE.md, ") || cites.starts_with("README, "),
            "{}: {cites}",
            p.name
        );
    }
}

#[test]
fn every_step_names_what_takes_it() {
    let model = WorkItem::new(max(2));
    let takers = [
        "(work_publish)",
        "(work_pull)",
        "(work_extend)",
        "(work_ack)",
        "(work_fail)",
        "(tick, expire)",
        "(tick, advance)",
        "(recover, replay)",
        "(the clock)",
    ];
    for s in reachable(&model) {
        let mut steps = Vec::new();
        model.actions(&s, &mut steps);
        for step in steps {
            let label = model.step_label(&s, &step);
            assert!(
                takers.iter().any(|t| label.ends_with(t)),
                "\"{label}\" names nothing that takes it"
            );
        }
    }
}

#[test]
fn labels_are_in_the_readers_words() {
    let names = [
        "PublishLost",
        "PullLost",
        "AckLost",
        "LeaseRunsOut",
        "Resend",
        "Leased {",
        "Backoff",
        "Claimed",
        "Extended",
        "Producer",
        "WorkItem",
        "Consumer",
        "::",
    ];
    for model in [
        WorkItem::new(max(2)),
        WorkItem::ack_on_expired_lease(max(2)),
    ] {
        for s in reachable(&model) {
            let mut labels = vec![model.state_label(&s)];
            let mut steps = Vec::new();
            model.actions(&s, &mut steps);
            labels.extend(steps.iter().map(|step| model.step_label(&s, step)));
            for label in labels {
                for name in names {
                    assert!(!label.contains(name), "label \"{label}\" names {name}");
                }
            }
        }
    }
}
