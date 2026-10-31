//! Every rule of one work item, checked over every reachable state, a seeded
//! bug that pins the shortest-path helper, and a seeded two-step cycle that
//! the cycle check fails on. A failure prints the numbered shortest path in
//! the reader's words.

use mansio_models::work_item::{
    Consumer, DEAD, ENDS, ONCE, ONE_LEASE, Phase, REACHES_DEAD, REFUSED, RETRY_ACKED, Step,
    WorkItem, live, replay,
};
use mansio_models::{Kind, Outcome, Readable, check};
use stateright::Model;

use helpers::{Seeded, assert_holds, assert_no_cycle, max, reachable};

/// The tests' helpers, under `#[cfg(test)]` (always set in a test target) so
/// that, like a `#[test]` function, they may unwrap (clippy.toml).
#[cfg(test)]
mod helpers {
    use std::collections::{HashMap, HashSet};
    use std::fmt::Debug;
    use std::hash::Hash;
    use std::num::NonZeroU8;

    use mansio_models::work_item::{ENDS, Item, Step, WorkItem, replay};
    use mansio_models::{Outcome, Readable, Report, check};
    use stateright::{Model, Property};

    /// An attempt cap (`MANSIO_MAX_ATTEMPTS`).
    pub fn max(n: u8) -> NonZeroU8 {
        NonZeroU8::new(n).unwrap()
    }

    pub fn assert_holds(report: &Report<WorkItem>, rule: &str) {
        let r = report.rule(rule).unwrap();
        assert_eq!(r.outcome, Outcome::Holds, "\n{}\n", report.explain(r));
    }

    /// Every state the model reaches.
    pub fn reachable<M>(model: &M) -> Vec<M::State>
    where
        M: Model,
        M::State: Clone + Hash + Eq,
    {
        let mut seen: Vec<M::State> = model.init_states();
        let mut known: HashSet<M::State> = seen.iter().cloned().collect();
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

    /// Fails on any cycle in the reachable graph, and checks that the check
    /// visits every reachable state.
    pub fn assert_no_cycle<M>(model: &M)
    where
        M: Readable + Clone + Send + Sync + 'static,
        M::State: Clone + Debug + Hash + Eq + Send + Sync + 'static,
        M::Action: Clone + Debug + PartialEq + Send + Sync + 'static,
    {
        let report = check(model.clone()).unwrap();
        assert_eq!(report.states(), reachable(model).len());
        if let Some(cycle) = find_cycle(model) {
            panic!(
                "{}: a cycle of {} steps: {cycle:#?}",
                model.title(),
                cycle.len()
            );
        }
    }

    /// A cycle among the reachable states, as the states along it, or `None`:
    /// a depth-first search that keeps the path it is on, where a step to a
    /// state on that path closes a cycle.
    pub fn find_cycle<M>(model: &M) -> Option<Vec<M::State>>
    where
        M: Model,
        M::State: Clone + Hash + Eq,
    {
        // `false` while the state is on the path, `true` once every state
        // after it has been searched.
        let mut done: HashMap<M::State, bool> = HashMap::new();
        for init in model.init_states() {
            if done.contains_key(&init) {
                continue;
            }
            done.insert(init.clone(), false);
            let next = successors(model, &init);
            let mut path: Vec<(M::State, Vec<M::State>)> = vec![(init, next)];
            while let Some((_, next)) = path.last_mut() {
                let Some(n) = next.pop() else {
                    let (s, _) = path.pop().unwrap();
                    done.insert(s, true);
                    continue;
                };
                match done.get(&n) {
                    Some(true) => {}
                    Some(false) => {
                        let at = path.iter().position(|(s, _)| *s == n).unwrap();
                        return Some(path.drain(at..).map(|(s, _)| s).collect());
                    }
                    None => {
                        done.insert(n.clone(), false);
                        let after = successors(model, &n);
                        path.push((n, after));
                    }
                }
            }
        }
        None
    }

    fn successors<M: Model>(model: &M, s: &M::State) -> Vec<M::State> {
        let mut steps = Vec::new();
        model.actions(s, &mut steps);
        steps
            .into_iter()
            .filter_map(|step| model.next_state(s, step))
            .collect()
    }

    /// A seeded bug: the engine drops its memory, and rebuilds it from the
    /// topics, without counting a restart, so the two steps make a cycle.
    #[derive(Clone, Debug)]
    pub struct Seeded(pub WorkItem);

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum SeededStep {
        Engine(Step),
        Forget,
        Rebuild,
    }

    impl Model for Seeded {
        type State = Item;
        type Action = SeededStep;

        fn init_states(&self) -> Vec<Item> {
            self.0.init_states()
        }

        fn actions(&self, s: &Item, out: &mut Vec<SeededStep>) {
            let mut steps = Vec::new();
            self.0.actions(s, &mut steps);
            out.extend(steps.into_iter().map(SeededStep::Engine));
            out.extend([SeededStep::Forget, SeededStep::Rebuild]);
        }

        fn next_state(&self, s: &Item, step: SeededStep) -> Option<Item> {
            let mut n = s.clone();
            match step {
                SeededStep::Engine(step) => return self.0.next_state(s, step),
                SeededStep::Forget if s.attempt > 0 && s.memory == Some(replay(s)) => {
                    n.memory = None;
                }
                SeededStep::Rebuild if s.attempt > 0 && s.memory.is_none() => {
                    n.memory = Some(replay(s));
                }
                SeededStep::Forget | SeededStep::Rebuild => return None,
            }
            Some(n)
        }

        fn properties(&self) -> Vec<Property<Self>> {
            // The engine's `eventually` rule, with the same condition.
            vec![Property::eventually(ENDS, |_, s: &Item| {
                s.acks == 1 || s.dead == 1
            })]
        }
    }

    impl Readable for Seeded {
        fn title(&self) -> String {
            format!(
                "{}, with an uncounted rebuild (a seeded bug)",
                self.0.title()
            )
        }

        fn state_label(&self, s: &Item) -> String {
            self.0.state_label(s)
        }

        fn step_label(&self, s: &Item, step: &SeededStep) -> String {
            match step {
                SeededStep::Engine(step) => self.0.step_label(s, step),
                SeededStep::Forget => "the engine drops its memory (recover)".to_string(),
                SeededStep::Rebuild => "the engine rebuilds it, uncounted (recover)".to_string(),
            }
        }

        fn cites(&self, rule: &str) -> Option<&'static str> {
            self.0.cites(rule)
        }
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
fn no_state_is_reachable_from_itself() {
    // The model has no cycle, of one step or of many, so every path ends and
    // the check judges `every item ends acked or dead` on every path: no loop
    // the item could circle forever is hidden from it. The check ends too:
    // it visits every reachable state once.
    for cap in [1, 2, 3] {
        assert_no_cycle(&WorkItem::new(max(cap)));
    }
}

#[test]
#[should_panic(expected = "a cycle of 2 steps")]
fn the_cycle_check_fails_on_a_seeded_two_step_cycle() {
    assert_no_cycle(&Seeded(WorkItem::new(max(2))));
}

#[test]
fn a_seeded_two_step_cycle_hides_from_the_eventually_rule() {
    // The seeded bug: the engine drops its memory and rebuilds it from the
    // topics without counting a restart, so the item can circle between the
    // two forever.
    let model = Seeded(WorkItem::new(max(2)));
    // No step leaves a state as it was, so a check of one-step loops alone
    // misses it.
    for s in reachable(&model) {
        let mut steps = Vec::new();
        model.actions(&s, &mut steps);
        for step in steps {
            assert_ne!(model.next_state(&s, step).as_ref(), Some(&s), "{step:?}");
        }
    }
    // And the check, which judges `eventually` only on paths that end, finds
    // the rule holding: the cycle hides the item that never ends.
    let report = check(model).unwrap();
    let r = report.rule(ENDS).unwrap();
    assert_eq!((r.kind, r.outcome), (Kind::Eventually, Outcome::Holds));
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
