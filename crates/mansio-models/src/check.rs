//! The check helper: a single-threaded breadth-first check that keeps, for each rule, the first hit in BFS
//! order, which is the shortest path to it.
//!
//! Stateright 0.31's `Checker::discovery()` is not used: it replaces an `always` or `sometimes` discovery
//! with every later hit, so it returns the last path BFS reaches. A [`CheckerVisitor`] sees every state's
//! path in BFS order instead, and the first one that hits a rule is kept. Stateright also stops expanding
//! once every property has a discovery, so the checker is given one property of its own that is never
//! discovered, an `always` that holds everywhere, and the model's rules are judged by the visitor alone:
//! every reachable state is visited. (An `eventually` that never holds would not do: Stateright
//! discovers it at the first state with no step out, and stops there.)
//!
//! An `eventually` rule breaks on a path that ends (a state with no step out) without the condition
//! holding on any state along it. As in Stateright, each state is judged on the first path that reaches
//! it, and a model with a loop must bound it, since a loop never ends.

use std::fmt;
use std::hash::Hash;
use std::sync::{Arc, Mutex, PoisonError};

use stateright::{Checker, CheckerVisitor, Expectation, Model, Path, Property};

use crate::Readable;

/// The path to a state: each state with the step taken out of it, `None` on the last.
pub type Steps<M> = Vec<(<M as Model>::State, Option<<M as Model>::Action>)>;

/// The three kinds of rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Holds in every reachable state.
    Always,
    /// Holds in at least one reachable state.
    Sometimes,
    /// Holds somewhere on every path that ends.
    Eventually,
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Kind::Always => "always",
            Kind::Sometimes => "sometimes",
            Kind::Eventually => "eventually",
        })
    }
}

/// Whether a rule held.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Holds,
    Broken,
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Outcome::Holds => "holds",
            Outcome::Broken => "broken",
        })
    }
}

/// Why a check gave no report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckError {
    /// A rule names no section it comes from.
    Uncited { rule: &'static str },
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CheckError::Uncited { rule } => write!(f, "the rule \"{rule}\" cites no section"),
        }
    }
}

impl std::error::Error for CheckError {}

/// One rule's result.
pub struct RuleReport<M: Model> {
    pub name: &'static str,
    pub kind: Kind,
    /// The section of ARCHITECTURE.md or the README the rule comes from.
    pub cites: &'static str,
    pub outcome: Outcome,
    /// The shortest path that breaks the rule, or for a `sometimes` rule that holds, the shortest example.
    pub path: Option<Steps<M>>,
}

/// Every rule's result for one model.
pub struct Report<M: Model> {
    model: M,
    rules: Vec<RuleReport<M>>,
    states: usize,
}

impl<M: Readable> Report<M> {
    pub fn model(&self) -> &M {
        &self.model
    }

    pub fn rules(&self) -> &[RuleReport<M>] {
        &self.rules
    }

    pub fn rule(&self, name: &str) -> Option<&RuleReport<M>> {
        self.rules.iter().find(|r| r.name == name)
    }

    /// Distinct states the check visited.
    pub fn states(&self) -> usize {
        self.states
    }

    /// The first broken `always` or `eventually` rule: the one whose path the chart marks.
    pub fn bug(&self) -> Option<&RuleReport<M>> {
        self.rules
            .iter()
            .find(|r| r.kind != Kind::Sometimes && r.outcome == Outcome::Broken)
    }

    /// A rule's result in the reader's words, with its path numbered.
    pub fn explain(&self, rule: &RuleReport<M>) -> String {
        let mut out = format!(
            "{}: {}: {} ({})",
            rule.kind, rule.name, rule.outcome, rule.cites
        );
        if let Some(path) = &rule.path {
            let what = match (rule.kind, rule.outcome) {
                (Kind::Sometimes, Outcome::Holds) => "Shortest example",
                _ => "Shortest path that breaks it",
            };
            let steps = path.len().saturating_sub(1);
            out.push_str(&format!(
                "\n{what}, {steps} step{}:",
                if steps == 1 { "" } else { "s" }
            ));
            out.push_str(&numbered(&self.model, path));
        } else if rule.kind == Kind::Sometimes {
            out.push_str("\nNo reachable state shows it.");
        }
        out
    }

    /// Every rule that did not hold, explained; empty when all hold.
    pub fn failures(&self) -> String {
        let broken: Vec<String> = self
            .rules
            .iter()
            .filter(|r| r.outcome == Outcome::Broken)
            .map(|r| self.explain(r))
            .collect();
        broken.join("\n\n")
    }
}

/// A path as numbered lines: where it starts, then each step and where it lands.
pub fn numbered<M: Readable>(model: &M, path: &Steps<M>) -> String {
    let mut out = String::new();
    if let Some((first, _)) = path.first() {
        out.push_str(&format!("\n  start: {}", model.state_label(first)));
    }
    for (i, pair) in path.windows(2).enumerate() {
        if let [(from, Some(step)), (to, _)] = pair {
            out.push_str(&format!(
                "\n  {}. {} -> {}",
                i + 1,
                model.step_label(from, step),
                model.state_label(to)
            ));
        }
    }
    out
}

/// Checks every rule of `model` over every reachable state.
pub fn check<M>(model: M) -> Result<Report<M>, CheckError>
where
    M: Readable + Clone + Send + Sync + 'static,
    M::State: Clone + fmt::Debug + Hash + Eq + Send + Sync + 'static,
    M::Action: Clone + fmt::Debug + PartialEq + Send + Sync + 'static,
{
    let props = model.properties();
    let mut cited = Vec::with_capacity(props.len());
    for p in &props {
        let cites = model
            .cites(p.name)
            .ok_or(CheckError::Uncited { rule: p.name })?;
        cited.push((p.name, kind(p), cites));
    }
    let hits: Arc<Mutex<Vec<Option<Steps<M>>>>> = Arc::new(Mutex::new(vec![None; props.len()]));
    let checker = Explore(model.clone())
        .checker()
        .threads(1)
        .visitor(FirstHits {
            rules: props,
            hits: Arc::clone(&hits),
        })
        .spawn_bfs()
        .join();
    let states = checker.unique_state_count();
    let found = std::mem::take(&mut *hits.lock().unwrap_or_else(PoisonError::into_inner));
    let rules = cited
        .into_iter()
        .zip(found)
        .map(|((name, kind, cites), path)| {
            let outcome = match (kind, path.is_some()) {
                (Kind::Sometimes, true) | (Kind::Always | Kind::Eventually, false) => {
                    Outcome::Holds
                }
                _ => Outcome::Broken,
            };
            RuleReport {
                name,
                kind,
                cites,
                outcome,
                path,
            }
        })
        .collect();
    Ok(Report {
        model,
        rules,
        states,
    })
}

fn kind<M: Model>(p: &Property<M>) -> Kind {
    match p.expectation {
        Expectation::Always => Kind::Always,
        Expectation::Sometimes => Kind::Sometimes,
        Expectation::Eventually => Kind::Eventually,
    }
}

/// Whether no step leads out of `state`.
fn ends<M: Model>(model: &M, state: &M::State) -> bool {
    let mut steps = Vec::new();
    model.actions(state, &mut steps);
    !steps.into_iter().any(|a| {
        model
            .next_state(state, a)
            .is_some_and(|n| model.within_boundary(&n))
    })
}

/// The model as Stateright explores it, with one property that is never discovered, so no state goes
/// unvisited.
#[derive(Clone)]
struct Explore<M>(M);

impl<M: Model> Model for Explore<M> {
    type State = M::State;
    type Action = M::Action;

    fn init_states(&self) -> Vec<Self::State> {
        self.0.init_states()
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        self.0.actions(state, actions);
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        self.0.next_state(state, action)
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        self.0.within_boundary(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![Property::always("every state is visited", |_, _| true)]
    }
}

/// Keeps the first path, in BFS order, that hits each rule.
struct FirstHits<M: Model> {
    /// The model's rules, built once.
    rules: Vec<Property<M>>,
    hits: Arc<Mutex<Vec<Option<Steps<M>>>>>,
}

impl<M> CheckerVisitor<Explore<M>> for FirstHits<M>
where
    M: Model,
    M::State: Clone,
    M::Action: Clone,
{
    fn visit(&self, model: &Explore<M>, path: Path<M::State, M::Action>) {
        let model = &model.0;
        let steps = path.into_vec();
        let Some((last, _)) = steps.last() else {
            return;
        };
        let mut hits = self.hits.lock().unwrap_or_else(PoisonError::into_inner);
        for (p, hit) in self.rules.iter().zip(hits.iter_mut()) {
            if hit.is_some() {
                continue;
            }
            let found = match p.expectation {
                Expectation::Always => !(p.condition)(model, last),
                Expectation::Sometimes => (p.condition)(model, last),
                Expectation::Eventually => {
                    ends(model, last) && !steps.iter().any(|(s, _)| (p.condition)(model, s))
                }
            };
            if found {
                *hit = Some(steps.clone());
            }
        }
    }
}
