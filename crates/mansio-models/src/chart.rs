//! The Mermaid exporter: the reachable graph as a `stateDiagram-v2` in the
//! reader's words.
//!
//! One node per state label, so states that read the same are one node, and
//! one arrow per pair of nodes, labelled with every step that takes an item
//! from one to the other, joined by "or". A node where every state behind it
//! breaks an `always` rule is drawn red; one where only some do is drawn red
//! and dashed. The bug path, the shortest path that breaks the first broken
//! rule, is drawn blue, and its steps carry their number.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;

use stateright::Expectation;

use crate::Readable;
use crate::check::{Outcome, Report, numbered};

/// One node: the label and how many of the states behind it break a rule.
struct Node {
    label: String,
    states: usize,
    breaking: usize,
    on_bug_path: bool,
}

/// One arrow: the nodes it joins and its steps, each with its number on the
/// bug path if it has one, in the order the check first took them.
struct Arrow {
    from: usize,
    to: usize,
    steps: Vec<String>,
}

/// The reachable graph of a report's model as Mermaid `stateDiagram-v2`.
pub fn mermaid<M>(report: &Report<M>) -> String
where
    M: Readable,
    M::State: Clone + Debug + Hash + Eq,
    M::Action: Clone + Debug,
{
    let model = report.model();
    let always: Vec<_> = model
        .properties()
        .into_iter()
        .filter(|p| p.expectation == Expectation::Always)
        .collect();
    let bug = report.bug().and_then(|r| r.path.as_ref());
    let bug_states: HashSet<&M::State> = bug
        .map(|p| p.iter().map(|(s, _)| s).collect())
        .unwrap_or_default();
    // Each bug-path step as (from label, to label, step label), in order.
    let bug_steps: Vec<(String, String, String)> = bug
        .map(|p| {
            p.windows(2)
                .filter_map(|w| match w {
                    [(from, Some(step)), (to, _)] => Some((
                        model.state_label(from),
                        model.state_label(to),
                        model.step_label(from, step),
                    )),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();

    let mut nodes: Vec<Node> = Vec::new();
    let mut ids: HashMap<String, usize> = HashMap::new();
    let mut node = |state: &M::State, nodes: &mut Vec<Node>| -> usize {
        let label = model.state_label(state);
        *ids.entry(label.clone()).or_insert_with(|| {
            nodes.push(Node {
                label,
                states: 0,
                breaking: 0,
                on_bug_path: false,
            });
            nodes.len() - 1
        })
    };
    let mut seen: HashSet<M::State> = HashSet::new();
    let mut queue: VecDeque<M::State> = VecDeque::new();
    let mut starts: Vec<usize> = Vec::new();
    let mut arrows: Vec<Arrow> = Vec::new();

    let mut admit =
        |state: M::State, id: usize, nodes: &mut Vec<Node>, queue: &mut VecDeque<M::State>| {
            if !seen.contains(&state) {
                if let Some(n) = nodes.get_mut(id) {
                    n.states += 1;
                    if always.iter().any(|p| !(p.condition)(model, &state)) {
                        n.breaking += 1;
                    }
                    if bug_states.contains(&state) {
                        n.on_bug_path = true;
                    }
                }
                seen.insert(state.clone());
                queue.push_back(state);
            }
        };
    for s in model.init_states() {
        let id = node(&s, &mut nodes);
        if !starts.contains(&id) {
            starts.push(id);
        }
        admit(s, id, &mut nodes, &mut queue);
    }
    let mut steps = Vec::new();
    while let Some(s) = queue.pop_front() {
        let from = node(&s, &mut nodes);
        model.actions(&s, &mut steps);
        for a in steps.drain(..) {
            let label = model.step_label(&s, &a);
            let Some(next) = model.next_state(&s, a) else {
                continue;
            };
            if !model.within_boundary(&next) {
                continue;
            }
            let to = node(&next, &mut nodes);
            let key = (model.state_label(&s), model.state_label(&next), label);
            let text = match bug_steps.iter().position(|b| *b == key) {
                Some(i) => format!("{}. {}", i + 1, key.2),
                None => key.2,
            };
            match arrows.iter_mut().find(|a| a.from == from && a.to == to) {
                Some(arrow) => {
                    if !arrow.steps.contains(&text) {
                        arrow.steps.push(text);
                    }
                }
                None => arrows.push(Arrow {
                    from,
                    to,
                    steps: vec![text],
                }),
            }
            admit(next, to, &mut nodes, &mut queue);
        }
    }

    let mut out = String::from("stateDiagram-v2\n");
    for (i, n) in nodes.iter().enumerate() {
        out.push_str(&format!("    s{i} : {}\n", escape(&n.label)));
    }
    for i in &starts {
        out.push_str(&format!("    [*] --> s{i}\n"));
    }
    for arrow in &arrows {
        let text: Vec<String> = arrow.steps.iter().map(|s| escape(s)).collect();
        out.push_str(&format!(
            "    s{} --> s{} : {}\n",
            arrow.from,
            arrow.to,
            text.join(" or ")
        ));
    }
    let class = |n: &Node| {
        if n.breaking > 0 && n.breaking == n.states {
            Some("breaksAll")
        } else if n.breaking > 0 {
            Some("breaksSome")
        } else if n.on_bug_path {
            Some("bugPath")
        } else {
            None
        }
    };
    let used: Vec<(usize, &'static str)> = nodes
        .iter()
        .enumerate()
        .filter_map(|(i, n)| class(n).map(|c| (i, c)))
        .collect();
    for (name, style) in [
        (
            "breaksAll",
            "fill:#ffecee,stroke:#e72e3b,stroke-width:3px,color:#cd2834",
        ),
        (
            "breaksSome",
            "fill:#fff4f5,stroke:#e72e3b,stroke-width:2px,stroke-dasharray:6,color:#cd2834",
        ),
        ("bugPath", "stroke:#1680f3,stroke-width:3px"),
    ] {
        if used.iter().any(|(_, c)| *c == name) {
            out.push_str(&format!("    classDef {name} {style}\n"));
        }
    }
    for (i, c) in used {
        out.push_str(&format!("    class s{i} {c}\n"));
    }
    out
}

/// The chart file for a model: a title, one line per rule with its outcome,
/// the path of each broken rule, and the chart in a mermaid block GitHub
/// renders.
pub fn chart_markdown<M>(report: &Report<M>) -> String
where
    M: Readable,
    M::State: Clone + Debug + Hash + Eq,
    M::Action: Clone + Debug,
{
    let model = report.model();
    let mut out = format!("# {}\n\n", model.title());
    out.push_str(
        "Checked by `cargo test -p mansio-models`, which fails when this file is stale; \
         `UPDATE_CHARTS=1 cargo test -p mansio-models` redraws it.\n\n",
    );
    out.push_str("## Rules\n\n");
    for r in report.rules() {
        out.push_str(&format!(
            "- {}: {}: **{}** ({})\n",
            r.kind, r.name, r.outcome, r.cites
        ));
    }
    for r in report
        .rules()
        .iter()
        .filter(|r| r.outcome == Outcome::Broken)
    {
        out.push_str(&format!("\n\"{}\" is broken.", r.name));
        if let Some(path) = &r.path {
            out.push_str(" The shortest path that breaks it:\n\n```text");
            out.push_str(&numbered(model, path));
            out.push_str("\n```\n");
        } else {
            out.push_str(" No reachable state shows it.\n");
        }
    }
    out.push_str(&format!(
        "\n## Chart\n\n{} states checked. An arrow lists every step that takes an item \
         from one state to the other.",
        report.states()
    ));
    if report.bug().is_some() {
        out.push_str(
            " Red: every item in that state breaks a rule. Red and dashed: some items in it do. \
             Blue: the shortest path to the first broken rule, its steps numbered.",
        );
    }
    out.push_str("\n\n");
    out.push_str("```mermaid\n");
    out.push_str(&mermaid(report));
    out.push_str("```\n");
    out
}

/// Mermaid reads `:` `;` `#` and quotes as syntax in a label.
fn escape(text: &str) -> String {
    text.replace([':', ';'], ",")
        .replace('#', "no. ")
        .replace('"', "'")
}
