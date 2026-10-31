//! State models of the mansio work engine, checked with Stateright as cargo
//! tests and drawn as Mermaid statecharts.
//!
//! A model is a Stateright [`Model`] whose properties are its rules
//! (`always`, `sometimes`, `eventually`), plus [`Readable`]: a label for every
//! state and step in the reader's words, and the section of ARCHITECTURE.md
//! or the README each rule comes from. [`check`] runs the rules and returns
//! the shortest path for each; [`chart_markdown`] draws the model. The charts
//! are checked in under `charts/`; `UPDATE_CHARTS=1 cargo test -p
//! mansio-models` redraws them.

pub mod chart;
pub mod check;
pub mod work_item;

pub use chart::{chart_markdown, mermaid};
pub use check::{CheckError, Kind, Outcome, Report, RuleReport, Steps, check, numbered};

use stateright::Model;

/// What a model says to a reader who does not read code.
pub trait Readable: Model {
    /// The chart's title.
    fn title(&self) -> String;
    /// A state in the reader's words. States that read the same are one node
    /// in the chart.
    fn state_label(&self, state: &Self::State) -> String;
    /// A step out of `from`, in the reader's words, with the condition in
    /// brackets when one decides the branch, and the engine function that
    /// takes it.
    fn step_label(&self, from: &Self::State, step: &Self::Action) -> String;
    /// The section of ARCHITECTURE.md or the README a rule comes from, by the
    /// rule's name; `None` for a rule that cites nothing, which [`check`]
    /// refuses.
    fn cites(&self, rule: &str) -> Option<&'static str>;
}
