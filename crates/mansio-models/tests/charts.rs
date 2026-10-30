//! The charts under charts/ match the models. `UPDATE_CHARTS=1 cargo test -p
//! mansio-models` redraws them.

use std::path::PathBuf;

use mansio_models::{Readable, check};
use stateright::{Model, Property};

use helpers::{charts, node};

/// The tests' helpers, under `#[cfg(test)]` (always set in a test target) so
/// that, like a `#[test]` function, they may unwrap (clippy.toml).
#[cfg(test)]
mod helpers {
    use std::num::NonZeroU8;

    use mansio_models::work_item::WorkItem;
    use mansio_models::{chart_markdown, check};

    /// The id of the node a chart draws with `label`.
    pub fn node(chart: &str, label: &str) -> String {
        chart
            .lines()
            .find_map(|l| {
                l.trim()
                    .strip_suffix(&format!(" : {label}"))
                    .map(String::from)
            })
            .unwrap()
    }

    /// Every chart: its file name under charts/ and its text. A new model adds
    /// its line here.
    pub fn charts() -> Vec<(&'static str, String)> {
        let max = NonZeroU8::new(2).unwrap();
        vec![
            (
                "work-item",
                chart_markdown(&check(WorkItem::new(max)).unwrap()),
            ),
            (
                "work-item-ack-on-expired-lease",
                chart_markdown(&check(WorkItem::ack_on_expired_lease(max)).unwrap()),
            ),
        ]
    }
}

#[test]
fn charts_are_current() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("charts");
    let update = std::env::var("UPDATE_CHARTS").is_ok_and(|v| v == "1");
    for (name, text) in charts() {
        let path = dir.join(format!("{name}.md"));
        if update {
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(&path, &text).unwrap();
            continue;
        }
        let on_disk = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            on_disk == text,
            "{} is stale; run UPDATE_CHARTS=1 cargo test -p mansio-models\n\nexpected:\n{text}",
            path.display()
        );
    }
}

#[test]
fn charts_stay_readable() {
    for (name, text) in charts() {
        assert!(text.starts_with("# "), "{name} has no title");
        assert!(text.contains("\n## Rules\n"), "{name} lists no rules");
        assert!(
            text.contains("```mermaid\nstateDiagram-v2\n"),
            "{name} has no mermaid block"
        );
        let arrows: Vec<&str> = text
            .lines()
            .filter(|l| l.contains(" --> ") && !l.contains("[*]"))
            .collect();
        assert!(
            arrows.len() <= 20,
            "{name} has {} arrows; a reader takes in about 20",
            arrows.len()
        );
        // One arrow per pair of states.
        let mut seen = std::collections::HashSet::new();
        for line in arrows {
            let (ends, _) = line.split_once(" : ").unwrap();
            assert!(seen.insert(ends.trim()), "{name} draws twice: {line}");
        }
    }
}

#[test]
fn every_rule_has_a_line_with_its_outcome() {
    for (name, text) in charts() {
        let rules = text
            .lines()
            .filter(|l| {
                l.starts_with("- ") && (l.contains("**holds**") || l.contains("**broken**"))
            })
            .count();
        assert_eq!(rules, 7, "{name}");
    }
}

#[test]
fn the_seeded_bug_is_marked() {
    let max = std::num::NonZeroU8::new(2).unwrap();
    let report = check(mansio_models::work_item::WorkItem::ack_on_expired_lease(
        max,
    ))
    .unwrap();
    let chart = mansio_models::mermaid(&report);
    // Acked holds items acked on a live lease and the one acked on a lease
    // that had run out.
    let acked = node(&chart, "Acked, done");
    assert!(
        chart.contains(&format!("class {acked} breaksSome\n")),
        "{chart}"
    );
    for label in [
        "Not published yet",
        "Waiting to be claimed",
        "Leased to a consumer",
        "Its lease has run out, and the timer has not yet expired it",
    ] {
        let id = node(&chart, label);
        assert!(chart.contains(&format!("class {id} bugPath\n")), "{chart}");
    }
    let ran_out = node(
        &chart,
        "Its lease has run out, and the timer has not yet expired it",
    );
    assert!(
        chart.contains(&format!(
            "{ran_out} --> {acked} : 4. its holder acks it (work_ack)"
        )),
        "{chart}"
    );
    let fixed =
        mansio_models::mermaid(&check(mansio_models::work_item::WorkItem::new(max)).unwrap());
    assert!(!fixed.contains("class "), "{fixed}");
}

/// A walk of three states whose last breaks an `always` rule in every state
/// behind its label.
#[derive(Clone)]
struct Walk;

const NOT_TOO_FAR: &str = "the walk never goes too far";

impl Model for Walk {
    type State = u8;
    type Action = ();

    fn init_states(&self) -> Vec<u8> {
        vec![0]
    }

    fn actions(&self, s: &u8, out: &mut Vec<()>) {
        if *s < 2 {
            out.push(());
        }
    }

    fn next_state(&self, s: &u8, (): ()) -> Option<u8> {
        Some(s + 1)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![Property::always(NOT_TOO_FAR, |_, s: &u8| *s < 2)]
    }
}

impl Readable for Walk {
    fn title(&self) -> String {
        "A walk".to_string()
    }

    fn state_label(&self, s: &u8) -> String {
        match s {
            0 => "Home",
            1 => "Down the road",
            _ => "Too far",
        }
        .to_string()
    }

    fn step_label(&self, _: &u8, (): &()) -> String {
        "a step".to_string()
    }

    fn cites(&self, rule: &str) -> Option<&'static str> {
        (rule == NOT_TOO_FAR).then_some("a seeded model")
    }
}

#[test]
fn a_state_every_item_in_which_breaks_is_marked_apart() {
    let chart = mansio_models::mermaid(&check(Walk).unwrap());
    let too_far = node(&chart, "Too far");
    assert!(
        chart.contains(&format!("class {too_far} breaksAll\n")),
        "{chart}"
    );
    assert!(chart.contains("classDef breaksAll "), "{chart}");
    assert!(!chart.contains("breaksSome"), "{chart}");
}

/// The walk's rule, uncited.
#[derive(Clone)]
struct Uncited;

impl Model for Uncited {
    type State = u8;
    type Action = ();

    fn init_states(&self) -> Vec<u8> {
        vec![0]
    }

    fn actions(&self, _: &u8, _: &mut Vec<()>) {}

    fn next_state(&self, _: &u8, (): ()) -> Option<u8> {
        None
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![Property::always(NOT_TOO_FAR, |_, _| true)]
    }
}

impl Readable for Uncited {
    fn title(&self) -> String {
        "Uncited".to_string()
    }

    fn state_label(&self, _: &u8) -> String {
        "Home".to_string()
    }

    fn step_label(&self, _: &u8, (): &()) -> String {
        "a step".to_string()
    }

    fn cites(&self, _: &str) -> Option<&'static str> {
        None
    }
}

#[test]
fn a_rule_that_cites_nothing_is_refused() {
    let error = check(Uncited).err().unwrap();
    assert_eq!(
        error.to_string(),
        "the rule \"the walk never goes too far\" cites no section"
    );
}
