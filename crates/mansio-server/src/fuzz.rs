//! Entry points for the fuzz targets in `fuzz/`, behind the `fuzzing` feature.
//!
//! Each function takes the bytes of one input that the server decodes from
//! outside itself and says whether it was accepted. None may panic or
//! allocate without bound, whatever the bytes are: a request body arrives from
//! any caller, and a record is replayed from a log that a restart reads whole.
//!
//! [`TARGETS`] lists them under the names of the fuzz targets. The tests
//! replay `fuzz/corpus/<target>/` (the seeds) and `fuzz/regressions/<target>/`
//! (an input that once crashed a target) through the same functions, so a
//! crash that is fixed stays fixed under `cargo test`.

use std::str::FromStr;

use mansio_core::api::{
    AckRequest, CommitRequest, CreatePrincipal, CreateWebhook, ExtendRequest, FailRequest,
    PullRequest,
};
use mansio_core::{AlertFact, Data, Envelope, Principal, TenantId, Timestamp, Topic, WorkFact};
use serde::de::DeserializeOwned;

use crate::app::{Origin, build_envelope, parse_body};

/// A fuzz target: its name, which is also the name of its directory of seeds,
/// and the function that decodes one input.
pub type Target = (&'static str, fn(&[u8]) -> bool);

/// Every fuzz target.
pub const TARGETS: &[Target] = &[
    ("envelope", envelope),
    ("body_publish", publish_body),
    ("body_work_item", work_item_body),
    ("body_principal", principal_body),
    ("body_webhook", webhook_body),
    ("body_commit", commit_body),
    ("body_pull", pull_body),
    ("body_extend", extend_body),
    ("body_ack", ack_body),
    ("body_fail", fail_body),
    ("record_principal", principal_record),
    ("record_cursor", cursor_record),
    ("record_webhook", webhook_record),
    ("record_idempotency", idempotency_record),
    ("record_ownership", ownership_record),
    ("record_snapshot", snapshot),
    ("record_work_fact", work_fact),
    ("record_alert_fact", alert_fact),
];

/// An envelope as stored and published: decoded, validated and written back.
#[must_use]
pub fn envelope(bytes: &[u8]) -> bool {
    let Ok(envelope) = Envelope::from_json_slice(bytes) else {
        return false;
    };
    envelope.validate().is_ok() && envelope.to_json_vec().is_ok()
}

/// A body to `POST /v1/tenants/{tenant}/topics/{topic}/messages` or to the
/// alerts route: an envelope without the attributes the server sets.
#[must_use]
pub fn publish_body(bytes: &[u8]) -> bool {
    envelope_body(bytes, Origin::Message)
}

/// A body to `POST /v1/tenants/{tenant}/work/{queue}/items`.
#[must_use]
pub fn work_item_body(bytes: &[u8]) -> bool {
    envelope_body(bytes, Origin::WorkItem)
}

fn envelope_body(bytes: &[u8], origin: Origin) -> bool {
    let (Ok(tenant), Ok(topic), Ok(principal), Ok(now)) = (
        TenantId::from_str("acme"),
        Topic::from_str("facts.builds"),
        Principal::from_str("agent:fuzz"),
        Timestamp::from_unix_millis(1_700_000_000_000),
    ) else {
        return false;
    };
    build_envelope(bytes, &tenant, &topic, &principal, &now, origin).is_ok()
}

fn body<T: DeserializeOwned>(bytes: &[u8]) -> bool {
    parse_body::<T>(bytes).is_ok()
}

/// A body to `POST /v1/principals`.
#[must_use]
pub fn principal_body(bytes: &[u8]) -> bool {
    body::<CreatePrincipal>(bytes)
}

/// A body to `POST /v1/webhooks`.
#[must_use]
pub fn webhook_body(bytes: &[u8]) -> bool {
    body::<CreateWebhook>(bytes)
}

/// A body to `.../consumers/{consumer}/commit`.
#[must_use]
pub fn commit_body(bytes: &[u8]) -> bool {
    body::<CommitRequest>(bytes)
}

/// A body to `.../work/{queue}/pull`.
#[must_use]
pub fn pull_body(bytes: &[u8]) -> bool {
    body::<PullRequest>(bytes)
}

/// A body to `.../items/{item}/extend`.
#[must_use]
pub fn extend_body(bytes: &[u8]) -> bool {
    body::<ExtendRequest>(bytes)
}

/// A body to `.../items/{item}/ack`.
#[must_use]
pub fn ack_body(bytes: &[u8]) -> bool {
    body::<AckRequest>(bytes)
}

/// A body to `.../items/{item}/fail`.
#[must_use]
pub fn fail_body(bytes: &[u8]) -> bool {
    body::<FailRequest>(bytes)
}

/// A record of the principals log.
#[must_use]
pub fn principal_record(bytes: &[u8]) -> bool {
    crate::registry::Entry::decode(bytes).is_ok()
}

/// A record of the cursors log.
#[must_use]
pub fn cursor_record(bytes: &[u8]) -> bool {
    crate::cursors::fuzz_record(bytes)
}

/// A record of the webhooks log.
#[must_use]
pub fn webhook_record(bytes: &[u8]) -> bool {
    crate::webhooks::fuzz_record(bytes)
}

/// A record of the idempotency log.
#[must_use]
pub fn idempotency_record(bytes: &[u8]) -> bool {
    crate::records::decode::<crate::idempotency::Entry>(bytes).is_ok()
}

/// A line of the owners log.
#[must_use]
pub fn ownership_record(bytes: &[u8]) -> bool {
    crate::ownership::fuzz_record(bytes)
}

/// A snapshot of a tenant's engine state.
#[must_use]
pub fn snapshot(bytes: &[u8]) -> bool {
    let Ok(tenant) = TenantId::from_str("acme") else {
        return false;
    };
    crate::snapshot::Snapshot::decode(&tenant, bytes).is_ok()
}

/// The `data` of a message on `facts.work`, as the replay reads it.
#[must_use]
pub fn work_fact(bytes: &[u8]) -> bool {
    fact::<WorkFact>(bytes)
}

/// The `data` of a message on `facts.alert`, as the replay reads it.
#[must_use]
pub fn alert_fact(bytes: &[u8]) -> bool {
    fact::<AlertFact>(bytes)
}

fn fact<T: DeserializeOwned>(bytes: &[u8]) -> bool {
    let Ok(data) = serde_json::from_slice::<Data>(bytes) else {
        return false;
    };
    data.parse::<T>().is_ok()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use super::*;

    fn root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz")
    }

    fn inputs(kind: &str, target: &str) -> Vec<(PathBuf, Vec<u8>)> {
        let Ok(entries) = fs::read_dir(root().join(kind).join(target)) else {
            return Vec::new();
        };
        entries
            .map(|entry| {
                let path = entry.unwrap().path();
                let bytes = fs::read(&path).unwrap();
                (path, bytes)
            })
            .collect()
    }

    #[test]
    fn every_target_has_seeds() {
        for (name, _) in TARGETS {
            assert!(
                !inputs("corpus", name).is_empty(),
                "fuzz/corpus/{name} has no seed"
            );
        }
    }

    #[test]
    fn no_seed_or_regression_input_crashes_its_target() {
        for (name, run) in TARGETS {
            for kind in ["corpus", "regressions"] {
                for (_, bytes) in inputs(kind, name) {
                    let _ = run(&bytes);
                }
            }
        }
    }

    /// A small deterministic stand-in for the fuzzer, so that `cargo test`
    /// catches the plain cases: every seed cut short, and with each byte
    /// replaced or a byte put in front of it by one that breaks JSON.
    #[test]
    fn a_seed_cut_or_corrupted_does_not_crash_its_target() {
        const BREAKERS: [u8; 7] = [0, b'"', b'{', b'-', b'9', 0xff, b'\\'];
        for (name, run) in TARGETS {
            for (path, seed) in inputs("corpus", name) {
                let check = |input: &[u8]| {
                    assert!(
                        std::panic::catch_unwind(|| run(input)).is_ok(),
                        "{name} panicked on {} changed to {:?}",
                        path.display(),
                        String::from_utf8_lossy(input)
                    );
                };
                for at in 0..seed.len() {
                    check(&seed[..at]);
                    for byte in BREAKERS {
                        let mut replaced = seed.clone();
                        replaced[at] = byte;
                        check(&replaced);
                        let mut inserted = seed.clone();
                        inserted.insert(at, byte);
                        check(&inserted);
                    }
                }
            }
        }
    }

    /// The bodies the server reads are cut off at its limit on a body, 1 MiB
    /// by default, so these are the worst a target is given.
    #[test]
    fn deep_and_long_input_does_not_crash_a_target() {
        let deep = "[".repeat(1 << 20);
        let long_number = format!("{{\"offset\":{}}}", "9".repeat(1 << 20));
        let long_string = format!("{{\"consumer\":\"{}\"}}", "a".repeat(1 << 20));
        for (name, run) in TARGETS {
            for input in [&deep, &long_number, &long_string] {
                assert!(
                    std::panic::catch_unwind(|| run(input.as_bytes())).is_ok(),
                    "{name} panicked on a long input"
                );
            }
        }
    }

    #[test]
    fn every_directory_of_seeds_names_a_target() {
        for kind in ["corpus", "regressions"] {
            let Ok(entries) = fs::read_dir(root().join(kind)) else {
                continue;
            };
            for entry in entries {
                let name = entry.unwrap().file_name().to_string_lossy().into_owned();
                assert!(
                    TARGETS.iter().any(|(target, _)| *target == name),
                    "fuzz/{kind}/{name} is not a target"
                );
            }
        }
    }

    #[test]
    fn every_target_is_built_and_run_by_the_workflow() {
        let manifest = fs::read_to_string(root().join("Cargo.toml")).unwrap();
        let workflow = fs::read_to_string(root().join("../.github/workflows/fuzz.yml")).unwrap();
        for (name, _) in TARGETS {
            assert!(
                manifest.contains(&format!(
                    "name = \"{name}\"\npath = \"fuzz_targets/{name}.rs\""
                )),
                "fuzz/Cargo.toml has no [[bin]] for {name}"
            );
            assert!(
                root()
                    .join("fuzz_targets")
                    .join(format!("{name}.rs"))
                    .is_file(),
                "fuzz/fuzz_targets/{name}.rs is missing"
            );
            assert!(
                workflow.contains(&format!("          - {name}\n")),
                "the matrix of fuzz.yml does not run {name}"
            );
        }
    }

    #[test]
    fn a_seed_that_is_valid_is_accepted() {
        // Seeds named `ok*` are inputs the target must accept, so a seed that
        // drifts from the format fails here instead of silently fuzzing
        // nothing.
        for (name, run) in TARGETS {
            for (path, bytes) in inputs("corpus", name) {
                let file = path.file_name().unwrap().to_string_lossy().into_owned();
                if file.starts_with("ok") {
                    assert!(run(&bytes), "{} was refused", path.display());
                }
            }
        }
    }
}
