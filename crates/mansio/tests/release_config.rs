//! release-plz.toml holds only keys release-plz takes. The release run stops at
//! its first step on an unknown key, so a bad key is caught here, on the pull
//! request that adds it.

use std::path::Path;

use helpers::{load, unknown_key};

/// The tests' helpers, under `#[cfg(test)]` (always set in a test target) so
/// that, like a `#[test]` function, they may unwrap (clippy.toml).
#[cfg(test)]
mod helpers {
    use std::path::Path;

    use toml_edit::{DocumentMut, Table};

    /// The parsed release-plz.toml at the repository root.
    pub fn load() -> DocumentMut {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../release-plz.toml");
        let text = std::fs::read_to_string(&path).unwrap();
        text.parse::<DocumentMut>().unwrap()
    }

    /// The first key of `table`, in file order, that is not in `allowed`.
    pub fn unknown_key(table: &Table, allowed: &[&str]) -> Option<String> {
        table
            .iter()
            .map(|(key, _)| key)
            .find(|key| !allowed.contains(key))
            .map(String::from)
    }
}

#[test]
fn top_level_keys_are_known() {
    let doc = load();
    let found = unknown_key(doc.as_table(), &["workspace", "changelog", "package"]);
    assert!(
        found.is_none(),
        "release-plz.toml (top level) has the unknown key `{}`",
        found.unwrap_or_default()
    );
}

// The ten names are release-plz 0.4.3's, the version pinned as
// RELEASE_PLZ_VERSION in .github/workflows/release.yml. A bump of that pin is
// a reason to look at this list.
#[test]
fn changelog_keys_are_known() {
    let doc = load();
    let table = doc["changelog"].as_table().unwrap();
    let found = unknown_key(
        table,
        &[
            "header",
            "body",
            "trim",
            "commit_preprocessors",
            "postprocessors",
            "sort_commits",
            "link_parsers",
            "commit_parsers",
            "protect_breaking_commits",
            "tag_pattern",
        ],
    );
    assert!(
        found.is_none(),
        "release-plz.toml [changelog] has the unknown key `{}`",
        found.unwrap_or_default()
    );
}

#[test]
fn workspace_keys_are_known() {
    let doc = load();
    let table = doc["workspace"].as_table().unwrap();
    let found = unknown_key(
        table,
        &[
            "changelog_update",
            "git_tag_enable",
            "git_release_enable",
            "git_tag_name",
            "semver_check",
            "pr_name",
        ],
    );
    assert!(
        found.is_none(),
        "release-plz.toml [workspace] has the unknown key `{}`",
        found.unwrap_or_default()
    );
}

#[test]
fn package_keys_are_known() {
    let doc = load();
    let packages = doc["package"].as_array_of_tables().unwrap();
    for table in packages {
        let found = unknown_key(
            table,
            &[
                "name",
                "changelog_update",
                "changelog_path",
                "changelog_include",
            ],
        );
        assert!(
            found.is_none(),
            "release-plz.toml [[package]] has the unknown key `{}`",
            found.unwrap_or_default()
        );
    }
}

#[test]
fn the_file_is_where_the_test_looks() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../release-plz.toml");
    assert!(path.is_file(), "{} is not a file", path.display());
}
