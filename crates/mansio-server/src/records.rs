//! The records of the server's own logs that carry a format version.
//!
//! A record says which version of its format it was written in, in `v`; one
//! without is version 1, which was written before the field was. A version
//! that changes what a record means adds a number to [`VERSION`] and keeps
//! reading the earlier ones. A record of a later version than this server
//! knows is unreadable, not read as an earlier one: a server that guessed at it
//! could drop a deletion or lose a secret.

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde::de::Error as _;
use serde_json::Value;

use crate::error::ApiError;

/// The version of the record formats written now.
pub(crate) const VERSION: u64 = 1;

/// A record as bytes, with its version in `v`.
///
/// # Errors
///
/// Returns [`ApiError::Internal`] if the record does not serialize to an
/// object, which is a fault in the server.
pub(crate) fn encode<T: Serialize>(record: &T) -> Result<Vec<u8>, ApiError> {
    let mut value = serde_json::to_value(record).map_err(|reason| {
        tracing::error!(%reason, "a record of a log did not serialize");
        ApiError::Internal
    })?;
    let Some(fields) = value.as_object_mut() else {
        tracing::error!("a record of a log did not serialize to an object");
        return Err(ApiError::Internal);
    };
    fields.insert("v".to_owned(), VERSION.into());
    serde_json::to_vec(&value).map_err(|reason| {
        tracing::error!(%reason, "a record of a log did not serialize");
        ApiError::Internal
    })
}

/// A record of any version up to this one.
///
/// # Errors
///
/// Returns an error for a record that is not one, or whose version is 0, not a
/// number, or newer than [`VERSION`].
pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, serde_json::Error> {
    let value: Value = serde_json::from_slice(bytes)?;
    let version = match value.get("v") {
        None => 1,
        Some(v) => v
            .as_u64()
            .ok_or_else(|| serde_json::Error::custom("the version `v` is not a number"))?,
    };
    if version == 0 || version > VERSION {
        return Err(serde_json::Error::custom(format!(
            "version {version} of the record format; this server reads up to {VERSION}"
        )));
    }
    serde_json::from_value(value)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "op", rename_all = "snake_case")]
    enum Sample {
        Made { n: u64 },
    }

    #[test]
    fn a_record_carries_its_version_and_reads_back() {
        let bytes = encode(&Sample::Made { n: 3 }).unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["v"], json!(VERSION));
        assert_eq!(value["op"], "made");
        assert_eq!(decode::<Sample>(&bytes).unwrap(), Sample::Made { n: 3 });
    }

    #[test]
    fn a_record_without_a_version_is_version_one() {
        let sample = decode::<Sample>(br#"{"op":"made","n":4}"#).unwrap();
        assert_eq!(sample, Sample::Made { n: 4 });
    }

    #[test]
    fn a_version_this_server_does_not_know_is_unreadable() {
        for bad in [
            &br#"{"op":"made","n":4,"v":0}"#[..],
            br#"{"op":"made","n":4,"v":2}"#,
            br#"{"op":"made","n":4,"v":"1"}"#,
            br#"{"op":"made","n":4,"v":-1}"#,
            br#"{"op":"made","v":1}"#,
            br#"{"op":"other","v":1}"#,
            b"not json",
            b"",
        ] {
            assert!(
                decode::<Sample>(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }
}
