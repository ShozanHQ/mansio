//! The secret a principal presents.

use std::fmt;

use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

/// A bearer credential: the personal access token of the broker user that was
/// made for a principal.
///
/// It never shows in `Debug` output. It serializes as its text because the
/// answer that issues it has to carry it, once; nothing else holds the type in
/// a body.
#[derive(Clone)]
pub struct Credential(SecretString);

impl Credential {
    /// A credential from the text a client presented or a broker minted.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self(SecretString::from(token.into()))
    }

    /// A random secret of 244 bits from the operating system's generator, as
    /// 64 hex digits: for a credential a test broker mints, and for the
    /// password of a broker user that nobody is ever told.
    #[must_use]
    pub fn random() -> Self {
        Self::new(format!(
            "{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        ))
    }

    /// The secret text. Pass it to the broker or to a client and nowhere else.
    #[must_use]
    pub fn expose(&self) -> &str {
        self.0.expose_secret()
    }

    /// A digest to look the credential up by, so that a table of sessions
    /// never holds the secret itself and compares digests, not secrets.
    #[must_use]
    pub fn digest(&self) -> CredentialDigest {
        CredentialDigest(*blake3::hash(self.expose().as_bytes()).as_bytes())
    }
}

impl fmt::Debug for Credential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Credential(..)")
    }
}

impl Serialize for Credential {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.expose())
    }
}

impl<'de> Deserialize<'de> for Credential {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::new)
    }
}

/// What identifies a credential in a table without being one: a BLAKE3 digest
/// of a credential that is itself 244 or more random bits, which no one can
/// turn back into the credential. It is written as 64 lowercase hex digits.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CredentialDigest([u8; 32]);

impl fmt::Debug for CredentialDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CredentialDigest(..)")
    }
}

impl fmt::Display for CredentialDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

impl Serialize for CredentialDigest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for CredentialDigest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        let text = String::deserialize(deserializer)?;
        let invalid = || D::Error::custom("a credential digest is 64 lowercase hex digits");
        if text.len() != 64 || !text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            return Err(invalid());
        }
        let mut bytes = [0_u8; 32];
        for (byte, pair) in bytes.iter_mut().zip(text.as_bytes().chunks(2)) {
            let pair = std::str::from_utf8(pair).map_err(|_| invalid())?;
            *byte = u8::from_str_radix(pair, 16).map_err(|_| invalid())?;
        }
        Ok(Self(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_never_shows_the_secret() {
        let credential = Credential::new("s3cret-token");
        assert!(!format!("{credential:?}").contains("s3cret"));
        assert!(!format!("{:?}", credential.digest()).contains("s3cret"));
    }

    #[test]
    fn the_digest_tells_credentials_apart_and_is_not_the_secret() {
        let a = Credential::new("one");
        assert_eq!(a.digest(), Credential::new("one").digest());
        assert_ne!(a.digest(), Credential::new("two").digest());
        assert_ne!(a.digest(), Credential::new("One").digest());
    }

    #[test]
    fn a_digest_is_written_as_hex_and_read_back() {
        let digest = Credential::new("abc").digest();
        let json = serde_json::to_string(&digest).unwrap();
        assert_eq!(json.len(), 66, "{json}");
        assert_eq!(digest.to_string(), json.trim_matches('"'));
        assert_eq!(
            serde_json::from_str::<CredentialDigest>(&json).unwrap(),
            digest
        );
        for bad in [
            "\"\"",
            "\"abc\"",
            &format!("\"{}\"", "g".repeat(64)),
            &format!("\"{}\"", "A".repeat(64)),
        ] {
            assert!(
                serde_json::from_str::<CredentialDigest>(bad).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn a_random_secret_is_long_and_new_each_time() {
        let a = Credential::random();
        assert_eq!(a.expose().len(), 64);
        assert!(a.expose().chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a.expose(), Credential::random().expose());
    }

    #[test]
    fn it_travels_as_a_string() {
        let credential = Credential::new("abc");
        assert_eq!(serde_json::to_string(&credential).unwrap(), "\"abc\"");
        let read: Credential = serde_json::from_str("\"abc\"").unwrap();
        assert_eq!(read.expose(), "abc");
        assert!(serde_json::from_str::<Credential>("7").is_err());
    }
}
