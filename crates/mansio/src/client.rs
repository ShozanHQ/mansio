//! A client for the mansio HTTP API.

use std::time::Duration;

use anyhow::{Context, bail};
use mansio_core::api::{ErrorBody, Polled, Published};
use mansio_core::{TenantId, Topic};
use reqwest::StatusCode;
use secrecy::{ExposeSecret, SecretString};
use serde_json::{Map, Value};

/// Where the server is and who is asking.
#[derive(Debug)]
pub struct Client {
    http: reqwest::Client,
    base: String,
    token: SecretString,
}

/// The default server address: a local `mansio-server`.
pub const DEFAULT_URL: &str = "http://127.0.0.1:7070";
/// The default request timeout, in seconds.
pub const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

impl Client {
    /// Build a client from `MANSIO_URL`, `MANSIO_TOKEN` and
    /// `MANSIO_TIMEOUT_SECONDS`, read through `lookup`.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let set = |name: &str| lookup(name).filter(|value| !value.is_empty());
        let token = set("MANSIO_TOKEN").context("MANSIO_TOKEN must be set")?;
        let base = set("MANSIO_URL").unwrap_or_else(|| DEFAULT_URL.to_owned());
        let seconds = match set("MANSIO_TIMEOUT_SECONDS") {
            Some(text) => text
                .parse::<u64>()
                .context("MANSIO_TIMEOUT_SECONDS must be a whole number of seconds")?,
            None => DEFAULT_TIMEOUT_SECONDS,
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(seconds))
            .build()
            .context("building the HTTP client")?;
        Ok(Self {
            http,
            base: base.trim_end_matches('/').to_owned(),
            token: SecretString::from(token),
        })
    }

    fn messages_url(&self, tenant: &TenantId, topic: &Topic) -> String {
        format!("{}/v1/tenants/{tenant}/topics/{topic}/messages", self.base)
    }

    /// Publish the envelope attributes in `body` to a topic.
    pub async fn publish(
        &self,
        tenant: &TenantId,
        topic: &Topic,
        body: &Map<String, Value>,
    ) -> anyhow::Result<Published> {
        let response = self
            .http
            .post(self.messages_url(tenant, topic))
            .bearer_auth(self.token.expose_secret())
            .json(body)
            .send()
            .await
            .with_context(|| format!("sending to {}", self.base))?;
        read(response).await
    }

    /// Read envelopes from a topic, starting at `offset`.
    pub async fn poll(
        &self,
        tenant: &TenantId,
        topic: &Topic,
        offset: u64,
        count: Option<u32>,
    ) -> anyhow::Result<Polled> {
        let mut query = vec![("offset", offset.to_string())];
        if let Some(count) = count {
            query.push(("count", count.to_string()));
        }
        let response = self
            .http
            .get(self.messages_url(tenant, topic))
            .bearer_auth(self.token.expose_secret())
            .query(&query)
            .send()
            .await
            .with_context(|| format!("sending to {}", self.base))?;
        read(response).await
    }
}

/// The body of a successful response, or the server's reason for refusing.
async fn read<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> anyhow::Result<T> {
    let status = response.status();
    let bytes = response.bytes().await.context("reading the response")?;
    if status.is_success() {
        return serde_json::from_slice(&bytes).context("the server's answer was not understood");
    }
    bail!("{}", refusal(status, &bytes))
}

fn refusal(status: StatusCode, body: &[u8]) -> String {
    match serde_json::from_slice::<ErrorBody>(body) {
        Ok(ErrorBody { error }) => format!("refused ({status}): {}: {}", error.code, error.message),
        Err(_) => format!("refused ({status})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup(pairs: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| (*v).to_owned())
        }
    }

    #[test]
    fn the_token_is_required() {
        let error = Client::from_lookup(lookup(&[])).unwrap_err();
        assert!(error.to_string().contains("MANSIO_TOKEN"));
    }

    #[test]
    fn the_url_defaults_and_loses_trailing_slashes() {
        let client = Client::from_lookup(lookup(&[("MANSIO_TOKEN", "t")])).unwrap();
        assert_eq!(client.base, DEFAULT_URL);
        let client = Client::from_lookup(lookup(&[
            ("MANSIO_TOKEN", "t"),
            ("MANSIO_URL", "http://h:1//"),
        ]))
        .unwrap();
        let url = client.messages_url(&"acme".parse().unwrap(), &"facts.build".parse().unwrap());
        assert_eq!(
            url,
            "http://h:1/v1/tenants/acme/topics/facts.build/messages"
        );
    }

    #[test]
    fn a_bad_timeout_is_refused() {
        let pairs = &[("MANSIO_TOKEN", "t"), ("MANSIO_TIMEOUT_SECONDS", "soon")];
        assert!(Client::from_lookup(lookup(pairs)).is_err());
    }

    #[test]
    fn refusals_carry_the_servers_reason() {
        let body = br#"{"error":{"code":"bad_request","message":"count must be between 1 and 5"}}"#;
        assert_eq!(
            refusal(StatusCode::BAD_REQUEST, body),
            "refused (400 Bad Request): bad_request: count must be between 1 and 5"
        );
        assert_eq!(
            refusal(StatusCode::BAD_GATEWAY, b"<html>"),
            "refused (502 Bad Gateway)"
        );
    }
}
