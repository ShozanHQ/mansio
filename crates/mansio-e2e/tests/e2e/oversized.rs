//! A payload above Iggy's cap (64 000 000 bytes) is refused by Iggy, which a
//! client must not retry: mansio answers 502, not 503.
//!
//! Only against Iggy, with `MANSIO_MAX_BODY_BYTES` raised above the cap, as
//! the e2e job sets it (the default, 1 MiB, would answer 413 first). In
//! process the memory broker has no cap.

use anyhow::ensure;
use reqwest::StatusCode;
use serde_json::json;

use crate::harness::Target;

/// Iggy's `MAX_PAYLOAD_SIZE`, in bytes.
const IGGY_PAYLOAD_CAP: u64 = 64_000_000;

#[tokio::test]
async fn a_payload_above_iggys_cap_is_a_502_a_client_does_not_retry() -> anyhow::Result<()> {
    let target = Target::start().await?;
    if target.is_local() {
        return Ok(());
    }
    let room = IGGY_PAYLOAD_CAP + 1_000_000;
    ensure!(
        target.settings.max_body_bytes > room,
        "MANSIO_MAX_BODY_BYTES is {}, and must be above {room} for this test; the e2e job raises it",
        target.settings.max_body_bytes
    );
    let big = "x".repeat(usize::try_from(IGGY_PAYLOAD_CAP)? + 1);
    let body = json!({
        "source": "e2e",
        "type": "build.finished",
        "subject": "build/oversized",
        "data": {"blob": big}
    });
    let (status, response) = target
        .authed(
            reqwest::Method::POST,
            &target.messages_uri("facts.build"),
            Some(&body),
        )
        .await?;
    ensure!(
        status == StatusCode::BAD_GATEWAY,
        "an oversized payload answered {status}, not 502: {response}"
    );
    Ok(())
}
