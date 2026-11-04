//! Metrics in the Prometheus text format, served at `GET /metrics`.
//!
//! The counters live in memory and start from zero with the process, as a
//! Prometheus counter does. The gauges (open items and active leases of each
//! queue) are read from the engine's state when the endpoint is scraped.
//! Every label is a value the server has parsed into a type (a tenant, a
//! topic) or a fixed name (a route pattern, a method, a status), never text a
//! client chose, so the set of series stays bounded by what the server holds.
//! With metrics off nothing is recorded.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

/// The upper bounds of the latency buckets, in seconds.
const BUCKETS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// The content type of the text format.
pub(crate) const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// The methods a request label may carry. Any other is `OTHER`.
const METHODS: [&str; 9] = [
    "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE", "CONNECT",
];

/// How long the requests of one method, route and status took.
#[derive(Debug, Default)]
struct Latency {
    count: u64,
    sum_seconds: f64,
    /// Requests that took at most each bound in [`BUCKETS`], and not the one
    /// before it.
    buckets: [u64; BUCKETS.len()],
}

/// What the timer has done to the items of one queue since the start.
#[derive(Debug, Default, Clone, Copy)]
struct QueueCounts {
    expired: u64,
    retried: u64,
    dead: u64,
}

/// A queue counter's name, its help text, and where its value is.
type Counter = (&'static str, &'static str, fn(&QueueCounts) -> u64);

/// One queue's items as they stand now.
#[derive(Debug, Clone)]
pub(crate) struct QueueGauge {
    pub(crate) tenant: String,
    pub(crate) queue: String,
    /// Items not yet acknowledged or dead lettered.
    pub(crate) open: usize,
    /// Items held under a lease.
    pub(crate) leases: usize,
}

/// The counters of one server.
#[derive(Debug)]
pub(crate) struct Metrics {
    enabled: bool,
    /// By method, route pattern and status.
    requests: Mutex<BTreeMap<(String, String, u16), Latency>>,
    /// Messages published, by tenant and topic.
    published: Mutex<BTreeMap<(String, String), u64>>,
    /// By tenant and queue.
    queues: Mutex<BTreeMap<(String, String), QueueCounts>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Metrics {
    /// Counters that record when `enabled`, and ignore everything otherwise.
    pub(crate) fn new(enabled: bool) -> Self {
        Self {
            enabled,
            requests: Mutex::default(),
            published: Mutex::default(),
            queues: Mutex::default(),
        }
    }

    pub(crate) fn enabled(&self) -> bool {
        self.enabled
    }

    /// Count a request that took `took` and was answered with `status`.
    pub(crate) fn request(&self, method: &str, route: &str, status: u16, took: Duration) {
        if !self.enabled {
            return;
        }
        let method = METHODS
            .iter()
            .find(|known| **known == method)
            .copied()
            .unwrap_or("OTHER");
        let seconds = took.as_secs_f64();
        let mut requests = lock(&self.requests);
        let latency = requests
            .entry((method.to_owned(), route.to_owned(), status))
            .or_default();
        latency.count = latency.count.saturating_add(1);
        latency.sum_seconds += seconds;
        if let Some(slot) = BUCKETS
            .iter()
            .position(|bound| seconds <= *bound)
            .and_then(|at| latency.buckets.get_mut(at))
        {
            *slot = slot.saturating_add(1);
        }
    }

    /// Count a message published to `topic` of `tenant`.
    pub(crate) fn published(&self, tenant: &str, topic: &str) {
        if !self.enabled {
            return;
        }
        let mut published = lock(&self.published);
        let count = published
            .entry((tenant.to_owned(), topic.to_owned()))
            .or_default();
        *count = count.saturating_add(1);
    }

    /// Count a lease that ran out.
    pub(crate) fn expired(&self, tenant: &str, queue: &str) {
        self.queue(tenant, queue, |counts| {
            counts.expired = counts.expired.saturating_add(1);
        });
    }

    /// Count an item given another attempt.
    pub(crate) fn retried(&self, tenant: &str, queue: &str) {
        self.queue(tenant, queue, |counts| {
            counts.retried = counts.retried.saturating_add(1);
        });
    }

    /// Count an item sent to `dead`.
    pub(crate) fn dead(&self, tenant: &str, queue: &str) {
        self.queue(tenant, queue, |counts| {
            counts.dead = counts.dead.saturating_add(1);
        });
    }

    fn queue(&self, tenant: &str, queue: &str, change: impl FnOnce(&mut QueueCounts)) {
        if !self.enabled {
            return;
        }
        let mut queues = lock(&self.queues);
        change(
            queues
                .entry((tenant.to_owned(), queue.to_owned()))
                .or_default(),
        );
    }

    /// The text of the metrics, with `gauges` for the queues as they stand.
    pub(crate) fn render(&self, gauges: &[QueueGauge]) -> String {
        let mut out = String::new();
        self.render_requests(&mut out);
        self.render_published(&mut out);
        self.render_queues(&mut out, gauges);
        out
    }

    fn render_requests(&self, out: &mut String) {
        let requests = lock(&self.requests);
        family(
            out,
            "mansio_http_requests_total",
            "counter",
            "Requests answered, by method, route and status.",
        );
        for ((method, route, status), latency) in requests.iter() {
            let labels = request_labels(method, route, *status);
            line(
                out,
                "mansio_http_requests_total",
                &labels,
                latency.count.to_string(),
            );
        }
        family(
            out,
            "mansio_http_request_duration_seconds",
            "histogram",
            "Time to the answer, by method, route and status.",
        );
        for ((method, route, status), latency) in requests.iter() {
            let labels = request_labels(method, route, *status);
            let mut cumulative: u64 = 0;
            for (bound, count) in BUCKETS.iter().zip(latency.buckets) {
                cumulative = cumulative.saturating_add(count);
                line(
                    out,
                    "mansio_http_request_duration_seconds_bucket",
                    &format!("{labels},le=\"{bound}\""),
                    cumulative.to_string(),
                );
            }
            line(
                out,
                "mansio_http_request_duration_seconds_bucket",
                &format!("{labels},le=\"+Inf\""),
                latency.count.to_string(),
            );
            line(
                out,
                "mansio_http_request_duration_seconds_sum",
                &labels,
                latency.sum_seconds.to_string(),
            );
            line(
                out,
                "mansio_http_request_duration_seconds_count",
                &labels,
                latency.count.to_string(),
            );
        }
    }

    fn render_published(&self, out: &mut String) {
        family(
            out,
            "mansio_published_messages_total",
            "counter",
            "Messages published, by tenant and topic.",
        );
        for ((tenant, topic), count) in lock(&self.published).iter() {
            let labels = format!("tenant=\"{}\",topic=\"{}\"", escape(tenant), escape(topic));
            line(
                out,
                "mansio_published_messages_total",
                &labels,
                count.to_string(),
            );
        }
    }

    fn render_queues(&self, out: &mut String, gauges: &[QueueGauge]) {
        let queue_labels = |tenant: &str, queue: &str| {
            format!("tenant=\"{}\",queue=\"{}\"", escape(tenant), escape(queue))
        };
        family(
            out,
            "mansio_queue_open_items",
            "gauge",
            "Items not yet acknowledged or dead lettered, by queue.",
        );
        for gauge in gauges {
            line(
                out,
                "mansio_queue_open_items",
                &queue_labels(&gauge.tenant, &gauge.queue),
                gauge.open.to_string(),
            );
        }
        family(
            out,
            "mansio_queue_active_leases",
            "gauge",
            "Items held under a lease, by queue.",
        );
        for gauge in gauges {
            line(
                out,
                "mansio_queue_active_leases",
                &queue_labels(&gauge.tenant, &gauge.queue),
                gauge.leases.to_string(),
            );
        }
        let queues = lock(&self.queues);
        let counters: [Counter; 3] = [
            (
                "mansio_queue_lease_expiries_total",
                "Leases that ran out, by queue.",
                |counts| counts.expired,
            ),
            (
                "mansio_queue_retries_total",
                "Items given another attempt, by queue.",
                |counts| counts.retried,
            ),
            (
                "mansio_queue_dead_letters_total",
                "Items sent to the dead topic, by queue.",
                |counts| counts.dead,
            ),
        ];
        for (name, help, pick) in counters {
            family(out, name, "counter", help);
            for ((tenant, queue), counts) in queues.iter() {
                line(
                    out,
                    name,
                    &queue_labels(tenant, queue),
                    pick(counts).to_string(),
                );
            }
        }
    }
}

fn request_labels(method: &str, route: &str, status: u16) -> String {
    format!(
        "method=\"{}\",route=\"{}\",status=\"{status}\"",
        escape(method),
        escape(route)
    )
}

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    // Writing to a String does not fail.
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

fn line(out: &mut String, name: &str, labels: &str, value: String) {
    let _ = writeln!(out, "{name}{{{labels}}} {value}");
}

/// A label value as the text format has it: backslash, quote and newline
/// escaped.
fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_are_counted_with_their_latency_by_route_and_status() {
        let metrics = Metrics::new(true);
        metrics.request("GET", "/healthz", 200, Duration::from_millis(3));
        metrics.request("GET", "/healthz", 200, Duration::from_millis(30));
        metrics.request("GET", "/healthz", 503, Duration::from_secs(60));
        let text = metrics.render(&[]);
        for expected in [
            "mansio_http_requests_total{method=\"GET\",route=\"/healthz\",status=\"200\"} 2",
            "mansio_http_requests_total{method=\"GET\",route=\"/healthz\",status=\"503\"} 1",
            "mansio_http_request_duration_seconds_bucket{method=\"GET\",route=\"/healthz\",status=\"200\",le=\"0.005\"} 1",
            "mansio_http_request_duration_seconds_bucket{method=\"GET\",route=\"/healthz\",status=\"200\",le=\"0.05\"} 2",
            "mansio_http_request_duration_seconds_bucket{method=\"GET\",route=\"/healthz\",status=\"503\",le=\"10\"} 0",
            "mansio_http_request_duration_seconds_bucket{method=\"GET\",route=\"/healthz\",status=\"503\",le=\"+Inf\"} 1",
            "mansio_http_request_duration_seconds_count{method=\"GET\",route=\"/healthz\",status=\"200\"} 2",
        ] {
            assert!(text.contains(expected), "{expected} is not in {text}");
        }
    }

    #[test]
    fn a_method_nobody_knows_is_one_label() {
        let metrics = Metrics::new(true);
        metrics.request("BREW", "unmatched", 404, Duration::ZERO);
        assert!(metrics.render(&[]).contains("method=\"OTHER\""));
        assert!(!metrics.render(&[]).contains("BREW"));
    }

    #[test]
    fn publishes_and_queue_counters_are_labelled_by_tenant_and_topic_or_queue() {
        let metrics = Metrics::new(true);
        metrics.published("acme", "facts.build");
        metrics.published("acme", "facts.build");
        metrics.expired("acme", "builds");
        metrics.retried("acme", "builds");
        metrics.dead("acme", "builds");
        let text = metrics.render(&[QueueGauge {
            tenant: "acme".to_owned(),
            queue: "builds".to_owned(),
            open: 4,
            leases: 1,
        }]);
        for expected in [
            "mansio_published_messages_total{tenant=\"acme\",topic=\"facts.build\"} 2",
            "mansio_queue_open_items{tenant=\"acme\",queue=\"builds\"} 4",
            "mansio_queue_active_leases{tenant=\"acme\",queue=\"builds\"} 1",
            "mansio_queue_lease_expiries_total{tenant=\"acme\",queue=\"builds\"} 1",
            "mansio_queue_retries_total{tenant=\"acme\",queue=\"builds\"} 1",
            "mansio_queue_dead_letters_total{tenant=\"acme\",queue=\"builds\"} 1",
        ] {
            assert!(text.contains(expected), "{expected} is not in {text}");
        }
    }

    #[test]
    fn metrics_that_are_off_record_nothing() {
        let metrics = Metrics::new(false);
        metrics.request("GET", "/healthz", 200, Duration::ZERO);
        metrics.published("acme", "facts.build");
        metrics.dead("acme", "builds");
        assert!(!metrics.render(&[]).contains('{'));
    }

    #[test]
    fn a_label_value_is_escaped() {
        assert_eq!(escape("a\\b\"c\nd"), "a\\\\b\\\"c\\nd");
    }
}
