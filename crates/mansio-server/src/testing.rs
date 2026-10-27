//! What the tests of several modules share.

/// A test that captures the log holds this for its life, and so does every
/// test that reaches a log line a capture looks for, in whatever module.
/// Tracing keeps subscribers and callsite interest in process-wide state. A
/// callsite first reached on a thread with no subscriber, while another
/// thread's capture is live, is cached as never interesting, and its events
/// vanish from that capture.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(crate) async fn serial() -> tokio::sync::MutexGuard<'static, ()> {
    SERIAL.lock().await
}
