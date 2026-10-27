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

/// What the tracing events emitted while it is in scope say.
#[derive(Clone, Default)]
pub(crate) struct Logs(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Ends the capture when dropped, then releases the lock. Fields drop in
/// declaration order, so the subscriber is gone before the next test runs.
pub(crate) struct CaptureGuard {
    _subscriber: tracing::subscriber::DefaultGuard,
    _lock: tokio::sync::MutexGuard<'static, ()>,
}

impl Logs {
    /// Start capturing, holding the lock that keeps other captures out.
    pub(crate) async fn capture() -> (Self, CaptureGuard) {
        let lock = serial().await;
        let logs = Self::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let subscriber = tracing::subscriber::set_default(subscriber);
        tracing::callsite::rebuild_interest_cache();
        let guard = CaptureGuard {
            _subscriber: subscriber,
            _lock: lock,
        };
        (logs, guard)
    }

    /// What has been logged so far.
    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}
