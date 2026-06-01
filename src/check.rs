//! The single object-safe check abstraction.
//!
//! Replaces the old `HealthIndicator` trait and the `Pingable` database trait.
//! A [`Check`] runs **one** probe attempt; the prober wraps it in a timeout and
//! drives the circuit breaker, so a `Check` impl must not loop, sleep on an
//! interval, or impose its own timeout.

use crate::status::HealthStatus;
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use tokio_util::sync::CancellationToken;

/// A single health check. Object-safe so the registry can hold `Box<dyn Check>`.
///
/// `check` performs exactly one attempt. The prober imposes the per-attempt
/// timeout (`CheckConfig::timeout`) and the retry cadence (`CheckConfig::interval`);
/// the implementation just does one round-trip and reports the outcome.
#[async_trait]
pub trait Check: Send + Sync + 'static {
    /// Stable identity for this check. Used as the map key in snapshots and as
    /// the `name` field in events; must be unique within a registry.
    fn name(&self) -> &str;

    /// Run one probe attempt and report the result.
    ///
    /// May consult `cx.is_cancelled()` to abandon a slow round-trip early during
    /// drain, but is not required to — the prober's timeout and the outer
    /// cancel branch bound the attempt regardless.
    async fn check(&self, cx: &CheckContext) -> CheckResult;
}

/// Forwarding impl so a `Box<dyn Check>` (e.g. the output of [`check_fn`]) is
/// itself a `Check` and can be passed anywhere an `impl Check` is expected —
/// `register`, `register_with`, and so on — not just to `register_boxed`.
#[async_trait]
impl Check for Box<dyn Check> {
    fn name(&self) -> &str {
        (**self).name()
    }

    async fn check(&self, cx: &CheckContext) -> CheckResult {
        (**self).check(cx).await
    }
}

/// Opaque context handed to each [`Check::check`] call.
///
/// Carries the prober's child cancellation token so cooperative checks can bail
/// out of an in-flight round-trip when the host begins draining.
#[derive(Clone, Debug)]
pub struct CheckContext {
    cancel: CancellationToken,
}

impl CheckContext {
    /// Construct a context wrapping a cancellation token. Probers build this
    /// from their child token; tests can build one directly.
    pub fn new(cancel: CancellationToken) -> Self {
        CheckContext { cancel }
    }

    /// Whether the host has begun draining and this check should abandon work.
    #[inline]
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// The underlying token, for checks that want to `select!` against it.
    #[inline]
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancel
    }
}

/// The outcome of a single probe attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckResult {
    /// Reported status. The prober classifies `is_serving()` as breaker success.
    pub status: HealthStatus,
    /// Optional human-readable detail (an error message, a note).
    pub detail: Option<String>,
    /// Arbitrary key/value diagnostics surfaced in the detail endpoint.
    pub data: BTreeMap<String, String>,
}

impl CheckResult {
    /// A healthy result with no detail.
    pub fn up() -> Self {
        CheckResult {
            status: HealthStatus::Up,
            detail: None,
            data: BTreeMap::new(),
        }
    }

    /// A serving-but-impaired result carrying a reason.
    pub fn degraded(detail: impl Into<String>) -> Self {
        CheckResult {
            status: HealthStatus::Degraded,
            detail: Some(detail.into()),
            data: BTreeMap::new(),
        }
    }

    /// A failing result carrying a reason.
    pub fn down(detail: impl Into<String>) -> Self {
        CheckResult {
            status: HealthStatus::Down,
            detail: Some(detail.into()),
            data: BTreeMap::new(),
        }
    }

    /// Attach a diagnostic key/value pair (builder style).
    pub fn with(mut self, key: impl Into<String>, val: impl ToString) -> Self {
        self.data.insert(key.into(), val.to_string());
        self
    }
}

impl From<CheckError> for CheckResult {
    fn from(err: CheckError) -> Self {
        CheckResult::down(err.to_string())
    }
}

/// A boxed error from a check's dependency, with a `Display` that flattens the
/// source chain. Useful for `?`-style check bodies that return `CheckError`.
#[derive(Debug)]
pub struct CheckError(Box<dyn StdError + Send + Sync + 'static>);

impl CheckError {
    /// Wrap any std error.
    pub fn new(err: impl StdError + Send + Sync + 'static) -> Self {
        CheckError(Box::new(err))
    }

    /// Wrap a message with no underlying error.
    pub fn msg(msg: impl Into<String>) -> Self {
        CheckError(Box::<dyn StdError + Send + Sync>::from(msg.into()))
    }
}

impl fmt::Display for CheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl StdError for CheckError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.0.as_ref())
    }
}

/// Closure adapter so trivial checks need no struct.
///
/// ```ignore
/// let c = check_fn("always-up", |_cx| async { CheckResult::up() });
/// builder.register_boxed(Probe::LIVENESS, c);
/// ```
pub fn check_fn<F, Fut>(name: impl Into<String>, f: F) -> Box<dyn Check>
where
    F: Fn(CheckContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = CheckResult> + Send + 'static,
{
    Box::new(FnCheck {
        name: name.into(),
        f,
    })
}

struct FnCheck<F> {
    name: String,
    f: F,
}

#[async_trait]
impl<F, Fut> Check for FnCheck<F>
where
    F: Fn(CheckContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = CheckResult> + Send + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self, cx: &CheckContext) -> CheckResult {
        (self.f)(cx.clone()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_builders() {
        let r = CheckResult::up().with("latency_ms", 3u64);
        assert_eq!(r.status, HealthStatus::Up);
        assert_eq!(r.data.get("latency_ms").map(String::as_str), Some("3"));

        let d = CheckResult::down("boom");
        assert_eq!(d.status, HealthStatus::Down);
        assert_eq!(d.detail.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn fn_check_runs() {
        let c = check_fn("x", |_cx| async { CheckResult::degraded("slow") });
        let cx = CheckContext::new(CancellationToken::new());
        assert_eq!(c.name(), "x");
        assert_eq!(c.check(&cx).await.status, HealthStatus::Degraded);
    }

    #[test]
    fn check_error_into_result() {
        let r: CheckResult = CheckError::msg("nope").into();
        assert_eq!(r.status, HealthStatus::Down);
        assert_eq!(r.detail.as_deref(), Some("nope"));
    }
}
