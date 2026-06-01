//! The health status ADT.
//!
//! Extracted from the old `service.rs` and trimmed. The discriminant order is
//! **intentional**: it makes `Ord` a worst-wins lattice so that aggregating a
//! set of statuses is just `max()`. `Up < Degraded < Down < Unknown`.
//!
//! `Up` and `Degraded` are *serving* (still take traffic, possibly with reduced
//! capability); `Down` and `Unknown` are not. The old `OutOfService` and
//! `Custom(String)` variants are gone — `Custom` was the only thing making the
//! type non-`Copy` and breaking a stable `Ord`, and `Degraded` covers the real
//! "serving but unhealthy" use case.

use serde::Serialize;

/// The status of a single check or an aggregated probe.
///
/// Ordering is worst-wins: `Up < Degraded < Down < Unknown`, so the aggregate
/// of a set of statuses is `statuses.max()`. An empty set is treated as `Up` by
/// the aggregator.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize)]
pub enum HealthStatus {
    /// Fully healthy and serving.
    Up,
    /// Serving, but with reduced capability or elevated latency.
    Degraded,
    /// Not serving; the check reported a failure or the breaker is open.
    Down,
    /// No determination yet (e.g. a prober that has not run its first probe).
    Unknown,
}

impl HealthStatus {
    /// Whether this status should be advertised as serving traffic.
    ///
    /// `Up` and `Degraded` serve; `Down` and `Unknown` do not.
    #[inline]
    pub fn is_serving(self) -> bool {
        matches!(self, HealthStatus::Up | HealthStatus::Degraded)
    }
}

#[cfg(test)]
mod tests {
    use super::HealthStatus::*;

    #[test]
    fn worst_wins_ordering() {
        assert!(Up < Degraded);
        assert!(Degraded < Down);
        assert!(Down < Unknown);
        // max() over a tagged subset is the aggregate
        assert_eq!([Up, Degraded, Up].into_iter().max(), Some(Degraded));
        assert_eq!([Up, Down, Degraded].into_iter().max(), Some(Down));
        assert_eq!([Up, Up].into_iter().max(), Some(Up));
    }

    #[test]
    fn serving_predicate() {
        assert!(Up.is_serving());
        assert!(Degraded.is_serving());
        assert!(!Down.is_serving());
        assert!(!Unknown.is_serving());
    }
}
