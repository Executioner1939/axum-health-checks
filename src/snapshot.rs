//! The `watch` payload — the cached source of truth read by handlers.
//!
//! The per-check map lives behind an `Arc`, so every `watch::borrow().clone()`
//! on the request hot path is a pointer bump rather than a `BTreeMap` copy. The
//! aggregator rebuilds the map copy-on-write whenever a check changes and bumps
//! `generation`.

use crate::breaker::BreakerState;
use crate::config::Probe;
use crate::status::HealthStatus;
use serde::Serialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

/// The full cached health state. Cheap to clone (the map is behind `Arc`).
#[derive(Clone, Debug, Serialize)]
pub struct HealthSnapshot {
    /// Aggregate over `STARTUP`-tagged checks.
    pub startup: HealthStatus,
    /// Aggregate over `LIVENESS`-tagged checks.
    pub liveness: HealthStatus,
    /// Aggregate over `READINESS`-tagged checks.
    pub readiness: HealthStatus,
    /// Per-check detail, keyed by `Check::name`.
    pub checks: Arc<BTreeMap<Box<str>, CheckSnapshot>>,
    /// Monotonically increasing on every meaningful change; lets consumers
    /// detect a missed `watch` update without diffing the whole snapshot.
    pub generation: u64,
}

impl HealthSnapshot {
    /// The initial snapshot before any prober has reported: everything `Unknown`,
    /// empty map, generation 0.
    pub(crate) fn initial() -> Self {
        HealthSnapshot {
            startup: HealthStatus::Unknown,
            liveness: HealthStatus::Unknown,
            readiness: HealthStatus::Unknown,
            checks: Arc::new(BTreeMap::new()),
            generation: 0,
        }
    }
}

/// The cached state of a single check.
#[derive(Clone, Debug, Serialize)]
pub struct CheckSnapshot {
    /// Latest reported status (or `Unknown` before the first probe).
    pub status: HealthStatus,
    /// The owning prober's breaker state.
    pub breaker: BreakerState,
    /// Which probe aggregates this check contributes to.
    pub probes: Probe,
    /// Wall-clock time of the last serving result, if any.
    pub last_ok: Option<SystemTime>,
    /// Most recent failure detail, if any (shared, cheap to clone).
    pub last_err: Option<Arc<str>>,
    /// Consecutive failures currently counted by the breaker.
    pub consecutive_failures: u32,
    /// Wall-clock time of the last probe attempt (any outcome).
    pub checked_at: Option<SystemTime>,
}

impl CheckSnapshot {
    /// A not-yet-probed snapshot for a freshly registered check.
    pub(crate) fn pending(probes: Probe) -> Self {
        CheckSnapshot {
            status: HealthStatus::Unknown,
            breaker: BreakerState::Closed,
            probes,
            last_ok: None,
            last_err: None,
            consecutive_failures: 0,
            checked_at: None,
        }
    }

    /// Compare only the semantically meaningful fields. The aggregator uses this
    /// to avoid waking request-path readers on timestamp-only churn.
    pub(crate) fn semantically_eq(&self, other: &CheckSnapshot) -> bool {
        self.status == other.status
            && self.breaker == other.breaker
            && self.probes == other.probes
            && self.consecutive_failures == other.consecutive_failures
    }
}
