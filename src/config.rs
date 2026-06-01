//! Per-check and default tuning, plus the probe tag bitflags.

use std::time::Duration;

bitflags::bitflags! {
    /// Which Kubernetes-style probe(s) a check participates in.
    ///
    /// A check is tagged with one or more of these at registration time; the
    /// aggregator computes each probe's status as the worst-wins aggregate over
    /// the checks carrying the corresponding flag.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub struct Probe: u8 {
        /// Gates the `startupProbe`: warm-up dependencies that must be healthy
        /// before liveness/readiness are evaluated.
        const STARTUP = 0b0000_0001;
        /// Gates the `livenessProbe`: process-fatal conditions only. External
        /// dependencies should generally NOT be tagged liveness.
        const LIVENESS = 0b0000_0010;
        /// Gates the `readinessProbe`: dependencies required to serve traffic.
        const READINESS = 0b0000_0100;
    }
}

impl serde::Serialize for Probe {
    /// Serialize as the list of set flag names, e.g. `["READINESS","LIVENESS"]`,
    /// which is stable and human-readable in the detail endpoint.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(None)?;
        if self.contains(Probe::STARTUP) {
            seq.serialize_element("STARTUP")?;
        }
        if self.contains(Probe::LIVENESS) {
            seq.serialize_element("LIVENESS")?;
        }
        if self.contains(Probe::READINESS) {
            seq.serialize_element("READINESS")?;
        }
        seq.end()
    }
}

/// Circuit-breaker tuning for a single check.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct BreakerConfig {
    /// Consecutive failures in `Closed` that trip the breaker to `Open`.
    pub failure_threshold: u32,
    /// Consecutive successes in `HalfOpen` that close the breaker.
    pub success_threshold: u32,
    /// How long the breaker stays `Open` before rolling to `HalfOpen` for a
    /// single trial probe.
    pub cooldown: Duration,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        BreakerConfig {
            failure_threshold: 3,
            success_threshold: 1,
            cooldown: Duration::from_secs(30),
        }
    }
}

/// Per-check scheduling and breaker configuration.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct CheckConfig {
    /// Interval between probe attempts (steady state).
    pub interval: Duration,
    /// Per-attempt timeout. An attempt exceeding this is folded into a failure.
    pub timeout: Duration,
    /// Delay before the prober's first tick, after which the interval applies.
    pub initial_delay: Duration,
    /// Circuit-breaker tuning.
    pub breaker: BreakerConfig,
}

impl Default for CheckConfig {
    fn default() -> Self {
        CheckConfig {
            interval: Duration::from_secs(10),
            timeout: Duration::from_secs(5),
            initial_delay: Duration::ZERO,
            breaker: BreakerConfig::default(),
        }
    }
}
