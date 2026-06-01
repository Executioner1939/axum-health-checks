//! Hand-rolled consecutive-count circuit breaker.
//!
//! Single-owned by exactly one prober task, so there is no `Mutex`/`Arc` and no
//! interior mutability — the prober calls `&mut self` methods from its own loop.
//! Cooldown is measured on `tokio::time::Instant` (monotonic, and advanceable
//! under `tokio::time::pause()` in tests).
//!
//! The breaker never calls the check itself. The prober asks `poll_cooldown` at
//! the top of each tick (to roll `Open -> HalfOpen` when the cooldown elapses),
//! decides whether to skip or run the probe, then feeds the [`Outcome`] back via
//! `record`. Both methods return an `Option<Transition>` describing an edge the
//! prober turns into a `HealthEvent`.

use crate::config::BreakerConfig;
use serde::Serialize;
use tokio::time::Instant;

/// The breaker's externally observable state.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize)]
pub enum BreakerState {
    /// Probing normally; failures are being counted.
    Closed,
    /// Tripped; the prober skips probes until the cooldown elapses.
    Open,
    /// Cooldown elapsed; running trial probes to decide whether to close.
    HalfOpen,
}

/// The classified result of a probe attempt, as seen by the breaker.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Outcome {
    /// The check reported a serving status within the timeout.
    Success,
    /// The check reported not-serving, errored, or timed out.
    Failure,
}

/// An observable state edge. The prober maps this to a `HealthEvent`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Transition {
    /// The state the breaker left.
    pub from: BreakerState,
    /// The state the breaker entered.
    pub to: BreakerState,
}

/// Consecutive-count circuit breaker, owned by a single prober.
#[derive(Debug)]
pub struct Breaker {
    config: BreakerConfig,
    state: BreakerState,
    /// Consecutive failures while `Closed`.
    consecutive_failures: u32,
    /// Consecutive successes while `HalfOpen`.
    consecutive_successes: u32,
    /// When the breaker last entered `Open`; basis for the cooldown.
    opened_at: Option<Instant>,
}

impl Breaker {
    /// A fresh breaker starts `Closed` with no failures recorded.
    pub fn new(config: BreakerConfig) -> Self {
        Breaker {
            config,
            state: BreakerState::Closed,
            consecutive_failures: 0,
            consecutive_successes: 0,
            opened_at: None,
        }
    }

    /// Current state.
    #[inline]
    pub fn state(&self) -> BreakerState {
        self.state
    }

    /// Consecutive failures counted in the current `Closed` run (0 once tripped
    /// or after any success).
    #[inline]
    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }

    /// Called at the top of each tick. If the breaker is `Open` and the cooldown
    /// has elapsed at `now`, roll to `HalfOpen` and return the transition;
    /// otherwise return `None`. The prober runs exactly one trial probe on the
    /// tick where this returns an `Open -> HalfOpen` edge.
    pub fn poll_cooldown(&mut self, now: Instant) -> Option<Transition> {
        if self.state != BreakerState::Open {
            return None;
        }
        let opened_at = self.opened_at?;
        if now.saturating_duration_since(opened_at) >= self.config.cooldown {
            self.consecutive_successes = 0;
            self.transition_to(BreakerState::HalfOpen)
        } else {
            None
        }
    }

    /// Whether the prober should skip the probe this tick (breaker `Open` and
    /// still cooling). Call after `poll_cooldown`, which would already have
    /// rolled to `HalfOpen` if the cooldown had elapsed.
    #[inline]
    pub fn is_cooling(&self) -> bool {
        self.state == BreakerState::Open
    }

    /// Feed a classified probe outcome into the breaker, returning any state edge.
    pub fn record(&mut self, outcome: Outcome, now: Instant) -> Option<Transition> {
        match (self.state, outcome) {
            (BreakerState::Closed, Outcome::Success) => {
                self.consecutive_failures = 0;
                None
            }
            (BreakerState::Closed, Outcome::Failure) => {
                self.consecutive_failures += 1;
                if self.consecutive_failures >= self.config.failure_threshold {
                    self.open(now)
                } else {
                    None
                }
            }
            (BreakerState::HalfOpen, Outcome::Success) => {
                self.consecutive_successes += 1;
                if self.consecutive_successes >= self.config.success_threshold {
                    self.consecutive_failures = 0;
                    self.consecutive_successes = 0;
                    self.opened_at = None;
                    self.transition_to(BreakerState::Closed)
                } else {
                    None
                }
            }
            (BreakerState::HalfOpen, Outcome::Failure) => {
                // Any trial failure re-opens with a fresh cooldown.
                self.consecutive_successes = 0;
                self.open(now)
            }
            (BreakerState::Open, _) => {
                // The prober does not probe while Open; reaching here means a
                // late result. Treat defensively: a success during Open is
                // ignored, a failure simply refreshes the cooldown clock.
                if outcome == Outcome::Failure {
                    self.opened_at = Some(now);
                }
                None
            }
        }
    }

    /// Transition into `Open`, recording the cooldown basis.
    fn open(&mut self, now: Instant) -> Option<Transition> {
        self.opened_at = Some(now);
        self.consecutive_successes = 0;
        self.transition_to(BreakerState::Open)
    }

    /// Set state and emit a transition unless it is a self-edge.
    fn transition_to(&mut self, to: BreakerState) -> Option<Transition> {
        let from = self.state;
        self.state = to;
        if from == to {
            None
        } else {
            Some(Transition { from, to })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn cfg() -> BreakerConfig {
        BreakerConfig {
            failure_threshold: 3,
            success_threshold: 1,
            cooldown: Duration::from_secs(30),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn trips_after_threshold_failures() {
        let now = Instant::now();
        let mut b = Breaker::new(cfg());
        assert_eq!(b.record(Outcome::Failure, now), None);
        assert_eq!(b.record(Outcome::Failure, now), None);
        let t = b
            .record(Outcome::Failure, now)
            .expect("third failure trips");
        assert_eq!(t.to, BreakerState::Open);
        assert_eq!(b.state(), BreakerState::Open);
    }

    #[tokio::test(start_paused = true)]
    async fn success_resets_failure_count() {
        let now = Instant::now();
        let mut b = Breaker::new(cfg());
        b.record(Outcome::Failure, now);
        b.record(Outcome::Failure, now);
        assert_eq!(b.record(Outcome::Success, now), None);
        assert_eq!(b.consecutive_failures(), 0);
        // back to needing a full run of failures
        b.record(Outcome::Failure, now);
        b.record(Outcome::Failure, now);
        assert_eq!(
            b.record(Outcome::Failure, now),
            Some(Transition {
                from: BreakerState::Closed,
                to: BreakerState::Open,
            })
        );
    }

    #[tokio::test(start_paused = true)]
    async fn cooldown_rolls_to_half_open_then_closes() {
        let start = Instant::now();
        let mut b = Breaker::new(cfg());
        for _ in 0..3 {
            b.record(Outcome::Failure, start);
        }
        assert_eq!(b.state(), BreakerState::Open);
        // not yet elapsed
        assert_eq!(b.poll_cooldown(start + Duration::from_secs(10)), None);
        assert!(b.is_cooling());
        // elapsed -> HalfOpen
        let t = b
            .poll_cooldown(start + Duration::from_secs(30))
            .expect("rolls to half-open");
        assert_eq!(
            t,
            Transition {
                from: BreakerState::Open,
                to: BreakerState::HalfOpen
            }
        );
        // a trial success closes (success_threshold = 1)
        let t = b
            .record(Outcome::Success, start + Duration::from_secs(30))
            .expect("closes");
        assert_eq!(
            t,
            Transition {
                from: BreakerState::HalfOpen,
                to: BreakerState::Closed
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn half_open_failure_reopens_with_fresh_cooldown() {
        let start = Instant::now();
        let mut b = Breaker::new(cfg());
        for _ in 0..3 {
            b.record(Outcome::Failure, start);
        }
        b.poll_cooldown(start + Duration::from_secs(30));
        assert_eq!(b.state(), BreakerState::HalfOpen);
        let t = b
            .record(Outcome::Failure, start + Duration::from_secs(30))
            .expect("reopens");
        assert_eq!(
            t,
            Transition {
                from: BreakerState::HalfOpen,
                to: BreakerState::Open
            }
        );
        // fresh cooldown: not elapsed at +40s (only 10s since reopen)
        assert_eq!(b.poll_cooldown(start + Duration::from_secs(40)), None);
        // elapsed at +60s
        assert!(b.poll_cooldown(start + Duration::from_secs(60)).is_some());
    }
}
