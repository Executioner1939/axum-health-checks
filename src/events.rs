//! The `broadcast` payload plus an ergonomic consumer that hides the
//! `broadcast::error::RecvError` plumbing.
//!
//! Events are **advisory edges only**: status transitions, breaker state
//! changes, readiness flips, drain start. Readiness is never gated on broadcast
//! delivery — that is what the `watch` snapshot is for. The broadcast ring is
//! lossy: a slow consumer is told it lagged and instructed to resync from
//! `HealthHandle::snapshot()`.

use crate::status::HealthStatus;
use std::sync::Arc;
use tokio::sync::broadcast;

/// A discrete health edge emitted to subscribers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HealthEvent {
    /// A check's reported status changed.
    CheckTransition {
        /// The name of the check that transitioned.
        name: Arc<str>,
        /// The status the check held before.
        from: HealthStatus,
        /// The status the check now reports.
        to: HealthStatus,
    },
    /// A check's breaker tripped to `Open`.
    BreakerOpened {
        /// The name of the check whose breaker opened.
        name: Arc<str>,
        /// Consecutive failures that tripped the breaker.
        after_failures: u32,
    },
    /// A check's breaker rolled to `HalfOpen` for a trial probe.
    BreakerHalfOpen {
        /// The name of the check whose breaker is trialling.
        name: Arc<str>,
    },
    /// A check's breaker recovered to `Closed`.
    BreakerClosed {
        /// The name of the check whose breaker recovered.
        name: Arc<str>,
    },
    /// The readiness aggregate flipped to serving.
    BecameReady,
    /// The readiness aggregate flipped to not-serving.
    BecameNotReady,
    /// Drain has begun; readiness is now forced `Down`.
    DrainStarted,
    /// Synthetic: the consumer lagged and skipped `skipped` events. The contract
    /// is to resync from `HealthHandle::snapshot()`.
    Lagged {
        /// The number of events the consumer missed before this resync point.
        skipped: u64,
    },
}

/// An ergonomic wrapper over a `broadcast::Receiver<HealthEvent>`.
///
/// `next` swallows `RecvError::Lagged(n)` into a synthetic
/// [`HealthEvent::Lagged`] and maps `RecvError::Closed` to `None`, so consumers
/// only ever see `HealthEvent`s and a clean end-of-stream.
pub struct EventStream {
    rx: broadcast::Receiver<HealthEvent>,
}

impl EventStream {
    pub(crate) fn new(rx: broadcast::Receiver<HealthEvent>) -> Self {
        EventStream { rx }
    }

    /// Await the next event. Returns `None` once all senders are dropped.
    pub async fn next(&mut self) -> Option<HealthEvent> {
        match self.rx.recv().await {
            Ok(event) => Some(event),
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                Some(HealthEvent::Lagged { skipped })
            }
            Err(broadcast::error::RecvError::Closed) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn delivers_events_in_order() {
        let tx = broadcast::Sender::new(8);
        let mut stream = EventStream::new(tx.subscribe());

        tx.send(HealthEvent::BecameReady).unwrap();
        tx.send(HealthEvent::DrainStarted).unwrap();

        assert_eq!(stream.next().await, Some(HealthEvent::BecameReady));
        assert_eq!(stream.next().await, Some(HealthEvent::DrainStarted));
    }

    #[tokio::test]
    async fn lag_is_swallowed_into_a_synthetic_event() {
        // Capacity 2, push 4 before reading: the consumer lagged by 2.
        let tx = broadcast::Sender::new(2);
        let mut stream = EventStream::new(tx.subscribe());

        for _ in 0..4 {
            tx.send(HealthEvent::BecameNotReady).unwrap();
        }

        // First recv reports the lag as a synthetic event rather than an error.
        match stream.next().await {
            Some(HealthEvent::Lagged { skipped }) => assert_eq!(skipped, 2),
            other => panic!("expected a synthetic Lagged event, got {other:?}"),
        }
        // After resync the remaining buffered events are still delivered.
        assert_eq!(stream.next().await, Some(HealthEvent::BecameNotReady));
    }

    #[tokio::test]
    async fn closed_stream_ends_cleanly() {
        let tx = broadcast::Sender::new(4);
        let mut stream = EventStream::new(tx.subscribe());
        drop(tx);
        assert_eq!(stream.next().await, None);
    }
}
