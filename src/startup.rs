//! The warm-up gate.
//!
//! Backed by a `watch::Sender<bool>`. The host flips it with `mark_ready()`
//! after migrations / cache warm / whatever the startup probe should gate on.
//! Probers and the aggregator subscribe to it as a change signal so readiness
//! recomputes the instant warm-up completes.

use tokio::sync::watch;

/// Host-facing handle for the warm-up gate. Cheaply cloneable.
#[derive(Clone, Debug)]
pub struct StartupController(watch::Sender<bool>);

impl StartupController {
    /// Construct a gate in the not-ready state, returning the controller and a
    /// receiver for the aggregator to watch.
    pub(crate) fn new() -> (Self, watch::Receiver<bool>) {
        let (tx, rx) = watch::channel(false);
        (StartupController(tx), rx)
    }

    /// Mark warm-up complete. Idempotent; only signals a change on the first flip.
    pub fn mark_ready(&self) {
        self.0.send_if_modified(|ready| {
            if *ready {
                false
            } else {
                *ready = true;
                true
            }
        });
    }

    /// Whether warm-up has completed.
    pub fn is_ready(&self) -> bool {
        *self.0.borrow()
    }
}
