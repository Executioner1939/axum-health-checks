//! The background prober (one task per check) and the aggregator (one task
//! total). Together they replace the old per-request fan-out: instead of running
//! every indicator on every `/health` hit, each check is probed on its own
//! schedule and the latest result is cached in a `watch` snapshot that handlers
//! read in microseconds.

use crate::breaker::{Breaker, BreakerState, Outcome, Transition};
use crate::check::{Check, CheckContext};
use crate::config::{CheckConfig, Probe};
use crate::events::HealthEvent;
use crate::snapshot::{CheckSnapshot, HealthSnapshot};
use crate::status::HealthStatus;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::{broadcast, watch};
use tokio::time::{Instant, MissedTickBehavior, interval_at};
use tokio_util::sync::CancellationToken;

/// Everything a single prober task needs to run. Built by the registry.
pub(crate) struct ProberTask {
    pub(crate) check: Box<dyn Check>,
    pub(crate) probes: Probe,
    pub(crate) config: CheckConfig,
    /// The prober's own per-check `watch` cell (it holds the sender).
    pub(crate) cell: watch::Sender<CheckSnapshot>,
    /// Shared advisory edge bus.
    pub(crate) events: broadcast::Sender<HealthEvent>,
    /// Coalescing wake signal to the aggregator; bumped on every cell write.
    pub(crate) dirty: watch::Sender<u64>,
    /// Child of the host drain token; its `cancelled()` arm stops the loop.
    pub(crate) cancel: CancellationToken,
}

impl ProberTask {
    /// Run the prober loop until cancelled. One task per check.
    pub(crate) async fn run(self) {
        let ProberTask {
            check,
            probes,
            config,
            cell,
            events,
            dirty,
            cancel,
        } = self;

        let name: Arc<str> = Arc::from(check.name());
        let cx = CheckContext::new(cancel.clone());
        let mut breaker = Breaker::new(config.breaker);

        // `interval_at` with an explicit start lets us honour `initial_delay`
        // without an extra immediate tick. `Delay` (not the default `Burst`)
        // means a stalled probe does not unleash a back-to-back catch-up storm
        // at a recovering dependency.
        let start = Instant::now() + config.initial_delay;
        let mut ticker = interval_at(start, config.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                _ = ticker.tick() => {
                    let now = Instant::now();

                    // Roll Open -> HalfOpen if the cooldown elapsed.
                    if let Some(t) = breaker.poll_cooldown(now) {
                        emit_transition(&events, &name, t, breaker.consecutive_failures());
                    }

                    // While Open and still cooling, skip the probe entirely and
                    // keep the cached status Down — do not hammer a dead dep.
                    if breaker.is_cooling() {
                        publish(
                            &cell,
                            &dirty,
                            &name,
                            &events,
                            probes,
                            &breaker,
                            HealthStatus::Down,
                            Some(Arc::from("circuit open")),
                        );
                        continue;
                    }

                    // Run exactly one attempt under the per-attempt timeout, but
                    // race it against cancellation so a drain preempts an
                    // in-flight probe immediately instead of waiting out
                    // `config.timeout`. `select!` only races futures at its top
                    // level, so the timeout future must be polled *inside* a
                    // select arm — not awaited sequentially — for cancel to win.
                    let probe = tokio::time::timeout(config.timeout, check.check(&cx));
                    tokio::pin!(probe);
                    let attempt = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        r = &mut probe => r,
                    };

                    let (status, detail, outcome) = match attempt {
                        Ok(r) if r.status.is_serving() => (r.status, r.detail, Outcome::Success),
                        Ok(r) => {
                            let d = r.detail.unwrap_or_else(|| "check reported down".into());
                            (r.status, Some(d), Outcome::Failure)
                        }
                        Err(_elapsed) => (
                            HealthStatus::Down,
                            Some("probe timed out".to_string()),
                            Outcome::Failure,
                        ),
                    };

                    let transition = breaker.record(outcome, now);

                    // The breaker, not the raw check result, decides what the
                    // world sees: an Open breaker advertises Down regardless of
                    // a late success, and a HalfOpen breaker is never serving.
                    let advertised = advertised_status(breaker.state(), status);
                    let err = match outcome {
                        Outcome::Failure => detail.map(Arc::from),
                        Outcome::Success => None,
                    };

                    publish(&cell, &dirty, &name, &events, probes, &breaker, advertised, err);

                    if let Some(t) = transition {
                        emit_transition(&events, &name, t, breaker.consecutive_failures());
                    }
                }
            }
        }
    }
}

/// Map breaker state + raw status to the status the snapshot advertises.
///
/// `Open => Down`, `HalfOpen => Down` (a trial success must not flap the pod in
/// before `success_threshold`), `Closed =>` the check's own status.
fn advertised_status(state: BreakerState, raw: HealthStatus) -> HealthStatus {
    match state {
        BreakerState::Closed => raw,
        BreakerState::Open | BreakerState::HalfOpen => HealthStatus::Down,
    }
}

/// Write the per-check cell (via `send_replace`, which updates even with zero
/// receivers) and bump the aggregator's dirty signal. Emits a `CheckTransition`
/// edge only when the advertised status actually changed.
#[allow(clippy::too_many_arguments)]
fn publish(
    cell: &watch::Sender<CheckSnapshot>,
    dirty: &watch::Sender<u64>,
    name: &Arc<str>,
    events: &broadcast::Sender<HealthEvent>,
    probes: Probe,
    breaker: &Breaker,
    status: HealthStatus,
    err: Option<Arc<str>>,
) {
    let now = SystemTime::now();
    let prev_status = cell.borrow().status;
    let prev_last_ok = cell.borrow().last_ok;
    let prev_last_err = cell.borrow().last_err.clone();

    let (last_ok, last_err) = if status.is_serving() {
        (Some(now), None)
    } else {
        (prev_last_ok, err.or(prev_last_err))
    };

    let snap = CheckSnapshot {
        status,
        breaker: breaker.state(),
        probes,
        last_ok,
        last_err,
        consecutive_failures: breaker.consecutive_failures(),
        checked_at: Some(now),
    };

    cell.send_replace(snap);
    dirty.send_modify(|n| *n = n.wrapping_add(1));

    if prev_status != status {
        let _ = events.send(HealthEvent::CheckTransition {
            name: name.clone(),
            from: prev_status,
            to: status,
        });
    }
}

/// Translate a breaker [`Transition`] into the matching breaker event.
fn emit_transition(
    events: &broadcast::Sender<HealthEvent>,
    name: &Arc<str>,
    t: Transition,
    after_failures: u32,
) {
    let event = match t.to {
        BreakerState::Open => HealthEvent::BreakerOpened {
            name: name.clone(),
            after_failures,
        },
        BreakerState::HalfOpen => HealthEvent::BreakerHalfOpen { name: name.clone() },
        BreakerState::Closed => HealthEvent::BreakerClosed { name: name.clone() },
    };
    let _ = events.send(event);
}

/// Inputs the aggregator subscribes to.
pub(crate) struct Aggregator {
    /// One `(name, receiver)` pair per check cell, in registration order.
    pub(crate) cells: Vec<(Box<str>, watch::Receiver<CheckSnapshot>)>,
    /// Coalescing wake signal bumped by every prober write.
    pub(crate) dirty: watch::Receiver<u64>,
    /// The warm-up gate.
    pub(crate) startup: watch::Receiver<bool>,
    /// Drain latch, set by `HealthRegistry::drain` phase 1 *before* the readiness
    /// flip and independent of the cancel token. While set, the aggregator clamps
    /// readiness to `Down` and never re-advertises serving, so grace-window
    /// re-probes and the post-cancel fold cannot un-drain the snapshot.
    pub(crate) draining: watch::Receiver<bool>,
    /// Authoritative top-level snapshot; the aggregator owns this sender.
    pub(crate) snapshot: watch::Sender<HealthSnapshot>,
    /// Advisory edge bus, for readiness flips.
    pub(crate) events: broadcast::Sender<HealthEvent>,
    /// Host drain token; on cancel the aggregator stops.
    pub(crate) cancel: CancellationToken,
}

impl Aggregator {
    /// Run the aggregator until cancelled. Recomputes the top-level snapshot on
    /// any per-check change or warm-up flip, publishing only on a semantic delta.
    pub(crate) async fn run(mut self) {
        // Seed once so the snapshot reflects the initial (pending) cells.
        self.recompute();

        // Once the startup receiver is closed (controller dropped) we must stop
        // selecting on it: a closed `watch::Receiver::changed()` resolves
        // immediately with `Err` on every poll, which with `biased` ordering
        // would spin the loop and starve the cancel/dirty arms. Watch a *clone*
        // here so `recompute()` keeps reading the canonical `self.startup`, and
        // fuse this clone to `None` once it closes so the arm is skipped.
        let mut startup_watch: Option<watch::Receiver<bool>> = Some(self.startup.clone());

        loop {
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    // Do not recompute on cancel: drain phase 1 has already
                    // published the authoritative readiness=Down, and a final
                    // recompute would only risk re-advertising a serving value
                    // (the draining latch guards against it, but there is no work
                    // to fold either). Just stop.
                    break;
                }
                changed = self.dirty.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.recompute();
                }
                changed = changed_or_pending(startup_watch.as_mut()) => {
                    match changed {
                        Ok(()) => self.recompute(),
                        // Startup controller dropped: stop watching it (fuse the
                        // arm) rather than re-polling a permanently-ready Err.
                        Err(()) => startup_watch = None,
                    }
                }
            }
        }
    }

    /// Recompute the aggregate snapshot and publish it iff it changed
    /// semantically (status/breaker/tags), ignoring timestamp-only churn.
    fn recompute(&mut self) {
        let startup_ready = *self.startup.borrow();

        let mut checks: BTreeMap<Box<str>, CheckSnapshot> = BTreeMap::new();
        let mut startup_agg = Agg::default();
        let mut liveness_agg = Agg::default();
        let mut readiness_agg = Agg::default();

        for (name, rx) in &self.cells {
            let snap = rx.borrow().clone();
            if snap.probes.contains(Probe::STARTUP) {
                startup_agg.fold(snap.status);
            }
            if snap.probes.contains(Probe::LIVENESS) {
                liveness_agg.fold(snap.status);
            }
            if snap.probes.contains(Probe::READINESS) {
                readiness_agg.fold(snap.status);
            }
            checks.insert(name.clone(), snap);
        }

        // Startup probe also gates on the warm-up controller.
        let startup = if startup_ready {
            startup_agg.finish()
        } else {
            HealthStatus::Unknown
        };
        let liveness = liveness_agg.finish();

        // The drain latch is authoritative over readiness. Once drain phase 1
        // sets it, every recompute (grace-window re-probes and any final fold)
        // must keep readiness clamped `Down`, otherwise the still-serving cells
        // would race the forced `Down` back to a serving value in the shared
        // snapshot and a draining pod would re-advertise ready.
        let draining = *self.draining.borrow();
        let readiness = if draining {
            HealthStatus::Down
        } else {
            readiness_agg.finish()
        };

        let prev = self.snapshot.borrow().clone();
        let was_ready = startup_ready && prev.readiness.is_serving();
        // While draining, never compute now_ready as serving, so the
        // `BecameReady` edge cannot fire mid-drain.
        let now_ready = !draining && startup_ready && readiness.is_serving();

        let changed = prev.startup != startup
            || prev.liveness != liveness
            || prev.readiness != readiness
            || !maps_semantically_eq(&prev.checks, &checks);

        if changed {
            let next = HealthSnapshot {
                startup,
                liveness,
                readiness,
                checks: Arc::new(checks),
                generation: prev.generation.wrapping_add(1),
            };
            self.snapshot.send_replace(next);

            if now_ready && !was_ready {
                let _ = self.events.send(HealthEvent::BecameReady);
            } else if was_ready && !now_ready {
                let _ = self.events.send(HealthEvent::BecameNotReady);
            }
        }
    }
}

/// Await `rx.changed()` when present, mapping its error to `Err(())`; when
/// `None`, never resolve. This lets the aggregator fuse a closed startup arm to
/// `None` and stop polling it, instead of spinning on an immediately-ready
/// `Err` future under `biased` selection.
async fn changed_or_pending(rx: Option<&mut watch::Receiver<bool>>) -> Result<(), ()> {
    match rx {
        Some(rx) => rx.changed().await.map_err(|_| ()),
        None => std::future::pending().await,
    }
}

/// Worst-wins fold over a tagged subset. An empty subset is `Up`.
#[derive(Default)]
struct Agg {
    worst: Option<HealthStatus>,
}

impl Agg {
    fn fold(&mut self, s: HealthStatus) {
        self.worst = Some(match self.worst {
            Some(w) => w.max(s),
            None => s,
        });
    }
    fn finish(self) -> HealthStatus {
        self.worst.unwrap_or(HealthStatus::Up)
    }
}

/// Semantic equality of two check maps: keys plus each value's meaningful
/// fields, ignoring timestamps so a successful re-probe does not wake readers.
fn maps_semantically_eq(
    a: &BTreeMap<Box<str>, CheckSnapshot>,
    b: &BTreeMap<Box<str>, CheckSnapshot>,
) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|((ak, av), (bk, bv))| ak == bk && av.semantically_eq(bv))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::check::{Check, CheckResult};
    use crate::config::BreakerConfig;
    use async_trait::async_trait;
    use std::future::pending;
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::time::Duration;
    use tokio::sync::broadcast;

    /// A check whose behaviour is flipped at runtime via an atomic `mode`, so a
    /// single instance can simulate a dependency that goes down, hangs past the
    /// timeout, then recovers — all while the prober task keeps running.
    struct ModeCheck {
        name: String,
        mode: Arc<AtomicU8>,
    }

    /// Report a serving `Up`.
    const MODE_UP: u8 = 0;
    /// Report a non-serving `Down` immediately.
    const MODE_DOWN: u8 = 1;
    /// Never return, forcing the prober's per-attempt timeout to fire.
    const MODE_HANG: u8 = 2;

    impl ModeCheck {
        fn new(name: &str, mode: Arc<AtomicU8>) -> Self {
            ModeCheck {
                name: name.to_string(),
                mode,
            }
        }
    }

    #[async_trait]
    impl Check for ModeCheck {
        fn name(&self) -> &str {
            &self.name
        }

        async fn check(&self, _cx: &CheckContext) -> CheckResult {
            match self.mode.load(Ordering::SeqCst) {
                MODE_UP => CheckResult::up(),
                MODE_DOWN => CheckResult::down("forced down"),
                _ => pending::<CheckResult>().await,
            }
        }
    }

    /// Drive a single prober with a tight config and hand back the levers a test
    /// needs: the mode switch, a receiver on the per-check cell, an event
    /// subscriber, and the cancel token.
    fn spawn_prober(
        mode: Arc<AtomicU8>,
        config: CheckConfig,
        probes: Probe,
    ) -> (
        watch::Receiver<CheckSnapshot>,
        broadcast::Receiver<HealthEvent>,
        CancellationToken,
    ) {
        let (cell_tx, cell_rx) = watch::channel(CheckSnapshot::pending(probes));
        let (dirty_tx, _dirty_rx) = watch::channel(0u64);
        let events = broadcast::Sender::new(64);
        let events_rx = events.subscribe();
        let cancel = CancellationToken::new();

        let task = ProberTask {
            check: Box::new(ModeCheck::new("db", mode)),
            probes,
            config,
            cell: cell_tx,
            events,
            dirty: dirty_tx,
            cancel: cancel.child_token(),
        };
        tokio::spawn(task.run());

        (cell_rx, events_rx, cancel)
    }

    fn tight() -> CheckConfig {
        CheckConfig {
            interval: Duration::from_secs(1),
            timeout: Duration::from_millis(200),
            initial_delay: Duration::ZERO,
            breaker: BreakerConfig {
                failure_threshold: 3,
                success_threshold: 1,
                cooldown: Duration::from_secs(30),
            },
        }
    }

    /// Advance virtual time by `step`, yielding around each jump so the spawned
    /// prober's `select!` actually wakes and processes the tick before we read
    /// the cell. A bare `advance` does not by itself reschedule the task.
    async fn tick(step: Duration) {
        tokio::task::yield_now().await;
        tokio::time::advance(step).await;
        tokio::task::yield_now().await;
    }

    #[tokio::test(start_paused = true)]
    async fn first_probe_populates_snapshot_up() {
        let mode = Arc::new(AtomicU8::new(MODE_UP));
        let (mut cell, _events, cancel) = spawn_prober(mode, tight(), Probe::READINESS);

        // Before the first tick the cell is still the pending seed.
        assert_eq!(cell.borrow().status, HealthStatus::Unknown);

        // Fire the first tick (interval = 1s, no initial delay).
        tick(Duration::from_secs(1)).await;
        cell.changed().await.unwrap();

        let snap = cell.borrow().clone();
        assert_eq!(snap.status, HealthStatus::Up);
        assert_eq!(snap.breaker, BreakerState::Closed);
        assert!(snap.last_ok.is_some());
        assert_eq!(snap.consecutive_failures, 0);

        cancel.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn breaker_trips_after_threshold_and_opens() {
        let mode = Arc::new(AtomicU8::new(MODE_DOWN));
        let (mut cell, mut events, cancel) = spawn_prober(mode, tight(), Probe::READINESS);

        // Three consecutive failing probes trip the breaker (threshold = 3).
        for _ in 0..3 {
            tick(Duration::from_secs(1)).await;
            let _ = cell.changed().await;
        }

        let snap = cell.borrow().clone();
        assert_eq!(snap.status, HealthStatus::Down);
        assert_eq!(snap.breaker, BreakerState::Open);

        // The edge stream carries a transition (Unknown->Down) and a BreakerOpened.
        let mut saw_opened = false;
        while let Ok(ev) = events.try_recv() {
            if let HealthEvent::BreakerOpened { after_failures, .. } = ev {
                assert_eq!(after_failures, 3);
                saw_opened = true;
            }
        }
        assert!(saw_opened, "expected a BreakerOpened event");

        cancel.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn timeout_is_folded_into_failure() {
        let mode = Arc::new(AtomicU8::new(MODE_HANG));
        let (mut cell, _events, cancel) = spawn_prober(mode.clone(), tight(), Probe::READINESS);

        // A hanging probe must not stall the prober: the 200ms timeout fires and
        // is recorded as a failure. Advance past one interval + the timeout.
        tick(Duration::from_secs(1)).await;
        tick(Duration::from_millis(200)).await;
        cell.changed().await.unwrap();

        let snap = cell.borrow().clone();
        assert_eq!(snap.status, HealthStatus::Down);
        // At least one timeout has been folded into a failure. The exact count is
        // not asserted: under paused time a hang lets the interval re-fire, so one
        // or more ticks may have elapsed — the point is the prober never stalls
        // and every elapsed attempt counts as a failure with the timeout reason.
        assert!(
            snap.consecutive_failures >= 1,
            "a hanging probe must record at least one failure, got {}",
            snap.consecutive_failures
        );
        assert_eq!(
            snap.last_err.as_deref(),
            Some("probe timed out"),
            "timeout should surface a distinct error string"
        );

        cancel.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn open_breaker_skips_probes_then_recovers_after_cooldown() {
        let mode = Arc::new(AtomicU8::new(MODE_DOWN));
        let (mut cell, mut events, cancel) = spawn_prober(mode.clone(), tight(), Probe::READINESS);

        // Trip the breaker.
        for _ in 0..3 {
            tick(Duration::from_secs(1)).await;
            let _ = cell.changed().await;
        }
        assert_eq!(cell.borrow().breaker, BreakerState::Open);

        // While Open the prober must SKIP the probe — flip the dependency healthy
        // and confirm a tick during the cooldown does not advertise Up.
        mode.store(MODE_UP, Ordering::SeqCst);
        tick(Duration::from_secs(1)).await;
        assert_eq!(
            cell.borrow().status,
            HealthStatus::Down,
            "a cooling breaker must not probe a now-healthy dep"
        );
        assert_eq!(cell.borrow().breaker, BreakerState::Open);

        // Advance past the 30s cooldown. The next tick rolls Open->HalfOpen and
        // runs exactly one trial probe; with the dep healthy and
        // success_threshold = 1 it closes immediately.
        tick(Duration::from_secs(30)).await;
        // Pump ticks until the breaker closes (HalfOpen trial then Closed).
        let mut closed = false;
        for _ in 0..4 {
            let _ = cell.changed().await;
            if cell.borrow().breaker == BreakerState::Closed {
                closed = true;
                break;
            }
            tick(Duration::from_secs(1)).await;
        }
        assert!(closed, "breaker should close after a successful trial");
        assert_eq!(cell.borrow().status, HealthStatus::Up);

        // The event stream must have carried the HalfOpen and Closed edges.
        let mut saw_half_open = false;
        let mut saw_closed = false;
        while let Ok(ev) = events.try_recv() {
            match ev {
                HealthEvent::BreakerHalfOpen { .. } => saw_half_open = true,
                HealthEvent::BreakerClosed { .. } => saw_closed = true,
                _ => {}
            }
        }
        assert!(saw_half_open, "expected a BreakerHalfOpen event");
        assert!(saw_closed, "expected a BreakerClosed event");

        cancel.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_stops_the_prober() {
        let mode = Arc::new(AtomicU8::new(MODE_UP));
        let (mut cell, _events, cancel) = spawn_prober(mode, tight(), Probe::READINESS);

        tick(Duration::from_secs(1)).await;
        cell.changed().await.unwrap();
        assert_eq!(cell.borrow().status, HealthStatus::Up);

        // After cancel the loop breaks and the cell sender is dropped, so the
        // receiver observes a closed channel on the next `changed()`.
        cancel.cancel();
        tokio::task::yield_now().await;
        assert!(
            cell.changed().await.is_err(),
            "cancelled prober drops its cell sender"
        );
    }

    /// End-to-end of the aggregator: two readiness checks, one of which flips
    /// down, must drive the readiness aggregate down and emit BecameNotReady.
    #[tokio::test(start_paused = true)]
    async fn aggregator_folds_readiness_and_emits_edges() {
        let (cell_a_tx, cell_a_rx) = watch::channel(CheckSnapshot::pending(Probe::READINESS));
        let (cell_b_tx, cell_b_rx) = watch::channel(CheckSnapshot::pending(Probe::READINESS));
        let (dirty_tx, dirty_rx) = watch::channel(0u64);
        let (startup_tx, startup_rx) = watch::channel(true);
        let (_draining_tx, draining_rx) = watch::channel(false);
        let (snap_tx, snap_rx) = watch::channel(HealthSnapshot::initial());
        let events = broadcast::Sender::new(64);
        let mut events_rx = events.subscribe();
        let cancel = CancellationToken::new();

        let agg = Aggregator {
            cells: vec![(Box::from("a"), cell_a_rx), (Box::from("b"), cell_b_rx)],
            dirty: dirty_rx,
            startup: startup_rx,
            draining: draining_rx,
            snapshot: snap_tx,
            events: events.clone(),
            cancel: cancel.child_token(),
        };
        tokio::spawn(agg.run());

        // Bring both checks Up. The aggregator wakes on the dirty bump.
        let up = |probes| CheckSnapshot {
            status: HealthStatus::Up,
            breaker: BreakerState::Closed,
            probes,
            last_ok: Some(SystemTime::now()),
            last_err: None,
            consecutive_failures: 0,
            checked_at: Some(SystemTime::now()),
        };
        cell_a_tx.send_replace(up(Probe::READINESS));
        cell_b_tx.send_replace(up(Probe::READINESS));
        dirty_tx.send_modify(|n| *n += 1);
        tokio::task::yield_now().await;

        // Wait for readiness to read Up.
        let mut rx = snap_rx.clone();
        loop {
            if rx.borrow_and_update().readiness == HealthStatus::Up {
                break;
            }
            rx.changed().await.unwrap();
        }
        // Drain the BecameReady edge.
        let mut saw_ready = false;
        while let Ok(ev) = events_rx.try_recv() {
            if ev == HealthEvent::BecameReady {
                saw_ready = true;
            }
        }
        assert!(saw_ready, "expected BecameReady once both checks were Up");

        // Now flip check B down; readiness must follow and BecameNotReady fire.
        let mut down_b = up(Probe::READINESS);
        down_b.status = HealthStatus::Down;
        down_b.breaker = BreakerState::Open;
        cell_b_tx.send_replace(down_b);
        dirty_tx.send_modify(|n| *n += 1);
        tokio::task::yield_now().await;

        loop {
            if rx.borrow_and_update().readiness == HealthStatus::Down {
                break;
            }
            rx.changed().await.unwrap();
        }
        let mut saw_not_ready = false;
        while let Ok(ev) = events_rx.try_recv() {
            if ev == HealthEvent::BecameNotReady {
                saw_not_ready = true;
            }
        }
        assert!(
            saw_not_ready,
            "expected BecameNotReady when a readiness dep fell"
        );

        // The detail map must still contain both checks.
        let snap = rx.borrow().clone();
        assert_eq!(snap.checks.len(), 2);
        assert!(snap.checks.contains_key("a"));
        assert!(snap.checks.contains_key("b"));

        cancel.cancel();
        let _ = startup_tx; // keep the gate alive until end of test
    }

    /// Startup gate: while the warm-up flag is false the startup aggregate is
    /// Unknown regardless of the underlying checks; flipping it true releases it.
    #[tokio::test(start_paused = true)]
    async fn aggregator_gates_startup_on_warmup() {
        let (cell_tx, cell_rx) = watch::channel(CheckSnapshot::pending(Probe::STARTUP));
        let (dirty_tx, dirty_rx) = watch::channel(0u64);
        let (startup_tx, startup_rx) = watch::channel(false);
        let (_draining_tx, draining_rx) = watch::channel(false);
        let (snap_tx, snap_rx) = watch::channel(HealthSnapshot::initial());
        let events = broadcast::Sender::new(64);
        let cancel = CancellationToken::new();

        let agg = Aggregator {
            cells: vec![(Box::from("warm"), cell_rx)],
            dirty: dirty_rx,
            startup: startup_rx,
            draining: draining_rx,
            snapshot: snap_tx,
            events,
            cancel: cancel.child_token(),
        };
        tokio::spawn(agg.run());

        // Check is Up but warm-up is still pending: startup stays Unknown.
        cell_tx.send_replace(CheckSnapshot {
            status: HealthStatus::Up,
            breaker: BreakerState::Closed,
            probes: Probe::STARTUP,
            last_ok: Some(SystemTime::now()),
            last_err: None,
            consecutive_failures: 0,
            checked_at: Some(SystemTime::now()),
        });
        dirty_tx.send_modify(|n| *n += 1);
        tokio::task::yield_now().await;

        let mut rx = snap_rx.clone();
        // Give the aggregator a moment; startup must not be serving yet.
        tokio::task::yield_now().await;
        assert_eq!(rx.borrow_and_update().startup, HealthStatus::Unknown);

        // Open the gate; startup now reflects the Up check.
        startup_tx.send_replace(true);
        loop {
            if rx.borrow_and_update().startup == HealthStatus::Up {
                break;
            }
            rx.changed().await.unwrap();
        }

        cancel.cancel();
    }
}
