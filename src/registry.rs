//! The builder, the host-app handle, and the task supervisor.
//!
//! Replaces the old `Health` / `HealthBuilder` / `impl Layer`. Wiring is now
//! explicit and `State`-based (see [`crate::router`]); there is no
//! `Extension`/`Layer` self-injection.

use crate::check::Check;
use crate::config::{CheckConfig, Probe};
use crate::events::{EventStream, HealthEvent};
use crate::prober::{Aggregator, ProberTask};
use crate::snapshot::{CheckSnapshot, HealthSnapshot};
use crate::startup::StartupController;
use crate::status::HealthStatus;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Broadcast ring capacity for the advisory event bus.
const EVENT_CAPACITY: usize = 256;

/// Default grace window between flipping readiness `Down` and stopping the
/// probers, so Kubernetes observes the 503 before the pod stops accepting.
const DEFAULT_DRAIN_GRACE: Duration = Duration::from_secs(5);

/// Outer deadline on joining tasks during drain, defending against the axum 0.8
/// idle-keep-alive graceful-shutdown hang (tokio-rs/axum#3326).
const DEFAULT_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

struct Registration {
    probes: Probe,
    config: CheckConfig,
    check: Box<dyn Check>,
}

/// Accumulates check registrations, then spawns the prober + aggregator tasks.
pub struct HealthBuilder {
    defaults: CheckConfig,
    registrations: Vec<Registration>,
    drain_grace: Duration,
    drain_timeout: Duration,
}

impl Default for HealthBuilder {
    fn default() -> Self {
        HealthBuilder {
            defaults: CheckConfig::default(),
            registrations: Vec::new(),
            drain_grace: DEFAULT_DRAIN_GRACE,
            drain_timeout: DEFAULT_DRAIN_TIMEOUT,
        }
    }
}

impl HealthBuilder {
    /// A builder with default tuning.
    pub fn new() -> Self {
        HealthBuilder::default()
    }

    /// Override the default [`CheckConfig`] applied to checks registered with
    /// [`register`](Self::register) / [`register_boxed`](Self::register_boxed).
    pub fn defaults(mut self, cfg: CheckConfig) -> Self {
        self.defaults = cfg;
        self
    }

    /// The grace window between phase 1 (readiness flips `Down`) and phase 3
    /// (probers stop) of [`HealthRegistry::drain`]. Set this `>=
    /// readinessProbe.periodSeconds * failureThreshold`.
    pub fn drain_grace(mut self, grace: Duration) -> Self {
        self.drain_grace = grace;
        self
    }

    /// The outer deadline on joining tasks during drain. Keep it below
    /// `terminationGracePeriodSeconds`.
    pub fn drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
        self
    }

    /// Register a check under one or more probes with the default config.
    pub fn register(self, probes: Probe, check: impl Check) -> Self {
        self.register_boxed(probes, Box::new(check))
    }

    /// Register a boxed check (e.g. from [`crate::check::check_fn`]).
    pub fn register_boxed(mut self, probes: Probe, check: Box<dyn Check>) -> Self {
        let config = self.defaults;
        self.registrations.push(Registration {
            probes,
            config,
            check,
        });
        self
    }

    /// Register a check with an explicit per-check config.
    pub fn register_with(mut self, probes: Probe, cfg: CheckConfig, check: impl Check) -> Self {
        self.registrations.push(Registration {
            probes,
            config: cfg,
            check: Box::new(check),
        });
        self
    }

    /// Spawn one prober per check plus the aggregator, deriving each prober's
    /// cancellation token as a child of `cancel` (the host's drain token).
    /// Returns the registry handle and the warm-up controller.
    pub fn build(self, cancel: CancellationToken) -> (HealthRegistry, StartupController) {
        let events = broadcast::Sender::new(EVENT_CAPACITY);
        let (snapshot_tx, snapshot_rx) = watch::channel(HealthSnapshot::initial());
        let (startup, startup_rx) = StartupController::new();
        // Coalescing wake signal: probers bump it, the aggregator waits on it.
        let (dirty_tx, dirty_rx) = watch::channel(0u64);
        // Drain latch, independent of the cancel token. `drain()` sets it in
        // phase 1 so the aggregator clamps readiness `Down` for the whole drain.
        let (draining_tx, draining_rx) = watch::channel(false);

        // An abortable supervisor: on drain_timeout expiry we `abort_all()` any
        // task that did not finish, so a check that ignores both its per-attempt
        // timeout and the cancel token cannot leak past drain holding a pool ref.
        let mut tasks: JoinSet<()> = JoinSet::new();
        let mut cells = Vec::with_capacity(self.registrations.len());

        for reg in self.registrations {
            let name: Box<str> = Box::from(reg.check.name());
            let (cell_tx, cell_rx) = watch::channel(CheckSnapshot::pending(reg.probes));
            cells.push((name, cell_rx));

            let task = ProberTask {
                check: reg.check,
                probes: reg.probes,
                config: reg.config,
                cell: cell_tx,
                events: events.clone(),
                dirty: dirty_tx.clone(),
                cancel: cancel.child_token(),
            };
            tasks.spawn(task.run());
        }

        let aggregator = Aggregator {
            cells,
            dirty: dirty_rx,
            startup: startup_rx,
            draining: draining_rx,
            snapshot: snapshot_tx.clone(),
            events: events.clone(),
            cancel: cancel.child_token(),
        };
        tasks.spawn(aggregator.run());

        let inner = Arc::new(RegistryInner {
            snapshot_tx,
            snapshot_rx,
            events,
            startup: startup.clone(),
            cancel,
            draining: draining_tx,
            tasks: Mutex::new(tasks),
            drain_grace: self.drain_grace,
            drain_timeout: self.drain_timeout,
        });

        (HealthRegistry { inner }, startup)
    }
}

struct RegistryInner {
    snapshot_tx: watch::Sender<HealthSnapshot>,
    snapshot_rx: watch::Receiver<HealthSnapshot>,
    events: broadcast::Sender<HealthEvent>,
    startup: StartupController,
    cancel: CancellationToken,
    /// Drain latch: set in `drain()` phase 1, read by the aggregator to keep
    /// readiness clamped `Down` for the duration of the drain.
    draining: watch::Sender<bool>,
    /// The supervised prober + aggregator tasks. Held behind a `Mutex` because
    /// `JoinSet` needs `&mut` to join/abort and the registry is shared via `Arc`.
    tasks: Mutex<JoinSet<()>>,
    drain_grace: Duration,
    drain_timeout: Duration,
}

impl RegistryInner {
    /// Join every supervised task bounded by `drain_timeout`. If the deadline
    /// fires, abort every task that has not finished and reap them, so that
    /// `drain()` upholds its invariant: after it returns, no supervised task is
    /// still running. A check that ignores both its per-attempt timeout and the
    /// cancel token is force-cancelled here rather than leaked.
    async fn join_or_abort(&self) {
        let mut tasks = self.tasks.lock().await;
        let joined = tokio::time::timeout(self.drain_timeout, async {
            while tasks.join_next().await.is_some() {}
        })
        .await;
        if joined.is_err() {
            // Deadline blown: force-cancel the stragglers and reap them so the
            // JoinSet is empty and no task survives drain.
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
    }
}

/// Host-app handle owning the supervised tasks. Cloneable handles for the
/// request path come from [`handle`](Self::handle).
#[derive(Clone)]
pub struct HealthRegistry {
    inner: Arc<RegistryInner>,
}

impl HealthRegistry {
    /// A cheap clone for handlers and event subscribers.
    pub fn handle(&self) -> HealthHandle {
        HealthHandle {
            snapshot: self.inner.snapshot_rx.clone(),
            events: self.inner.events.clone(),
            startup: self.inner.startup.clone(),
            cancel: self.inner.cancel.clone(),
        }
    }

    /// An axum `Router` mounting the probe + detail endpoints over a handle.
    pub fn router(&self) -> axum::Router {
        crate::router::health_router(self.handle())
    }

    /// Consume the registry into a router. The supervised tasks stay alive via
    /// the `Arc` captured in the handler state.
    pub fn into_router(self) -> axum::Router {
        crate::router::health_router(self.handle())
    }

    /// Fail-ready-first two-phase drain. Idempotent.
    ///
    /// 1. Flip the snapshot's `readiness` to `Down`, emit `DrainStarted` +
    ///    `BecameNotReady`. Liveness is untouched and stays `200`.
    /// 2. Sleep `drain_grace` so Kubernetes observes the 503 and deregisters the
    ///    endpoint before the server stops accepting.
    /// 3. Cancel the drain token (stopping every prober + the aggregator) and
    ///    wait for the `TaskTracker` to drain, bounded by `drain_timeout`.
    pub async fn drain(&self) {
        // Idempotency: the token being already cancelled means a prior drain ran.
        if self.inner.cancel.is_cancelled() {
            // Still await task completion so a second caller does not return
            // before the first drain's tasks have exited. The first drain may
            // already have joined and emptied the set; `join_all` on an empty
            // set returns immediately.
            self.inner.join_or_abort().await;
            return;
        }

        // Phase 1: set the drain latch BEFORE the readiness flip so any
        // grace-window recompute keeps readiness clamped `Down`, then force
        // readiness Down and emit the advisory edges, all before any accept-stop.
        let _ = self.inner.draining.send(true);
        let mut became_not_ready = false;
        self.inner.snapshot_tx.send_if_modified(|snap| {
            if snap.readiness.is_serving() {
                became_not_ready = true;
            }
            if snap.readiness != HealthStatus::Down {
                snap.readiness = HealthStatus::Down;
                snap.generation = snap.generation.wrapping_add(1);
                true
            } else {
                false
            }
        });
        let _ = self.inner.events.send(HealthEvent::DrainStarted);
        if became_not_ready {
            let _ = self.inner.events.send(HealthEvent::BecameNotReady);
        }

        // Phase 2: grace window.
        tokio::time::sleep(self.inner.drain_grace).await;

        // Phase 3: stop probers + aggregator, join with an outer deadline, and
        // abort anything still running when that deadline fires.
        self.inner.cancel.cancel();
        self.inner.join_or_abort().await;
    }
}

/// Read-side handle for the request path and host subscribers. Cheap to clone.
#[derive(Clone)]
pub struct HealthHandle {
    snapshot: watch::Receiver<HealthSnapshot>,
    events: broadcast::Sender<HealthEvent>,
    startup: StartupController,
    cancel: CancellationToken,
}

impl HealthHandle {
    /// The current cached snapshot (`borrow().clone()`, `Arc` inside, O(1)).
    pub fn snapshot(&self) -> HealthSnapshot {
        self.snapshot.borrow().clone()
    }

    /// Whether the pod should advertise ready: not draining, warm-up complete,
    /// and the readiness aggregate serving.
    pub fn is_ready(&self) -> bool {
        !self.cancel.is_cancelled()
            && self.startup.is_ready()
            && self.snapshot.borrow().readiness.is_serving()
    }

    /// Whether warm-up has completed.
    pub fn is_started(&self) -> bool {
        self.startup.is_ready()
    }

    /// Whether the host has begun draining.
    pub fn is_draining(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Await the next snapshot change. `None` once the producer is gone.
    pub async fn changed(&mut self) -> Option<HealthSnapshot> {
        self.snapshot.changed().await.ok()?;
        Some(self.snapshot.borrow_and_update().clone())
    }

    /// Subscribe to the advisory event stream. Subscribe-then-seed: the host
    /// treats [`snapshot`](Self::snapshot) as the synthetic initial event.
    pub fn events(&self) -> EventStream {
        EventStream::new(self.events.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::check::{check_fn, CheckResult};
    use crate::events::HealthEvent;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    /// Poll `f` on a short cadence until it returns true or ~2s elapses.
    async fn eventually(mut f: impl FnMut() -> bool) -> bool {
        for _ in 0..200 {
            if f() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        false
    }

    fn fast() -> CheckConfig {
        CheckConfig {
            interval: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
            initial_delay: Duration::ZERO,
            ..CheckConfig::default()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn build_probe_and_drain_lifecycle() {
        let cancel = CancellationToken::new();
        let healthy = Arc::new(AtomicBool::new(true));
        let flag = healthy.clone();

        let (registry, startup) = HealthBuilder::new()
            .drain_grace(Duration::from_millis(10))
            .drain_timeout(Duration::from_secs(2))
            .register_with(
                Probe::READINESS | Probe::LIVENESS,
                fast(),
                check_fn("dep", move |_cx| {
                    let up = flag.load(Ordering::SeqCst);
                    async move {
                        if up {
                            CheckResult::up()
                        } else {
                            CheckResult::down("dep down")
                        }
                    }
                }),
            )
            .build(cancel.clone());

        let handle = registry.handle();

        // Not ready before the startup gate opens, even once the probe is Up.
        assert!(!handle.is_ready());
        startup.mark_ready();

        assert!(
            eventually(|| handle.is_ready()).await,
            "should become ready after warm-up and a healthy probe"
        );

        // Flip the dependency down; readiness should follow (default breaker
        // threshold is 3, interval 10ms, so well within the poll budget).
        healthy.store(false, Ordering::SeqCst);
        assert!(
            eventually(|| !handle.is_ready()).await,
            "readiness should fall once the breaker trips"
        );
        // Liveness is unaffected by a tripped readiness/liveness dep? It IS tagged
        // liveness here, so it falls too — assert the snapshot reflects Down.
        assert!(eventually(|| handle.snapshot().liveness == HealthStatus::Down).await);

        // Recover, then drain. Drain forces readiness Down regardless.
        healthy.store(true, Ordering::SeqCst);
        assert!(eventually(|| handle.is_ready()).await, "should recover");

        registry.drain().await;
        assert!(handle.is_draining());
        assert!(!handle.is_ready(), "drain forces not-ready");
        // A second drain is idempotent and returns promptly.
        registry.drain().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drain_emits_drain_started_and_not_ready() {
        let cancel = CancellationToken::new();
        let (registry, startup) = HealthBuilder::new()
            .drain_grace(Duration::from_millis(10))
            .register_with(
                Probe::READINESS,
                fast(),
                check_fn("ok", |_cx| async { CheckResult::up() }),
            )
            .build(cancel.clone());

        let handle = registry.handle();
        let mut events = handle.events();
        startup.mark_ready();
        assert!(eventually(|| handle.is_ready()).await);

        registry.drain().await;

        // Collect the edges that drain produced; order is DrainStarted then
        // BecameNotReady (readiness was serving at drain time).
        let mut saw_drain = false;
        let mut saw_not_ready = false;
        for _ in 0..32 {
            match tokio::time::timeout(Duration::from_millis(50), events.next()).await {
                Ok(Some(HealthEvent::DrainStarted)) => saw_drain = true,
                Ok(Some(HealthEvent::BecameNotReady)) => saw_not_ready = true,
                Ok(Some(_)) => {}
                _ => break,
            }
            if saw_drain && saw_not_ready {
                break;
            }
        }
        assert!(saw_drain, "drain must emit DrainStarted");
        assert!(
            saw_not_ready,
            "drain of a serving pod must emit BecameNotReady"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn changed_observes_a_snapshot_delta() {
        let cancel = CancellationToken::new();
        let healthy = Arc::new(AtomicBool::new(true));
        let flag = healthy.clone();
        let (registry, startup) = HealthBuilder::new()
            .register_with(
                Probe::READINESS,
                fast(),
                check_fn("dep", move |_cx| {
                    let up = flag.load(Ordering::SeqCst);
                    async move {
                        if up {
                            CheckResult::up()
                        } else {
                            CheckResult::down("down")
                        }
                    }
                }),
            )
            .build(cancel.clone());

        let mut handle = registry.handle();
        startup.mark_ready();
        assert!(eventually(|| handle.snapshot().readiness == HealthStatus::Up).await);

        // Trip it and confirm `changed()` yields a snapshot whose readiness fell.
        healthy.store(false, Ordering::SeqCst);
        let mut fell = false;
        for _ in 0..50 {
            match tokio::time::timeout(Duration::from_millis(100), handle.changed()).await {
                Ok(Some(snap)) if snap.readiness == HealthStatus::Down => {
                    fell = true;
                    break;
                }
                Ok(Some(_)) => continue,
                _ => break,
            }
        }
        assert!(fell, "changed() should surface the readiness drop");

        registry.drain().await;
    }

    /// Regression for the fail-ready-first contract: `/health/ready` must report
    /// `503` for the *whole* grace window — between phase 1 (readiness forced
    /// `Down`) and phase 3 (token cancelled) — not just after cancellation.
    /// Previously the handler recomputed readiness from the still-`Up` per-check
    /// cells and gated only on the cancel token, so it served `200` for the
    /// entire grace window and Kubernetes kept routing traffic.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ready_probe_is_503_during_grace_window() {
        use axum::http::StatusCode;
        use axum_test::TestServer;

        let cancel = CancellationToken::new();
        let (registry, startup) = HealthBuilder::new()
            // A long grace window so the assertion lands mid-drain, before cancel.
            .drain_grace(Duration::from_secs(3))
            .drain_timeout(Duration::from_secs(2))
            .register_with(
                Probe::READINESS,
                fast(),
                check_fn("ok", |_cx| async { CheckResult::up() }),
            )
            .build(cancel.clone());

        let handle = registry.handle();
        let server = TestServer::new(registry.router()).unwrap();
        startup.mark_ready();

        // Become ready first.
        assert!(
            eventually(|| handle.is_ready()).await,
            "should be ready before drain"
        );
        assert_eq!(
            server.get("/health/ready").await.status_code(),
            StatusCode::OK
        );

        // Drive drain on a separate task; with a 3s grace it sits in phase 2.
        let drain_reg = registry.clone();
        let drain = tokio::spawn(async move { drain_reg.drain().await });

        // Wait for phase 1 to have forced readiness Down (latch + snapshot),
        // while the token is still un-cancelled (phase 2).
        assert!(
            eventually(|| !handle.snapshot().readiness.is_serving()).await,
            "phase 1 must force readiness Down"
        );
        assert!(
            !handle.is_draining(),
            "token must still be un-cancelled during the grace window"
        );

        // The probe must already be 503 here — the whole point of the grace
        // window is deregistration lead time.
        assert_eq!(
            server.get("/health/ready").await.status_code(),
            StatusCode::SERVICE_UNAVAILABLE,
            "/health/ready must be 503 during the grace window, before cancel"
        );

        drain.await.unwrap();
    }

    /// Regression for the leak finding: a check that ignores both its
    /// per-attempt timeout and the cancel token must not survive drain. The old
    /// `TaskTracker::wait()` only observed completion; on `drain_timeout` expiry
    /// the wedged task was leaked. The `JoinSet` supervisor aborts it instead, so
    /// `drain()` returns within roughly `drain_grace + drain_timeout`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drain_aborts_a_wedged_task() {
        use crate::check::{Check, CheckContext};
        use async_trait::async_trait;
        use std::future::pending;

        // A check that ignores cx and never returns, with a timeout far larger
        // than drain_timeout so the per-attempt budget cannot save us.
        struct Wedged;
        #[async_trait]
        impl Check for Wedged {
            fn name(&self) -> &str {
                "wedged"
            }
            async fn check(&self, _cx: &CheckContext) -> CheckResult {
                pending::<CheckResult>().await
            }
        }

        let cancel = CancellationToken::new();
        let wedged_cfg = CheckConfig {
            interval: Duration::from_millis(10),
            // Timeout >> drain_timeout: a pure timeout cannot bound this attempt.
            timeout: Duration::from_secs(3600),
            initial_delay: Duration::ZERO,
            ..CheckConfig::default()
        };
        let (registry, startup) = HealthBuilder::new()
            .drain_grace(Duration::from_millis(10))
            .drain_timeout(Duration::from_millis(200))
            .register_with(Probe::READINESS, wedged_cfg, Wedged)
            .build(cancel.clone());

        startup.mark_ready();
        // Let the prober enter its in-flight (forever-pending) probe.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // drain() must return promptly: grace (10ms) + timeout (200ms) + slack.
        // If the wedged task were leaked, the JoinSet would never empty and the
        // abort path would not run; the timeout below bounds the regression.
        let drained = tokio::time::timeout(Duration::from_secs(2), registry.drain()).await;
        assert!(
            drained.is_ok(),
            "drain must return after aborting the wedged task, not hang on it"
        );
    }

    /// Regression for the drain-flap findings: a healthy-pod drain must not emit
    /// a spurious `BecameReady` after `BecameNotReady`, and the snapshot readiness
    /// must stay `Down` once drain has begun (the draining latch clamps the
    /// aggregator's recompute).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drain_does_not_re_advertise_ready() {
        let cancel = CancellationToken::new();
        let (registry, startup) = HealthBuilder::new()
            .drain_grace(Duration::from_millis(100))
            .drain_timeout(Duration::from_secs(2))
            .register_with(
                // A perpetually-healthy readiness check whose probers keep ticking
                // through the grace window, attempting to recompute readiness Up.
                Probe::READINESS,
                fast(),
                check_fn("ok", |_cx| async { CheckResult::up() }),
            )
            .build(cancel.clone());

        let handle = registry.handle();
        startup.mark_ready();
        assert!(eventually(|| handle.is_ready()).await);

        // Subscribe AFTER the pod is already ready, so the pre-drain BecameReady
        // (emitted on warm-up) is not in our view; we only observe drain edges.
        let mut events = handle.events();
        registry.drain().await;

        // After drain the snapshot readiness must be Down and must not have
        // flapped back to serving.
        assert!(
            !handle.snapshot().readiness.is_serving(),
            "snapshot readiness must remain Down after drain"
        );

        // The event stream for a healthy drain is DrainStarted -> BecameNotReady,
        // and crucially NO BecameReady afterwards.
        let mut saw_not_ready = false;
        for _ in 0..64 {
            match tokio::time::timeout(Duration::from_millis(50), events.next()).await {
                Ok(Some(HealthEvent::BecameNotReady)) => saw_not_ready = true,
                Ok(Some(HealthEvent::BecameReady)) => {
                    panic!("drain must not emit a spurious BecameReady");
                }
                Ok(Some(_)) => {}
                _ => break,
            }
        }
        assert!(
            saw_not_ready,
            "drain of a serving pod must emit BecameNotReady"
        );
    }
}
