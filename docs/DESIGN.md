# axum-health redesign: probes + circuit breaker + reactive notification

Status: architecture, ready to build. Branch `redesign/probes-circuit-breaker`. Toolchain rustc/cargo 1.95, edition 2021, axum 0.8.1, tokio-only.

This is a **hard break with zero backward compatibility**. The Spring-Boot-style per-request health library is deleted outright and replaced with the .NET-HealthChecks pattern adapted for tokio: tag-split probe endpoints, background prober tasks with per-attempt timeouts, a per-check circuit breaker driving a cached status, a `watch` snapshot plus a `broadcast` transition stream out to the host app, a warm-up gate, and a fail-ready-first graceful drain on a `CancellationToken`.

Everything below is grounded in the current source, not assumed:

- `src/service.rs:13` — `Health(Arc<BTreeMap<String, Arc<dyn HealthIndicator>>>)`.
- `src/service.rs:20-38` — `details()` runs **every** indicator on **every** request via `futures::stream::iter(...).then(...).collect()`. This is the per-request fan-out being deleted and the sole `futures` consumer.
- `src/service.rs:41-47` — `impl<S> Layer<S> for Health` injecting itself as an axum `Extension`. The `tower-layer` dep (`Cargo.toml:21`) exists only for this and is also dropped.
- `src/service.rs:66-79` — `HealthIndicator` trait and the 5-variant `HealthStatus` (`Up/Down/OutOfService/Unknown/Custom`) with a worst-wins `Ord`.
- `src/lib.rs:7-11` — the single `health` handler over `Extension<Health>`.
- `src/database/mod.rs:15-18` — `Pingable { async fn ping(&self) -> bool }`, wrapped by `DatabaseHealthIndicator` (`mod.rs:20-53`). It throws away the error and has **no timeout** — a dead Postgres `acquire().await` hangs forever (`sqlx.rs:11-16`).
- `src/database/diesel.rs:33-38` — the r2d2 `conn.ping()` is **synchronous/blocking**; it must run under `spawn_blocking` in the new model.
- `Cargo.toml:49` — `tokio` is currently a dev-dependency only; it must move to `[dependencies]`.

---

## 1. Module plan (file by file)

All files under `src/`. Old files are either deleted or fully rewritten; nothing is preserved.

### `src/lib.rs` (rewritten)
Crate root and the only `pub use` surface. Declares modules, re-exports the public API listed in section 3, and contains nothing executable (no handler). Documents the three-probe model and the hard-break note for docs.rs.
Key items: `pub mod status; pub mod check; pub mod breaker; pub mod config; pub mod prober; pub mod registry; pub mod snapshot; pub mod events; pub mod startup; pub mod router; pub mod database;` plus flat `pub use` of `HealthStatus`, `Check`, `CheckContext`, `CheckResult`, `CheckError`, `Probe`, `CheckConfig`, `BreakerConfig`, `BreakerState`, `HealthBuilder`, `HealthRegistry`, `HealthHandle`, `StartupController`, `HealthSnapshot`, `CheckSnapshot`, `HealthEvent`, `EventStream`, `health_router`.

### `src/status.rs` (new — extracted from `service.rs`)
The status ADT. `HealthStatus { Up, Degraded, Down, Unknown }` with an **intentional** discriminant order so `max()` = worst-wins: `Up < Degraded < Down`, and `Unknown` placed below `Down` but treated as not-serving (see section 7 for the readiness collapse rule). Derives `Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Serialize`. The old `OutOfService` and `Custom(String)` variants are deleted — `Custom` made the type non-`Copy` and non-stable-`Ord`; `Degraded` covers the real use case.
Key items: `HealthStatus`, `impl HealthStatus { fn is_serving(self) -> bool }` (true for `Up`/`Degraded`).

### `src/check.rs` (new — replaces `HealthIndicator` + `Pingable`)
The single object-safe check abstraction.
Key items: `#[async_trait] pub trait Check`, `CheckContext`, `CheckResult` (status + detail + data), `CheckError`, the `CheckResult::{up,degraded,down}` constructors and `with(k,v)` builder, plus `check_fn(name, closure)` adapter producing a `Box<dyn Check>` from an async closure.

### `src/breaker.rs` (new)
Hand-rolled consecutive-count circuit breaker. No external crate. Single-owned by one prober, so no `Mutex`/`Arc`. Uses `tokio::time::Instant` (monotonic, `pause()`-able) for cooldown.
Key items: `BreakerState { Closed, Open, HalfOpen }`, `BreakerConfig`, `Outcome { Success, Failure }`, `Breaker`, `Transition { from, to }`, and `Breaker::{new, state, poll_cooldown, record}`.

### `src/config.rs` (new)
Per-check and default tuning.
Key items: `CheckConfig { interval, timeout, initial_delay, breaker }`, `BreakerConfig { failure_threshold, success_threshold, cooldown }`, `Default` impls (interval 10s, timeout 5s, initial_delay 0, failure_threshold 3, success_threshold 1, cooldown 30s), and `Probe` bitflags `{ STARTUP, LIVENESS, READINESS }`.

### `src/snapshot.rs` (new)
The `watch` payload — the cached source of truth read by handlers.
Key items: `HealthSnapshot { startup, liveness, readiness, checks: Arc<BTreeMap<Box<str>, CheckSnapshot>>, generation: u64 }`, `CheckSnapshot { status, breaker, probes, last_ok, last_err, consecutive_failures, checked_at }`, all `Serialize`. The per-check map is behind an `Arc` so every `watch::borrow().clone()` on the hot path is a pointer bump, not a map copy (prober rebuilds copy-on-write on each change).

### `src/events.rs` (new)
The `broadcast` payload plus the ergonomic consumer wrapper.
Key items: `HealthEvent` enum (transitions only), `EventStream` with `async fn next(&mut self) -> Option<HealthEvent>` that swallows `RecvError::Lagged` (returns a synthetic `HealthEvent::Lagged { skipped }`) and maps `Closed` to `None`.

### `src/startup.rs` (new)
Warm-up gate.
Key items: `StartupController(watch::Sender<bool>)` with `mark_ready()` and `is_ready()`, cheaply cloneable, doubling as a change signal source for the aggregator.

### `src/prober.rs` (new — replaces the per-request fan-out)
One supervised async task per check. Owns its `Breaker`. Applies `tokio::time::timeout` per attempt, folds `Elapsed` into `Outcome::Failure`, drives the breaker, and publishes per-check state to its own `watch` cell plus edge transitions to the shared `broadcast`.
Key items: `async fn prober(...)`, `ProbeCfg` (internal), the internal `CheckCell` (`watch::Sender<CheckSnapshot>` per check) and the `MissedTickBehavior::Delay` interval. Also the `aggregator` task that subscribes to every per-check cell + the startup gate + the drain token and recomputes `HealthSnapshot`, emitting `BecameReady`/`BecameNotReady` on readiness edges.

### `src/registry.rs` (new — replaces `Health`/`HealthBuilder`/the `Layer`)
The builder and the host-app handle. Owns the supervisor.
Key items: `HealthBuilder`, `HealthRegistry`, `HealthHandle`, and the `JoinSet`/`TaskTracker` + `CancellationToken` supervisor wiring (`build` spawns probers, `into_router`/`router` mounts endpoints, `drain` triggers fail-ready-first).

### `src/router.rs` (new — replaces `lib.rs::health`)
The axum 0.8 `Router` factory using `State` (not `Extension`/`Layer`).
Key items: `pub fn health_router(handle: HealthHandle) -> Router`, the three handlers `startup_probe`, `live_probe`, `ready_probe`, and an optional `detail` handler returning the full JSON snapshot.

### `src/database/mod.rs` (rewritten)
Drops `Pingable` and `DatabaseHealthIndicator`. Re-exports the per-driver `Check` impls behind their feature gates. No generic adapter — each driver gets a concrete `Check`.
Key items: `#[cfg(feature = "sqlx")] pub use sqlx::SqlxCheck;` etc.

### `src/database/sqlx.rs` / `sea_orm.rs` / `diesel.rs` (rewritten)
Each implements `Check` natively (section 6), reusing the existing `acquire()+ping()` bodies but returning a rich `CheckResult` with latency/error instead of a `bool`, and with the timeout now imposed by the prober. The diesel `async_ping_impl!` macro structure is kept across bb8/deadpool/mobc; the r2d2 branch wraps its blocking `conn.ping()` in `spawn_blocking`.

### Examples and tests
`examples/{sqlx,diesel,sea_orm}.rs` and `tests/{postgres,mysql,sqlite}.rs` are rewritten to the new builder/router API. Not part of the public-API contract but must compile; they currently reference deleted symbols (`axum_health::health`, `Health`, `DatabaseHealthIndicator`, `HealthIndicator`, `HealthDetails`).

---

## 2. Cargo.toml changes

```toml
[dependencies]
async-trait = "0.1.86"                                    # KEEP — object-safe dyn Check
axum       = "0.8.1"                                       # KEEP
serde      = { version = "1.0.217", features = ["derive"] }# KEEP
bitflags   = "2"                                           # NEW — Probe { STARTUP|LIVENESS|READINESS }
tokio      = { version = "1.43", default-features = false, # MOVED from dev-deps -> deps
               features = ["sync", "time", "rt", "macros"] }
tokio-util = { version = "0.7", features = ["rt"] }        # NEW — CancellationToken, TaskTracker

# DROPPED entirely:
#   futures    = "0.3.31"   (Cargo.toml:22) — sole user was service.rs:21 stream fan-out
#   tower-layer = "0.3.3"   (Cargo.toml:21) — sole user was the impl Layer for Health (service.rs:41)

# DB drivers unchanged in version/feature matrix; only the trait they implement changes.
diesel       = { version = "2.2.7", default-features = false, optional = true }
diesel-async = { version = "0.5.2", default-features = false, optional = true }
sea-orm      = { version = "1.1.5", default-features = false, optional = true }
sqlx         = { version = "0.8.3", default-features = false, optional = true }

[features]
default = []
# Existing diesel/sea-orm/sqlx feature graph (Cargo.toml:32-41) is UNCHANGED.
# NEW optional feature for tokio-stream interop, off by default to keep core futures-free:
stream = ["dep:tokio-stream"]

[dependencies.tokio-stream]
version = "0.1"
optional = true            # gated behind `stream`; provides WatchStream/BroadcastStream only if asked
```

`tokio` keeps `test-util` in `[dev-dependencies]` (for `time::pause()` in unit/property tests). `rt-multi-thread`, `signal`, and `net` belong in examples/tests, not the library — the library never spawns a runtime or reads signals; the host app does. `[dev-dependencies]` otherwise unchanged.

Notes on tokio feature minimalism: `sync` gives `watch`+`broadcast`, `time` gives `interval`/`timeout`/`Instant`, `rt` gives `tokio::spawn`/`JoinSet`, `macros` gives `select!`. No `rt-multi-thread` in the lib.

---

## 3. Public API surface

The host app touches exactly this. No `Extension`, no `Layer`, no magic — routing is explicit.

```rust
// ---- status.rs ----
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, serde::Serialize)]
pub enum HealthStatus { Up, Degraded, Down, Unknown } // Ord worst-wins: Up < Degraded < Down < Unknown
impl HealthStatus { pub fn is_serving(self) -> bool; } // Up | Degraded => true

// ---- check.rs ----
#[async_trait::async_trait]
pub trait Check: Send + Sync + 'static {
    fn name(&self) -> &str;
    async fn check(&self, cx: &CheckContext) -> CheckResult; // ONE attempt; prober wraps in timeout
}
pub struct CheckContext { /* opaque; carries the cancel child-token for cooperative checks */ }
impl CheckContext { pub fn is_cancelled(&self) -> bool; }
pub struct CheckResult {
    pub status: HealthStatus,
    pub detail: Option<String>,
    pub data:   std::collections::BTreeMap<String, String>,
}
impl CheckResult {
    pub fn up() -> Self;
    pub fn degraded(detail: impl Into<String>) -> Self;
    pub fn down(detail: impl Into<String>) -> Self;
    pub fn with(self, key: impl Into<String>, val: impl ToString) -> Self;
}
pub struct CheckError(/* boxed source */);
/// Closure adapter so trivial checks need no struct.
pub fn check_fn<F, Fut>(name: impl Into<String>, f: F) -> Box<dyn Check>
where F: Fn(CheckContext) -> Fut + Send + Sync + 'static,
      Fut: std::future::Future<Output = CheckResult> + Send + 'static;

// ---- config.rs ----
bitflags::bitflags! {
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub struct Probe: u8 { const STARTUP = 1; const LIVENESS = 2; const READINESS = 4; }
}
#[derive(Clone, Debug)]
pub struct CheckConfig { pub interval: Duration, pub timeout: Duration,
                         pub initial_delay: Duration, pub breaker: BreakerConfig }
impl Default for CheckConfig; // 10s / 5s / 0s / default breaker
#[derive(Clone, Copy, Debug)]
pub struct BreakerConfig { pub failure_threshold: u32, pub success_threshold: u32, pub cooldown: Duration }
impl Default for BreakerConfig; // 3 / 1 / 30s

// ---- breaker.rs (public for testing / custom probers; the prober uses it internally) ----
#[derive(Clone, Copy, PartialEq, Eq, Debug, serde::Serialize)]
pub enum BreakerState { Closed, Open, HalfOpen }

// ---- registry.rs ----
pub struct HealthBuilder { /* Vec<Registration> + default CheckConfig */ }
impl HealthBuilder {
    pub fn new() -> Self;
    pub fn defaults(self, cfg: CheckConfig) -> Self;
    pub fn register(self, probes: Probe, check: impl Check) -> Self;
    pub fn register_boxed(self, probes: Probe, check: Box<dyn Check>) -> Self;     // for check_fn
    pub fn register_with(self, probes: Probe, cfg: CheckConfig, check: impl Check) -> Self;
    /// Spawns one prober per check + the aggregator into an owned JoinSet, returns handles.
    /// `cancel` is the host's drain token; probers derive child tokens from it.
    pub fn build(self, cancel: tokio_util::sync::CancellationToken)
        -> (HealthRegistry, StartupController);
}

pub struct HealthRegistry { /* Arc inner: watch tx/rx, broadcast tx, JoinSet, cancel */ }
impl HealthRegistry {
    pub fn handle(&self) -> HealthHandle;            // cheap clone for handlers + subscribers
    pub fn router(&self) -> axum::Router;            // == health_router(self.handle())
    pub fn into_router(self) -> axum::Router;        // give up the handle, keep tasks alive via Arc
    /// Idempotent fail-ready-first drain: flips readiness snapshot to Down, emits DrainStarted,
    /// THEN cancels prober tasks. Returns when the JoinSet/TaskTracker is drained.
    pub async fn drain(&self);
}

#[derive(Clone)]
pub struct HealthHandle { snapshot: watch::Receiver<HealthSnapshot>, events: broadcast::Sender<HealthEvent>, /* gate, drain token */ }
impl HealthHandle {
    pub fn snapshot(&self) -> HealthSnapshot;                 // borrow().clone(), Arc inside
    pub fn is_ready(&self) -> bool;                           // readiness == serving && gate && !draining
    pub async fn changed(&mut self) -> Option<HealthSnapshot>;// borrow_and_update; None when producer gone
    pub fn events(&self) -> EventStream;                      // subscribe-then-seed against snapshot
}

// ---- startup.rs ----
#[derive(Clone)]
pub struct StartupController(/* watch::Sender<bool> */);
impl StartupController { pub fn mark_ready(&self); pub fn is_ready(&self) -> bool; }

// ---- snapshot.rs ----
#[derive(Clone, Debug, serde::Serialize)]
pub struct HealthSnapshot {
    pub startup: HealthStatus, pub liveness: HealthStatus, pub readiness: HealthStatus,
    pub checks: std::sync::Arc<std::collections::BTreeMap<Box<str>, CheckSnapshot>>,
    pub generation: u64,
}
#[derive(Clone, Debug, serde::Serialize)]
pub struct CheckSnapshot {
    pub status: HealthStatus, pub breaker: BreakerState, pub probes: Probe,
    pub last_ok: Option<std::time::SystemTime>, pub last_err: Option<std::sync::Arc<str>>,
    pub consecutive_failures: u32,
}

// ---- events.rs ----
#[derive(Clone, Debug)]
pub enum HealthEvent {
    CheckTransition { name: std::sync::Arc<str>, from: HealthStatus, to: HealthStatus },
    BreakerOpened   { name: std::sync::Arc<str>, after_failures: u32 },
    BreakerHalfOpen { name: std::sync::Arc<str> },
    BreakerClosed   { name: std::sync::Arc<str> },
    BecameReady,
    BecameNotReady,
    DrainStarted,
    Lagged { skipped: u64 },                          // synthetic; tells consumer to resync from snapshot()
}
pub struct EventStream { /* broadcast::Receiver */ }
impl EventStream { pub async fn next(&mut self) -> Option<HealthEvent>; }

// ---- router.rs ----
pub fn health_router(handle: HealthHandle) -> axum::Router;
// routes: GET /health/startup, /health/live, /health/ready, /health (detail JSON)
```

Host wiring (replacing `examples/sqlx.rs:18-29`):

```rust
let cancel = CancellationToken::new();
let (registry, startup) = HealthBuilder::new()
    .register(Probe::READINESS, SqlxCheck::new("postgres", pool.clone()))
    .build(cancel.clone());

let app = Router::new()
    .merge(registry.router())
    .route("/things", get(things))
    .with_state(pool);

// reactive pool drain in the host:
let mut ev = registry.handle().events();
tokio::spawn(async move {
    while let Some(e) = ev.next().await {
        match e {
            HealthEvent::BreakerOpened { name, .. }                       => pool.quarantine(&name),
            HealthEvent::CheckTransition { to: HealthStatus::Down, .. }   => pool.close_idle(),
            HealthEvent::Lagged { .. }                                    => { /* re-read snapshot */ },
            _ => {}
        }
    }
});

startup.mark_ready(); // after migrations/warm-up

axum::serve(listener, app.into_make_service())
    .with_graceful_shutdown(async move { registry.drain().await; }) // see section 5 two-phase
    .await?;
```

---

## 4. Hard-removal plan (what gets deleted, exactly)

These items are **deleted outright** — no compatibility adapter, no deprecated shim, no re-export of any old type. A consumer on `0.1.x` will not compile against the new crate; that is intentional.

- `service.rs` is removed as a module path; its contents are split into `status.rs`/`registry.rs`/`snapshot.rs` as new types. Specifically deleted:
  - `HealthIndicator` trait (`service.rs:66-70`) — replaced by `Check` (`check.rs`). Different signature (`fn name(&self) -> &str` not `-> String`; `check(&self, &CheckContext) -> CheckResult` not `details(&self) -> HealthDetail`).
  - `Health` struct + its `impl<S> Layer<S> for Health` (`service.rs:13`, `41-47`) — the Extension/Layer injection trick is gone. Wiring is now `State`-based via `health_router`.
  - `HealthBuilder::with_indicator/build` (`service.rs:52-64`) — replaced by `HealthBuilder::{register,register_with,build}` with `Probe` tags and a `CancellationToken`.
  - `HealthDetails` and its `IntoResponse` (`service.rs:81-96`) — replaced by `HealthSnapshot` + the router handlers' 200/503 collapse.
  - `HealthDetail` (`service.rs:98-124`) — replaced by `CheckResult`/`CheckSnapshot`.
  - `HealthStatus::OutOfService` and `HealthStatus::Custom(String)` variants (`service.rs:76,78`) — deleted; `Custom` was the only thing making the enum non-`Copy`.
- `lib.rs::health` handler (`lib.rs:7-11`) and its `Extension<Health>` extraction — deleted; no free-standing handler remains.
- `database/mod.rs::Pingable` (`mod.rs:15-18`) and `DatabaseHealthIndicator` (`mod.rs:20-53`) — deleted entirely. The DB checks are reimplemented natively against `Check` (section 6).
- `futures` dependency (`Cargo.toml:22`) — dropped; its only call site was `service.rs:21`.
- `tower-layer` dependency (`Cargo.toml:21`) — dropped; its only call site was the `impl Layer for Health` at `service.rs:41`.
- The `service.rs` unit tests (`service.rs:126-234`) and the `tests/*.rs` `HealthIndicator`/`Health::builder().with_indicator()` usages — rewritten against the new API.

### DB checks reimplemented natively against `Check`

There is no `Pingable`-to-`Check` adapter. Each driver gets a fresh `Check` impl whose body reuses the proven `acquire()+ping()` logic but returns `CheckResult` and no longer owns a timeout (the prober imposes it):

- **sqlx** (`database/sqlx.rs`): `SqlxCheck<DB: Database> { name, pool }`. `check()` does `pool.acquire().await` then `conn.ping().await`, timing the round-trip; `Ok` => `CheckResult::up().with("latency_ms", ...)`, acquire/ping error => `CheckResult::down(...)`. Same calls as the old `sqlx.rs:11-16`, error string preserved instead of discarded.
- **sea-orm** (`database/sea_orm.rs`): `SeaOrmCheck { name, conn: DatabaseConnection }`. `check()` calls `conn.ping().await` (same as old `sea_orm.rs:7-9`) into a `CheckResult`.
- **diesel** (`database/diesel.rs`): the `async_ping_impl!` macro is kept across bb8/deadpool/mobc and now emits a `Check` impl producing `CheckResult` rather than a `Pingable` bool. The r2d2 branch's `conn.ping()` is **synchronous** (confirmed `diesel.rs:35`), so it is wrapped in `tokio::task::spawn_blocking` to avoid stalling a runtime worker — a latent bug in the old code that this rewrite fixes.

---

## 5. Endpoint / breaker / drain / channel semantics

### Probe endpoints (cache-only reads; never await a dependency)

All three handlers do a single `handle.snapshot()` (a `watch::borrow().clone()`, Arc inside) and collapse to `200 OK` / `503 SERVICE_UNAVAILABLE`. They never call `Check::check`. A dead Postgres is already reflected in the cached snapshot by its prober, so the handler answers in microseconds.

- `GET /health/startup` -> `200` iff `StartupController::is_ready()` **and** every `STARTUP`-tagged check is serving. Mirrors k8s `startupProbe`: gates liveness/readiness until first warm-up success.
- `GET /health/live` -> `200` iff every `LIVENESS`-tagged check is serving. **Ignores the drain token and never includes external dependencies by default.** During drain it stays `200` so the kubelet does not restart a pod that is intentionally draining. Coupling liveness to a DB breaker would turn a dependency blip into a fleet-wide restart storm.
- `GET /health/ready` -> ordering is load-bearing (fail-ready-first):
  1. if `draining.is_cancelled()` -> `503` immediately (before any snapshot read);
  2. else if `!startup.is_ready()` -> `503` (warm-up gate is a precondition, not just another tagged check);
  3. else aggregate `READINESS`-tagged checks from the cached snapshot -> `200` iff all serving, else `503`.
- `GET /health/ready` aggregate reflects `Closed => Up`, `Open => Down`, `HalfOpen => Down`. A `HalfOpen` breaker is **not** advertised ready — only a `Closed`/serving check counts, so one trial success cannot flap the pod back into rotation before `success_threshold` is met.
- `GET /health` (detail) -> the full `HealthSnapshot` as JSON with `200`/`503` driven by `readiness`, for humans/dashboards.

### Circuit breaker (per check, single-owned by its prober)

State machine, consecutive-count thresholds, `tokio::time::Instant` cooldown, no lock:

- `Closed`: count consecutive failures. At `failure_threshold` -> `Open`, record `opened_at`, emit `BreakerOpened`. Any success resets the failure counter.
- `Open`: the prober **skips the probe call entirely** while `now - opened_at < cooldown` (do not hammer a dead dependency). The cached status stays `Down`. At the first tick where the cooldown has elapsed, `poll_cooldown` rolls `Open -> HalfOpen` (emit `BreakerHalfOpen`) and that same tick performs exactly one trial probe.
- `HalfOpen`: a single sequential trial probe. On success, increment consecutive successes; at `success_threshold` -> `Closed` (emit `BreakerClosed`), status `Up`. Any failure -> back to `Open` with a fresh `opened_at` (emit `BreakerOpened` again), status `Down`. Only one trial probe is in flight because the prober loop is strictly sequential per check.

`Breaker::record(outcome, now)` and `Breaker::poll_cooldown(now)` each return `Option<Transition>`; the prober translates a `Transition` into the corresponding `HealthEvent` and the per-check snapshot update. The timeout `Elapsed` arm is folded into `Outcome::Failure` before `record` is called — a slow/dead Postgres counts as a failure, never hangs the prober.

### Prober task semantics

One task per check. `tokio::time::interval(cfg.interval)` with `MissedTickBehavior::Delay` (not the default `Burst`, which would fire catch-up ticks back-to-back at a recovering dependency). The loop is a `biased` `select!` with the cancel branch **first**:

```
loop select! { biased;
  _ = cancel.cancelled() => break,                 // clean stop; no task leak
  _ = tick.tick() => {
     poll_cooldown -> maybe emit BreakerHalfOpen
     if breaker Open and still cooling: publish Down snapshot, continue
     outcome = match timeout(cfg.timeout, check.check(&cx)).await {
        Ok(r) if r.status.is_serving() => Success(r),
        Ok(r)                          => Failure(r),   // check itself reported Down/Degraded
        Err(Elapsed)                   => Failure(timeout),
     }
     transition = breaker.record(outcome)
     publish per-check CheckSnapshot via watch::Sender::send_replace   // updates even with 0 receivers
     if transition: broadcast.send(HealthEvent::from(transition))      // edge-only
  }
}
```

`send_replace` (not `send`) is used on the per-check `watch` so a prober keeps updating its cell even when nothing is currently reading; `send` errors with zero receivers. Transition events are emitted **only on an actual edge**, never every tick, so the broadcast ring does not churn into `Lagged` under steady state.

### Aggregator task

Subscribes to every per-check `watch` cell + the `StartupController` gate + the drain token. On any change it recomputes the top-level `HealthSnapshot` (per-probe worst-wins over the tagged subset, rebuilding the `Arc<BTreeMap>` copy-on-write, bumping `generation`) and publishes via `watch::Sender::send_if_modified`, comparing only the semantically meaningful fields (status/breaker/tags), **not** `last_ok`/`checked_at` timestamps — otherwise every successful probe would wake every request-path reader. It emits `BecameReady`/`BecameNotReady` on readiness edges.

### Channels (watch = truth, broadcast = advisory deltas)

- **`watch::Sender<HealthSnapshot>`** is the authoritative current state. Request path and host app read it synchronously via `borrow().clone()` (cheap — `Arc` inside). Latest-wins; a new receiver sees the current value immediately. The registry holds the `Sender` (never a stray receiver) so the channel stays open with zero subscribers.
- **`broadcast::Sender<HealthEvent>`**, capacity 256, carries discrete edges only (status transitions, breaker open/half-open/close, became-ready/not-ready, drain-started). Lossy by design: a slow consumer gets `RecvError::Lagged(n)`; `EventStream::next` swallows it and returns a synthetic `HealthEvent::Lagged { skipped }`, the documented contract being "resync from `snapshot()`". `RecvError::Closed` terminates the stream (`None`). Readiness is **never** gated on broadcast delivery — it reads `watch`, which cannot lag.
- **Initial-value pairing**: `broadcast` does not replay current state to a new subscriber. `HealthHandle::events()` subscribes via `Sender::subscribe()` first, then the host treats `handle.snapshot()` as the synthetic initial event — race-free because any transition between subscribe and read still arrives on the broadcast (worst case one redundant resync).

### Graceful drain (fail-ready-first, two-phase, no task leak)

`HealthRegistry::drain()` is idempotent and ordered:

1. Publish the readiness-Down edge **first**: flip the snapshot's `readiness` to `Down`, emit `DrainStarted` + `BecameNotReady`. `live_probe` is untouched and stays `200`. From this instant `ready_probe` returns `503`, so k8s removes the pod from Service `EndpointSlices`.
2. The host's serve loop pairs `drain()` with `axum::serve(...).with_graceful_shutdown(...)`. The **load-bearing two-phase detail**: readiness-503 and the actual axum stop-accepting must be separated by a grace window `>= readinessProbe.periodSeconds * failureThreshold`, so k8s observes the 503 and deregisters the endpoint *before* axum stops accepting; collapsing them into one future drops exactly the requests drain exists to save. `drain()` therefore sleeps a configurable `drain_grace` between phase 1 and phase 3.
3. `cancel.cancel()` fires every prober's biased `cancelled()` branch; each loop breaks. `JoinSet`/`TaskTracker::wait()` returns only once every prober and the aggregator have actually exited — that is the no-task-leak guarantee. A hard outer deadline (race the join against `tokio::time::timeout`) bounds the wait below `terminationGracePeriodSeconds`, defending against the known axum 0.8 idle-keep-alive graceful-shutdown hang (tokio-rs/axum#3326).

The probers' cancel tokens are **child tokens** of the host's single drain `CancellationToken`, so `with_graceful_shutdown` resolving and the prober tasks stopping are driven off the same cancel — one source of truth for shutdown.
