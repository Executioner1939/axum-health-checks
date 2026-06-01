//! The axum 0.8 `Router` factory. Uses `State` (not `Extension`/`Layer`).
//!
//! All four handlers are cache-only: each does a single `handle.snapshot()`
//! (a `watch::borrow().clone()`, `Arc` inside) and collapses to `200`/`503`.
//! None of them ever call `Check::check` — a dead dependency is already cached
//! `Down` by its prober, so a probe answers in microseconds.

use crate::config::Probe;
use crate::registry::HealthHandle;
use crate::snapshot::HealthSnapshot;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json, Response};
use axum::routing::get;

/// Build the health router over a [`HealthHandle`].
///
/// Routes:
/// - `GET /health/startup` — k8s `startupProbe`
/// - `GET /health/live` — k8s `livenessProbe`
/// - `GET /health/ready` — k8s `readinessProbe`
/// - `GET /health` — full JSON snapshot for humans/dashboards
pub fn health_router(handle: HealthHandle) -> Router {
    Router::new()
        .route("/health/startup", get(startup_probe))
        .route("/health/live", get(live_probe))
        .route("/health/ready", get(ready_probe))
        .route("/health", get(detail))
        .with_state(handle)
}

/// `200` iff warm-up is complete AND every `STARTUP`-tagged check is serving.
async fn startup_probe(State(handle): State<HealthHandle>) -> StatusCode {
    let snap = handle.snapshot();
    if handle.is_started() && all_serving(&snap, Probe::STARTUP) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// `200` iff every `LIVENESS`-tagged check is serving. Ignores the drain token
/// so a draining pod is not restarted by the kubelet.
///
/// Cold-start window: a `LIVENESS` check seeds as `Unknown` (not serving), so
/// this returns `503` from process start until the check's first probe lands
/// (up to `initial_delay + interval`). With a `livenessProbe` configured and no
/// `startupProbe`, the kubelet could restart the pod inside that window. Shield
/// it with a Kubernetes `startupProbe` (pointed at `/health/startup`) so the
/// kubelet does not evaluate liveness until startup succeeds, or raise
/// `livenessProbe.initialDelaySeconds` / `failureThreshold` past the cold-start
/// budget. See the crate-level docs for the recommended probe wiring.
async fn live_probe(State(handle): State<HealthHandle>) -> StatusCode {
    let snap = handle.snapshot();
    if all_serving(&snap, Probe::LIVENESS) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Strict fail-ready-first ordering:
/// 1. draining => `503` immediately (before any snapshot read);
/// 2. not warmed up => `503`;
/// 3. else `200` iff the readiness *aggregate* in the snapshot is serving.
///
/// Step 3 reads `snap.readiness` — the value `drain()` phase 1 forces `Down` —
/// rather than recomputing from the per-check statuses. Recomputing would ignore
/// the forced flip for the whole grace window (the underlying probers keep their
/// cells `Up` until phase 3 cancels them), so Kubernetes would keep routing
/// traffic and the grace window would give zero deregistration lead time. Mirror
/// `detail` and [`HealthHandle::is_ready`], which already read the aggregate.
async fn ready_probe(State(handle): State<HealthHandle>) -> StatusCode {
    if handle.is_draining() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if !handle.is_started() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    let snap = handle.snapshot();
    if snap.readiness.is_serving() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

/// Full snapshot as JSON; status code driven by readiness.
async fn detail(State(handle): State<HealthHandle>) -> Response {
    let snap = handle.snapshot();
    let code = if handle.is_draining() || !handle.is_started() || !snap.readiness.is_serving() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    (code, Json(snap)).into_response()
}

/// Whether every check carrying `probe` is serving. An empty tagged subset is
/// vacuously serving (matches the aggregator's empty-set-is-`Up` rule).
fn all_serving(snap: &HealthSnapshot, probe: Probe) -> bool {
    snap.checks
        .values()
        .filter(|c| c.probes.contains(probe))
        .all(|c| c.status.is_serving())
}
