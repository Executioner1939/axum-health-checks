//! End-to-end example of the redesigned probe-oriented API.
//!
//! It demonstrates the four pieces a production service wires together:
//!
//! 1. A **Postgres/sqlx readiness check** with an explicit per-check
//!    [`CheckConfig`], including a tuned circuit [`BreakerConfig`].
//! 2. A **warm-up gate** ([`StartupController`]) the host opens once migrations
//!    and cache warm-up are done, before which `/health/ready` and
//!    `/health/startup` return `503`.
//! 3. An **event subscriber** task that reacts to advisory edges — logging a
//!    connection-pool *recycle* intent when the breaker opens and a *drain*
//!    intent when the host begins draining.
//! 4. A **fail-ready-first graceful drain** wired into
//!    `axum::serve(..).with_graceful_shutdown(..)`.
//!
//! The pool is created with `connect_lazy`, so the example compiles and starts
//! without a live database — the background prober simply reports the dependency
//! `Down` until Postgres is reachable, which is exactly the behaviour you want
//! for a dependency that may come up after the pod does.
//!
//! Run it with:
//!
//! ```text
//! cargo run --example probes --features sqlx,sqlx/postgres,sqlx/runtime-tokio
//! ```

use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use axum_health::database::SqlxCheck;
use axum_health::{BreakerConfig, CheckConfig, HealthBuilder, HealthEvent, HealthHandle, Probe};
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    // A lazy pool: no connection is opened until first use, so this never blocks
    // startup. The prober will acquire+ping on its own schedule.
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_owned());
    let pool = PgPool::connect_lazy(&url).expect("invalid DATABASE_URL");

    // The host owns one drain token. Every prober's token is a child of it, so a
    // single `cancel.cancel()` (driven by `drain()`) stops them all.
    let cancel = CancellationToken::new();

    // Tune the Postgres check: probe every 5s, fail an attempt that takes longer
    // than 2s, and trip the breaker after 3 consecutive failures. Once open it
    // cools for 15s before a single trial probe, and one success closes it.
    let pg_config = CheckConfig {
        interval: Duration::from_secs(5),
        timeout: Duration::from_secs(2),
        initial_delay: Duration::ZERO,
        breaker: BreakerConfig {
            failure_threshold: 3,
            success_threshold: 1,
            cooldown: Duration::from_secs(15),
        },
    };

    let (registry, startup) = HealthBuilder::new()
        // Phase the readiness flip ahead of accept-stop so Kubernetes pulls the
        // pod from its EndpointSlice before axum stops taking connections.
        .drain_grace(Duration::from_secs(5))
        .register_with(
            Probe::READINESS,
            pg_config,
            SqlxCheck::new("postgres", pool.clone()),
        )
        .build(cancel.clone());

    // Subscribe to advisory edges and react. This is the "react to events" piece:
    // a breaker opening is a strong signal that the pool is full of dead
    // connections, so we log a recycle intent; drain start logs a drain intent.
    // A real service would call into its pool's recycle/close-idle API here.
    let event_handle = registry.handle();
    let events_task = tokio::spawn(react_to_events(event_handle));

    let app = Router::new()
        .route("/things", get(things))
        .with_state(pool)
        // Mounts /health/startup, /health/live, /health/ready and /health.
        .merge(registry.router());

    // Pretend migrations / cache warm-up happened here, then open the gate. Until
    // this fires, /health/startup and /health/ready are 503 even if Postgres is
    // already reachable.
    startup.mark_ready();
    println!("warm-up complete; startup gate open");

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("listening on http://0.0.0.0:3000 (probes under /health)");

    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(async move {
            // On Ctrl-C, run the two-phase drain: readiness 503 first, grace
            // window, then stop the probers and join them.
            let _ = tokio::signal::ctrl_c().await;
            println!("shutdown signal received; draining");
            registry.drain().await;
            println!("drain complete; probers stopped");
        })
        .await
        .unwrap();

    // The event task ends once every sender is dropped (the registry is gone).
    let _ = events_task.await;
}

/// Consume the advisory event stream and turn edges into operational intents.
///
/// The stream is lossy by design: if this task falls behind it receives a
/// synthetic [`HealthEvent::Lagged`] and resyncs from the snapshot rather than
/// blocking the probers.
async fn react_to_events(handle: HealthHandle) {
    let mut events = handle.events();
    while let Some(event) = events.next().await {
        match event {
            HealthEvent::BreakerOpened {
                name,
                after_failures,
            } => {
                // The dependency is unreachable; the pool likely holds dead
                // connections. Log a recycle intent. A real impl would call e.g.
                // `pool.close_idle()` / a driver-specific recycle here.
                println!(
                    "[recycle] breaker for '{name}' opened after {after_failures} failures; \
                     recycling idle connections"
                );
            }
            HealthEvent::BreakerHalfOpen { name } => {
                println!("[recycle] breaker for '{name}' half-open; trial probe in flight");
            }
            HealthEvent::BreakerClosed { name } => {
                println!("[recycle] breaker for '{name}' closed; dependency recovered");
            }
            HealthEvent::DrainStarted => {
                // Readiness is now forced Down. Begin shedding background work,
                // flush buffers, stop accepting new units of work, etc.
                println!("[drain] drain started; readiness is now 503, shedding work");
            }
            HealthEvent::BecameNotReady => {
                println!("[drain] readiness fell; pod will be pulled from rotation");
            }
            HealthEvent::BecameReady => {
                println!("[ready] pod is now serving readiness");
            }
            HealthEvent::CheckTransition { name, from, to } => {
                println!("[check] '{name}' transitioned {from:?} -> {to:?}");
            }
            HealthEvent::Lagged { skipped } => {
                // Resync contract: the watch snapshot is always authoritative.
                let snap = handle.snapshot();
                println!(
                    "[events] lagged, skipped {skipped} edges; resynced from snapshot \
                     (readiness {:?})",
                    snap.readiness
                );
            }
        }
    }
}

async fn things(State(_pool): State<PgPool>) -> impl IntoResponse {
    // Do whatever the service does.
    StatusCode::OK
}
