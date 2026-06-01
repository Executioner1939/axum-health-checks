//! Kubernetes-style health probes for axum, on tokio.
//!
//! This crate is a **hard break** from the `0.1.x` Spring-Boot-style per-request
//! health library. Instead of running every indicator on every `/health` hit,
//! each [`Check`] is probed on its own schedule by a background task, driven
//! through a per-check circuit [`breaker`], and the latest result is cached in a
//! `watch` snapshot that the probe endpoints read in microseconds.
//!
//! # The three-probe model
//!
//! - `GET /health/startup` — gated on a warm-up controller plus `STARTUP` checks.
//! - `GET /health/live` — `LIVENESS` checks only; stays `200` during drain so a
//!   draining pod is not restarted.
//! - `GET /health/ready` — fail-ready-first: draining or un-warmed => `503`,
//!   else the `READINESS` aggregate.
//! - `GET /health` — full JSON snapshot for humans/dashboards.
//!
//! # Wiring
//!
//! ```ignore
//! let cancel = tokio_util::sync::CancellationToken::new();
//! let (registry, startup) = HealthBuilder::new()
//!     .register(Probe::READINESS, SqlxCheck::new("postgres", pool.clone()))
//!     .build(cancel.clone());
//!
//! let app = axum::Router::new()
//!     .merge(registry.router())
//!     .route("/things", axum::routing::get(things));
//!
//! startup.mark_ready(); // after migrations / warm-up
//!
//! axum::serve(listener, app.into_make_service())
//!     .with_graceful_shutdown(async move { registry.drain().await; })
//!     .await?;
//! ```

pub mod breaker;
pub mod check;
pub mod config;
pub mod database;
pub mod events;
pub mod prober;
pub mod registry;
pub mod router;
pub mod snapshot;
pub mod startup;
pub mod status;

pub use crate::breaker::BreakerState;
pub use crate::check::{Check, CheckContext, CheckError, CheckResult, check_fn};
pub use crate::config::{BreakerConfig, CheckConfig, Probe};
pub use crate::events::{EventStream, HealthEvent};
pub use crate::registry::{HealthBuilder, HealthHandle, HealthRegistry};
pub use crate::router::health_router;
pub use crate::snapshot::{CheckSnapshot, HealthSnapshot};
pub use crate::startup::StartupController;
pub use crate::status::HealthStatus;
