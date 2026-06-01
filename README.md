# axum-health

<div>
<a href="https://github.com/alanbaumgartner/axum-health/actions/workflows/rust.yml"><img src="https://github.com/alanbaumgartner/axum-health/actions/workflows/rust.yml/badge.svg" /></a>
<a href="https://crates.io/crates/axum-health"><img src="https://img.shields.io/crates/v/axum-health.svg" /></a>
<a href="https://docs.rs/axum-health"><img src="https://docs.rs/axum-health/badge.svg" /></a>
</div>

Kubernetes-style health probes for [axum](https://github.com/tokio-rs/axum), on
[tokio](https://tokio.rs). Each dependency is probed by a background task on its
own schedule, driven through a per-check circuit breaker, and the latest result
is cached in a snapshot that the probe endpoints read in microseconds. The
endpoints never touch a dependency, so a dead database cannot make
`/health/ready` hang.

This is a hard break from the `0.1.x` Spring-Boot-style library, which ran every
indicator on every `/health` request. See the
[migration](#migrating-from-01x) section below.

## Model

The crate exposes four endpoints that map onto the three Kubernetes probe types
plus a human-facing detail view:

| Endpoint | Probe | `200` when |
| --- | --- | --- |
| `GET /health/startup` | `startupProbe` | warm-up is complete **and** every `STARTUP` check is serving |
| `GET /health/live` | `livenessProbe` | every `LIVENESS` check is serving (stays `200` while draining) |
| `GET /health/ready` | `readinessProbe` | not draining, warm-up complete, every `READINESS` check serving |
| `GET /health` | — | full JSON snapshot; status code mirrors readiness |

A [`Check`] is tagged at registration with one or more [`Probe`] flags. Each
probe's status is the worst-wins aggregate over the checks carrying its flag, so
a single failing dependency takes down exactly the probes it is tagged into and
no others. Tagging an external dependency `LIVENESS` is almost always a mistake:
it couples a transient outage to a kubelet restart and can cause a fleet-wide
restart storm. Tag external dependencies `READINESS`.

Each check is owned by one background prober task. The prober runs the check on
a fixed interval under a per-attempt timeout, classifies the outcome, and feeds
it to a consecutive-count circuit breaker. While the breaker is open the prober
skips the probe entirely and serves a cached `Down`, so a dead dependency is not
hammered. After a cooldown the breaker rolls to half-open for a single trial
probe before closing.

## Usage

```rust
use std::time::Duration;

use axum::Router;
use axum_health::database::SqlxCheck;
use axum_health::{CheckConfig, HealthBuilder, Probe};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let pool = sqlx::PgPool::connect_lazy("postgres://localhost/app").unwrap();

    // The host owns one drain token; every prober token is a child of it.
    let cancel = CancellationToken::new();

    let (registry, startup) = HealthBuilder::new()
        .register_with(
            Probe::READINESS,
            CheckConfig {
                interval: Duration::from_secs(5),
                timeout: Duration::from_secs(2),
                ..CheckConfig::default()
            },
            SqlxCheck::new("postgres", pool.clone()),
        )
        .build(cancel.clone());

    let app = Router::new()
        .merge(registry.router()) // /health/startup,/live,/ready,/health
        .with_state(pool);

    // After migrations and cache warm-up, open the startup gate.
    startup.mark_ready();

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app.into_make_service())
        .with_graceful_shutdown(async move { registry.drain().await })
        .await
        .unwrap();
}
```

`GET /health` responds with the full snapshot:

```json
{
  "startup": "Up",
  "liveness": "Up",
  "readiness": "Up",
  "checks": {
    "postgres": {
      "status": "Up",
      "breaker": "Closed",
      "probes": ["READINESS"],
      "last_ok": { "secs_since_epoch": 1717200000, "nanos_since_epoch": 0 },
      "last_err": null,
      "consecutive_failures": 0,
      "checked_at": { "secs_since_epoch": 1717200000, "nanos_since_epoch": 0 }
    }
  },
  "generation": 12
}
```

## Custom checks

Any `async` round-trip can be a check. For trivial cases use the
[`check_fn`](crate::check::check_fn) closure adapter; for anything stateful
implement the trait. A check performs exactly one attempt — the prober imposes
the timeout and the retry cadence, so a check must not loop, sleep, or time
itself out.

```rust
use axum_health::{check_fn, CheckResult, Probe};

let cache = check_fn("redis", |_cx| async {
    match ping_redis().await {
        Ok(latency) => CheckResult::up().with("latency_ms", latency.as_millis()),
        Err(e) => CheckResult::down(format!("redis unreachable: {e}")),
    }
});

builder.register(Probe::READINESS, cache);
```

`CheckResult::degraded(..)` reports a serving-but-impaired state: it counts as a
breaker success and keeps the probe `200`, but surfaces in the detail view.

## Reacting to events

Alongside the authoritative snapshot, the registry publishes an advisory
[`HealthEvent`] stream of state edges: breaker transitions, readiness flips, and
drain start. The stream is lossy by design; a slow consumer is told it lagged
and resyncs from the snapshot rather than blocking the probers. Use it to drive
side effects — recycling a connection pool when a breaker opens, shedding
background work when drain begins:

```rust
let mut events = registry.handle().events();
tokio::spawn(async move {
    while let Some(event) = events.next().await {
        match event {
            HealthEvent::BreakerOpened { name, after_failures } => {
                tracing::warn!(%name, after_failures, "breaker open; recycling pool");
            }
            HealthEvent::DrainStarted => {
                tracing::info!("drain started; shedding background work");
            }
            _ => {}
        }
    }
});
```

The [`probes`](examples/probes.rs) example wires the warm-up gate, a Postgres
check with a tuned breaker, and an event subscriber together end to end.

## Graceful drain

[`HealthRegistry::drain`] is a fail-ready-first two-phase shutdown, wired into
`with_graceful_shutdown`:

1. Flip readiness to `Down` and emit `DrainStarted` / `BecameNotReady`. Liveness
   is untouched and stays `200`, so the kubelet does not restart the draining
   pod. `/health/ready` now returns `503` and Kubernetes removes the pod from its
   EndpointSlices.
2. Sleep `drain_grace` so Kubernetes observes the `503` before the server stops
   accepting. Set it `>= readinessProbe.periodSeconds * failureThreshold`.
3. Cancel the drain token, stopping every prober and the aggregator, and join
   them under an outer deadline (`drain_timeout`). Any task still running when
   that deadline fires is aborted, so a check that ignores both its per-attempt
   timeout and the cancel token cannot leak past drain.

Drain is idempotent and leaves no orphaned tasks: once `drain()` returns, no
supervised prober or aggregator is still running. The forced readiness `Down` is
durable for the whole drain (a dedicated latch keeps the aggregator from racing
it back to serving), so `/health/ready` reports `503` for the entire grace
window, not just after the token is cancelled.

## Configuration

[`CheckConfig`] tunes a single check (`interval`, `timeout`, `initial_delay`,
and the nested [`BreakerConfig`]). [`HealthBuilder::defaults`] sets the config
applied by `register` / `register_boxed`; `register_with` overrides it per
check. Defaults are a 10s interval, 5s timeout, and a breaker that trips after 3
consecutive failures and cools for 30s.

## Database checks

Native [`Check`] implementations ship behind feature gates:

| Driver | Type | Feature |
| --- | --- | --- |
| sqlx | `database::SqlxCheck` | `sqlx` |
| sea-orm | `database::SeaOrmCheck` | `sea-orm` |
| diesel (r2d2) | `database::DieselR2d2Check` | `diesel-r2d2` |
| diesel-async (bb8) | `database::bb8::DieselCheck` | `diesel-bb8` |
| diesel-async (deadpool) | `database::deadpool::DieselCheck` | `diesel-deadpool` |
| diesel-async (mobc) | `database::mobc::DieselCheck` | `diesel-mobc` |

Each acquires a pooled connection and pings it, reporting round-trip latency on
success and the preserved error string on failure. The r2d2 ping is synchronous
and runs under `spawn_blocking` so it never stalls a runtime worker.

## Migrating from 0.1.x

There is no compatibility shim; a `0.1.x` consumer will not compile. The renames
are mechanical:

- `HealthIndicator` becomes [`Check`]. `name(&self) -> String` becomes
  `name(&self) -> &str`; `details(&self) -> HealthDetail` becomes
  `async check(&self, &CheckContext) -> CheckResult`.
- `Health::builder().with_indicator(..).build()` (a tower `Layer`) becomes
  `HealthBuilder::new().register(probe, ..).build(cancel)`, returning a
  registry and a startup controller.
- The single `axum_health::health` handler over `Extension<Health>` becomes the
  four-route [`health_router`], wired with `.merge(registry.router())` —
  `State`-based, no `Extension`/`Layer`.
- `DatabaseHealthIndicator` / `Pingable` become the per-driver checks above.
- `HealthStatus::OutOfService` and `HealthStatus::Custom(String)` are removed;
  `Degraded` covers the serving-but-unhealthy case.

## Examples

The [examples](examples) directory covers each database driver
([sqlx](examples/sqlx.rs), [sea-orm](examples/sea_orm.rs),
[diesel](examples/diesel.rs)) plus the full
[probe-oriented walkthrough](examples/probes.rs).

## License

Licensed under either of MIT or Apache-2.0 at your option.
