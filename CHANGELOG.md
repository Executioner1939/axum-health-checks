# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-06-01

A full redesign from the Spring-Boot-style per-request health library into a
Kubernetes-oriented probe library on tokio. This is a breaking release with no
compatibility shim; a `0.1.x` consumer will not compile. See the "Migrating from
0.1.x" section of the README for the mechanical renames.

### Added

- Tag-based probe model: a `Check` is registered with one or more `Probe` flags
  (`STARTUP`, `LIVENESS`, `READINESS`); each probe's status is the worst-wins
  aggregate over the checks carrying its flag.
- Four endpoints via `health_router`: `GET /health/startup`, `/health/live`,
  `/health/ready`, and a detail `GET /health` returning the full JSON snapshot.
- Background prober: one task per check runs it on a fixed interval under a
  per-attempt timeout, so the probe endpoints read a cached snapshot in
  microseconds and never touch a dependency on the request path.
- Per-check circuit breaker (consecutive-count) with a cooldown and a half-open
  trial probe; an open breaker serves a cached `Down` instead of hammering a
  dead dependency.
- Warm-up gate: `StartupController::mark_ready` opens the startup gate after
  migrations and cache hydration; readiness is gated until ready.
- Notification channels: an authoritative `watch<HealthSnapshot>` plus an
  advisory `broadcast<HealthEvent>` stream of state edges (breaker transitions,
  readiness flips, drain start) for host-side reactions such as recycling a pool.
- Fail-ready-first two-phase `HealthRegistry::drain`, wired into
  `with_graceful_shutdown`: readiness flips to `Down` while liveness stays `200`,
  a `drain_grace` window lets Kubernetes deregister the pod, then probers are
  cancelled and joined under a `drain_timeout`, aborting any straggler so no task
  leaks past drain. The forced readiness `Down` is durable for the whole window.
- `CheckResult::degraded` for a serving-but-impaired state that counts as a
  breaker success and keeps the probe at `200` while surfacing in the detail view.
- `check_fn` closure adapter for trivial custom checks.
- Native `Check` implementations for sqlx, sea-orm, and diesel (r2d2 and the
  diesel-async bb8/deadpool/mobc pools), behind per-driver feature gates.
- `probes` example demonstrating the warm-up gate, a Postgres check with a tuned
  breaker, and an event subscriber end to end.

### Changed

- Migrated to Rust edition 2024; declared `rust-version = "1.85"`.
- Upgraded all dependencies to their latest releases: axum 0.8.9, serde 1.0.228,
  tokio 1.52, async-trait 0.1.89, diesel 2.3, diesel-async 0.9, sea-orm 1.1.20,
  sqlx 0.9 (and the dev-dependencies axum-test 20, testcontainers 0.27).
- The diesel r2d2 ping now runs under `spawn_blocking`, so its synchronous
  round-trip never stalls a runtime worker.
- Updated crate metadata: keywords, categories, description, and repository.

### Removed

- The `HealthIndicator` trait, the `Health` `Extension`/`Layer` type, and the
  single `axum_health::health` handler.
- The `Pingable` trait and `DatabaseHealthIndicator`, replaced by the per-driver
  `Check` implementations.
- The `HealthStatus::OutOfService` and `HealthStatus::Custom(String)` variants;
  `Degraded` covers the serving-but-unhealthy case.
- The `futures` and `tower-layer` dependencies.

## [0.1.2] and earlier

Spring-Boot-style health indicators served on a single `/health` endpoint, with
every indicator evaluated on each request. No changelog was kept for these
releases.

[Unreleased]: https://github.com/Executioner1939/axum-health-checks/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/Executioner1939/axum-health-checks/releases/tag/v0.2.0
