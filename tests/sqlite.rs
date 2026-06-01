//! SQLite integration tests against the redesigned background-prober health API.
//!
//! Unlike the old per-request model, checks are probed on a background schedule
//! and the endpoints read a cached snapshot. Each test registers a check on a
//! short interval, opens the startup gate, then polls `/health/ready` until the
//! first probe has populated the snapshot before asserting.

#![cfg_attr(
    not(any(feature = "diesel-r2d2", feature = "sqlx", feature = "sea-orm")),
    allow(unused_imports)
)]

use axum::http::StatusCode;
use axum::Router;
use axum_health::{CheckConfig, HealthBuilder, Probe};
use axum_test::TestServer;
use std::fs::OpenOptions;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// A fast probing config so tests do not wait the 10s default interval.
#[cfg(any(feature = "diesel-r2d2", feature = "sqlx", feature = "sea-orm"))]
fn fast() -> CheckConfig {
    CheckConfig {
        interval: Duration::from_millis(20),
        timeout: Duration::from_secs(5),
        initial_delay: Duration::ZERO,
        ..CheckConfig::default()
    }
}

/// Create an empty file at `dir/test.db` and return its path string.
#[cfg(any(feature = "diesel-r2d2", feature = "sqlx", feature = "sea-orm"))]
fn db_file(dir: &std::path::Path) -> String {
    let path = dir.join("test.db");
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    path.to_str().unwrap().to_owned()
}

#[cfg(feature = "diesel-r2d2")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_diesel() {
    use axum_health::database::DieselR2d2Check;
    use diesel::r2d2::{ConnectionManager, Pool};

    let dir = tempfile::tempdir().unwrap();
    let url = db_file(dir.path());

    let manager = ConnectionManager::<diesel::SqliteConnection>::new(url);
    let pool = Pool::builder().build(manager).unwrap();
    let check = DieselR2d2Check::new("diesel-sqlite", pool);

    run_test(check).await;
}

#[cfg(feature = "sqlx")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sqlx() {
    use axum_health::database::SqlxCheck;

    let dir = tempfile::tempdir().unwrap();
    let url = db_file(dir.path());

    let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
    let check = SqlxCheck::new("sqlx-sqlite", pool);
    run_test(check).await;
}

#[cfg(feature = "sea-orm")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sea_orm() {
    use axum_health::database::SeaOrmCheck;
    use sea_orm::DatabaseConnection;

    let dir = tempfile::tempdir().unwrap();
    let url = db_file(dir.path());

    let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
    let database = DatabaseConnection::from(pool);
    let check = SeaOrmCheck::new("sea-orm-sqlite", database);

    run_test(check).await;
}

/// Wire a single readiness check through the registry, open the startup gate,
/// and assert that every probe endpoint converges to healthy once the first
/// background probe has run.
#[cfg(any(feature = "diesel-r2d2", feature = "sqlx", feature = "sea-orm"))]
async fn run_test(check: impl axum_health::Check) {
    let name = check.name().to_owned();
    let cancel = CancellationToken::new();

    let (registry, startup) = HealthBuilder::new()
        .register_with(
            Probe::STARTUP | Probe::READINESS | Probe::LIVENESS,
            fast(),
            check,
        )
        .build(cancel.clone());

    let router = Router::new().merge(registry.router());
    let server = TestServer::new(router).unwrap();

    // Before the startup gate is opened, readiness must be 503.
    let response = server.get("/health/ready").await;
    assert_eq!(response.status_code(), StatusCode::SERVICE_UNAVAILABLE);

    startup.mark_ready();

    // Poll until the first background probe populates the snapshot and the pod
    // advertises ready.
    let ready = await_status(&server, "/health/ready", StatusCode::OK).await;
    assert!(ready, "readiness never became OK");

    let live = server.get("/health/live").await;
    assert_eq!(live.status_code(), StatusCode::OK);

    let startup_probe = server.get("/health/startup").await;
    assert_eq!(startup_probe.status_code(), StatusCode::OK);

    // The detail endpoint reports the check Up and 200.
    let detail = server.get("/health").await;
    let json: serde_json::Value = detail.json();
    assert_eq!(detail.status_code(), StatusCode::OK, "detail body: {json}");
    assert_eq!(json["readiness"], "Up");
    assert_eq!(json["checks"][&name]["status"], "Up");

    // Drain flips readiness Down while liveness stays Up.
    registry.drain().await;
    let response = server.get("/health/ready").await;
    assert_eq!(response.status_code(), StatusCode::SERVICE_UNAVAILABLE);
    let live = server.get("/health/live").await;
    assert_eq!(live.status_code(), StatusCode::OK);
}

/// Poll `path` up to ~2s until it returns `want`, returning whether it did.
#[cfg(any(feature = "diesel-r2d2", feature = "sqlx", feature = "sea-orm"))]
async fn await_status(server: &TestServer, path: &str, want: StatusCode) -> bool {
    for _ in 0..200 {
        if server.get(path).await.status_code() == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    false
}
