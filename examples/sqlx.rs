use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum_health::database::SqlxCheck;
use axum_health::{HealthBuilder, Probe};
use sqlx::SqlitePool;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let pool = SqlitePool::connect("test.db").await.unwrap();

    // The host owns a single drain token; prober tokens are children of it.
    let cancel = CancellationToken::new();

    // Register the pool as a readiness dependency. Background probers poll it on
    // their own schedule; the endpoints only ever read the cached snapshot.
    let (registry, startup) = HealthBuilder::new()
        .register(Probe::READINESS, SqlxCheck::new("sqlx", pool.clone()))
        .build(cancel.clone());

    let router = Router::new()
        .route("/things", get(things))
        .with_state(pool)
        // Mounts /health/startup, /health/live, /health/ready and /health.
        .merge(registry.router());

    // After migrations / warm-up are complete, open the startup gate.
    startup.mark_ready();

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();

    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(async move { registry.drain().await })
        .await
        .unwrap()
}

async fn things(State(_pool): State<SqlitePool>) -> impl IntoResponse {
    // Do whatever
    StatusCode::OK
}
