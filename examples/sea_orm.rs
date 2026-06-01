//! sea-orm readiness-probe example.
//!
//! sea-orm bundles its own sqlx major, so this example builds the
//! [`DatabaseConnection`] through sea-orm's own pool via [`Database::connect`]
//! instead of converting a separately-constructed sqlx pool. That keeps it
//! decoupled from this crate's direct sqlx dependency.
//!
//! Because sea-orm now owns the pool, it needs its own async runtime feature.
//! Build/run it with one of sea-orm's `runtime-tokio*` features enabled:
//!
//! ```text
//! cargo run --example sea_orm \
//!     --features sea-orm,sea-orm/sqlx-sqlite,sea-orm/runtime-tokio-rustls
//! ```

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum_health::database::SeaOrmCheck;
use axum_health::{HealthBuilder, Probe};
use sea_orm::{Database, DatabaseConnection};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    // sea-orm bundles its own sqlx; build the connection through sea-orm's own
    // pool from a URL rather than sharing an externally-constructed sqlx pool,
    // which would couple this example to a specific sqlx major.
    let database_connection: DatabaseConnection =
        Database::connect("sqlite://test.db").await.unwrap();

    let cancel = CancellationToken::new();

    let (registry, startup) = HealthBuilder::new()
        .register(
            Probe::READINESS,
            SeaOrmCheck::new("sea-orm", database_connection.clone()),
        )
        .build(cancel.clone());

    let router = Router::new()
        .route("/things", get(things))
        .with_state(database_connection)
        .merge(registry.router());

    startup.mark_ready();

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();

    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(async move { registry.drain().await })
        .await
        .unwrap()
}

async fn things(State(_pool): State<DatabaseConnection>) -> impl IntoResponse {
    // Do whatever
    StatusCode::OK
}
