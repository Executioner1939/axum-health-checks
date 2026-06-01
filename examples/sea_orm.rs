use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use axum_health::database::SeaOrmCheck;
use axum_health::{HealthBuilder, Probe};
use sea_orm::DatabaseConnection;
use sqlx::SqlitePool;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let pool = SqlitePool::connect("test.db").await.unwrap();
    let database_connection = DatabaseConnection::from(pool);

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
