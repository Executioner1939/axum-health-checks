use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use axum_health::database::DieselR2d2Check;
use axum_health::{HealthBuilder, Probe};
use diesel::r2d2::{ConnectionManager, Pool};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let manager = ConnectionManager::<diesel::SqliteConnection>::new("test.db");
    let pool = Pool::builder().build(manager).unwrap();

    let cancel = CancellationToken::new();

    // The r2d2 ping is synchronous; DieselR2d2Check runs it under spawn_blocking
    // so it never stalls a runtime worker.
    let (registry, startup) = HealthBuilder::new()
        .register(
            Probe::READINESS,
            DieselR2d2Check::new("diesel", pool.clone()),
        )
        .build(cancel.clone());

    let router = Router::new()
        .route("/things", get(things))
        .with_state(pool)
        .merge(registry.router());

    startup.mark_ready();

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();

    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(async move { registry.drain().await })
        .await
        .unwrap()
}

async fn things(
    State(_pool): State<Pool<ConnectionManager<diesel::SqliteConnection>>>,
) -> impl IntoResponse {
    // Do whatever
    StatusCode::OK
}
