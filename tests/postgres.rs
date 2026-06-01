//! Postgres integration tests against the redesigned background-prober API.
//!
//! Gated behind the `local` feature (a live database via testcontainers), so it
//! is skipped in CI. Registers every supported driver as a readiness check on a
//! fast interval, waits for the first probe, asserts all healthy, then stops the
//! container and waits for the breakers to drive readiness Down.

#[cfg(feature = "local")]
mod local {
    use axum::http::StatusCode;
    use axum::Router;
    use axum_health::database::{DieselR2d2Check, SeaOrmCheck, SqlxCheck};
    use axum_health::{Check, CheckConfig, HealthBuilder, Probe};
    use axum_test::TestServer;
    use diesel::r2d2::ConnectionManager;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use diesel_async::AsyncPgConnection;
    use sea_orm::DatabaseConnection;
    use std::time::Duration;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::ContainerAsync;
    use testcontainers_modules::postgres::Postgres;
    use tokio_util::sync::CancellationToken;

    fn fast() -> CheckConfig {
        CheckConfig {
            interval: Duration::from_millis(50),
            timeout: Duration::from_secs(5),
            initial_delay: Duration::ZERO,
            ..CheckConfig::default()
        }
    }

    fn diesel(url: &str) -> impl Check {
        let manager = ConnectionManager::<diesel::PgConnection>::new(url.to_owned());
        let pool = diesel::r2d2::Pool::builder()
            .max_size(1)
            .connection_timeout(Duration::from_secs(5))
            .build(manager)
            .unwrap();
        DieselR2d2Check::new("diesel-postgres", pool)
    }

    async fn async_diesel_bb8(url: &str) -> impl Check {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url.to_owned());
        let pool = diesel_async::pooled_connection::bb8::Pool::builder()
            .max_size(1)
            .connection_timeout(Duration::from_secs(5))
            .build(manager)
            .await
            .unwrap();
        axum_health::database::bb8::DieselCheck::new("diesel-bb8", pool)
    }

    fn async_diesel_deadpool(url: &str) -> impl Check {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url.to_owned());
        let pool = diesel_async::pooled_connection::deadpool::Pool::builder(manager)
            .max_size(1)
            .build()
            .unwrap();
        axum_health::database::deadpool::DieselCheck::new("diesel-deadpool", pool)
    }

    fn async_diesel_mobc(url: &str) -> impl Check {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(url.to_owned());
        let pool = diesel_async::pooled_connection::mobc::Pool::builder()
            .max_open(1)
            .build(manager);
        axum_health::database::mobc::DieselCheck::new("diesel-mobc", pool)
    }

    async fn sqlx(url: &str) -> impl Check {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .unwrap();
        SqlxCheck::new("sqlx", pool)
    }

    async fn sea_orm(url: &str) -> impl Check {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .unwrap();
        SeaOrmCheck::new("sea-orm", DatabaseConnection::from(pool))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_all() {
        let container = Postgres::default().start().await.unwrap();
        let url = get_url(&container).await;
        let url = url.as_str();

        let cancel = CancellationToken::new();
        let (registry, startup) = HealthBuilder::new()
            .register_with(Probe::READINESS, fast(), diesel(url))
            .register_with(Probe::READINESS, fast(), async_diesel_bb8(url).await)
            .register_with(Probe::READINESS, fast(), async_diesel_deadpool(url))
            .register_with(Probe::READINESS, fast(), async_diesel_mobc(url))
            .register_with(Probe::READINESS, fast(), sqlx(url).await)
            .register_with(Probe::READINESS, fast(), sea_orm(url).await)
            .build(cancel.clone());

        let server = TestServer::new(Router::new().merge(registry.router())).unwrap();
        startup.mark_ready();

        assert!(
            await_status(&server, "/health/ready", StatusCode::OK).await,
            "readiness never became OK with a live database"
        );

        // Stop the database; every breaker should trip and readiness fall to 503.
        container.stop().await.unwrap();
        assert!(
            await_status(&server, "/health/ready", StatusCode::SERVICE_UNAVAILABLE).await,
            "readiness never became 503 after the database stopped"
        );
    }

    async fn await_status(server: &TestServer, path: &str, want: StatusCode) -> bool {
        // Allow time for the breakers to reach the failure threshold.
        for _ in 0..600 {
            if server.get(path).await.status_code() == want {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    async fn get_url(container: &ContainerAsync<Postgres>) -> String {
        format!(
            "postgresql://postgres:postgres@{}:{}/postgres",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(5432).await.unwrap()
        )
    }
}
