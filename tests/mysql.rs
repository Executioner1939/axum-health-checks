//! MySQL integration tests against the redesigned background-prober API.
//!
//! Gated behind the `local` feature (a live database via testcontainers), so it
//! is skipped in CI. Mirrors `postgres.rs`: register every driver as a readiness
//! check on a fast interval, wait for the first probe, assert healthy, then stop
//! the container and wait for the breakers to drive readiness Down.

#[cfg(feature = "local")]
mod local {
    use axum::Router;
    use axum::http::StatusCode;
    use axum_health::database::{DieselR2d2Check, SeaOrmCheck, SqlxCheck};
    use axum_health::{Check, CheckConfig, HealthBuilder, Probe};
    use axum_test::TestServer;
    use diesel::r2d2::ConnectionManager;
    use diesel_async::AsyncMysqlConnection;
    use diesel_async::pooled_connection::AsyncDieselConnectionManager;
    use std::time::Duration;
    use testcontainers::ContainerAsync;
    use testcontainers::runners::AsyncRunner;
    use testcontainers_modules::mysql::Mysql;
    use tokio_util::sync::CancellationToken;

    fn fast() -> CheckConfig {
        CheckConfig {
            interval: Duration::from_millis(50),
            timeout: Duration::from_secs(5),
            initial_delay: Duration::ZERO,
            ..CheckConfig::default()
        }
    }

    fn diesel(url: &str) -> impl Check + use<> {
        let manager = ConnectionManager::<diesel::MysqlConnection>::new(url.to_owned());
        let pool = diesel::r2d2::Pool::builder()
            .max_size(1)
            .connection_timeout(Duration::from_secs(5))
            .build(manager)
            .unwrap();
        DieselR2d2Check::new("diesel-mysql", pool)
    }

    async fn async_diesel_bb8(url: &str) -> impl Check + use<> {
        let manager = AsyncDieselConnectionManager::<AsyncMysqlConnection>::new(url.to_owned());
        let pool = diesel_async::pooled_connection::bb8::Pool::builder()
            .max_size(1)
            .connection_timeout(Duration::from_secs(5))
            .build(manager)
            .await
            .unwrap();
        axum_health::database::bb8::DieselCheck::new("diesel-bb8", pool)
    }

    fn async_diesel_deadpool(url: &str) -> impl Check + use<> {
        let manager = AsyncDieselConnectionManager::<AsyncMysqlConnection>::new(url.to_owned());
        let pool = diesel_async::pooled_connection::deadpool::Pool::builder(manager)
            .max_size(1)
            .build()
            .unwrap();
        axum_health::database::deadpool::DieselCheck::new("diesel-deadpool", pool)
    }

    fn async_diesel_mobc(url: &str) -> impl Check + use<> {
        let manager = AsyncDieselConnectionManager::<AsyncMysqlConnection>::new(url.to_owned());
        let pool = diesel_async::pooled_connection::mobc::Pool::builder()
            .max_open(1)
            .build(manager);
        axum_health::database::mobc::DieselCheck::new("diesel-mobc", pool)
    }

    async fn sqlx(url: &str) -> impl Check + use<> {
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(url)
            .await
            .unwrap();
        SqlxCheck::new("sqlx", pool)
    }

    async fn sea_orm(url: &str) -> impl Check + use<> {
        // sea-orm bundles its own sqlx major; build its connection through
        // sea-orm's own pool rather than sharing our direct sqlx 0.9 pool.
        let database = sea_orm::Database::connect(url).await.unwrap();
        SeaOrmCheck::new("sea-orm", database)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_all() {
        let container = Mysql::default().start().await.unwrap();
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

        let server = TestServer::new(Router::new().merge(registry.router()));
        startup.mark_ready();

        assert!(
            await_status(&server, "/health/ready", StatusCode::OK).await,
            "readiness never became OK with a live database"
        );

        container.stop().await.unwrap();
        assert!(
            await_status(&server, "/health/ready", StatusCode::SERVICE_UNAVAILABLE).await,
            "readiness never became 503 after the database stopped"
        );
    }

    async fn await_status(server: &TestServer, path: &str, want: StatusCode) -> bool {
        for _ in 0..600 {
            if server.get(path).await.status_code() == want {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }

    async fn get_url(container: &ContainerAsync<Mysql>) -> String {
        format!(
            "mysql://root@{}:{}/test",
            container.get_host().await.unwrap(),
            container.get_host_port_ipv4(3306).await.unwrap()
        )
    }
}
