//! Native `Check` implementations for diesel connection pools.
//!
//! The async pools (bb8 / deadpool / mobc) share an internal `async_ping_impl!`
//! macro, which emits a `Check` producing a `CheckResult` rather than a
//! `Pingable` bool. The r2d2 pool's `conn.ping()` is **synchronous** (a blocking
//! call), so its branch wraps the ping in `tokio::task::spawn_blocking` — fixing
//! a latent runtime-worker stall in the `0.1.x` code, which called the blocking
//! ping directly inside an async fn.

/// Emit an async-pool `Check` impl wrapping `$pool<Conn>`, timing the ping and
/// preserving any error string. Only used by the bb8/deadpool/mobc modules, so
/// it is gated on the async-pool feature to avoid an unused-macro warning on a
/// pure `diesel-r2d2` build.
#[cfg(feature = "_diesel-async")]
macro_rules! async_ping_impl {
    ($pool:tt) => {
        /// A diesel-async pool health check, timing the verified-recycling ping.
        ///
        /// The query bounds appear on the struct as well as the impl because
        /// naming the concrete pool alias (`Pool<Conn> =
        /// SomePool<AsyncDieselConnectionManager<Conn>>`) requires the manager to
        /// be a valid pool `Manager`, which carries exactly these bounds.
        pub struct DieselCheck<Conn>
        where
            Conn: diesel_async::pooled_connection::PoolableConnection + Send + 'static,
            diesel::dsl::select<diesel::dsl::AsExprOf<i32, diesel::sql_types::Integer>>:
                diesel_async::methods::ExecuteDsl<Conn>,
            diesel::query_builder::SqlQuery: diesel::query_builder::QueryFragment<Conn::Backend>,
        {
            name: String,
            pool: $pool<Conn>,
        }

        impl<Conn> DieselCheck<Conn>
        where
            Conn: diesel_async::pooled_connection::PoolableConnection + Send + 'static,
            diesel::dsl::select<diesel::dsl::AsExprOf<i32, diesel::sql_types::Integer>>:
                diesel_async::methods::ExecuteDsl<Conn>,
            diesel::query_builder::SqlQuery: diesel::query_builder::QueryFragment<Conn::Backend>,
        {
            /// Build a check named `name` over `pool`.
            pub fn new(name: impl Into<String>, pool: $pool<Conn>) -> Self {
                DieselCheck {
                    name: name.into(),
                    pool,
                }
            }
        }

        #[async_trait::async_trait]
        impl<Conn> crate::check::Check for DieselCheck<Conn>
        where
            Conn: diesel_async::pooled_connection::PoolableConnection + Send + 'static,
            diesel::dsl::select<diesel::dsl::AsExprOf<i32, diesel::sql_types::Integer>>:
                diesel_async::methods::ExecuteDsl<Conn>,
            diesel::query_builder::SqlQuery: diesel::query_builder::QueryFragment<Conn::Backend>,
        {
            fn name(&self) -> &str {
                &self.name
            }

            async fn check(&self, cx: &crate::check::CheckContext) -> crate::check::CheckResult {
                let start = tokio::time::Instant::now();
                // Abandon a wedged get/ping promptly on drain rather than
                // blocking the prober until the per-attempt timeout.
                let probe = async {
                    match self.pool.get().await {
                        Ok(mut conn) => match conn
                            .ping(&diesel_async::pooled_connection::RecyclingMethod::Verified)
                            .await
                        {
                            Ok(()) => {
                                let latency = start.elapsed().as_millis();
                                crate::check::CheckResult::up().with("latency_ms", latency)
                            }
                            Err(e) => crate::check::CheckResult::down(format!("ping failed: {e}")),
                        },
                        Err(e) => crate::check::CheckResult::down(format!("get failed: {e}")),
                    }
                };
                tokio::select! {
                    biased;
                    _ = cx.cancellation_token().cancelled() => {
                        crate::check::CheckResult::down("cancelled: draining")
                    }
                    r = probe => r,
                }
            }
        }
    };
}

#[cfg(feature = "diesel-r2d2")]
mod r2d2 {
    use crate::check::{Check, CheckContext, CheckResult};
    use diesel::r2d2::{ConnectionManager, Pool};

    /// An r2d2 pool health check. The blocking `get()`/`ping()` round-trip runs
    /// under `spawn_blocking` so it never stalls a tokio runtime worker.
    pub struct DieselR2d2Check<Conn>
    where
        Conn: diesel::r2d2::R2D2Connection + Send + 'static,
        Conn::Backend: diesel::backend::DieselReserveSpecialization,
    {
        name: String,
        pool: Pool<ConnectionManager<Conn>>,
    }

    impl<Conn> DieselR2d2Check<Conn>
    where
        Conn: diesel::r2d2::R2D2Connection + Send + 'static,
        Conn::Backend: diesel::backend::DieselReserveSpecialization,
    {
        /// Build a check named `name` over `pool`.
        pub fn new(name: impl Into<String>, pool: Pool<ConnectionManager<Conn>>) -> Self {
            DieselR2d2Check {
                name: name.into(),
                pool,
            }
        }
    }

    #[async_trait::async_trait]
    impl<Conn> Check for DieselR2d2Check<Conn>
    where
        Conn: diesel::r2d2::R2D2Connection + Send + 'static,
        Conn::Backend: diesel::backend::DieselReserveSpecialization,
    {
        fn name(&self) -> &str {
            &self.name
        }

        async fn check(&self, _cx: &CheckContext) -> CheckResult {
            let pool = self.pool.clone();
            let result = tokio::task::spawn_blocking(move || {
                let start = std::time::Instant::now();
                match pool.get() {
                    Ok(mut conn) => match conn.ping() {
                        Ok(()) => Ok(start.elapsed().as_millis()),
                        Err(e) => Err(format!("ping failed: {e}")),
                    },
                    Err(e) => Err(format!("get failed: {e}")),
                }
            })
            .await;

            match result {
                Ok(Ok(latency)) => CheckResult::up().with("latency_ms", latency),
                Ok(Err(msg)) => CheckResult::down(msg),
                Err(join) => CheckResult::down(format!("blocking ping panicked: {join}")),
            }
        }
    }
}

#[cfg(feature = "diesel-r2d2")]
pub use r2d2::DieselR2d2Check;

/// `Check` for a diesel-async `bb8` pool. Exposes [`bb8::DieselCheck`].
#[cfg(feature = "diesel-bb8")]
pub mod bb8 {
    use diesel_async::pooled_connection::bb8::Pool;
    async_ping_impl!(Pool);
}

/// `Check` for a diesel-async `deadpool` pool. Exposes [`deadpool::DieselCheck`].
#[cfg(feature = "diesel-deadpool")]
pub mod deadpool {
    use diesel_async::pooled_connection::deadpool::Pool;
    async_ping_impl!(Pool);
}

/// `Check` for a diesel-async `mobc` pool. Exposes [`mobc::DieselCheck`].
#[cfg(feature = "diesel-mobc")]
pub mod mobc {
    use diesel_async::pooled_connection::mobc::Pool;
    async_ping_impl!(Pool);
}
