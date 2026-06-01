//! Native [`Check`](crate::check::Check) implementations for common database
//! drivers, behind feature gates.
//!
//! The old `Pingable` trait and `DatabaseHealthIndicator` adapter are gone.
//! Each driver gets a concrete `Check` whose body reuses the proven
//! `acquire()`/`ping()` round-trip but returns a rich `CheckResult` (latency,
//! preserved error) instead of a `bool`. The per-attempt timeout is imposed by
//! the prober, not the check.

#[cfg(feature = "_diesel")]
pub mod diesel;
#[cfg(feature = "sea-orm")]
pub mod sea_orm;
#[cfg(feature = "sqlx")]
pub mod sqlx;

#[cfg(feature = "sqlx")]
pub use sqlx::SqlxCheck;

#[cfg(feature = "sea-orm")]
pub use sea_orm::SeaOrmCheck;

#[cfg(feature = "diesel-r2d2")]
pub use diesel::DieselR2d2Check;

// Re-export the per-async-pool diesel check modules at the database level so
// consumers write `database::bb8::DieselCheck` rather than reaching through the
// `diesel` submodule.
#[cfg(feature = "diesel-bb8")]
pub use diesel::bb8;
#[cfg(feature = "diesel-deadpool")]
pub use diesel::deadpool;
#[cfg(feature = "diesel-mobc")]
pub use diesel::mobc;
