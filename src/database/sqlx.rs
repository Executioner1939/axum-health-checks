//! Native `Check` for an `sqlx::Pool`.

use crate::check::{Check, CheckContext, CheckResult};
use async_trait::async_trait;
use sqlx::pool::Pool;
use sqlx::{Connection, Database};
use tokio::time::Instant;

/// A health check that acquires a connection from an `sqlx` pool and pings it,
/// reporting the round-trip latency on success and the error string on failure.
pub struct SqlxCheck<DB: Database> {
    name: String,
    pool: Pool<DB>,
}

impl<DB: Database> SqlxCheck<DB> {
    /// Build a check named `name` over `pool`.
    pub fn new(name: impl Into<String>, pool: Pool<DB>) -> Self {
        SqlxCheck {
            name: name.into(),
            pool,
        }
    }
}

#[async_trait]
impl<DB: Database> Check for SqlxCheck<DB> {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self, cx: &CheckContext) -> CheckResult {
        let start = Instant::now();
        // Race the round-trip against drain so a wedged acquire/ping abandons
        // promptly instead of holding the prober (and a pool slot) until the
        // per-attempt timeout. The prober also races the whole attempt against
        // cancellation, but cooperating here releases the connection sooner.
        let probe = async {
            match self.pool.acquire().await {
                Ok(mut conn) => match conn.ping().await {
                    Ok(()) => {
                        let latency = start.elapsed().as_millis();
                        CheckResult::up().with("latency_ms", latency)
                    }
                    Err(e) => CheckResult::down(format!("ping failed: {e}")),
                },
                Err(e) => CheckResult::down(format!("acquire failed: {e}")),
            }
        };
        tokio::select! {
            biased;
            _ = cx.cancellation_token().cancelled() => CheckResult::down("cancelled: draining"),
            r = probe => r,
        }
    }
}
