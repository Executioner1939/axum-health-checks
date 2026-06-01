//! Native `Check` for a `sea_orm::DatabaseConnection`.

use crate::check::{Check, CheckContext, CheckResult};
use async_trait::async_trait;
use sea_orm::DatabaseConnection;
use tokio::time::Instant;

/// A health check that pings a `sea-orm` connection, reporting round-trip
/// latency on success and the error string on failure.
pub struct SeaOrmCheck {
    name: String,
    conn: DatabaseConnection,
}

impl SeaOrmCheck {
    /// Build a check named `name` over `conn`.
    pub fn new(name: impl Into<String>, conn: DatabaseConnection) -> Self {
        SeaOrmCheck {
            name: name.into(),
            conn,
        }
    }
}

#[async_trait]
impl Check for SeaOrmCheck {
    fn name(&self) -> &str {
        &self.name
    }

    async fn check(&self, cx: &CheckContext) -> CheckResult {
        let start = Instant::now();
        // Abandon a wedged ping promptly on drain rather than blocking the
        // prober until the per-attempt timeout.
        let probe = async {
            match self.conn.ping().await {
                Ok(()) => {
                    let latency = start.elapsed().as_millis();
                    CheckResult::up().with("latency_ms", latency)
                }
                Err(e) => CheckResult::down(format!("ping failed: {e}")),
            }
        };
        tokio::select! {
            biased;
            _ = cx.cancellation_token().cancelled() => CheckResult::down("cancelled: draining"),
            r = probe => r,
        }
    }
}
