use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info};

#[derive(Debug, Clone, Default)]
pub struct HealthStatus {
    pub sfu: bool,
}

impl HealthStatus {
    pub fn is_ready(&self) -> bool {
        self.sfu
    }
}

pub struct HealthBackgroundService {
    http_client: reqwest::Client,
    status: Arc<RwLock<HealthStatus>>,
    check_interval: Duration,
    sfu_http_url: String,
}

impl HealthBackgroundService {
    pub fn new(
        sfu_http_url: String,
        check_interval: Duration,
    ) -> (Self, Arc<RwLock<HealthStatus>>) {
        let status = Arc::new(RwLock::new(HealthStatus::default()));
        let service = Self {
            http_client: reqwest::Client::new(),
            status: status.clone(),
            check_interval,
            sfu_http_url,
        };
        (service, status)
    }

    pub async fn run(self) {
        info!(
            "Starting health background service (interval: {:?})",
            self.check_interval
        );

        let mut ticker = interval(self.check_interval);

        loop {
            ticker.tick().await;
            self.check_all().await;
        }
    }

    async fn check_all(&self) {
        let sfu = self.check_sfu().await;

        debug!("Health check: sfu={}", sfu);

        let mut status = self.status.write().await;
        *status = HealthStatus { sfu };
    }

    async fn check_sfu(&self) -> bool {
        let url = format!("{}/health/live", &self.sfu_http_url);

        match self.http_client
            .get(url)
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(e) => {
                error!("SFU health check failed: {e}");
                false
            }
        }
    }
}
