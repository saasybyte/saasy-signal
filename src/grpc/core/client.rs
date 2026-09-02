use saasy_proto_rust::core::v1::{
    core_service_client::CoreServiceClient,
    RecordUsageRequest,
    RecordUsageResponse,
};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use super::error::CoreClientError;

pub struct CoreClient {
    inner: Mutex<CoreServiceClient<Channel>>,
}

impl CoreClient {
    pub async fn connect(url: impl Into<String>) -> Result<Self, CoreClientError> {
        let channel = Channel::from_shared(url.into())?
            .connect()
            .await?;

        Ok(Self {
            inner: Mutex::new(CoreServiceClient::new(channel)),
        })
    }

    pub async fn record_usage(
        &self,
        invite_code_id: &str,
        seconds_consumed: i32,
    ) -> Result<RecordUsageResponse, CoreClientError> {
        let request = RecordUsageRequest {
            invite_code_id: invite_code_id.to_string(),
            seconds_consumed,
        };

        let response = self.inner
            .lock()
            .await
            .record_usage(request)
            .await?
            .into_inner();

        Ok(response)
    }
}
