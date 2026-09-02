use saasy_proto_rust::sfu::{
    sfu_request_envelope,
    sfu_response_envelope,
    RegisterSessionRequest,
    SfuEvent,
    SfuRequestEnvelope,
    SfuServiceClient,
};
use saasy_proto_rust::shared::{
    CloseSessionRequest,
    CloseSessionResponse,
    ConnectTransportRequest,
    ConnectTransportResponse,
    ConsumerId,
    CreateConsumerRequest,
    CreateConsumerResponse,
    CreateProducerRequest,
    CreateProducerResponse,
    CreateTransportRequest,
    CreateTransportResponse,
    DtlsParameters,
    GetRouterRtpCapabilitiesRequest,
    GetRouterRtpCapabilitiesResponse,
    JoinSessionRequest,
    JoinSessionResponse,
    ParticipantId,
    ProducerId,
    RegisterSessionResponse,
    ResumeConsumerRequest,
    ResumeConsumerResponse,
    RtpCapabilities,
    RtpParameters,
    SessionId,
    SetRtpCapabilitiesRequest,
    SetRtpCapabilitiesResponse,
    SubscribeToEventsRequest,
    TransportId,
};
use tokio::sync::Mutex;
use tonic::Streaming;
use tonic::transport::Channel;

use super::error::SfuClientError;

pub struct SfuClient {
    inner: Mutex<SfuServiceClient<Channel>>,
}

impl SfuClient {
    pub async fn connect(url: impl Into<String>) -> Result<Self, SfuClientError> {
        let channel = Channel::from_shared(url.into())?
            .connect()
            .await?;

        Ok(Self {
            inner: Mutex::new(SfuServiceClient::new(channel)),
        })
    }

    pub async fn register_session(
        &self,
        participant_id: &str,
    ) -> Result<RegisterSessionResponse, SfuClientError> {
        let request = SfuRequestEnvelope {
            r#type: "register_session".to_string(),
            session_id: String::new(),
            participant_id: participant_id.to_string(),
            data: Some(sfu_request_envelope::Data::RegisterSessionRequest(
                RegisterSessionRequest {}
            )),
        };

        let response = self.inner
            .lock()
            .await
            .register_session(request)
            .await?
            .into_inner();

        if response.session_id.is_empty() {
            return Err(SfuClientError::UnexpectedResponse(
                "No session ID received in registration response".to_string()
            ));
        }

        match response.data {
            Some(sfu_response_envelope::Data::RegisterSessionResponse(data)) => {
                Ok(data)
            },
            Some(sfu_response_envelope::Data::ErrorResponse(error)) => {
                Err(SfuClientError::SfuError(format!(
                    "SFU error: {} - {}", 
                    error.code, 
                    error.message
                )))
            },
            Some(_) => {
                Err(SfuClientError::UnexpectedResponse(
                    "Received unexpected response type".to_string()
                ))
            },
            None => {
                Err(SfuClientError::UnexpectedResponse(
                    "Empty response received".to_string()
                ))
            }
        }
    }

    pub async fn join_session(
        &self,
        session_id: &str,
        participant_id: &str,
        participant_type: i32,
    ) -> Result<JoinSessionResponse, SfuClientError> {
        let request = SfuRequestEnvelope {
            r#type: "join_session".to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(sfu_request_envelope::Data::JoinSessionRequest(
                JoinSessionRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    participant_id: Some(ParticipantId { id: participant_id.to_string() }),
                    participant_type,
                }
            )),
        };
    
        let response = self.inner
            .lock()
            .await
            .join_session(request)
            .await?
            .into_inner();
    
        match response.data {
            Some(sfu_response_envelope::Data::JoinSessionResponse(data)) => {
                Ok(data)
            },
            Some(sfu_response_envelope::Data::ErrorResponse(error)) => {
                Err(SfuClientError::SfuError(format!(
                    "SFU error: {} - {}", 
                    error.code, 
                    error.message
                )))
            },
            Some(_) => {
                Err(SfuClientError::UnexpectedResponse(
                    "Received unexpected response type".to_string()
                ))
            },
            None => {
                Err(SfuClientError::UnexpectedResponse(
                    "Empty response received".to_string()
                ))
            }
        }
    }

    pub async fn get_router_rtp_capabilities(
        &self,
        session_id: &str,
        participant_id: &str,
    ) -> Result<GetRouterRtpCapabilitiesResponse, SfuClientError> {
        let request = SfuRequestEnvelope {
            r#type: "get_router_rtp_capabilities".to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(sfu_request_envelope::Data::GetRouterRtpCapabilitiesRequest(
                GetRouterRtpCapabilitiesRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                },
            )),
        };

        let response = self.inner
            .lock()
            .await
            .get_router_rtp_capabilities(request)
            .await?
            .into_inner();

        if response.session_id != session_id {
            return Err(SfuClientError::UnexpectedResponse(
                format!("Session ID mismatch: expected '{}', got '{}'", 
                    session_id, response.session_id)
            ));
        }

        match response.data {
            Some(sfu_response_envelope::Data::GetRouterRtpCapabilitiesResponse(data)) => {
                Ok(data)
            },
            Some(sfu_response_envelope::Data::ErrorResponse(error)) => {
                Err(SfuClientError::SfuError(format!(
                    "SFU error: {} - {}", 
                    error.code, 
                    error.message
                )))
            },
            Some(_) => {
                Err(SfuClientError::UnexpectedResponse(
                    "Received unexpected response type".to_string()
                ))
            },
            None => {
                Err(SfuClientError::UnexpectedResponse(
                    "Empty response received".to_string()
                ))
            }
        }
    }

    pub async fn set_rtp_capabilities(
        &self,
        session_id: &str,
        participant_id: &str,
        rtp_capabilities: RtpCapabilities,
    ) -> Result<SetRtpCapabilitiesResponse, SfuClientError> {
        let request = SfuRequestEnvelope {
            r#type: "set_rtp_capabilities".to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(sfu_request_envelope::Data::SetRtpCapabilitiesRequest(
                SetRtpCapabilitiesRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    rtp_capabilities: Some(rtp_capabilities),
                },
            )),
        };

        let response = self.inner
            .lock()
            .await
            .set_rtp_capabilities(request)
            .await?
            .into_inner();

        match response.data {
            Some(sfu_response_envelope::Data::SetRtpCapabilitiesResponse(data)) => {
                Ok(data)
            },
            Some(sfu_response_envelope::Data::ErrorResponse(error)) => {
                Err(SfuClientError::SfuError(format!(
                    "SFU error: {} - {}", 
                    error.code, 
                    error.message
                )))
            },
            Some(_) => {
                Err(SfuClientError::UnexpectedResponse(
                    "Received unexpected response type".to_string()
                ))
            },
            None => {
                Err(SfuClientError::UnexpectedResponse(
                    "Empty response received".to_string()
                ))
            }
        }
    }

    pub async fn create_transport(
        &self,
        session_id: &str,
        participant_id: &str,
        direction: i32,
    ) -> Result<CreateTransportResponse, SfuClientError> {
        let request = SfuRequestEnvelope {
            r#type: "create_transport".to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(sfu_request_envelope::Data::CreateTransportRequest(
                CreateTransportRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    direction,
                },
            )),
        };
    
        let response = self.inner
            .lock()
            .await
            .create_transport(request)
            .await?
            .into_inner();
    
        match response.data {
            Some(sfu_response_envelope::Data::CreateTransportResponse(data)) => {
                Ok(data)
            },
            Some(sfu_response_envelope::Data::ErrorResponse(error)) => {
                Err(SfuClientError::SfuError(format!(
                    "SFU error: {} - {}", 
                    error.code, 
                    error.message
                )))
            },
            Some(_) => {
                Err(SfuClientError::UnexpectedResponse(
                    "Received unexpected response type".to_string()
                ))
            },
            None => {
                Err(SfuClientError::UnexpectedResponse(
                    "Empty response received".to_string()
                ))
            }
        }
    }

    pub async fn connect_transport(
        &self,
        session_id: &str,
        participant_id: &str,
        transport_id: TransportId,
        dtls_parameters: DtlsParameters,
    ) -> Result<ConnectTransportResponse, SfuClientError> {
        let request = SfuRequestEnvelope {
            r#type: "connect_transport".to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(sfu_request_envelope::Data::ConnectTransportRequest(
                ConnectTransportRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    transport_id: Some(transport_id),
                    dtls_parameters: Some(dtls_parameters),
                },
            )),
        };

        let response = self.inner
            .lock()
            .await
            .connect_transport(request)
            .await?
            .into_inner();

        match response.data {
            Some(sfu_response_envelope::Data::ConnectTransportResponse(data)) => {
                Ok(data)
            },
            Some(sfu_response_envelope::Data::ErrorResponse(error)) => {
                Err(SfuClientError::SfuError(format!(
                    "SFU error: {} - {}", 
                    error.code, 
                    error.message
                )))
            },
            Some(_) => {
                Err(SfuClientError::UnexpectedResponse(
                    "Received unexpected response type".to_string()
                ))
            },
            None => {
                Err(SfuClientError::UnexpectedResponse(
                    "Empty response received".to_string()
                ))
            }
        }
    }

    pub async fn create_producer(
        &self,
        session_id: &str,
        participant_id: &str,
        transport_id: TransportId,
        kind: i32,
        rtp_parameters: RtpParameters,
    ) -> Result<CreateProducerResponse, SfuClientError> {
        let request = SfuRequestEnvelope {
            r#type: "create_producer".to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(sfu_request_envelope::Data::CreateProducerRequest(
                CreateProducerRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    transport_id: Some(transport_id),
                    rtp_parameters: Some(rtp_parameters),
                    kind,
                },
            )),
        };

        let response = self.inner
            .lock()
            .await
            .create_producer(request)
            .await?
            .into_inner();

        match response.data {
            Some(sfu_response_envelope::Data::CreateProducerResponse(data)) => {
                Ok(data)
            },
            Some(sfu_response_envelope::Data::ErrorResponse(error)) => {
                Err(SfuClientError::SfuError(format!(
                    "SFU error: {} - {}", 
                    error.code, 
                    error.message
                )))
            },
            Some(_) => {
                Err(SfuClientError::UnexpectedResponse(
                    "Received unexpected response type".to_string()
                ))
            },
            None => {
                Err(SfuClientError::UnexpectedResponse(
                    "Empty response received".to_string()
                ))
            }
        }
    }

    pub async fn create_consumer(
        &self,
        session_id: &str,
        participant_id: &str,
        transport_id: TransportId,
        producer_id: ProducerId,
        rtp_capabilities: RtpCapabilities,
    ) -> Result<CreateConsumerResponse, SfuClientError> {
        let request = SfuRequestEnvelope {
            r#type: "create_consumer".to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(sfu_request_envelope::Data::CreateConsumerRequest(
                CreateConsumerRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    transport_id: Some(transport_id),
                    producer_id: Some(producer_id),
                    rtp_capabilities: Some(rtp_capabilities),
                },
            )),
        };

        let response = self.inner
            .lock()
            .await
            .create_consumer(request)
            .await?
            .into_inner();

        match response.data {
            Some(sfu_response_envelope::Data::CreateConsumerResponse(data)) => {
                Ok(data)
            },
            Some(sfu_response_envelope::Data::ErrorResponse(error)) => {
                Err(SfuClientError::SfuError(format!(
                    "SFU error: {} - {}", 
                    error.code, 
                    error.message
                )))
            },
            Some(_) => {
                Err(SfuClientError::UnexpectedResponse(
                    "Received unexpected response type".to_string()
                ))
            },
            None => {
                Err(SfuClientError::UnexpectedResponse(
                    "Empty response received".to_string()
                ))
            }
        }
    }

    pub async fn resume_consumer(
        &self,
        session_id: &str,
        participant_id: &str,
        consumer_id: ConsumerId,
    ) -> Result<ResumeConsumerResponse, SfuClientError> {
        let request = SfuRequestEnvelope {
            r#type: "resume_consumer".to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(sfu_request_envelope::Data::ResumeConsumerRequest(
                ResumeConsumerRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                    consumer_id: Some(consumer_id),
                }
            )),
        };

        let response = self.inner
            .lock()
            .await
            .resume_consumer(request)
            .await?
            .into_inner();

        match response.data {
            Some(sfu_response_envelope::Data::ResumeConsumerResponse(data)) => {
                Ok(data)
            },
            Some(sfu_response_envelope::Data::ErrorResponse(error)) => {
                Err(SfuClientError::SfuError(format!(
                    "SFU error: {} - {}", 
                    error.code, 
                    error.message
                )))
            },
            Some(_) => {
                Err(SfuClientError::UnexpectedResponse(
                    "Received unexpected response type".to_string()
                ))
            },
            None => {
                Err(SfuClientError::UnexpectedResponse(
                    "Empty response received".to_string()
                ))
            }
        }
    }

    pub async fn close_session(
        &self,
        session_id: &str,
        participant_id: &str,
    ) -> Result<CloseSessionResponse, SfuClientError> {
        let request = SfuRequestEnvelope {
            r#type: "close_session".to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(sfu_request_envelope::Data::CloseSessionRequest(
                CloseSessionRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                },
            )),
        };

        let response = self.inner
            .lock()
            .await
            .close_session(request)
            .await?
            .into_inner();

        match response.data {
            Some(sfu_response_envelope::Data::CloseSessionResponse(data)) => {
                Ok(data)
            },
            Some(sfu_response_envelope::Data::ErrorResponse(error)) => {
                Err(SfuClientError::SfuError(format!(
                    "SFU error: {} - {}", 
                    error.code, 
                    error.message
                )))
            },
            Some(_) => {
                Err(SfuClientError::UnexpectedResponse(
                    "Received unexpected response type".to_string()
                ))
            },
            None => {
                Err(SfuClientError::UnexpectedResponse(
                    "Empty response received".to_string()
                ))
            }
        }
    }

    pub async fn subscribe_to_events(
        &self,
        session_id: &str,
        participant_id: &str,
    ) -> Result<Streaming<SfuEvent>, SfuClientError> {
        let request = SfuRequestEnvelope {
            r#type: "subscribe_to_events".to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(sfu_request_envelope::Data::SubscribeToEventsRequest(
                SubscribeToEventsRequest {
                    session_id: Some(SessionId { id: session_id.to_string() }),
                },
            )),
        };

        // For streaming responses, we return the Streaming<T> type
        // instead of unwrapping to a single response
        let response = self.inner
            .lock()
            .await
            .subscribe_to_events(request)
            .await?;

        Ok(response.into_inner())
    }
}
