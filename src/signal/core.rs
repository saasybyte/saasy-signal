use std::sync::Arc;

use prost::Message;
use saasy_proto_rust::sfu::SessionEndReason;
use saasy_proto_rust::shared::{
    CloseSessionResponse,
    ConnectTransportRequest,
    ConnectTransportResponse,
    CreateConsumerRequest,
    CreateProducerRequest,
    CreateTransportRequest,
    ErrorResponse,
    JoinSessionRequest,
    JoinSessionResponse,
    ResumeConsumerRequest,
    ResumeConsumerResponse,
    SetRtpCapabilitiesRequest,
    SetRtpCapabilitiesResponse,
};
use saasy_proto_rust::signal::{
    signal_request_envelope,
    signal_response_envelope,
    RegisterSessionRequest,
    SignalRequestEnvelope,
    SignalResponseEnvelope,
};
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::auth::{AuthError, JwtValidator};
use crate::background::UsageTrackingCommand;
use crate::grpc::SfuClient;
use crate::signal::manager::SessionManager;
use crate::turn::CoturnConfig;

pub struct SignalCore {
    sender: mpsc::Sender<Vec<u8>>,
    session_manager: Arc<SessionManager>,
    sfu_client: Arc<SfuClient>,
    jwt_validator: Arc<JwtValidator>,
    usage_tracking_command_tx: mpsc::Sender<UsageTrackingCommand>,
    coturn_config: Option<Arc<CoturnConfig>>,
}

impl SignalCore {
    pub fn new(
        sender: mpsc::Sender<Vec<u8>>,
        session_manager: Arc<SessionManager>,
        sfu_client: Arc<SfuClient>,
        jwt_validator: Arc<JwtValidator>,
        usage_tracking_command_tx: mpsc::Sender<UsageTrackingCommand>,
        coturn_config: Option<Arc<CoturnConfig>>,
    ) -> Self {
        Self {
            sender,
            session_manager,
            sfu_client,
            jwt_validator,
            usage_tracking_command_tx,
            coturn_config,
        }
    }

    pub async fn handle_request_envelope(
        &self,
        envelope: SignalRequestEnvelope,
    ) -> Result<SignalResponseEnvelope, String> {
        debug!("Handling request envelope: {:?}", envelope);

        // Match on the data field to determine request type
        let response = match envelope.data {
            Some(data) => match data {
                signal_request_envelope::Data::RegisterSessionRequest(request) => {
                    self.register_session(&envelope.request_id, &envelope.participant_id, request).await
                },
                signal_request_envelope::Data::JoinSessionRequest(request) => {
                    self.join_session(&envelope.request_id, &envelope.session_id, &envelope.participant_id, request).await
                },
                signal_request_envelope::Data::GetRouterRtpCapabilitiesRequest(_) => {
                    self.get_router_rtp_capabilities(&envelope.request_id, &envelope.session_id, &envelope.participant_id).await
                },
                signal_request_envelope::Data::SetRtpCapabilitiesRequest(request) => {
                    self.set_rtp_capabilities(&envelope.request_id, &envelope.session_id, &envelope.participant_id, request).await
                },
                signal_request_envelope::Data::CreateTransportRequest(request) => {
                    self.create_transport(&envelope.request_id, &envelope.session_id, &envelope.participant_id, request).await
                },
                signal_request_envelope::Data::ConnectTransportRequest(request) => {
                    self.connect_transport(&envelope.request_id, &envelope.session_id, &envelope.participant_id, request).await
                },
                signal_request_envelope::Data::CreateProducerRequest(request) => {
                    self.create_producer(&envelope.request_id, &envelope.session_id, &envelope.participant_id, request).await
                },
                signal_request_envelope::Data::CreateConsumerRequest(request) => {
                    self.create_consumer(&envelope.request_id, &envelope.session_id, &envelope.participant_id, request).await
                },
                signal_request_envelope::Data::ResumeConsumerRequest(request) => {
                    self.resume_consumer(&envelope.request_id, &envelope.session_id, &envelope.participant_id, request).await
                },
                signal_request_envelope::Data::CloseSessionRequest(_) => {
                    self.close_session(&envelope.request_id, &envelope.session_id, &envelope.participant_id).await
                },
                signal_request_envelope::Data::SubscribeToEventsRequest(_) => {
                    self.subscribe_to_events(&envelope.request_id, &envelope.session_id, &envelope.participant_id).await
                },
            },
            None => {
                // Handle the case where no data was provided
                return Err("Missing request data in envelope".to_string());
            }
        };

        match response {
            Ok(response_envelope) => Ok(response_envelope),
            Err(error_envelope) => Ok(error_envelope),
        }
    }

    // New method to send binary responses - handles success and error cases
    pub async fn send_binary_message(
        &self,
        response: SignalResponseEnvelope,
    ) -> Result<(), String> {
        // Encode the envelope
        let mut bytes = Vec::new();
        response.encode(&mut bytes)
            .map_err(|e| format!("Failed to encode response: {e}"))?;

        // Send the encoded message
        self.sender
            .send(bytes)
            .await
            .map_err(|_| "Failed to send message to client".to_string())?;

        Ok(())
    }

    fn error_response(
        request_id: &str,
        session_id: &str,
        participant_id: &str,
        code: &str,
        message: &str,
    ) -> SignalResponseEnvelope {
        SignalResponseEnvelope {
            r#type: "error".to_string(),
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_response_envelope::Data::ErrorResponse(
                ErrorResponse {
                    code: code.to_string(),
                    message: message.to_string(),
                }
            )),
        }
    }

    async fn register_session(
        &self,
        request_id: &str,
        participant_id: &str,
        request: RegisterSessionRequest,
    ) -> Result<SignalResponseEnvelope, SignalResponseEnvelope> {
        let empty_session = "";

        // Validate JWT and extract claims
        let claims = self.jwt_validator
            .validate_token(&request.auth_token)
            .map_err(|e| {
                let code = match e {
                    AuthError::ExpiredToken => "token_expired",
                    _ => "auth_failed",
                };
                Self::error_response(request_id, empty_session, participant_id, code, &e.to_string())
            })?;

        debug!(
            "JWT validated: invite_code_id={}, usage_remaining={}s, window_expires_at={}, exp={}",
            claims.invite_code_id,
            claims.usage_remaining_seconds,
            claims.window_expires_at,
            claims.exp
        );

        let invite_code_id = claims.invite_code_id;

        let sfu_response = self.sfu_client
            .register_session(participant_id)
            .await
            .map_err(|e| Self::error_response(
                request_id,
                empty_session,
                participant_id,
                "internal_error",
                &format!("Failed to register session with SFU: {e}"),
            ))?;

        let session_id = sfu_response.session_id
            .as_ref()
            .ok_or_else(|| Self::error_response(
                request_id,
                empty_session,
                participant_id,
                "internal_error",
                "Session ID missing in SFU response"
            ))?
            .id
            .clone();

        self.session_manager
            .register_session(
                self.sender.clone(),
                session_id.clone(),
                participant_id.to_string(),
            )
            .await
            .map_err(|e| Self::error_response(
                request_id,
                &session_id,
                participant_id,
                "internal_error",
                &format!("Failed to register session: {e}"),
            ))?;

        // Start usage tracking for this session
        let command = UsageTrackingCommand::Start {
            session_id: session_id.clone(),
            invite_code_id: invite_code_id.clone(),
        };
        if let Err(e) = self.usage_tracking_command_tx.send(command).await {
            warn!("Failed to start usage tracking for session {}: {}", session_id, e);
        }

        // Populate ICE servers if coturn is configured
        let mut sfu_response = sfu_response;
        if let Some(ref coturn) = self.coturn_config {
            match coturn.generate_ice_servers() {
                Ok(ice_servers) => sfu_response.ice_servers = ice_servers,
                Err(e) => warn!("Failed to generate ICE servers for session {}: {}", session_id, e),
            }
        }

        let requires_ai = true; // TODO: Determine if all sessions require AI participation
        let event = SessionManager::session_created_event(
            &session_id,
            requires_ai,
            &request.llm_provider,
            &request.llm_model_id,
            &request.tts_provider,
            &request.tts_model_id,
            &request.stt_provider,
            &request.stt_model_id,
        );
        if let Err(e) = self.session_manager.broadcast_to_system_subscribers(event).await {
            tracing::error!("Failed to broadcast session created event: {}", e);
        }

        Ok(SignalResponseEnvelope {
            r#type: "register_session".to_string(),
            request_id: request_id.to_string(),
            session_id,
            participant_id: participant_id.to_string(),
            data: Some(signal_response_envelope::Data::RegisterSessionResponse(
                sfu_response
            )),
        })
    }

    async fn join_session(
        &self,
        request_id: &str,
        session_id: &str,
        participant_id: &str,
        request: JoinSessionRequest,
    ) -> Result<SignalResponseEnvelope, SignalResponseEnvelope> {
        let participant_type = request.participant_type;

        self.sfu_client
            .join_session(session_id, participant_id, participant_type)
            .await
            .map_err(|e| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "internal_error",
                &format!("Failed to join session with SFU: {e}"),
            ))?;

        self.session_manager
            .register_session(
                self.sender.clone(),
                session_id.to_string(),
                participant_id.to_string(),
            )
            .await
            .map_err(|e| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "internal_error",
                &format!("Failed to register participant in session: {e}"),
            ))?;

        Ok(SignalResponseEnvelope {
            r#type: "join_session".to_string(),
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_response_envelope::Data::JoinSessionResponse(
                JoinSessionResponse {}
            )),
        })
    }

    async fn get_router_rtp_capabilities(
        &self,
        request_id: &str,
        session_id: &str,
        participant_id: &str,
    ) -> Result<SignalResponseEnvelope, SignalResponseEnvelope> {
        let sfu_response = self.sfu_client
            .get_router_rtp_capabilities(session_id, participant_id)
            .await
            .map_err(|e| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "internal_error",
                &format!("Failed to get router RTP capabilities from SFU: {e}"),
            ))?;

        Ok(SignalResponseEnvelope {
            r#type: "get_router_rtp_capabilities".to_string(),
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_response_envelope::Data::GetRouterRtpCapabilitiesResponse(
                sfu_response
            )),
        })
    }

    async fn set_rtp_capabilities(
        &self,
        request_id: &str,
        session_id: &str,
        participant_id: &str,
        request: SetRtpCapabilitiesRequest,
    ) -> Result<SignalResponseEnvelope, SignalResponseEnvelope> {
        let rtp_capabilities = request.rtp_capabilities
            .ok_or_else(|| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "invalid_request",
                "RTP capabilities missing in request",
            ))?;

        self.sfu_client
            .set_rtp_capabilities(
                session_id,
                participant_id,
                rtp_capabilities,
            )
            .await
            .map_err(|e| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "internal_error",
                &format!("Failed to set RTP capabilities via SFU: {e}"),
            ))?;

        Ok(SignalResponseEnvelope {
            r#type: "set_rtp_capabilities".to_string(),
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_response_envelope::Data::SetRtpCapabilitiesResponse(
                SetRtpCapabilitiesResponse {}
            )),
        })
    }

    async fn create_transport(
        &self,
        request_id: &str,
        session_id: &str,
        participant_id: &str,
        request: CreateTransportRequest,
    ) -> Result<SignalResponseEnvelope, SignalResponseEnvelope> {
        let sfu_response = self.sfu_client
            .create_transport(
                session_id,
                participant_id,
                request.direction,
            )
            .await
            .map_err(|e| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "internal_error",
                &format!("Failed to create transport via SFU: {e}"),
            ))?;

        Ok(SignalResponseEnvelope {
            r#type: "create_transport".to_string(),
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_response_envelope::Data::CreateTransportResponse(
                sfu_response
            )),
        })
    }

    async fn connect_transport(
        &self,
        request_id: &str,
        session_id: &str,
        participant_id: &str,
        request: ConnectTransportRequest,
    ) -> Result<SignalResponseEnvelope, SignalResponseEnvelope> {
        let transport_id = request.transport_id
            .ok_or_else(|| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "invalid_request",
                "Transport ID missing in request",
            ))?;

        let dtls_parameters = request.dtls_parameters
            .ok_or_else(|| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "invalid_request",
                "DTLS parameters missing in request",
            ))?;

        self.sfu_client
            .connect_transport(
                session_id,
                participant_id,
                transport_id,
                dtls_parameters,
            )
            .await
            .map_err(|e| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "internal_error",
                &format!("Failed to connect transport via SFU: {e}"),
            ))?;

        Ok(SignalResponseEnvelope {
            r#type: "connect_transport".to_string(),
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_response_envelope::Data::ConnectTransportResponse(
                ConnectTransportResponse {}
            )),
        })
    }

     async fn create_producer(
        &self,
        request_id: &str,
        session_id: &str,
        participant_id: &str,
        request: CreateProducerRequest,
    ) -> Result<SignalResponseEnvelope, SignalResponseEnvelope> {
        let transport_id = request.transport_id
            .ok_or_else(|| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "invalid_request",
                "Transport ID missing in request",
            ))?;

        let rtp_parameters = request.rtp_parameters
            .ok_or_else(|| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "invalid_request",
                "RTP parameters missing in request",
            ))?;

        let sfu_response = self.sfu_client
            .create_producer(
                session_id,
                participant_id,
                transport_id,
                request.kind,
                rtp_parameters,
            )
            .await
            .map_err(|e| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "internal_error",
                &format!("Failed to create producer via SFU: {e}"),
            ))?;

        Ok(SignalResponseEnvelope {
            r#type: "create_producer".to_string(),
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_response_envelope::Data::CreateProducerResponse(
                sfu_response
            )),
        })
    }

    async fn create_consumer(
        &self,
        request_id: &str,
        session_id: &str,
        participant_id: &str,
        request: CreateConsumerRequest,
    ) -> Result<SignalResponseEnvelope, SignalResponseEnvelope> {
        let transport_id = request.transport_id
            .ok_or_else(|| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "invalid_request",
                "Transport ID missing in request",
            ))?;

        let producer_id = request.producer_id
            .ok_or_else(|| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "invalid_request",
                "Producer ID missing in request",
            ))?;

        let rtp_capabilities = request.rtp_capabilities
            .ok_or_else(|| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "invalid_request",
                "RTP capabilities missing in request",
            ))?;

        let sfu_response = self.sfu_client
            .create_consumer(
                session_id,
                participant_id,
                transport_id,
                producer_id,
                rtp_capabilities,
            )
            .await
            .map_err(|e| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "internal_error",
                &format!("Failed to create consumer via SFU: {e}"),
            ))?;

        Ok(SignalResponseEnvelope {
            r#type: "create_consumer".to_string(),
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_response_envelope::Data::CreateConsumerResponse(
                sfu_response
            )),
        })
    }

    async fn resume_consumer(
        &self,
        request_id: &str,
        session_id: &str,
        participant_id: &str,
        request: ResumeConsumerRequest,
    ) -> Result<SignalResponseEnvelope, SignalResponseEnvelope> {
        let consumer_id = request.consumer_id
            .ok_or_else(|| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "invalid_request",
                "Consumer ID missing in request",
            ))?;

        self.sfu_client
            .resume_consumer(
                session_id,
                participant_id,
                consumer_id,
            )
            .await
            .map_err(|e| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "internal_error",
                &format!("Failed to resume consumer via SFU: {e}"),
            ))?;

        Ok(SignalResponseEnvelope {
            r#type: "resume_consumer".to_string(),
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_response_envelope::Data::ResumeConsumerResponse(
                ResumeConsumerResponse {}
            )),
        })
    }

    async fn close_session(
        &self,
        request_id: &str,
        session_id: &str,
        participant_id: &str,
    ) -> Result<SignalResponseEnvelope, SignalResponseEnvelope> {
        // Validate requester is participant
        if !self.session_manager.is_participant_in_session(session_id, participant_id).await {
            return Err(Self::error_response(
                request_id,
                session_id,
                participant_id,
                "forbidden",
                "Participant not found in session",
            ));
        }

        // Terminate session
        self.session_manager.terminate_session(session_id, SessionEndReason::Normal).await;

        Ok(SignalResponseEnvelope {
            r#type: "close_session".to_string(),
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: Some(signal_response_envelope::Data::CloseSessionResponse(
                CloseSessionResponse {}
            )),
        })
    }

    async fn subscribe_to_events(
        &self,
        request_id: &str,
        session_id: &str,
        participant_id: &str,
    ) -> Result<SignalResponseEnvelope, SignalResponseEnvelope> {
        self.session_manager
            .subscribe_to_events(session_id, participant_id)
            .await
            .map_err(|e| Self::error_response(
                request_id,
                session_id,
                participant_id,
                "internal_error",
                &format!("Failed to subscribe to events: {e}"),
            ))?;

        Ok(SignalResponseEnvelope {
            r#type: "subscribe_to_events".to_string(),
            request_id: request_id.to_string(),
            session_id: session_id.to_string(),
            participant_id: participant_id.to_string(),
            data: None,  // Empty success response
        })
    }
}
