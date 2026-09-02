use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_web::{web, Error, HttpRequest, HttpResponse};
use actix_ws::{AggregatedMessage, MessageStream, Session};
use futures_util::StreamExt;
use prost::Message;
use saasy_proto_rust::sfu::SessionEndReason;
use saasy_proto_rust::shared::{
    ErrorResponse,
};
use saasy_proto_rust::signal::{
    signal_request_envelope,
    signal_response_envelope,
    SignalRequestEnvelope,
    SignalResponseEnvelope,
};
use tokio::select;
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::auth::JwtValidator;
use crate::background::{HealthStatus, UsageTrackingCommand};
use crate::grpc::SfuClient;
use crate::signal::{SessionManager, SignalCore};
use crate::turn::CoturnConfig;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30); // TODO: env var?

const CLIENT_TIMEOUT: Duration = Duration::from_secs(60); // TODO: env var?

/// # Why Clippy's `future_not_send` is suppressed in this file
/// Clippy warns that this `async fn` may not produce a `Send` future, due to use of `HttpRequest`
/// and `Payload` (which internally use `Rc` and `RefCell`). However, we do not `.await` before
/// those types are fully consumed. Only `Send + Sync` values (`Session`, `MessageStream`, and `Arc<_>` clones)
/// are moved into the spawned future.
///
/// Because Actix Web runs on a per-thread executor, the lack of `Send` is not a problem. This is
/// a known safe pattern in Actix Web, and Clippy's warning is a false positive in this context.
#[allow(clippy::future_not_send)]
#[allow(clippy::too_many_arguments)]
pub async fn upgrade_to_session_ws(
    request: HttpRequest,
    payload: web::Payload,
    sfu_client: web::Data<Arc<SfuClient>>,
    session_manager: web::Data<Arc<SessionManager>>,
    jwt_validator: web::Data<Arc<JwtValidator>>,
    health_status: web::Data<Arc<RwLock<HealthStatus>>>,
    usage_tracking_command_tx: web::Data<mpsc::Sender<UsageTrackingCommand>>,
    coturn_config: web::Data<Option<Arc<CoturnConfig>>>,
) -> Result<HttpResponse, Error> {
    // Gate: reject if SFU is unhealthy
    if !health_status.read().await.is_ready() {
        return Ok(HttpResponse::ServiceUnavailable().json(serde_json::json!({
            "error": "service_unavailable",
            "message": "SFU is not available"
        })));
    }

    // Upgrade HTTP connection to WebSocket session
    let (response, session, message_stream) = actix_ws::handle(&request, payload)?;

    // Spawn the WebSocket handler task
    let sfu_client_clone = sfu_client.get_ref().clone();
    let session_manager_clone = session_manager.get_ref().clone();
    let jwt_validator_clone = jwt_validator.get_ref().clone();
    let usage_tracking_command_tx_clone = usage_tracking_command_tx.get_ref().clone();
    let coturn_config_clone = coturn_config.get_ref().clone();

    actix_web::rt::spawn(async move {
        handle_session_websocket(
            session,
            message_stream,
            sfu_client_clone,
            session_manager_clone,
            jwt_validator_clone,
            usage_tracking_command_tx_clone,
            coturn_config_clone,
        ).await;
    });

    Ok(response)
}

#[allow(clippy::future_not_send)]
pub async fn upgrade_to_system_ws(
    request: HttpRequest,
    payload: web::Payload,
    sfu_client: web::Data<Arc<SfuClient>>,
    session_manager: web::Data<Arc<SessionManager>>,
) -> Result<HttpResponse, Error> {
    // Upgrade HTTP connection to WebSocket session
    let (response, session, message_stream) = actix_ws::handle(&request, payload)?;

    // Spawn the WebSocket handler task
    let sfu_client = sfu_client.get_ref().clone();
    let session_manager = session_manager.get_ref().clone();

    actix_web::rt::spawn(async move {
        handle_system_websocket(session, message_stream, sfu_client, session_manager).await;
    });

    Ok(response)
}

#[allow(clippy::future_not_send)]
pub async fn handle_session_websocket(
    mut session: Session,
    stream: MessageStream,
    sfu_client: Arc<SfuClient>,
    session_manager: Arc<SessionManager>,
    jwt_validator: Arc<JwtValidator>,
    usage_tracking_command_tx: mpsc::Sender<UsageTrackingCommand>,
    coturn_config: Option<Arc<CoturnConfig>>,
) {
    info!("Session WebSocket connection established");

    let mut last_heartbeat = Instant::now();
    let mut heartbeat_interval = interval(HEARTBEAT_INTERVAL);

    // Configure message stream from client
    // TODO: env var?
    let mut stream = stream
        .max_frame_size(128 * 1024)
        .aggregate_continuations()
        .max_continuation_size(2 * 1024 * 1024);

    // Create a channel for sending messages to the client
    // TODO: env var?
    let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(150);

    // Create a message handler for this connection
    // Clone usage_tracking_command_tx for cleanup (SignalCore will own its copy)
    let usage_tracking_command_tx_clone = usage_tracking_command_tx.clone();
    let signal_core = Arc::new(SignalCore::new(
        sender.clone(),
        session_manager.clone(),
        sfu_client.clone(),
        jwt_validator,
        usage_tracking_command_tx,
        coturn_config,
    ));

    // Session ID and Participant ID will be set after registration/join
    let mut session_id: Option<String> = None;
    let mut participant_id: Option<String> = None;

    let close_reason = loop {
        select! {
            biased; // Prioritizes branches in top-down order

            // Check heartbeat
            _ = heartbeat_interval.tick() => {
                if Instant::now().duration_since(last_heartbeat) > CLIENT_TIMEOUT {
                    info!("Session WebSocket heartbeat timeout: no response from client in {:?}", CLIENT_TIMEOUT);
                    break None;
                }
                let _ = session.ping(b"").await; // empty payload
            },

            // Signal Server → Client: Send outbound messages from signaling layer to the client
            Some(msg) = receiver.recv() => {
                if let Err(e) = session.binary(msg).await {
                    error!("Failed to send message to client: {e}");
                    break None;
                }
            },

            // Client → Signal Server: Handle incoming WebSocket frames from the client
            msg = stream.next() => {
                match msg {
                    Some(Ok(aggregated_msg)) => match aggregated_msg {
                        AggregatedMessage::Ping(bytes) => {
                            last_heartbeat = Instant::now();
                            let _ = session.pong(&bytes).await;
                        },
                        AggregatedMessage::Pong(_) => {
                            last_heartbeat = Instant::now();
                        },
                        AggregatedMessage::Binary(bin) => {
                            let result = handle_binary_message(
                                &mut session, 
                                &signal_core, 
                                bin, 
                                session_id.clone(),
                                participant_id.clone()
                            ).await;
                            session_id = result.0;
                            participant_id = result.1;
                        },
                        AggregatedMessage::Text(text) => {
                            warn!("Unexpected text message: {} characters", text.len());
                        },
                        AggregatedMessage::Close(reason) => {
                            info!("Signal WebSocket closing: {:?}", reason);
                            break reason;
                        },
                    },
                    Some(Err(e)) => {
                        error!("Signal WebSocket stream error: {e}");
                        break None;
                    },
                    None => {
                        info!("Signal WebSocket stream ended");
                        break None;
                    },
                }
            },
        }
    };

    // Cleanup
    if let (Some(sid), Some(pid)) = (session_id.as_ref(), participant_id.as_ref()) {
        info!("Participant {pid} disconnected, terminating session {sid}");

        // Stop usage tracking for this session
        let command = UsageTrackingCommand::Stop { session_id: sid.clone() };
        if let Err(e) = usage_tracking_command_tx_clone.send(command).await {
            warn!("Failed to stop usage tracking for session {}: {}", sid, e);
        }

        session_manager.terminate_session(sid, SessionEndReason::Normal).await;
    }

    let _ = session.close(close_reason.clone()).await;
    info!("Session WebSocket connection closed. Reason: {:?}", close_reason);
}

#[allow(clippy::future_not_send)]
pub async fn handle_system_websocket(
    mut session: Session,
    stream: MessageStream,
    _sfu_client: Arc<SfuClient>,
    session_manager: Arc<SessionManager>,
) {
    info!("System WebSocket connection established");

    let mut last_heartbeat = Instant::now();
    let mut heartbeat_interval = interval(HEARTBEAT_INTERVAL);

    // Configure message stream from client
    // TODO: env var?
    let mut stream = stream
        .max_frame_size(128 * 1024)
        .aggregate_continuations()
        .max_continuation_size(2 * 1024 * 1024);

    // Create a channel for sending messages to the client
    // TODO: env var?
    let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(150);

    let system_subscriber_id = format!("system-subscriber-{}", uuid::Uuid::new_v4());
    
    // Subscribe to system-wide events
    if let Err(e) = session_manager.subscribe_to_system(&system_subscriber_id, sender.clone()).await {
        error!("Failed to subscribe: {e}");
        let _ = session.close(None).await;
        return;
    }

    let close_reason = loop {
        select! {
            biased; // Prioritizes branches in top-down order

            // Check heartbeat
            _ = heartbeat_interval.tick() => {
                if Instant::now().duration_since(last_heartbeat) > CLIENT_TIMEOUT {
                    info!("System WebSocket heartbeat timeout: no response from system subscriber in {:?}", CLIENT_TIMEOUT);
                    break None;
                }
                let _ = session.ping(b"").await; // empty payload
            },

            // Signal Server → System Subscriber: Send outbound messages from signaling layer to the system subscriber
            Some(msg) = receiver.recv() => {
                if let Err(e) = session.binary(msg).await {
                    error!("Failed to send event to system subscriber: {e}");
                    break None;
                }
            },

            // System Subscriber → Signal Server: Handle incoming WebSocket frames from the system subscriber
            msg = stream.next() => {
                match msg {
                    Some(Ok(aggregated_msg)) => match aggregated_msg {
                        AggregatedMessage::Ping(bytes) => {
                            last_heartbeat = Instant::now();
                            let _ = session.pong(&bytes).await;
                        },
                        AggregatedMessage::Pong(_) => {
                            last_heartbeat = Instant::now();
                        },
                        AggregatedMessage::Binary(bin) => {
                            warn!("Unexpected binary message from system subscriber: {} bytes", bin.len());
                        },
                        AggregatedMessage::Text(text) => {
                            warn!("Unexpected text message from system subscriber: {} characters", text.len());
                        },
                        AggregatedMessage::Close(reason) => {
                            info!("System WebSocket closing: {:?}", reason);
                            break reason;
                        },
                    },
                    Some(Err(e)) => {
                        error!("System WebSocket stream error: {e}");
                        break None;
                    },
                    None => {
                        info!("System WebSocket stream ended");
                        break None;
                    },
                }
            },
        }
    };

    // Cleanup
    if let Err(e) = session_manager.unsubscribe_to_system(&system_subscriber_id).await {
        error!("Error unsubscribing subscriber: {e}");
    }

    let _ = session.close(close_reason.clone()).await;
    info!("System WebSocket connection closed for System Subscriber {}. Reason: {:?}", system_subscriber_id, close_reason);
}

async fn handle_binary_message(
    session: &mut Session,
    signal_core: &Arc<SignalCore>,
    bin: impl AsRef<[u8]>,
    current_session_id: Option<String>,
    current_participant_id: Option<String>,
) -> (Option<String>, Option<String>) {
    debug!("Received binary proto message: {} bytes", bin.as_ref().len());
    
    // Track the session ID and participant ID for the return value
    let mut session_id = current_session_id;
    let mut participant_id = current_participant_id;

    match SignalRequestEnvelope::decode(bin.as_ref()) {
        Ok(envelope) => {
            let is_register_session = envelope.r#type == "register_session" || 
                matches!(envelope.data.as_ref(), 
                    Some(signal_request_envelope::Data::RegisterSessionRequest(_)));
            
            let is_join_session = envelope.r#type == "join_session" || 
                matches!(envelope.data.as_ref(), 
                    Some(signal_request_envelope::Data::JoinSessionRequest(_)));
            
            match signal_core.handle_request_envelope(envelope.clone()).await {
                Ok(response) => {
                    if is_register_session || is_join_session {
                        session_id = Some(response.session_id.clone());
                        participant_id = Some(envelope.participant_id.clone());
                        debug!("Session: {} Participant: {}", response.session_id, envelope.participant_id);
                    }

                    if let Err(e) = signal_core.send_binary_message(response).await {
                        error!("Failed to send response: {e}");
                    }
                },
                Err(e) => {
                    error!("Error handling message: {e}");
                    let error_msg = create_proto_error_message("internal_error", &e);
                    if let Err(e) = session.binary(error_msg).await {
                        error!("Failed to send error message: {e}");
                    }
                }
            }
        },
        Err(e) => {
            error!("Failed to parse client proto message: {e}");
            let error_msg = create_proto_error_message(
                "invalid_message", 
                &format!("Failed to parse message: {e}")
            );
            if let Err(e) = session.binary(error_msg).await {
                error!("Failed to send error message: {e}");
            }
        }
    }
    
    (session_id, participant_id)
}

fn create_proto_error_message(code: &str, message: &str) -> Vec<u8> {
    let envelope = SignalResponseEnvelope {
        r#type: "error".to_string(),
        request_id: String::new(), // No request ID for parsing errors
        session_id: String::new(), // No session ID for parsing errors
        participant_id: String::new(), // No participant ID for parsing errors
        data: Some(signal_response_envelope::Data::ErrorResponse(
            ErrorResponse {
                code: code.to_string(),
                message: message.to_string(),
            }
        )),
    };
    
    // Encode the envelope to binary
    let mut response_bytes = Vec::new();
    match envelope.encode(&mut response_bytes) {
        Ok(()) => response_bytes,
        Err(e) => {
            error!("Failed to encode error response: {e}");
            Vec::new()
        }
    }
}
