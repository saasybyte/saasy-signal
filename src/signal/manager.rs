use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use prost::Message;
use saasy_proto_rust::sfu::{
    sfu_event,
    SessionCreatedEvent,
    SessionEndedEvent,
    SessionEndReason,
    SfuEvent,
};
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::grpc::SfuClient;
use super::session::{Participant, SessionState};

type EventTaskHandles = HashMap<(String, String), JoinHandle<()>>;

pub struct SessionManager {
    sessions: Arc<RwLock<HashMap<String, SessionState>>>,
    system_subscribers: Arc<RwLock<HashMap<String, mpsc::Sender<Vec<u8>>>>>,
    sfu_client: Arc<SfuClient>,
    event_task_handles: Arc<Mutex<EventTaskHandles>>,
    terminating_sessions: Arc<RwLock<HashSet<String>>>,
}

impl SessionManager {
    pub fn new(sfu_client: Arc<SfuClient>) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            system_subscribers: Arc::new(RwLock::new(HashMap::new())),
            sfu_client,
            event_task_handles: Arc::new(Mutex::new(HashMap::new())),
            terminating_sessions: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub async fn subscribe_to_system(
        &self,
        system_subscriber_id: &str,
        sender: mpsc::Sender<Vec<u8>>,
    ) -> Result<(), String> {
        {
            let mut system_subscribers = self.system_subscribers.write().await;
            
            if system_subscribers.contains_key(system_subscriber_id) {
                return Err(format!("System Subscriber {system_subscriber_id} already subscribed"));
            }
            
            system_subscribers.insert(system_subscriber_id.to_string(), sender);
        }
        
        info!("System Subscriber {} subscribed to system events", system_subscriber_id);
        
        Ok(())
    }

    pub async fn unsubscribe_to_system(
        &self,
        system_subscriber_id: &str,
    ) -> Result<(), String> {
        let mut system_subscribers = self.system_subscribers.write().await;
        
        if system_subscribers.remove(system_subscriber_id).is_some() {
            info!("System Subscriber {} unsubscribed from system events", system_subscriber_id);
            Ok(())
        } else {
            Err(format!("System Subscriber {system_subscriber_id} not found"))
        }
    }

    pub async fn subscribe_to_events(&self, session_id: &str, participant_id: &str) -> Result<(), String> {
        let task_key = (session_id.to_string(), participant_id.to_string());
        let mut event_task_handles = self.event_task_handles.lock().await;
        if event_task_handles.contains_key(&task_key) {
            return Err("Already subscribed".to_string());
        }

        let mut event_stream = self.sfu_client
            .subscribe_to_events(session_id, participant_id)
            .await
            .map_err(|e| format!("Failed to subscribe to SFU events: {e}"))?;

        // Clone what we need for the task handle
        let session_manager = self.clone();
        let session_id_for_task = session_id.to_string();
        let participant_id_for_task = participant_id.to_string();

        // Spawn the event forwarding task
        let task_handle = tokio::spawn(async move {
            while let Ok(Some(event)) = event_stream.message().await {
                if let Some(
                    sfu_event::Event::TransportClosed(_)
                    | sfu_event::Event::ProducerClosed(_)
                    | sfu_event::Event::ConsumerClosed(_)
                ) = &event.event
                {
                    // Only terminate if not already in progress
                    if !session_manager.is_terminating(&session_id_for_task).await {
                        info!("Media failure detected for session {}, terminating", session_id_for_task);
                        session_manager
                            .terminate_session(&session_id_for_task, SessionEndReason::Error)
                            .await;
                    }
                    return;
                }
                
                // Forward other events to participant
                if let Err(e) = session_manager
                    .forward_event_to_participant(&session_id_for_task, &participant_id_for_task, event)
                    .await
                {
                    error!("Failed to forward event to participant {} in session {}: {}", 
                        participant_id_for_task,
                        session_id_for_task,
                        e
                    );
                }
            }
        });

        event_task_handles.insert(task_key, task_handle);
        drop(event_task_handles); // releases mutex lock

        Ok(())
    }
    
    async fn forward_event_to_participant(
        &self,
        session_id: &str,
        participant_id: &str,
        event: SfuEvent,
    ) -> Result<(), String> {
        let participant = {
            let sessions = self.sessions.read().await;
            sessions
                .get(session_id)
                .and_then(|state| state.participants.get(participant_id))
                .cloned()
        }
        .ok_or_else(|| format!("Participant {participant_id} not found in session {session_id}"))?;

        // Encode the event
        let mut bytes = Vec::new();
        event.encode(&mut bytes)
            .map_err(|e| format!("Failed to encode event: {e}"))?;

        participant.sender.send(bytes).await
            .map_err(|e| format!("Failed to send to participant {participant_id}: {e}"))?;

        Ok(())
    }

    pub async fn broadcast_to_system_subscribers(&self, event: SfuEvent) -> Result<(), String> {
        let system_subscribers_map = {
            let system_subscribers = self.system_subscribers.read().await;
            
            if system_subscribers.is_empty() {
                return Ok(()); // No system_subscribers connected
            }
            
            // Clone the HashMap to release the lock early
            system_subscribers.clone()
        };

        let mut bytes = Vec::new();
        event.encode(&mut bytes)
            .map_err(|e| format!("Failed to encode event: {e}"))?;

        let mut errors = Vec::new();
        for (system_subscriber_id, sender) in &system_subscribers_map { 
            if let Err(e) = sender.send(bytes.clone()).await {
                errors.push(format!("Failed to send to System Subscriber {system_subscriber_id}: {e}"));
            }
        }

        if !errors.is_empty() {
            return Err(errors.join("; "));
        }

        Ok(())
    }
    
    pub async fn broadcast_to_session_participants(
        &self,
        session_id: &str,
        event: SfuEvent,
    ) -> Result<(), String> {
        let participants = {
            let sessions = self.sessions.read().await;
            sessions
                .get(session_id)
                .map(|state| state.participants.clone())
                .unwrap_or_default()
        };

        if participants.is_empty() {
            return Ok(());
        }

        let mut bytes = Vec::new();
        event.encode(&mut bytes)
            .map_err(|e| format!("Failed to encode event: {e}"))?;

        let mut errors = Vec::new();
        for (participant_id, participant) in &participants {
            if let Err(e) = participant.sender.send(bytes.clone()).await {
                errors.push(format!("Failed to send to participant {participant_id}: {e}"));
            }
        }

        if !errors.is_empty() {
            return Err(errors.join("; "));
        }

        Ok(())
    }

    pub async fn is_participant_in_session(
        &self,
        session_id: &str,
        participant_id: &str,
    ) -> bool {
        let sessions = self.sessions.read().await;
        sessions
            .get(session_id)
            .is_some_and(|state| state.participants.contains_key(participant_id))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn session_created_event(
        session_id: &str,
        requires_ai: bool,
        llm_provider: &str,
        llm_model_id: &str,
        tts_provider: &str,
        tts_model_id: &str,
        stt_provider: &str,
        stt_model_id: &str,
    ) -> SfuEvent {
        SfuEvent {
            event: Some(sfu_event::Event::SessionCreated(
                SessionCreatedEvent {
                    session_id: session_id.to_string(),
                    requires_ai,
                    llm_provider: llm_provider.to_string(),
                    llm_model_id: llm_model_id.to_string(),
                    tts_provider: tts_provider.to_string(),
                    tts_model_id: tts_model_id.to_string(),
                    stt_provider: stt_provider.to_string(),
                    stt_model_id: stt_model_id.to_string(),
                }
            )),
        }
    }

    pub fn session_ended_event(session_id: &str, reason: SessionEndReason) -> SfuEvent {
        SfuEvent {
            event: Some(sfu_event::Event::SessionEnded(
                SessionEndedEvent {
                    session_id: session_id.to_string(),
                    reason: reason.into(),
                }
            )),
        }
    }

    pub async fn register_session(
        &self,
        sender: mpsc::Sender<Vec<u8>>,
        session_id: String,
        participant_id: String,
    ) -> Result<(), String> {
        let participant = Participant::new(sender);

        self.sessions
            .write()
            .await
            .entry(session_id)
            .or_insert_with(SessionState::new)
            .participants
            .insert(participant_id, participant);

        Ok(())
    }

    pub async fn terminate_session(&self, session_id: &str, reason: SessionEndReason) {
        // Mark as terminating first (before any async calls)
        {
            let mut terminating = self.terminating_sessions.write().await;
            if !terminating.insert(session_id.to_string()) {
                // Already terminating, skip
                warn!("Session {session_id} already terminating, skipping");
                return;
            }
        }

        // Close SFU resources (continue on failure)
        if let Err(e) = self.sfu_client.close_session(session_id, "").await {
            error!("Failed to close session in SFU (continuing anyway): {}", e);
        }
    
        // Broadcast to all session participants
        let event = Self::session_ended_event(session_id, reason);
        if let Err(e) = self.broadcast_to_session_participants(session_id, event.clone()).await {
            error!("Failed to broadcast session ended to participants: {}", e);
        }
    
        // Broadcast to system subscribers (Orchestrator)
        // Note: Orchestrator may receive this twice (as participant + system subscriber) — this is intentional, handle idempotently
        if let Err(e) = self.broadcast_to_system_subscribers(event).await {
            error!("Failed to broadcast session ended to system subscribers: {}", e);
        }
    
        // Abort all event task handles for this session
        {
            let mut event_task_handles = self.event_task_handles.lock().await;
            let keys_to_remove: Vec<_> = event_task_handles
                .keys()
                .filter(|(sid, _)| sid == session_id)
                .cloned()
                .collect();

            for key in keys_to_remove {
                if let Some(task_handle) = event_task_handles.remove(&key) {
                    task_handle.abort();
                    info!("Aborted event subscription task handle for participant: {}", key.1);
                }
            }
        }
    
        // Remove session and all participants
        {
            let mut sessions = self.sessions.write().await;
            if sessions.remove(session_id).is_some() {
                info!("Removed session: {session_id} and all its participants");
            } else {
                warn!("Session already terminated: {session_id}");
            }
        }
    }

    pub async fn is_terminating(&self, session_id: &str) -> bool {
        self.terminating_sessions.read().await.contains(session_id)
    }
}

impl Clone for SessionManager {
    fn clone(&self) -> Self {
        Self {
            sessions: Arc::clone(&self.sessions),
            system_subscribers: Arc::clone(&self.system_subscribers),
            sfu_client: Arc::clone(&self.sfu_client),
            event_task_handles: Arc::clone(&self.event_task_handles),
            terminating_sessions: Arc::clone(&self.terminating_sessions),
        }
    }
}
