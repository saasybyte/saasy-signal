use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use saasy_proto_rust::sfu::{
    sfu_event, FarewellRequestedEvent, SessionEndReason, SfuEvent, UsageStatusEvent,
};
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::{error, info, warn};

use crate::grpc::CoreClient;
use crate::signal::SessionManager;

const USAGE_TICK_INTERVAL: Duration = Duration::from_secs(10);
const REPORT_INTERVAL_SECS: i32 = 60;
const REPORT_INTERVAL: Duration = Duration::from_secs(REPORT_INTERVAL_SECS as u64);
const TWO_MIN_WARNING_THRESHOLD_SECS: i32 = 120;
const START_COUNTDOWN_THRESHOLD_SECS: i32 = 30;
const WAIT_AFTER_FAREWELL: Duration = Duration::from_secs(7); // TODO: test for optimal duration

struct UsageTrackingState {
    invite_code_id: String,
    last_reported_at: Instant,
    warned_two_min: bool,
    warned_countdown: bool,
}

pub enum UsageTrackingCommand {
    Start { session_id: String, invite_code_id: String },
    Stop { session_id: String },
}

pub struct UsageTrackerBackgroundService {
    tracked_sessions: Arc<RwLock<HashMap<String, UsageTrackingState>>>,
    core_client: Arc<CoreClient>,
    session_manager: Arc<SessionManager>,
    usage_tracking_command_tx: mpsc::Sender<UsageTrackingCommand>,
}

impl UsageTrackerBackgroundService {
    pub fn new(core_client: Arc<CoreClient>, session_manager: Arc<SessionManager>) -> Self {
        let (usage_tracking_command_tx, usage_tracking_command_rx) = mpsc::channel(100);

        let usage_tracker = Self {
            tracked_sessions: Arc::new(RwLock::new(HashMap::new())),
            core_client,
            session_manager,
            usage_tracking_command_tx,
        };

        // Spawn the main loop
        usage_tracker.spawn_loop(usage_tracking_command_rx);

        usage_tracker
    }

    pub fn usage_tracking_command_tx(&self) -> mpsc::Sender<UsageTrackingCommand> {
        self.usage_tracking_command_tx.clone()
    }

    fn spawn_loop(&self, mut usage_tracking_command_rx: mpsc::Receiver<UsageTrackingCommand>) {
        let tracked_sessions = Arc::clone(&self.tracked_sessions);
        let core_client = Arc::clone(&self.core_client);
        let session_manager = Arc::clone(&self.session_manager);

        tokio::spawn(async move {
            let mut usage_tick_interval = interval(USAGE_TICK_INTERVAL);

            loop {
                tokio::select! {
                    // Handle incoming channel commands
                    Some(command) = usage_tracking_command_rx.recv() => {
                        match command {
                            UsageTrackingCommand::Start { session_id, invite_code_id } => {
                                tracked_sessions.write().await.insert(session_id.clone(), UsageTrackingState {
                                    invite_code_id,
                                    last_reported_at: Instant::now(),
                                    warned_two_min: false,
                                    warned_countdown: false,
                                });
                                info!("Started usage tracking for session {session_id}");
                            }
                            UsageTrackingCommand::Stop { session_id } => {
                                let mut tracked_sessions_guard = tracked_sessions.write().await;
                                if tracked_sessions_guard.remove(&session_id).is_some() {
                                    info!("Stopped usage tracking for session {}", session_id);
                                }
                            }
                        }
                    }

                    // Periodic tick to check and report usage
                    _ = usage_tick_interval.tick() => {
                        Self::process_usage(&tracked_sessions, &core_client, &session_manager).await;
                    }
                }
            }
        });
    }

    async fn process_usage(
        tracked_sessions: &Arc<RwLock<HashMap<String, UsageTrackingState>>>,
        core_client: &Arc<CoreClient>,
        session_manager: &Arc<SessionManager>,
    ) {
        // Collect tracked_sessions that need reporting
        let tracked_sessions_to_report: Vec<(String, String)> = {
            let tracked_sessions_guard = tracked_sessions.read().await;
            tracked_sessions_guard
                .iter()
                .filter(|(_, state)| state.last_reported_at.elapsed() >= REPORT_INTERVAL)
                .map(|(session_id, state)| (session_id.clone(), state.invite_code_id.clone()))
                .collect()
        };

        for (session_id, invite_code_id) in tracked_sessions_to_report {
            // Call Core to record usage
            let response = match core_client
                .record_usage(&invite_code_id, REPORT_INTERVAL_SECS)
                .await
            {
                Ok(response) => response,
                Err(e) => {
                    error!(
                        "Failed to record usage for session {session_id} (continuing): {}", e
                    );
                    // Update last_reported_at so we don't spam retries
                    {
                        let mut tracked_sessions_guard = tracked_sessions.write().await;
                        if let Some(state) = tracked_sessions_guard.get_mut(&session_id) {
                            state.last_reported_at = Instant::now();
                        }
                    }
                    continue;
                }
            };

            let usage_remaining_seconds = response.usage_remaining_seconds;
            let budget_exhausted = response.budget_exhausted;

            // Update tracking state and determine what events to send
            let (send_two_min_warning, send_start_countdown, should_terminate) = {
                let mut tracked_sessions = tracked_sessions.write().await;
                if let Some(state) = tracked_sessions.get_mut(&session_id) {
                    state.last_reported_at = Instant::now();

                    let send_two_min_warning = usage_remaining_seconds <= TWO_MIN_WARNING_THRESHOLD_SECS && !state.warned_two_min;
                    let send_start_countdown = usage_remaining_seconds <= START_COUNTDOWN_THRESHOLD_SECS && !state.warned_countdown;

                    if send_two_min_warning {
                        state.warned_two_min = true;
                    }
                    if send_start_countdown {
                        state.warned_countdown = true;
                    }

                    (send_two_min_warning, send_start_countdown, budget_exhausted || usage_remaining_seconds <= 0)
                } else {
                    continue;
                }
            };

            // Send warning event to client
            if send_two_min_warning || send_start_countdown {
                let event = SfuEvent {
                    event: Some(sfu_event::Event::UsageStatus(UsageStatusEvent {
                        usage_remaining_seconds,
                        budget_exhausted,
                    })),
                };

                if let Err(e) = session_manager
                    .broadcast_to_session_participants(&session_id, event)
                    .await
                {
                    warn!("Failed to send usage status to session {}: {}", session_id, e);
                }
            }

            // Handle budget exhaustion
            if should_terminate {
                info!(
                    "Budget exhausted for session {}, initiating farewell",
                    session_id
                );

                // Send farewell request to orchestrator (system subscribers)
                let farewell_event = SfuEvent {
                    event: Some(sfu_event::Event::FarewellRequested(FarewellRequestedEvent {
                        session_id: session_id.clone(),
                    })),
                };

                if let Err(e) = session_manager
                    .broadcast_to_system_subscribers(farewell_event)
                    .await
                {
                    warn!(
                        "Failed to send farewell request for session {}: {}",
                        session_id, e
                    );
                }

                // Wait for farewell to complete (best effort)
                tokio::time::sleep(WAIT_AFTER_FAREWELL).await;

                // Remove from tracking before terminating
                {
                    let mut tracked_sessions = tracked_sessions.write().await;
                    tracked_sessions.remove(&session_id);
                }

                // Terminate the session
                session_manager
                    .terminate_session(&session_id, SessionEndReason::Timeout)
                    .await;

                info!("Session {} terminated due to budget exhaustion", session_id);
            }
        }
    }
}
