mod auth;
mod background;
mod config;
mod grpc;
mod http;
mod signal;
mod turn;
mod websocket;

use std::io;
use std::sync::Arc;

use actix_web::web;
use tokio::signal::ctrl_c;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::auth::JwtValidator;
use crate::background::{HealthBackgroundService, UsageTrackingCommand, UsageTrackerBackgroundService};
use crate::config::ServerConfig;
use crate::grpc::{CoreClient, SfuClient};
use crate::signal::SessionManager;
use crate::turn::CoturnConfig;
use crate::websocket::{upgrade_to_session_ws, upgrade_to_system_ws};

/// Entry point for the Saasy Signaling server
///
/// Initializes configuration, sets up logging,
/// connects to the SFU service via gRPC,
/// and starts the Actix Web HTTP/WebSocket server for WebRTC signaling
#[actix_web::main]
async fn main() -> io::Result<()> {
    // Load server configuration from .env, default.toml, and environment
    let config = ServerConfig::from_env()
        .map_err(|e| io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Failed to load config: {e}")
        ))?;

    // Initialize structured logging with optional RUST_LOG override
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Initialize JWT validator for auth token verification
    let jwt_validator = Arc::new(
        JwtValidator::from_pem(&config.jwt_public_key)
            .map_err(|e| io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Failed to load JWT public key: {e}")
            ))?
    );
    info!("Initialized JWT validator");

    // Initialize optional Coturn config for TURN credential generation
    let coturn_fields = (
        &config.coturn_host,
        &config.coturn_port,
        &config.coturn_shared_secret,
        &config.coturn_credential_ttl,
    );
    let coturn_config: Option<Arc<CoturnConfig>> = match coturn_fields {
        (Some(host), Some(port), Some(secret), Some(ttl)) => {
            let c = CoturnConfig::new(host.clone(), *port, secret.clone(), *ttl);
            info!("Coturn configured: {}:{}", c.host, c.port);
            Some(Arc::new(c))
        }
        (None, None, None, None) => {
            info!("Coturn not configured — ICE servers will not be included in session responses");
            None
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Partial coturn configuration: all of COTURN_HOST, COTURN_PORT, COTURN_SHARED_SECRET, and COTURN_CREDENTIAL_TTL must be set together, or none at all",
            ));
        }
    };

    // Initialize gRPC SFU client to communicate with SFU service
    let sfu_grpc_url = &config.sfu_grpc_url;
    let sfu_client = Arc::new(
        SfuClient::connect(sfu_grpc_url)
            .await
            .map_err(|e| io::Error::other(format!("Failed to connect to SFU gRPC service: {e}")))?
    );
    info!("Connected to SFU at {sfu_grpc_url}");

    // Initialize session manager
    let session_manager = Arc::new(SessionManager::new(sfu_client.clone()));

    // Initialize gRPC Core client and UsageTracker for session time tracking
    let core_grpc_url = &config.core_grpc_url;
    let core_client = Arc::new(
        CoreClient::connect(core_grpc_url)
            .await
            .map_err(|e| io::Error::other(format!("Failed to connect to Core gRPC service: {e}")))?
    );
    info!("Connected to Core at {core_grpc_url}");

    let usage_tracker = UsageTrackerBackgroundService::new(core_client, session_manager.clone());
    let usage_tracking_command_tx: mpsc::Sender<UsageTrackingCommand> = usage_tracker.usage_tracking_command_tx();

    // Spawn a shutdown signal handler

    // TODO: we need better shutdown
    tokio::spawn(async move {
        if let Err(e) = ctrl_c().await {
            error!("Failed to listen for shutdown signal: {e}");
            return;
        }

        info!("Shutdown signal received. Cleaning up...");
    });

    // Spawn health background service and start Actix HTTP/WebSocket server for signaling
    let bind_address = config.socket_addr()
        .map_err(|e| io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Invalid bind address: {e}")
        ))?;
    info!("Starting HTTP and Websocket servers on {}", bind_address);
    let actix_workers = config.effective_actix_workers();
    info!("Configured to use {actix_workers} actix workers");

    let (health_service, health_status) = HealthBackgroundService::new(
        config.sfu_http_url.clone(),
        std::time::Duration::from_secs(5),
    );
    tokio::spawn(health_service.run());

    actix_web::HttpServer::new(move || {
        actix_web::App::new()
            .app_data(web::Data::new(sfu_client.clone()))
            .app_data(web::Data::new(session_manager.clone()))
            .app_data(web::Data::new(jwt_validator.clone()))
            .app_data(web::Data::new(health_status.clone()))
            .app_data(web::Data::new(usage_tracking_command_tx.clone()))
            .app_data(web::Data::new(coturn_config.clone()))
            .service(http::health::liveness)
            .service(http::health::readiness)
            .route("/ws/session", web::get().to(upgrade_to_session_ws))
            .route("/ws/system", web::get().to(upgrade_to_system_ws))
    })
        .workers(actix_workers)
        .bind(bind_address)?
        .run()
        .await
}
