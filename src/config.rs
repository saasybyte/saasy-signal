use std::net::{AddrParseError, SocketAddr};

use config::{Config, ConfigError, File, Environment};
use dotenvy::dotenv;
use serde::Deserialize;

/// Represents the server configuration for the Signal service
#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    /// The host address to bind the WebSocket server to
    pub host: String,

    /// The port number for the WebSocket server
    pub port: u16,

    /// Number of worker threads to spawn for Actix
    pub actix_workers: usize,

    pub sfu_http_url: String,

    pub sfu_grpc_url: String,

    pub core_grpc_url: String,

    /// ES256 public key PEM content for JWT validation
    pub jwt_public_key: String,

    /// Coturn host (optional — if unset, no ICE servers are returned)
    pub coturn_host: Option<String>,

    /// Coturn port
    pub coturn_port: Option<u16>,

    /// Shared secret for TURN credential generation (HMAC-SHA1)
    pub coturn_shared_secret: Option<String>,

    /// TURN credential TTL in seconds
    pub coturn_credential_ttl: Option<u64>,
}

impl ServerConfig {
    /// Initializes the config from `.env` and environment variables
    pub fn from_env() -> Result<Self, ConfigError> {
        dotenv().ok();

        Config::builder()
            .add_source(File::with_name("config/default").required(false))
            .add_source(Environment::default())
            .build()?
            .try_deserialize()
    }

    /// Returns the socket address to bind to
    pub fn socket_addr(&self) -> Result<SocketAddr, AddrParseError> {
        format!("{}:{}", self.host, self.port).parse()
    }    

    /// Returns the number of Actix workers to spawn
    /// Falls back to the number of physical CPUs if unspecified (0)
    pub fn effective_actix_workers(&self) -> usize {
        let requested = if self.actix_workers == 0 {
            num_cpus::get_physical()
        } else {
            self.actix_workers
        };
        requested.clamp(1, num_cpus::get()) // Ensures 1 ≤ workers ≤ logical CPUs
    }
}
