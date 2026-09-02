use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use saasy_proto_rust::shared::IceServer;
use sha1::Sha1;

type HmacSha1 = Hmac<Sha1>;

pub struct CoturnConfig {
    pub host: String,
    pub port: u16,
    pub shared_secret: String,
    pub credential_ttl: u64,
}

impl CoturnConfig {
    pub fn new(
        host: String,
        port: u16,
        shared_secret: String,
        credential_ttl: u64,
    ) -> Self {
        Self { host, port, shared_secret, credential_ttl }
    }

    pub fn generate_ice_servers(&self) -> Result<Vec<IceServer>, String> {
        let expiry = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| format!("System clock error: {e}"))?
            .as_secs()
            + self.credential_ttl;

        let random_id = uuid::Uuid::new_v4();
        let username = format!("{expiry}:{random_id}");

        let mut mac = HmacSha1::new_from_slice(self.shared_secret.as_bytes())
            .map_err(|e| format!("HMAC key error: {e}"))?;
        mac.update(username.as_bytes());
        let credential = STANDARD.encode(mac.finalize().into_bytes());

        let stun_url = format!("stun:{}:{}", self.host, self.port);
        let turn_udp_url = format!("turn:{}:{}?transport=udp", self.host, self.port);
        let turn_tcp_url = format!("turn:{}:{}?transport=tcp", self.host, self.port);

        Ok(vec![
            IceServer {
                urls: vec![stun_url],
                username_opt: None,
                credential_opt: None,
            },
            IceServer {
                urls: vec![turn_udp_url, turn_tcp_url],
                username_opt: Some(saasy_proto_rust::shared::v1::ice_server::UsernameOpt::Username(username)),
                credential_opt: Some(saasy_proto_rust::shared::v1::ice_server::CredentialOpt::Credential(credential)),
            },
        ])
    }
}
