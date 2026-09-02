use jsonwebtoken::{decode, DecodingKey, Validation, Algorithm};
use serde::Deserialize;

use super::error::AuthError;

#[derive(Debug, Deserialize)]
pub struct Claims {
    pub invite_code_id: String,
    pub window_expires_at: i64,
    pub usage_remaining_seconds: i64,
    pub exp: i64,
}

/// Validates JWTs signed by saasy-core using ES256
pub struct JwtValidator {
    decoding_key: DecodingKey,
}

impl JwtValidator {
    pub fn from_pem(pem: &str) -> Result<Self, AuthError> {
        let decoding_key = DecodingKey::from_ec_pem(pem.as_bytes())
            .map_err(|e| AuthError::KeyLoadError(format!("Failed to parse EC public key: {e}")))?;

        Ok(Self { decoding_key })
    }

    pub fn validate_token(&self, token: &str) -> Result<Claims, AuthError> {
        let validation = Validation::new(Algorithm::ES256);

        let token_data = decode::<Claims>(token, &self.decoding_key, &validation)
            .map_err(|e| {
                match e.kind() {
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::ExpiredToken,
                    _ => AuthError::InvalidToken(e.to_string()),
                }
            })?;

        Ok(token_data.claims)
    }
}
