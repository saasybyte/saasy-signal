#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("Invalid token: {0}")]
    InvalidToken(String),

    #[error("Token has expired")]
    ExpiredToken,

    #[error("Failed to load key: {0}")]
    KeyLoadError(String),
}
