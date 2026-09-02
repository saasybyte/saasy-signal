#[derive(Debug, thiserror::Error)]
pub enum SfuClientError {
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    
    #[error("gRPC error: {0}")]
    Status(#[from] tonic::Status),

    #[error("Invalid gRPC URI: {0}")]
    Uri(#[from] tonic::codegen::http::uri::InvalidUri),

    #[error("SFU service error")]
    SfuError(String),

    #[error("Unexpected SFU service error")]
    UnexpectedResponse(String),

    // #[error("Missing field in response: {0}")]
    // MissingField(String),
    
    // #[error("Session error: {0}")]
    // Session(String),
    
    // #[error("Not connected: {0}")]
    // NotConnected(String),
}
