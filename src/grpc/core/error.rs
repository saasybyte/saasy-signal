#[derive(Debug, thiserror::Error)]
pub enum CoreClientError {
    #[error("Transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("gRPC error: {0}")]
    Status(#[from] tonic::Status),

    #[error("Invalid gRPC URI: {0}")]
    Uri(#[from] tonic::codegen::http::uri::InvalidUri),
}
