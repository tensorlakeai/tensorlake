//! Error types for the Applications client

use thiserror::Error;

/// Errors that can occur when using the Applications client
#[derive(Debug, Error)]
pub enum ApplicationsError {
    /// Application not found
    #[error("Application not found: {name}")]
    ApplicationNotFound { name: String },

    /// Function call not found
    #[error("Function call not found: {id}")]
    FunctionCallNotFound { id: String },

    /// HTTP request failed
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// Invalid request data
    #[error("Invalid request data: {0}")]
    InvalidRequest(String),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Request not found
    #[error("Request not found: {id}")]
    RequestNotFound { id: String },
}
