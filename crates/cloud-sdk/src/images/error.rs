//! Error types for the Images client

use thiserror::Error;

/// Errors that can occur when using the Images client
#[derive(Debug, Error)]
pub enum ImagesError {
    /// Build failed
    #[error("Build failed: {id} - {reason}")]
    BuildFailed { id: String, reason: String },

    /// Build not found
    #[error("Build not found: {id}")]
    BuildNotFound { id: String },

    /// Build timeout
    #[error("Build timed out after {attempts} attempts")]
    BuildTimeout { attempts: u32 },

    /// HTTP request failed
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// Invalid build request
    #[error("Invalid build request: {0}")]
    InvalidBuildRequest(String),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
