//! Error types for the Secrets client

use thiserror::Error;

/// Errors that can occur when using the Secrets client
#[derive(Debug, Error)]
pub enum SecretsError {
    /// HTTP request failed
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// Invalid secret data
    #[error("Invalid secret data: {0}")]
    InvalidSecretData(String),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Organization not found
    #[error("Organization not found: {id}")]
    OrganizationNotFound { id: String },

    /// Project not found
    #[error("Project not found: {id}")]
    ProjectNotFound { id: String },

    /// Secret not found
    #[error("Secret not found: {id}")]
    SecretNotFound { id: String },
}
