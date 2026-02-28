//! # SDK Error Types
//!
//! This module provides comprehensive error handling for the Tensorlake Cloud SDK.
//! It includes a general `SdkError` type that encompasses all possible error scenarios
//! across the different clients, including authentication and authorization errors.

use reqwest_eventsource::CannotCloneRequestError;
use thiserror::Error;

use crate::{
    applications::error::ApplicationsError, images::error::ImagesError,
    secrets::error::SecretsError,
};

/// The main error type for the Tensorlake Cloud SDK.
///
/// This enum encompasses all possible errors that can occur when using the SDK,
/// including client-specific errors, authentication issues, and general HTTP errors.
#[derive(Debug, Error)]
pub enum SdkError {
    /// Errors specific to the Applications client
    #[error(transparent)]
    Applications(#[from] ApplicationsError),

    /// Authentication error (HTTP 401)
    #[error("Authentication failed: {0}")]
    Authentication(String),

    /// Authorization error (HTTP 403)
    #[error("Authorization failed: {0}")]
    Authorization(String),

    /// General HTTP errors
    #[error(transparent)]
    Http(#[from] reqwest::Error),

    /// Reqwest middleware errors
    #[error(transparent)]
    Middleware(#[from] reqwest_middleware::Error),

    /// Errors specific to the Images client
    #[error(transparent)]
    Images(#[from] ImagesError),

    /// Invalid header value during client initialization
    #[error("Invalid header value: {0}")]
    InvalidHeaderValue(String),

    /// Client configuration error
    #[error("Client error: {0}")]
    ClientError(String),

    /// General IO errors
    #[error(transparent)]
    Io(#[from] std::io::Error),

    /// JSON serialization/deserialization errors
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// JSON serialization/deserialization errors
    #[error(transparent)]
    JsonWithError(#[from] serde_path_to_error::Error<serde_json::Error>),

    /// Errors specific to the Secrets client
    #[error(transparent)]
    Secrets(#[from] SecretsError),

    /// Server returned an error status
    #[error("Server error: {status} - {message}")]
    ServerError {
        status: reqwest::StatusCode,
        message: String,
    },

    /// Client returned an error initializing the EventSource stream
    #[error(transparent)]
    EventSourceConnectionError(#[from] CannotCloneRequestError),

    /// EventSource client returned an unexpected error
    #[error(transparent)]
    EventSourceError(#[from] Box<reqwest_eventsource::Error>),
}
