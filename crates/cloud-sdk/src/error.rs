//! # SDK Error Types
//!
//! This module provides comprehensive error handling for the Tensorlake Cloud SDK.
//! It includes a general `SdkError` type that encompasses all possible error scenarios
//! across the different clients, including authentication and authorization errors.

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

    /// EventSource (SSE) stream error
    #[error("EventSource error: {0}")]
    EventSourceError(String),
}

/// Why a request failed below the HTTP status layer.
///
/// The distinction is load-bearing for retries: a [`TransportFailure::Connect`]
/// error means the request was never written to the wire, so replaying it
/// cannot duplicate a side effect, even for a non-idempotent operation. A
/// [`TransportFailure::Timeout`] carries no such guarantee — the request may
/// have been received and executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportFailure {
    /// DNS resolution, TCP connect, or the TLS handshake failed. The server
    /// never saw the request.
    Connect,
    /// The request was sent but no response completed within the deadline.
    Timeout,
}

impl SdkError {
    /// The underlying [`reqwest::Error`], whether it surfaced directly or
    /// wrapped by `reqwest_middleware`.
    ///
    /// Every client in this crate issues requests through
    /// [`reqwest_middleware::ClientWithMiddleware`], so transport failures
    /// arrive as [`SdkError::Middleware`] and never as [`SdkError::Http`].
    /// Callers that need `reqwest`'s own classification must go through here
    /// rather than matching on [`SdkError::Http`] alone.
    pub fn as_reqwest(&self) -> Option<&reqwest::Error> {
        match self {
            Self::Http(error) => Some(error),
            Self::Middleware(reqwest_middleware::Error::Reqwest(error)) => Some(error),
            _ => None,
        }
    }

    /// Classify a transport-level failure, or `None` when the request reached
    /// the server and failed for some other reason.
    ///
    /// Do not attempt this by inspecting the message: `reqwest` renders every
    /// connect failure as `error sending request for url (...)`, and the words
    /// that identify it (`tcp connect error`, `dns error`) appear only in the
    /// [`std::error::Error::source`] chain.
    pub fn transport_failure(&self) -> Option<TransportFailure> {
        let error = self.as_reqwest()?;
        if error.is_timeout() {
            Some(TransportFailure::Timeout)
        } else if error.is_connect() {
            Some(TransportFailure::Connect)
        } else {
            None
        }
    }

    /// The error's `Display` output followed by every `source()` in its chain.
    ///
    /// `reqwest` keeps the useful part of a transport failure in the chain, so
    /// reporting only `to_string()` yields `error sending request for url
    /// (...)` with no indication of whether DNS, TCP, or TLS failed.
    pub fn detail(&self) -> String {
        format_error_chain(self)
    }
}

/// Join an error's `Display` with every `source()` below it.
pub fn format_error_chain(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_message = cause.to_string();
        if !cause_message.is_empty() && !message.ends_with(&cause_message) {
            message.push_str(": ");
            message.push_str(&cause_message);
        }
        source = cause.source();
    }
    message
}

#[cfg(test)]
mod transport_failure_tests {
    use super::{SdkError, TransportFailure};

    /// Build the error a refused TCP connect produces, routed through
    /// `reqwest_middleware` exactly as every client in this crate does.
    async fn middleware_connect_error() -> SdkError {
        crate::client::ensure_rustls_provider();
        let client =
            reqwest_middleware::ClientBuilder::new(reqwest::Client::builder().build().unwrap())
                .build();
        // Port 1 on loopback refuses immediately; no network access needed.
        SdkError::from(client.get("http://127.0.0.1:1/x").send().await.unwrap_err())
    }

    #[tokio::test]
    async fn middleware_connect_failure_is_classified_as_connect() {
        let error = middleware_connect_error().await;

        assert_eq!(
            error.transport_failure(),
            Some(TransportFailure::Connect),
            "a connect failure through middleware must classify as Connect, not fall through \
             to the untyped arm"
        );
    }

    #[tokio::test]
    async fn middleware_connect_failure_message_alone_names_no_cause() {
        let error = middleware_connect_error().await;

        // This is the regression that made the failure undiagnosable in the
        // field: the rendered message never contains "connect" or "timeout",
        // so any substring-based classifier reports it as an internal error.
        let rendered = error.to_string().to_lowercase();
        assert!(
            !rendered.contains("connect") && !rendered.contains("timeout"),
            "reqwest's Display is expected to hide the cause; got {rendered:?}"
        );
    }

    /// A timeout must not be mistaken for a connect failure: only a connect
    /// failure proves the request never ran, and that is what gates replaying
    /// non-idempotent operations.
    #[tokio::test]
    async fn middleware_timeout_is_classified_as_timeout() {
        crate::client::ensure_rustls_provider();
        // A listener that accepts the connection and then never answers.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("listener");
        let address = listener.local_addr().expect("address");
        std::thread::spawn(move || {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept() {
                held.push(stream);
            }
        });

        let client = reqwest_middleware::ClientBuilder::new(
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_millis(200))
                .build()
                .unwrap(),
        )
        .build();
        let error = SdkError::from(
            client
                .get(format!("http://{address}/x"))
                .send()
                .await
                .unwrap_err(),
        );

        assert_eq!(error.transport_failure(), Some(TransportFailure::Timeout));
    }

    #[tokio::test]
    async fn detail_surfaces_the_underlying_cause() {
        let error = middleware_connect_error().await;

        let detail = error.detail().to_lowercase();
        assert!(
            detail.contains("tcp connect error"),
            "detail() must expose the source chain; got {detail:?}"
        );
    }
}
