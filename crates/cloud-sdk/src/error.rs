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

/// Error codes the Sandbox Proxy returns when it could not complete a request
/// *before* forwarding it to the sandbox: the Indexify lookup did not answer
/// (`SANDBOX_UPSTREAM_ERROR`), the sandbox has no route yet
/// (`SANDBOX_NOT_READY`), or the dataplane could not be reached
/// (`SANDBOX_UNREACHABLE`). In every case the request never reached the
/// sandbox, so replaying it cannot duplicate a side effect — and every case is
/// what an Indexify server rollout looks like from the client. The proxy's
/// error body is `{"error": "...", "code": "..."}`.
pub const SANDBOX_PROXY_UNDELIVERED_CODES: &[&str] = &[
    "SANDBOX_UPSTREAM_ERROR",
    "SANDBOX_NOT_READY",
    "SANDBOX_UNREACHABLE",
];

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

    /// The `code` from a JSON error body of the form
    /// `{"error": "...", "code": "..."}`, when the server sent one.
    ///
    /// [`SdkError::ServerError`] keeps the raw body as its message, so the code
    /// is recovered by parsing it here rather than carried as a field.
    pub fn upstream_error_code(&self) -> Option<String> {
        let Self::ServerError { message, .. } = self else {
            return None;
        };
        #[derive(serde::Deserialize)]
        struct Body {
            code: Option<String>,
        }
        serde_json::from_str::<Body>(message)
            .ok()
            .and_then(|body| body.code)
    }

    /// Whether the request provably never reached the server that would have
    /// executed it, making a replay safe even for a non-idempotent operation.
    ///
    /// True for a connect failure (see [`SdkError::transport_failure`]) and for
    /// a 502/503 from the Sandbox Proxy carrying one of
    /// [`SANDBOX_PROXY_UNDELIVERED_CODES`]. A timeout, or a 5xx without such a
    /// code, gives no such guarantee.
    pub fn never_reached_server(&self) -> bool {
        if matches!(self.transport_failure(), Some(TransportFailure::Connect)) {
            return true;
        }
        let Self::ServerError { status, .. } = self else {
            return false;
        };
        if *status != reqwest::StatusCode::SERVICE_UNAVAILABLE
            && *status != reqwest::StatusCode::BAD_GATEWAY
        {
            return false;
        }
        self.upstream_error_code()
            .is_some_and(|code| SANDBOX_PROXY_UNDELIVERED_CODES.contains(&code.as_str()))
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
        let client = reqwest_middleware::ClientBuilder::new(
            crate::http_transport::https_builder().build().unwrap(),
        )
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
            crate::http_transport::https_builder()
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

#[cfg(test)]
mod undelivered_tests {
    use super::SdkError;

    fn server_error(status: u16, body: &str) -> SdkError {
        SdkError::ServerError {
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            message: body.to_string(),
        }
    }

    #[test]
    fn upstream_error_code_reads_the_proxy_body() {
        let error = server_error(
            503,
            r#"{"error":"Sandbox routing is temporarily unavailable","code":"SANDBOX_UPSTREAM_ERROR"}"#,
        );
        assert_eq!(
            error.upstream_error_code().as_deref(),
            Some("SANDBOX_UPSTREAM_ERROR")
        );
        assert_eq!(server_error(503, "plain text").upstream_error_code(), None);
        assert_eq!(
            server_error(503, r#"{"error":"no code"}"#).upstream_error_code(),
            None
        );
    }

    #[test]
    fn only_pre_forward_proxy_failures_count_as_undelivered() {
        let undelivered = [
            (503, "SANDBOX_UPSTREAM_ERROR"),
            (503, "SANDBOX_NOT_READY"),
            (502, "SANDBOX_UNREACHABLE"),
        ];
        for (status, code) in undelivered {
            let body = format!(r#"{{"error":"x","code":"{code}"}}"#);
            assert!(
                server_error(status, &body).never_reached_server(),
                "{status} {code} must be replayable"
            );
        }

        // Same codes on a status the proxy does not use for them: not trusted.
        assert!(
            !server_error(500, r#"{"error":"x","code":"SANDBOX_UPSTREAM_ERROR"}"#)
                .never_reached_server()
        );
        // A 503 from something else — the sandbox daemon, an ingress — may have
        // executed the request.
        assert!(!server_error(503, r#"{"error":"daemon overloaded"}"#).never_reached_server());
        assert!(
            !server_error(503, r#"{"error":"x","code":"SANDBOX_NOT_FOUND"}"#)
                .never_reached_server()
        );
        assert!(
            !server_error(404, r#"{"error":"x","code":"SANDBOX_NOT_FOUND"}"#)
                .never_reached_server()
        );
    }
}
