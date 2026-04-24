//! HTTP client that interacts with the Tensorlake Cloud API.
use eventsource_stream::Eventsource;
use futures::{Stream, StreamExt};
use reqwest::{
    Method, Request, Response, StatusCode,
    header::{ACCEPT, HeaderMap, HeaderValue, InvalidHeaderValue},
};
use reqwest_middleware::{ClientBuilder as ReqwestClientBuilder, ClientWithMiddleware, Middleware};
use serde::de::DeserializeOwned;
use std::{
    ops::{Deref, DerefMut},
    pin::Pin,
    result::Result,
    sync::{Arc, Once},
};

use crate::error::SdkError;

/// Wraps any SDK operation result with the W3C `trace_id` so callers can
/// correlate the request with its server-side spans in Datadog APM or any
/// other OTEL-compatible backend.
///
/// All fields and methods of `T` are accessible via `Deref`/`DerefMut`, so
/// most existing code compiles unchanged. Use `into_inner()` when you need an
/// owned `T`.
///
/// # Example
///
/// ```rust,ignore
/// let result = sdk.sandboxes("ns", false).create(&request).await?;
/// println!("{}", result.sandbox_id); // existing field — accessible via Deref
/// println!("{}", result.trace_id);   // new: look this up in Datadog APM
/// ```
#[derive(Debug, Clone)]
pub struct Traced<T> {
    /// W3C trace ID for this request (32 lowercase hex chars).
    pub trace_id: String,
    value: T,
}

impl<T> Traced<T> {
    pub fn new(trace_id: String, value: T) -> Self {
        Self { trace_id, value }
    }

    pub fn into_inner(self) -> T {
        self.value
    }

    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Traced<U> {
        Traced {
            trace_id: self.trace_id,
            value: f(self.value),
        }
    }
}

impl<T> Deref for Traced<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}

impl<T> DerefMut for Traced<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.value
    }
}

/// HTTP client that interacts with the Tensorlake Cloud API.
#[derive(Clone)]
pub struct Client {
    /// Base URL of the API, used to construct the full URL for each request.
    base_url: String,
    /// Base client to construct more specialized clients, used to construct EventSource requests.
    base_client: reqwest::Client,
    /// Client with user provided middlewares. Used to perform regular HTTP requests.
    client: ClientWithMiddleware,
    /// Default headers configured on the underlying clients.
    default_headers: HeaderMap,
}

/// Builder for creating a [`Client`] with a fluent API.
///
/// The base URL is required, while bearer token, middlewares, and scope are optional.
pub struct ClientBuilder {
    base_url: String,
    bearer_token: Option<String>,
    middlewares: Vec<Arc<dyn Middleware + 'static>>,
    organization_id: Option<String>,
    project_id: Option<String>,
    user_agent: Option<String>,
}

impl ClientBuilder {
    /// Create a new [`ClientBuilder`] with the specified base URL.
    ///
    /// # Arguments
    ///
    /// * `base_url` - The base URL of the API
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.to_string(),
            bearer_token: None,
            middlewares: Vec::new(),
            organization_id: None,
            project_id: None,
            user_agent: None,
        }
    }

    /// Override the User-Agent header sent with every request.
    ///
    /// Defaults to `tensorlake-rust-sdk/{CARGO_PKG_VERSION}`. Callers that
    /// wrap this client (e.g. the Python SDK via PyO3) should set a value like
    /// `tensorlake-python-sdk/{version}` so server logs can distinguish traffic
    /// by SDK language.
    pub fn user_agent(mut self, ua: &str) -> Self {
        self.user_agent = Some(ua.to_string());
        self
    }

    /// Set the bearer token for authentication.
    pub fn bearer_token(mut self, token: &str) -> Self {
        self.bearer_token = Some(token.to_string());
        self
    }

    /// Add middleware to the client.
    pub fn middleware<M>(mut self, middleware: M) -> Self
    where
        M: Middleware + 'static,
    {
        self.middlewares.push(Arc::new(middleware));
        self
    }

    /// Add multiple middlewares to the client.
    pub fn middlewares(mut self, middlewares: Vec<Arc<dyn Middleware + 'static>>) -> Self {
        self.middlewares = middlewares;
        self
    }

    /// Set the organization and project scope.
    pub fn scope(mut self, organization_id: &str, project_id: &str) -> Self {
        self.organization_id = Some(organization_id.to_string());
        self.project_id = Some(project_id.to_string());
        self
    }

    /// Build the [`Client`].
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created or configured.
    pub fn build(self) -> Result<Client, SdkError> {
        let mut default_headers = HeaderMap::new();

        // Add bearer token if provided
        if let Some(token) = &self.bearer_token {
            default_headers = new_default_headers(token)?;
        }

        // Add scope headers if provided
        if let Some(org_id) = &self.organization_id {
            default_headers.insert("X-Forwarded-Organization-Id", str_to_header_value(org_id)?);
        }
        if let Some(project_id) = &self.project_id {
            default_headers.insert("X-Forwarded-Project-Id", str_to_header_value(project_id)?);
        }

        let ua = self
            .user_agent
            .as_deref()
            .unwrap_or(concat!("tensorlake-rust-sdk/", env!("CARGO_PKG_VERSION")));
        let base_client = new_base_client(&default_headers, ua)?;
        let mut builder = ReqwestClientBuilder::new(base_client.clone());

        for middleware in &self.middlewares {
            builder = builder.with_arc(middleware.clone());
        }

        let client = builder.build();

        Ok(Client {
            base_url: self.base_url,
            base_client,
            client,
            default_headers,
        })
    }
}

type EventSourceStream<T> = Pin<Box<dyn Stream<Item = Result<T, SdkError>> + Send>>;

impl Client {
    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    pub(crate) fn default_headers(&self) -> HeaderMap {
        self.default_headers.clone()
    }

    /// Create a new client that shares the same underlying HTTP connection pool but uses a
    /// different base URL. Cloning `reqwest::Client` shares its Arc-backed connection pool, so
    /// HTTP/2 connections established for one base URL can be reused (via connection coalescing)
    /// when talking to another host that resolves to the same IP with the same TLS certificate.
    pub fn with_base_url(&self, new_url: &str) -> Self {
        let client = ReqwestClientBuilder::new(self.base_client.clone()).build();
        Self {
            base_url: new_url.to_string(),
            base_client: self.base_client.clone(),
            client,
            default_headers: self.default_headers.clone(),
        }
    }

    /// Execute an HTTP request. Injects a W3C `traceparent` header for server-side
    /// correlation. Use [`execute_traced`] or [`execute_json`] when you need the
    /// `trace_id` returned to the caller.
    pub async fn execute(&self, mut request: Request) -> Result<Response, SdkError> {
        let (traceparent, _) = generate_traceparent();
        if let Ok(value) = traceparent.parse::<HeaderValue>() {
            request.headers_mut().insert("traceparent", value);
        }
        let response = self.client.execute(request).await?;
        self.handle_response(response).await
    }

    /// Execute an HTTP request without mapping non-success statuses to [`SdkError`].
    /// Injects a W3C `traceparent` header for server-side correlation.
    pub async fn execute_raw(&self, mut request: Request) -> Result<Response, SdkError> {
        let (traceparent, _) = generate_traceparent();
        if let Ok(value) = traceparent.parse::<HeaderValue>() {
            request.headers_mut().insert("traceparent", value);
        }
        let response = self.client.execute(request).await?;
        Ok(response)
    }

    /// Execute an HTTP request, inject a W3C `traceparent` header, and return
    /// the raw response alongside the `trace_id`. Use this when you need the
    /// `trace_id` but want to handle the response body yourself.
    pub async fn execute_traced(&self, mut request: Request) -> Result<Traced<Response>, SdkError> {
        let (traceparent, trace_id) = generate_traceparent();
        if let Ok(value) = traceparent.parse::<HeaderValue>() {
            request.headers_mut().insert("traceparent", value);
        }
        let response = self.client.execute(request).await?;
        let response = self.handle_response(response).await?;
        Ok(Traced::new(trace_id, response))
    }

    /// Execute an HTTP request, inject a W3C `traceparent` header, deserialize
    /// the JSON response body, and return both alongside the `trace_id`.
    ///
    /// This is the primary building block for service-level methods that return
    /// structured responses. The `trace_id` lets callers look up the full
    /// server-side cascade in Datadog APM.
    pub async fn execute_json<T: DeserializeOwned>(
        &self,
        request: Request,
    ) -> Result<Traced<T>, SdkError> {
        let traced = self.execute_traced(request).await?;
        let trace_id = traced.trace_id.clone();
        let bytes = traced.into_inner().bytes().await?;
        let jd = &mut serde_json::Deserializer::from_slice(bytes.as_ref());
        let value: T = serde_path_to_error::deserialize(jd)?;
        Ok(Traced::new(trace_id, value))
    }

    pub fn request(
        &self,
        method: reqwest::Method,
        path: &str,
    ) -> reqwest_middleware::RequestBuilder {
        self.client.request(method, self.base_url.clone() + path)
    }

    pub async fn build_event_source_request<T>(
        &self,
        path: &str,
    ) -> Result<Traced<EventSourceStream<T>>, SdkError>
    where
        T: DeserializeOwned,
    {
        let (traceparent, trace_id) = generate_traceparent();
        let response = self
            .base_client
            .get(self.base_url.clone() + path)
            .header(ACCEPT, "text/event-stream")
            .header("traceparent", traceparent)
            .send()
            .await?;

        let stream = response
            .bytes_stream()
            .eventsource()
            .filter_map(move |event| async move {
                match event {
                    Ok(msg) => match serde_json::from_str::<T>(&msg.data) {
                        Ok(evt) => Some(Ok(evt)),
                        Err(error) => Some(Err(SdkError::Json(error))),
                    },
                    Err(error) => Some(Err(SdkError::EventSourceError(error.to_string()))),
                }
            });
        Ok(Traced::new(trace_id, Box::pin(stream)))
    }

    pub fn build_multipart_request(
        &self,
        method: reqwest::Method,
        path: &str,
        form: reqwest::multipart::Form,
    ) -> Result<reqwest::Request, SdkError> {
        self.request(method, path)
            .multipart(form)
            .build()
            .map_err(Into::into)
    }

    /// Helper function to build POST, PUT or PATCH requests with JSON body
    pub fn build_post_json_request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: &impl serde::Serialize,
    ) -> Result<reqwest::Request, SdkError> {
        Ok(self.request(method, path).json(body).build()?)
    }

    /// Helper function to build GET requests that return JSON responses
    pub fn build_get_json_request(
        &self,
        path: &str,
        query: Option<&[(&str, &str)]>,
    ) -> Result<reqwest::Request, SdkError> {
        let mut req_builder = self.request(Method::GET, path);
        if let Some(query) = query {
            req_builder = req_builder.query(query);
        }
        Ok(req_builder.header(ACCEPT, "application/json").build()?)
    }

    /// Helper function to handle HTTP responses and convert status codes to appropriate errors
    async fn handle_response(
        &self,
        response: reqwest::Response,
    ) -> Result<reqwest::Response, SdkError> {
        let status = response.status();

        match status {
            StatusCode::UNAUTHORIZED => {
                let message = body_message_or_default(response, "Unauthorized").await;
                Err(SdkError::Authentication(message))
            }
            StatusCode::FORBIDDEN => {
                let message = body_message_or_default(response, "Forbidden").await;
                Err(SdkError::Authorization(message))
            }
            status if status.is_server_error() => {
                let message = body_message_or_default(response, "Server error").await;
                Err(SdkError::ServerError { status, message })
            }
            status if !status.is_success() => {
                let message = body_message_or_default(response, "Request failed").await;
                Err(SdkError::ServerError { status, message })
            }
            _ => Ok(response),
        }
    }
}

fn generate_traceparent() -> (String, String) {
    let trace_id = hex::encode(rand::random::<[u8; 16]>());
    let span_id = hex::encode(rand::random::<[u8; 8]>());
    (format!("00-{trace_id}-{span_id}-01"), trace_id)
}

async fn body_message_or_default(response: Response, default: &str) -> String {
    let message = response
        .text()
        .await
        .unwrap_or_else(|_| default.to_string());
    if message.is_empty() {
        default.to_string()
    } else {
        message
    }
}

fn new_default_headers(bearer_token: &str) -> Result<HeaderMap, SdkError> {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Authorization",
        str_to_header_value(&format!("Bearer {}", bearer_token))?,
    );
    Ok(headers)
}

fn str_to_header_value(value: &str) -> Result<HeaderValue, SdkError> {
    value
        .parse()
        .map_err(|e: InvalidHeaderValue| SdkError::InvalidHeaderValue(e.to_string()))
}

fn ensure_rustls_provider() {
    static INSTALL_PROVIDER: Once = Once::new();
    INSTALL_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn new_base_client(headers: &HeaderMap, user_agent: &str) -> Result<reqwest::Client, SdkError> {
    ensure_rustls_provider();

    let client = reqwest::Client::builder()
        .user_agent(user_agent)
        .default_headers(headers.clone())
        .build()?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::ensure_rustls_provider;

    #[test]
    fn installs_rustls_provider() {
        ensure_rustls_provider();
        assert!(
            rustls::crypto::CryptoProvider::get_default().is_some(),
            "rustls crypto provider should be installed"
        );
    }
}
