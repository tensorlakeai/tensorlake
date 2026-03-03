//! HTTP client that interacts with the Tensorlake Cloud API.
use eventsource_stream::Eventsource;
use futures::{Stream, StreamExt};
use reqwest::{
    Method, Request, Response, StatusCode,
    header::{ACCEPT, HeaderMap, HeaderValue, InvalidHeaderValue},
};
use reqwest_middleware::{ClientBuilder as ReqwestClientBuilder, ClientWithMiddleware, Middleware};
use serde::de::DeserializeOwned;
use std::{pin::Pin, result::Result, sync::Arc};

use crate::error::SdkError;

/// HTTP client that interacts with the Tensorlake Cloud API.
#[derive(Clone)]
pub struct Client {
    /// Base URL of the API, used to construct the full URL for each request.
    base_url: String,
    /// Base client to construct more specialized clients, used to construct EventSource requests.
    base_client: reqwest::Client,
    /// Client with user provided middlewares. Used to perform regular HTTP requests.
    client: ClientWithMiddleware,
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
        }
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

        let base_client = new_base_client(&default_headers)?;
        let mut builder = ReqwestClientBuilder::new(base_client.clone());

        for middleware in &self.middlewares {
            builder = builder.with_arc(middleware.clone());
        }

        let client = builder.build();

        Ok(Client {
            base_url: self.base_url,
            base_client,
            client,
        })
    }
}

type EventSourceStream<T> = Pin<Box<dyn Stream<Item = Result<T, SdkError>> + Send>>;

impl Client {
    /// Execute an HTTP request.
    pub async fn execute(&self, request: Request) -> Result<Response, SdkError> {
        let response = self.client.execute(request).await?;
        self.handle_response(response).await
    }

    /// Execute an HTTP request without mapping non-success statuses to [`SdkError`].
    pub async fn execute_raw(&self, request: Request) -> Result<Response, SdkError> {
        let response = self.client.execute(request).await?;
        Ok(response)
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
    ) -> Result<EventSourceStream<T>, SdkError>
    where
        T: DeserializeOwned,
    {
        let response = self
            .base_client
            .get(self.base_url.clone() + path)
            .header(ACCEPT, "text/event-stream")
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
        Ok(Box::pin(stream))
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

fn new_base_client(headers: &HeaderMap) -> Result<reqwest::Client, SdkError> {
    let client = reqwest::Client::builder()
        .user_agent(format!(
            "Tensorlake Cloud SDK/{}",
            env!("CARGO_PKG_VERSION")
        ))
        .default_headers(headers.clone())
        .build()?;
    Ok(client)
}
