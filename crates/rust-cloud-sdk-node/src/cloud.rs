//! Cloud HTTP operations for the TypeScript SDK. The JS layer marshals API
//! arguments; transport, tracing, retries, deadlines and cancellation live here.

use std::{collections::HashMap, io, pin::Pin, sync::Arc, time::Duration};

use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use eventsource_stream::Eventsource;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use napi::{
    Env,
    bindgen_prelude::{Buffer, Either, Promise},
};
use napi_derive::napi;
use reqwest::{
    Method,
    multipart::{Form, Part},
};
use serde::Deserialize;
use tensorlake::retry::{
    RetryDecision, RetryPolicy, RetryState, UNDELIVERED_REPLAY_BUDGET, is_transient,
};
use tensorlake::{Client, ClientBuilder, Traced, error::SdkError};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::sandbox::{DeferredHttpClient, NativeStreamCall, into_napi_error, usage_error};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CloudOptions {
    base_url: String,
    api_key: Option<String>,
    organization_id: Option<String>,
    project_id: Option<String>,
    user_agent: String,
    max_retries: u32,
    retry_backoff_ms: f64,
    timeout_ms: f64,
}

#[napi(object)]
pub struct NativeCloudPart {
    pub name: String,
    pub data: Buffer,
    pub filename: Option<String>,
    pub content_type: Option<String>,
}

#[napi(object)]
pub struct NativeCloudRequest {
    pub method: String,
    pub path: String,
    pub headers_json: String,
    pub body: Option<Buffer>,
    pub parts: Option<Vec<NativeCloudPart>>,
    pub status_only_codes: Vec<u32>,
}

// Release references to JavaScript-owned buffers before borrowing the request
// across await points. Retries and multipart assembly use owned Rust bytes.
struct CloudPart {
    name: String,
    data: Vec<u8>,
    filename: Option<String>,
    content_type: Option<String>,
}

struct CloudRequest {
    method: String,
    path: String,
    headers_json: String,
    body: Option<Vec<u8>>,
    parts: Option<Vec<CloudPart>>,
    status_only_codes: Vec<u32>,
}

impl CloudRequest {
    fn is_safe_read(&self) -> bool {
        matches!(self.method.as_str(), "GET" | "HEAD" | "OPTIONS")
    }
}

impl From<NativeCloudRequest> for CloudRequest {
    fn from(request: NativeCloudRequest) -> Self {
        Self {
            method: request.method,
            path: request.path,
            headers_json: request.headers_json,
            body: request.body.map(|body| body.to_vec()),
            parts: request.parts.map(|parts| {
                parts
                    .into_iter()
                    .map(|part| CloudPart {
                        name: part.name,
                        data: part.data.to_vec(),
                        filename: part.filename,
                        content_type: part.content_type,
                    })
                    .collect()
            }),
            status_only_codes: request.status_only_codes,
        }
    }
}

#[napi(object, object_from_js = false)]
pub struct NativeCloudResponse {
    pub status: u32,
    pub headers_json: String,
    pub data: Buffer,
    pub trace_id: String,
}

#[napi]
#[derive(Clone)]
pub struct NativeCloudClient {
    client: DeferredHttpClient,
    mutation_client: DeferredHttpClient,
    options: Arc<CloudOptions>,
    closed: tokio::sync::watch::Sender<bool>,
}

fn connection_error(message: &str) -> napi::Error {
    napi::Error::from_reason(
        serde_json::json!({
            "category": "connection", "status": null, "message": message,
        })
        .to_string(),
    )
}

fn is_transient_cloud_error(error: &SdkError) -> bool {
    // Only cloud GET/HEAD/OPTIONS requests opt into these additional retries.
    // Other bindings use the shared policy for operations such as stdin writes,
    // so broadening its defaults would replay possibly delivered mutations.
    is_transient(error)
        || matches!(error, SdkError::Io(_))
        || matches!(error, SdkError::ServerError { status, .. } if *status == reqwest::StatusCode::TOO_MANY_REQUESTS)
        || error.as_reqwest().is_some_and(|error| {
            !error.is_timeout() && (error.is_request() || error.is_body() || error.is_decode())
        })
}

fn into_cloud_error(error: SdkError) -> napi::Error {
    match error {
        SdkError::ServerError { status, message } => {
            let message = serde_json::from_str::<serde_json::Value>(&message)
                .ok()
                .and_then(|body| {
                    body.get("message")
                        .or_else(|| body.get("error"))
                        .and_then(|value| value.as_str())
                        .map(str::to_owned)
                })
                .unwrap_or(message);
            into_napi_error(SdkError::ServerError { status, message })
        }
        error @ (SdkError::Http(_)
        | SdkError::Middleware(_)
        | SdkError::EventSourceError(_)
        | SdkError::Io(_)) => connection_error(&error.detail()),
        error => into_napi_error(error),
    }
}

fn is_gzip(response: &reqwest::Response) -> bool {
    response
        .headers()
        .get(reqwest::header::CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("gzip"))
}

fn decode_error(error: io::Error) -> SdkError {
    let kind = error.kind();
    match error.into_inner() {
        // StreamReader wraps transport errors in io::Error. Preserve their
        // classification so a request timeout never becomes a decoding retry.
        Some(inner) => match inner.downcast::<reqwest::Error>() {
            Ok(error) => SdkError::Http(*error),
            Err(inner) => SdkError::Io(io::Error::new(kind, inner)),
        },
        None => SdkError::Io(io::Error::from(kind)),
    }
}

async fn response_stream(
    response: reqwest::Response,
) -> BoxStream<'static, Result<Bytes, SdkError>> {
    let gzip = is_gzip(&response);
    let bytes = response.bytes_stream();
    if !gzip {
        return bytes.map_err(SdkError::Http).boxed();
    }
    let mut bytes = bytes
        .try_filter(|bytes| std::future::ready(!bytes.is_empty()))
        .boxed()
        .peekable();
    // HEAD, 204 and empty responses can retain Content-Encoding. An absent
    // body is valid and must not be fed into a decoder expecting a header.
    if !matches!(Pin::new(&mut bytes).peek().await, Some(Ok(_))) {
        return bytes.map_err(SdkError::Http).boxed();
    }
    let reader = StreamReader::new(bytes.map_err(io::Error::other));
    let mut decoded = GzipDecoder::new(reader);
    // Read through HTTP EOF, including concatenated gzip members. A complete
    // member alone must not hide a truncated or stalled HTTP response.
    decoded.multiple_members(true);
    ReaderStream::new(decoded).map_err(decode_error).boxed()
}

async fn response_bytes(response: reqwest::Response) -> Result<Vec<u8>, SdkError> {
    response_stream(response)
        .await
        .try_fold(Vec::new(), |mut body, chunk| async move {
            body.extend_from_slice(&chunk);
            Ok(body)
        })
        .await
}

#[napi]
impl NativeCloudClient {
    #[napi(constructor)]
    pub fn new(options_json: String) -> napi::Result<Self> {
        let options: CloudOptions =
            serde_json::from_str(&options_json).map_err(|error| usage_error(error.to_string()))?;
        if !options.timeout_ms.is_finite()
            || options.timeout_ms <= 0.0
            || !options.retry_backoff_ms.is_finite()
            || options.retry_backoff_ms < 0.0
        {
            return Err(usage_error(
                "invalid cloud request timeout or retry backoff".into(),
            ));
        }
        let timeout = Duration::try_from_secs_f64(options.timeout_ms / 1000.0)
            .map_err(|error| usage_error(error.to_string()))?;
        let mut builder = ClientBuilder::new(options.base_url.trim_end_matches('/'))
            .user_agent(&options.user_agent)
            .timeout(timeout);
        if let Some(key) = options.api_key.as_ref().filter(|key| !key.is_empty()) {
            builder = builder.bearer_token(key);
        }
        Ok(Self {
            // Following a mutation's redirect can hide that the original
            // request executed. A connect failure on a later hop must never
            // cause the original mutation to be replayed.
            mutation_client: DeferredHttpClient::new(builder.clone().follow_redirects(false)),
            client: DeferredHttpClient::new(builder),
            options: Arc::new(options),
            closed: tokio::sync::watch::channel(false).0,
        })
    }

    #[napi]
    pub fn close(&self) {
        self.closed.send_replace(true);
    }

    #[napi]
    pub async fn request(&self, request: NativeCloudRequest) -> napi::Result<NativeCloudResponse> {
        let request = CloudRequest::from(request);
        let mut closed = self.closed.subscribe();
        let timeout = Duration::from_secs_f64(self.options.timeout_ms / 1000.0);
        tokio::select! {
            biased;
            _ = closed.wait_for(|value| *value) => Err(connection_error("Tensorlake native client is closed")),
            result = tokio::time::timeout(timeout, self.buffered_request(request)) => {
                result.unwrap_or_else(|_| Err(connection_error("cloud request deadline exceeded")))
            }
        }
    }

    #[napi]
    pub fn stream(
        &self,
        env: Env,
        request: NativeCloudRequest,
        emit: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    ) -> napi::Result<NativeStreamCall> {
        let request = CloudRequest::from(request);
        let client = self.clone();
        let control = tokio::sync::watch::channel(false).0;
        let cancel_signal = control.clone();
        let cancel = env.create_function_from_closure("cancel", move |_| {
            cancel_signal.send_replace(true);
            Ok(())
        })?;
        let result = env.spawn_future(async move {
            let mut cancelled = control.subscribe();
            let mut closed = client.closed.subscribe();
            tokio::select! {
                biased;
                _ = cancelled.wait_for(|value| *value) => Ok(String::new()),
                _ = closed.wait_for(|value| *value) => Err(connection_error("Tensorlake native client is closed")),
                result = client.stream_events(request, emit) => result,
            }
        })?;
        Ok(NativeStreamCall { result, cancel })
    }
}

impl NativeCloudClient {
    async fn http_client(&self, spec: &CloudRequest) -> napi::Result<Client> {
        if spec.is_safe_read() {
            self.client.get().await
        } else {
            self.mutation_client.get().await
        }
    }

    fn build_request(
        &self,
        client: &Client,
        spec: &CloudRequest,
    ) -> Result<reqwest::Request, SdkError> {
        if !spec.path.starts_with('/') || spec.path.starts_with("//") {
            return Err(SdkError::ClientError(
                "cloud request path must be relative to the configured API origin".into(),
            ));
        }
        let method = Method::from_bytes(spec.method.as_bytes())
            .map_err(|error| SdkError::ClientError(error.to_string()))?;
        let mut request = client.request(method, &spec.path);
        // Decoding is local to this binding. Enabling reqwest's compression
        // features would also change Function Agent's raw blob transfers.
        request = request.header(reqwest::header::ACCEPT_ENCODING, "gzip");
        if let Some(org) = self
            .options
            .organization_id
            .as_ref()
            .filter(|org| !org.is_empty())
        {
            request = request.header("X-Forwarded-Organization-Id", org);
        }
        if let Some(project) = self
            .options
            .project_id
            .as_ref()
            .filter(|project| !project.is_empty())
        {
            request = request.header("X-Forwarded-Project-Id", project);
        }
        let headers: HashMap<String, String> = serde_json::from_str(&spec.headers_json)?;
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if let Some(parts) = &spec.parts {
            let mut form = Form::new();
            for part in parts {
                let mut value = Part::bytes(part.data.to_vec());
                if let Some(filename) = &part.filename {
                    value = value.file_name(filename.clone());
                }
                if let Some(content_type) = &part.content_type {
                    value = value.mime_str(content_type)?;
                }
                form = form.part(part.name.clone(), value);
            }
            request = request.multipart(form);
        } else if let Some(body) = &spec.body {
            request = request.body(body.to_vec());
        } else if matches!(spec.method.as_str(), "POST" | "PUT" | "PATCH") {
            // Match Fetch's empty-body framing and the shared Rust POST helpers.
            request = request.header(reqwest::header::CONTENT_LENGTH, "0");
        }
        Ok(request.build()?)
    }

    fn retry_state(&self, spec: &CloudRequest) -> RetryState {
        // Mutations may have executed before a connection drops. The shared
        // Rust policy only replays those when delivery provably never occurred.
        RetryState::new(RetryPolicy {
            idempotent: spec.is_safe_read(),
            max_transient_retries: self.options.max_retries as usize,
        })
    }

    async fn retry_delay(
        &self,
        state: &mut RetryState,
        error: &SdkError,
        started: tokio::time::Instant,
    ) -> bool {
        match state.decide_with_transient(error, started.elapsed(), is_transient_cloud_error(error))
        {
            RetryDecision::Stop => false,
            RetryDecision::Retry(wait) => {
                let wait = if error.never_reached_server() {
                    wait
                } else {
                    // Preserve the TS client's configurable transient backoff;
                    // undelivered requests retain the Rust rollout budget.
                    let millis = self.options.retry_backoff_ms
                        * 2_f64.powi((state.transient_retries() - 1).min(31) as i32);
                    Duration::from_secs_f64((millis / 1000.0).min(15.0))
                };
                tokio::time::sleep(wait).await;
                !error.never_reached_server() || started.elapsed() < UNDELIVERED_REPLAY_BUDGET
            }
        }
    }

    async fn send(
        &self,
        client: &Client,
        spec: &CloudRequest,
    ) -> Result<Traced<reqwest::Response>, SdkError> {
        let response = client
            .execute_raw_traced(self.build_request(client, spec)?)
            .await?;
        let status = response.status();
        if status.is_redirection() && !spec.is_safe_read() {
            return Err(SdkError::ServerError {
                status,
                message:
                    "redirects are disabled for cloud mutations to prevent duplicate execution"
                        .into(),
            });
        }
        if status.is_success() || spec.status_only_codes.contains(&u32::from(status.as_u16())) {
            return Ok(response);
        }
        // Preserve structured gateway codes until the shared retry policy has
        // decided whether the operation was delivered. Format errors afterward.
        let message = response_bytes(response.into_inner())
            .await
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_else(|error| {
                // The status is already definitive. A truncated error body must
                // not turn (for example) a 401 into a retryable connection error.
                format!(
                    "could not read HTTP {status} error body: {}",
                    error.detail()
                )
            });
        Err(SdkError::ServerError { status, message })
    }

    async fn buffered_request(&self, spec: CloudRequest) -> napi::Result<NativeCloudResponse> {
        let client = self.http_client(&spec).await?;
        let started = tokio::time::Instant::now();
        let mut retries = self.retry_state(&spec);
        loop {
            let result: Result<NativeCloudResponse, SdkError> = async {
                let traced = self.send(&client, &spec).await?;
                let trace_id = traced.trace_id.clone();
                let response = traced.into_inner();
                let status = u32::from(response.status().as_u16());
                let mut headers = response.headers().clone();
                if is_gzip(&response) {
                    headers.remove(reqwest::header::CONTENT_ENCODING);
                    headers.remove(reqwest::header::CONTENT_LENGTH);
                }
                let headers: HashMap<_, _> = headers
                    .iter()
                    .filter_map(|(name, value)| {
                        value
                            .to_str()
                            .ok()
                            .map(|value| (name.to_string(), value.to_string()))
                    })
                    .collect();
                let headers_json = serde_json::to_string(&headers)?;
                // Lookup 404s and upsert 409s drive caller control flow using
                // status alone. Drop their responses immediately: an unused
                // diagnostic body must not stall, fail, or retry that outcome.
                let data = if spec.status_only_codes.contains(&status) {
                    Vec::new().into()
                } else {
                    response_bytes(response).await?.into()
                };
                Ok(NativeCloudResponse {
                    status,
                    headers_json,
                    data,
                    trace_id,
                })
            }
            .await;
            match result {
                Ok(value) => return Ok(value),
                Err(error) => {
                    if !self.retry_delay(&mut retries, &error, started).await {
                        return Err(into_cloud_error(error));
                    }
                }
            }
        }
    }

    async fn stream_events(
        &self,
        spec: CloudRequest,
        emit: ThreadsafeFunction<String, ErrorStrategy::Fatal>,
    ) -> napi::Result<String> {
        let timeout = Duration::from_secs_f64(self.options.timeout_ms / 1000.0);
        // Bound establishment and retries, but allow progress/log streams to
        // remain open until completion or explicit cancellation.
        let response = tokio::time::timeout(timeout, async {
            let client = self
                .http_client(&spec)
                .await?
                .with_base_url_without_timeout(self.options.base_url.trim_end_matches('/'))
                .map_err(into_napi_error)?;
            let started = tokio::time::Instant::now();
            let mut retries = self.retry_state(&spec);
            loop {
                match self.send(&client, &spec).await {
                    Ok(response) => return Ok(response),
                    Err(error) => {
                        if !self.retry_delay(&mut retries, &error, started).await {
                            return Err(into_cloud_error(error));
                        }
                    }
                }
            }
        })
        .await
        .map_err(|_| connection_error("cloud stream connection deadline exceeded"))??;
        let trace_id = response.trace_id.clone();
        let mut events = response_stream(response.into_inner()).await.eventsource();
        while let Some(event) = events.next().await {
            let event = event.map_err(|error| connection_error(&error.to_string()))?;
            // Preserve the existing CloudClient convention of skipping invalid JSON.
            if serde_json::from_str::<serde_json::Value>(&event.data).is_err() {
                continue;
            }
            let ack: Either<(), Promise<()>> = emit.call_async(event.data).await?;
            if let Either::B(promise) = ack {
                promise.await?;
            }
        }
        Ok(trace_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    #[allow(clippy::disallowed_methods)] // Exercise the plain client used by Function Agent core.
    async fn cloud_dependencies_preserve_raw_reqwest_defaults_for_function_agent_blobs() {
        // Function Agent core constructs plain reqwest clients and checks blob
        // sizes/digests over stored bytes. Cloud decoding must not enable
        // automatic decoding on those clients through Cargo feature unification.
        let _ = tensorlake::http_transport::https_builder(); // Install the TLS provider.
        let stored = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x02\xff\x2b\x2e\xc9\x2f\x4a\x4d\x51\x48\x2b\xcd\x4b\x2e\xc9\xcc\xcf\x53\xc8\xcc\x2b\x28\x2d\x01\x00\x80\x29\xbe\x8a\x15\x00\x00\x00";
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await.unwrap());
            }
            socket.write_all(format!(
                "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\n\r\n",
                stored.len(),
            ).as_bytes()).await.unwrap();
            socket.write_all(stored).await.unwrap();
        });
        let response = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap()
            .get(url)
            .send()
            .await
            .unwrap();
        assert_eq!(response.bytes().await.unwrap().as_ref(), stored);
        server.await.unwrap();
    }
}
