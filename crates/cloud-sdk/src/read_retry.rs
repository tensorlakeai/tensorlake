//! Bounded retries for finite, explicitly read-only metadata requests.
//!
//! Each direct attempt chooses one address from system DNS and a pool scoped to
//! that origin/address. A timed-out HTTP/2 stream therefore cannot pin every
//! retry to the same multiplexed connection. Mutations and streams never enter
//! this path. System proxy settings remain in force; a forwarding proxy owns
//! origin DNS/routing, so address selection cannot control its upstream leg.

use super::*;
use rand::Rng;
use std::{collections::HashSet, net::SocketAddr, sync::Mutex};
use tokio::time::Instant;

const MAX_POOLS: usize = 32;
const POOL_IDLE: Duration = Duration::from_secs(60);
const RETRY_CAPACITY: f64 = 20.0;

/// Retry limits for sandbox metadata reads. All attempts, DNS lookups, body
/// reads and backoff share `total_timeout`, further capped by request timeout.
/// Long-running operations and response streams do not use this policy.
#[derive(Clone, Debug)]
pub struct ReadRetryConfig {
    pub max_attempts: usize,
    pub attempt_timeout: Duration,
    pub total_timeout: Duration,
}

impl Default for ReadRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            attempt_timeout: Duration::from_secs(3),
            total_timeout: Duration::from_secs(10),
        }
    }
}

impl ReadRetryConfig {
    pub(super) fn validate(&self) -> Result<(), SdkError> {
        if !(1..=10).contains(&self.max_attempts)
            || self.attempt_timeout.is_zero()
            || self.total_timeout.is_zero()
        {
            return Err(SdkError::ClientError(
                "read retry limits require 1..=10 attempts and nonzero timeouts".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Endpoint {
    origin: String,
    address: Option<SocketAddr>,
}

struct Pool {
    endpoint: Endpoint,
    client: ClientWithMiddleware,
    used_at: Instant,
}

struct State {
    pools: Vec<Pool>,
    tokens: f64,
    refilled_at: Instant,
    suspect: Vec<(Endpoint, Instant)>,
}

pub(super) struct ReadTransport {
    config: ReadRetryConfig,
    headers: HeaderMap,
    user_agent: String,
    middlewares: Vec<Arc<dyn Middleware>>,
    follow_redirects: bool,
    system_proxy: hyper_util::client::proxy::matcher::Matcher,
    state: Mutex<State>,
    resolver: tokio::sync::OnceCell<Option<hickory_resolver::TokioResolver>>,
    #[cfg(test)]
    test_options: Mutex<TestOptions>,
}

#[cfg(test)]
#[derive(Default)]
struct TestOptions {
    addresses: Option<Vec<SocketAddr>>,
    certificate: Option<reqwest::tls::Certificate>,
}

impl ReadTransport {
    pub(super) fn new(
        config: ReadRetryConfig,
        headers: HeaderMap,
        user_agent: String,
        middlewares: Vec<Arc<dyn Middleware>>,
        follow_redirects: bool,
    ) -> Self {
        Self {
            config,
            headers,
            user_agent,
            middlewares,
            follow_redirects,
            system_proxy: hyper_util::client::proxy::matcher::Matcher::from_system(),
            resolver: tokio::sync::OnceCell::new(),
            state: Mutex::new(State {
                pools: Vec::new(),
                tokens: RETRY_CAPACITY,
                refilled_at: Instant::now(),
                suspect: Vec::new(),
            }),
            #[cfg(test)]
            test_options: Mutex::new(TestOptions::default()),
        }
    }

    // Hickory caches records only for their DNS TTL and uses the machine's
    // configured nameservers and hosts file. If unavailable, retain the system
    // resolver path (including platform-specific name services).
    async fn addresses(&self, url: &url::Url) -> Vec<SocketAddr> {
        #[cfg(test)]
        if let Some(addresses) = self.test_options.lock().unwrap().addresses.clone() {
            return addresses;
        }
        if let Ok(uri) = url.as_str().parse()
            && self.system_proxy.intercept(&uri).is_some()
        {
            // Origin DNS belongs to a forwarding proxy. Do not require local
            // resolution, or bypass a proxy to reach a selected address.
            return Vec::new();
        }
        let Some(host) = url.host_str() else {
            return Vec::new();
        };
        let Some(port) = url.port_or_known_default() else {
            return Vec::new();
        };
        match url.host() {
            Some(url::Host::Ipv4(ip)) => return vec![SocketAddr::new(ip.into(), port)],
            Some(url::Host::Ipv6(ip)) => return vec![SocketAddr::new(ip.into(), port)],
            _ => {}
        }
        let resolver = self
            .resolver
            .get_or_init(|| async {
                hickory_resolver::TokioResolver::builder_tokio()
                    .ok()
                    .map(|mut builder| {
                        builder.options_mut().cache_size = 64;
                        builder.options_mut().use_hosts_file =
                            hickory_resolver::config::ResolveHosts::Always;
                        builder.options_mut().ip_strategy =
                            hickory_resolver::config::LookupIpStrategy::Ipv4AndIpv6;
                        builder.build()
                    })
            })
            .await;
        let mut addresses = Vec::new();
        if let Some(resolver) = resolver
            && let Ok(result) = resolver.lookup_ip(host).await
        {
            addresses.extend(result.iter().map(|ip| SocketAddr::new(ip, port)));
        }
        if addresses.is_empty()
            && let Ok(result) = tokio::net::lookup_host((host, port)).await
        {
            addresses.extend(result);
        }
        // A system HTTP proxy can resolve a name the local host cannot.
        // An empty result leaves origin resolution to the normal proxy path.
        addresses.sort_unstable();
        addresses.dedup();
        if !addresses.is_empty() {
            let start = rand::rng().random_range(0..addresses.len());
            addresses.rotate_left(start);
        }
        addresses
    }

    fn select(
        &self,
        origin: &str,
        addresses: &[SocketAddr],
        tried: &HashSet<SocketAddr>,
    ) -> Option<SocketAddr> {
        let mut state = self.state.lock().unwrap();
        state.suspect.retain(|(_, until)| *until > Instant::now());
        let healthy = |address: &&SocketAddr| {
            !state
                .suspect
                .iter()
                .any(|(e, _)| e.origin == origin && e.address == Some(**address))
        };
        // Prefer an untried healthy peer, but never block every address. A
        // briefly suspect endpoint remains usable when no alternative exists.
        addresses
            .iter()
            .filter(|a| !tried.contains(*a))
            .find(healthy)
            .or_else(|| addresses.iter().find(|a| !tried.contains(*a)))
            .or_else(|| addresses.iter().find(healthy))
            .or(addresses.first())
            .copied()
    }

    fn pool(&self, endpoint: &Endpoint, host: &str) -> Result<ClientWithMiddleware, SdkError> {
        let mut state = self.state.lock().unwrap();
        state
            .pools
            .retain(|pool| pool.used_at.elapsed() < POOL_IDLE);
        if let Some(pool) = state.pools.iter_mut().find(|p| p.endpoint == *endpoint) {
            pool.used_at = Instant::now();
            return Ok(pool.client.clone());
        }
        let mut builder = crate::http_transport::https_builder()
            .default_headers(self.headers.clone())
            .user_agent(&self.user_agent)
            .redirect(if self.follow_redirects {
                reqwest::redirect::Policy::default()
            } else {
                reqwest::redirect::Policy::none()
            })
            .retry(reqwest::retry::never())
            .connect_timeout(self.config.attempt_timeout.min(Duration::from_secs(2)));
        if let Some(address) = endpoint.address {
            builder = builder.resolve(host, address);
        }
        #[cfg(test)]
        {
            let options = self.test_options.lock().unwrap();
            if options.addresses.is_some() {
                builder = builder.no_proxy();
            }
            if let Some(cert) = &options.certificate {
                builder = builder.tls_certs_merge([cert.clone()]);
            }
        }
        let mut builder = ReqwestClientBuilder::new(builder.build()?);
        for middleware in &self.middlewares {
            builder = builder.with_arc(middleware.clone());
        }
        let client = builder.build();
        if state.pools.len() >= MAX_POOLS {
            let oldest = state
                .pools
                .iter()
                .enumerate()
                .min_by_key(|(_, p)| p.used_at)
                .unwrap()
                .0;
            state.pools.swap_remove(oldest);
        }
        state.pools.push(Pool {
            endpoint: endpoint.clone(),
            client: client.clone(),
            used_at: Instant::now(),
        });
        Ok(client)
    }

    fn retire(&self, endpoint: &Endpoint) {
        // Dropping the cache's handle does not cancel other in-flight streams.
        // Their handles live until their own requests/deadlines finish.
        let mut state = self.state.lock().unwrap();
        state.pools.retain(|p| p.endpoint != *endpoint);
        state
            .suspect
            .retain(|(e, until)| e != endpoint && *until > Instant::now());
        if state.suspect.len() >= MAX_POOLS {
            state.suspect.remove(0);
        }
        state
            .suspect
            .push((endpoint.clone(), Instant::now() + Duration::from_secs(10)));
    }

    fn take_retry_token(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        // A small burst, replenished by successes and at one retry per second
        // during an outage. Cloned clients share this budget.
        state.tokens =
            (state.tokens + state.refilled_at.elapsed().as_secs_f64()).min(RETRY_CAPACITY);
        state.refilled_at = Instant::now();
        if state.tokens < 1.0 {
            return false;
        }
        state.tokens -= 1.0;
        true
    }

    fn success(&self) {
        let mut state = self.state.lock().unwrap();
        state.tokens = (state.tokens + 1.0).min(RETRY_CAPACITY);
    }
}

fn jitter(attempt: usize) -> Duration {
    let cap_ms = 100_u64 * (1_u64 << attempt.min(4));
    Duration::from_millis(rand::rng().random_range(0..=cap_ms))
}

fn retryable_status(status: StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 502 | 503 | 504)
}

fn retryable(error: &SdkError) -> bool {
    if let SdkError::ServerError { status, .. } = error {
        return retryable_status(*status);
    }
    if matches!(error, SdkError::StreamTimeout { .. }) {
        return true;
    }
    let Some(error) = error.as_reqwest() else {
        return false;
    };
    // Never reinterpret invalid certificates or other TLS policy failures as
    // endpoint impairment. Inspection uses typed causes, not error strings.
    let mut cause = std::error::Error::source(error);
    while let Some(current) = cause {
        if current.downcast_ref::<rustls::Error>().is_some() {
            return false;
        }
        // io::Error::source() can skip its inner error. hyper-rustls wraps
        // tokio-rustls' io::Error again, so walk get_ref() at every layer.
        cause = current
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .map(|inner| inner as &(dyn std::error::Error + 'static))
            .or_else(|| current.source());
    }

    !error.is_builder()
        && (error.is_timeout()
            || error.is_connect()
            || error.is_request()
            || error.is_body()
            || error.is_decode())
}

fn exhausted(error: SdkError) -> SdkError {
    SdkError::ReadRetryExhausted(Box::new(error))
}

impl Client {
    /// Execute a finite, side-effect-free metadata GET with bounded retries.
    /// The body is fully received before anything is exposed to the caller.
    /// Use `execute_json` for mutations, long polls and large/streaming reads.
    pub async fn execute_read_json<T: DeserializeOwned>(
        &self,
        request: Request,
    ) -> Result<Traced<T>, SdkError> {
        self.execute_metadata_read(request, self.reads.config.max_attempts)
            .await
    }

    /// Readiness polling owns its own deadline and retry loop. Give each poll
    /// address selection and pool retirement, without nesting another loop or
    /// hiding the final observed state behind a retry-boundary error.
    pub(crate) async fn execute_read_json_once<T: DeserializeOwned>(
        &self,
        request: Request,
    ) -> Result<Traced<T>, SdkError> {
        self.execute_metadata_read(request, 1)
            .await
            .map_err(SdkError::into_original)
    }

    async fn execute_metadata_read<T: DeserializeOwned>(
        &self,
        request: Request,
        max_attempts: usize,
    ) -> Result<Traced<T>, SdkError> {
        if request.method() != Method::GET || request.body().is_some() {
            return Err(SdkError::ClientError(
                "resilient reads require a bodyless GET".into(),
            ));
        }
        let policy = &self.reads.config;
        let total = request
            .timeout()
            .or(self.timeout.as_ref())
            .copied()
            .map_or(policy.total_timeout, |timeout| {
                timeout.min(policy.total_timeout)
            });
        let started = Instant::now();
        let (_, read_id) = generate_traceparent();
        let deadline = started + total;
        let mut tried = HashSet::new();
        let mut last_error = None;
        for attempt in 1..=max_attempts {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let timeout = policy.attempt_timeout.min(deadline - now);
            let mut endpoint = None;
            let mut retry_after = None;
            let mut response_status = None;
            let mut stage = "dns";
            let mut trace_id = String::new();
            let result = tokio::time::timeout(timeout, async {
                let addresses = self.reads.addresses(request.url()).await;
                let origin = request.url().origin().ascii_serialization();
                let address = self.reads.select(&origin, &addresses, &tried);
                if let Some(address) = address {
                    tried.insert(address);
                }
                let selected = Endpoint { origin, address };
                let transport = self
                    .reads
                    .pool(&selected, request.url().host_str().unwrap_or(""))?;
                endpoint = Some(selected);
                let mut current = request.try_clone().expect("bodyless GET is replayable");
                trace_id = self.prepare_traced_request(&mut current);
                *current.timeout_mut() = Some(timeout);
                current.headers_mut().insert(
                    REQUEST_TIMEOUT_HEADER,
                    HeaderValue::from(timeout.as_millis() as u64),
                );
                stage = "response_headers";
                let response = transport.execute(current).await?;
                response_status = Some(response.status());
                retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| {
                        value
                            .parse::<u64>()
                            .ok()
                            .map(Duration::from_secs)
                            .or_else(|| {
                                httpdate::parse_http_date(value).ok().map(|date| {
                                    date.duration_since(std::time::SystemTime::now())
                                        .unwrap_or_default()
                                })
                            })
                    });
                stage = "response_body";
                let response = self.handle_response(response).await?;
                Ok::<_, SdkError>(response.bytes().await?)
            })
            .await
            .unwrap_or_else(|_| {
                Err(SdkError::StreamTimeout {
                    waiting_for: "a sandbox metadata read",
                    timeout,
                })
            });
            match result {
                Ok(bytes) => {
                    let jd = &mut serde_json::Deserializer::from_slice(&bytes);
                    let value = serde_path_to_error::deserialize(jd)?;
                    self.reads.success();
                    if attempt > 1 {
                        tracing::debug!(attempt, %read_id, %trace_id, selected_address = ?endpoint.as_ref().and_then(|e| e.address), elapsed_ms = started.elapsed().as_millis() as u64, "sandbox metadata read recovered");
                    }
                    return Ok(Traced::new(trace_id, value));
                }
                Err(error) => {
                    // Once a definitive status arrived, a stalled/truncated
                    // error body must not turn a 401/403/404 into a replay.
                    if retryable(&error)
                        && let Some(status) =
                            response_status.filter(|s| !s.is_success() && !retryable_status(*s))
                    {
                        let message = "response body did not complete".to_string();
                        return Err(match status {
                            StatusCode::UNAUTHORIZED => SdkError::Authentication(message),
                            StatusCode::FORBIDDEN => SdkError::Authorization(message),
                            _ => SdkError::ServerError { status, message },
                        });
                    }
                    if !retryable(&error) {
                        return Err(if error.never_reached_server() {
                            exhausted(error)
                        } else {
                            error
                        });
                    }
                    let transport_failure = !matches!(error, SdkError::ServerError { .. });
                    if transport_failure && let Some(endpoint) = &endpoint {
                        self.reads.retire(endpoint);
                    }
                    let failure = match error.transport_failure() {
                        Some(crate::error::TransportFailure::Timeout) => "timeout",
                        Some(crate::error::TransportFailure::Connect) => "connect",
                        None if response_status.is_some_and(|s| !s.is_success()) => "http_status",
                        None => "connection_or_body",
                    };
                    tracing::debug!(attempt, %read_id, %trace_id, stage, failure, status = ?response_status.map(|s| s.as_u16()), selected_address = ?endpoint.as_ref().and_then(|e| e.address), elapsed_ms = started.elapsed().as_millis() as u64, "sandbox metadata read attempt failed");
                    last_error = Some(error);
                    if attempt == max_attempts || !self.reads.take_retry_token() {
                        break;
                    }
                    let wait = retry_after.unwrap_or_else(|| jitter(attempt));
                    if wait >= deadline.saturating_duration_since(Instant::now()) {
                        break;
                    }
                    tokio::time::sleep(wait).await;
                }
            }
        }
        Err(exhausted(last_error.unwrap_or(SdkError::StreamTimeout {
            waiting_for: "a sandbox metadata read",
            timeout: total,
        })))
    }
}

#[cfg(test)]
#[path = "read_retry_tests.rs"]
mod tests;
