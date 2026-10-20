//! Bounded retries for finite, explicitly read-only metadata requests.
//!
//! Direct connections use current DNS answers and pools scoped to each
//! origin/address, with address-family fallback before request delivery.
//! A timed-out HTTP/2 stream therefore cannot pin every
//! retry to the same multiplexed connection. Mutations and streams never enter
//! this path. System proxy settings remain in force; a forwarding proxy owns
//! origin DNS/routing, so address selection cannot control its upstream leg.

use super::*;
use rand::Rng;
use std::{collections::HashSet, net::IpAddr, net::SocketAddr, sync::Mutex};
use tokio::time::Instant;

const MAX_POOLS: usize = 32;
const POOL_IDLE: Duration = Duration::from_secs(60);
const RETRY_CAPACITY: f64 = 20.0;
const DNS_FAMILY_GRACE: Duration = Duration::from_millis(50);
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

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
    connect_timeout: Duration,
    client: Arc<ClientWithMiddleware>,
    used_at: Instant,
}

struct State {
    pools: Vec<Pool>,
    tokens: f64,
    refilled_at: Instant,
    suspect: Vec<(Endpoint, Instant)>,
}

#[derive(Debug)]
struct ReadResolvers {
    ipv4: hickory_resolver::TokioResolver,
    ipv6: hickory_resolver::TokioResolver,
}

impl ReadResolvers {
    fn new(
        mut builder: hickory_resolver::ResolverBuilder<
            hickory_resolver::name_server::TokioConnectionProvider,
        >,
    ) -> Self {
        use hickory_resolver::config::{LookupIpStrategy, ResolveHosts};
        builder.options_mut().cache_size = 64;
        builder.options_mut().use_hosts_file = ResolveHosts::Always;
        builder.options_mut().ip_strategy = LookupIpStrategy::Ipv4Only;
        let ipv4 = builder.build();
        let mut options = ipv4.options().clone();
        options.ip_strategy = LookupIpStrategy::Ipv6Only;
        let ipv6 = hickory_resolver::TokioResolver::builder_with_config(
            ipv4.config().clone(),
            hickory_resolver::name_server::TokioConnectionProvider::default(),
        )
        .with_options(options)
        .build();
        Self { ipv4, ipv6 }
    }

    async fn lookup(&self, host: &str, deadline: Instant) -> Vec<IpAddr> {
        use futures::future::{Either, select};
        let ipv4 = Box::pin(self.ipv4.lookup_ip(host));
        let ipv6 = Box::pin(self.ipv6.lookup_ip(host));
        let Ok(first_completed) = tokio::time::timeout_at(deadline, select(ipv4, ipv6)).await
        else {
            return Vec::new();
        };
        let (first, remaining) = match first_completed {
            Either::Left(pair) | Either::Right(pair) => pair,
        };
        let mut addresses = first
            .map(|result| result.iter().collect::<Vec<_>>())
            .unwrap_or_default();
        // A missing answer is not a reason to abandon the other query. Once
        // one family is usable, collect the other only if it arrives promptly.
        // Hickory's combined strategy instead waits through its DNS timeout.
        let other_deadline = if addresses.is_empty() {
            deadline
        } else {
            deadline.min(Instant::now() + DNS_FAMILY_GRACE)
        };
        if let Ok(Ok(other)) = tokio::time::timeout_at(other_deadline, remaining).await {
            addresses.extend(other.iter());
        }
        addresses
    }
}

pub(super) struct ReadTransport {
    config: ReadRetryConfig,
    headers: HeaderMap,
    user_agent: String,
    middlewares: Vec<Arc<dyn Middleware>>,
    follow_redirects: bool,
    system_proxy: hyper_util::client::proxy::matcher::Matcher,
    state: Mutex<State>,
    resolver: tokio::sync::OnceCell<Option<ReadResolvers>>,
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
    async fn addresses(&self, url: &url::Url, timeout: Duration) -> Vec<SocketAddr> {
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
                    .map(ReadResolvers::new)
            })
            .await;
        // Reserve time for the platform resolver and connection establishment
        // even when the configured DNS servers never answer either query.
        let dns_budget = (timeout / 2).min(Duration::from_secs(1));
        let dns_deadline = Instant::now() + dns_budget;
        let mut addresses = Vec::new();
        if let Some(resolver) = resolver {
            let result = resolver.lookup(host, Instant::now() + dns_budget / 2).await;
            addresses.extend(result.into_iter().map(|ip| SocketAddr::new(ip, port)));
        }
        if addresses.is_empty()
            && let Ok(Ok(result)) =
                tokio::time::timeout_at(dns_deadline, tokio::net::lookup_host((host, port))).await
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
        if addresses.is_empty() {
            return None;
        }
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

    async fn resolve_other_family(
        &self,
        url: &url::Url,
        failed: SocketAddr,
        remaining: Duration,
    ) -> Vec<SocketAddr> {
        let (Some(url::Host::Domain(host)), Some(port), Some(Some(resolvers))) =
            (url.host(), url.port_or_known_default(), self.resolver.get())
        else {
            return Vec::new();
        };
        let resolver = if failed.is_ipv4() {
            &resolvers.ipv6
        } else {
            &resolvers.ipv4
        };
        // The other family's initial query may have exceeded the short grace
        // period. If the first family cannot connect, give that answer another
        // bounded chance instead of repeatedly choosing the fast, broken one.
        match tokio::time::timeout(
            (remaining / 2).min(Duration::from_secs(1)),
            resolver.lookup_ip(host),
        )
        .await
        {
            Ok(Ok(result)) => result.iter().map(|ip| SocketAddr::new(ip, port)).collect(),
            _ => Vec::new(),
        }
    }

    fn pool(
        &self,
        endpoint: &Endpoint,
        host: &str,
        connect_budget: Duration,
    ) -> Result<Arc<ClientWithMiddleware>, SdkError> {
        // Reqwest's connection timeout belongs to the pooled client. Round
        // down to a shared timeout bucket so similar caller deadlines reuse
        // connections instead of creating a pool for every elapsed nanosecond.
        let mut connect_timeout = MAX_CONNECT_TIMEOUT;
        while connect_timeout > connect_budget {
            connect_timeout /= 2;
        }
        let mut state = self.state.lock().unwrap();
        state
            .pools
            .retain(|pool| pool.used_at.elapsed() < POOL_IDLE);
        if let Some(pool) = state
            .pools
            .iter_mut()
            .find(|p| p.endpoint == *endpoint && p.connect_timeout == connect_timeout)
        {
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
            .connect_timeout(connect_timeout);
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
        let client = Arc::new(builder.build());
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
            connect_timeout,
            client: client.clone(),
            used_at: Instant::now(),
        });
        Ok(client)
    }

    fn retire(&self, endpoint: &Endpoint, client: &Arc<ClientWithMiddleware>) {
        // Dropping the cache's handle does not cancel other in-flight streams.
        // Their handles live until their own requests/deadlines finish.
        let mut state = self.state.lock().unwrap();
        let Some(index) = state
            .pools
            .iter()
            .position(|p| p.endpoint == *endpoint && Arc::ptr_eq(&p.client, client))
        else {
            // A different request already retired this pool. Its replacement
            // must survive late failures from the old connection, including
            // their attempts to extend the endpoint's suspect interval.
            return;
        };
        state.pools.swap_remove(index);
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
        return retryable_status(*status) || error.never_reached_server();
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
            // Charge only retries that actually start, after backoff and the
            // deadline check. An unusable Retry-After or cancelled wait must
            // not consume the quota shared with other reads.
            if attempt > 1 && !self.reads.take_retry_token() {
                break;
            }
            let timeout = policy.attempt_timeout.min(deadline - now);
            let attempt_deadline = now + timeout;
            let mut endpoint = None;
            let mut attempted_pool = None;
            let mut retry_after = None;
            let mut response_status = None;
            let mut stage = "dns";
            let mut trace_id = String::new();
            let result = tokio::time::timeout(timeout, async {
                let addresses = self.reads.addresses(request.url(), timeout).await;
                let origin = request.url().origin().ascii_serialization();
                let address = self.reads.select(&origin, &addresses, &tried);
                let other_family: Vec<_> = addresses
                    .iter()
                    .copied()
                    .filter(|a| address.is_some_and(|first| a.is_ipv4() != first.is_ipv4()))
                    .collect();
                let mut fallback = self.reads.select(&origin, &other_family, &tried);
                let mut candidate = address;
                let mut response = None;
                for connection_attempt in 0..2 {
                    if let Some(address) = candidate {
                        tried.insert(address);
                    }
                    let selected = Endpoint {
                        origin: origin.clone(),
                        address: candidate,
                    };
                    let remaining = attempt_deadline.saturating_duration_since(Instant::now());
                    // A pinned address cannot use the connector's normal
                    // family racing. Reserve half the remaining time for a
                    // fallback connection, even for short caller deadlines.
                    // Limit connection establishment, not HTTP delivery: a
                    // slow response must still spend an HTTP retry attempt.
                    let connect_budget = if connection_attempt == 0 && candidate.is_some() {
                        remaining / 2
                    } else {
                        remaining
                    };
                    if connect_budget.is_zero() {
                        return Err(SdkError::StreamTimeout {
                            waiting_for: "a sandbox metadata read",
                            timeout,
                        });
                    }
                    let transport = self
                        .reads
                        .pool(&selected, request.url().host_str().unwrap_or(""), connect_budget)?;
                    endpoint = Some(selected);
                    attempted_pool = Some(transport.clone());
                    let mut current = request.try_clone().expect("bodyless GET is replayable");
                    trace_id = self.prepare_traced_request(&mut current);
                    let remaining = attempt_deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(SdkError::StreamTimeout {
                            waiting_for: "a sandbox metadata read",
                            timeout,
                        });
                    }
                    *current.timeout_mut() = Some(remaining);
                    current.headers_mut().insert(
                        REQUEST_TIMEOUT_HEADER,
                        HeaderValue::from(remaining.as_millis() as u64),
                    );
                    stage = "response_headers";
                    match transport.execute(current).await {
                        Ok(received) => {
                            response = Some(received);
                            break;
                        }
                        Err(error) => {
                            let error = SdkError::from(error);
                            // Like the default connector's family fallback,
                            // this is still one HTTP attempt: no request was
                            // delivered. Never fall back on a TLS policy error
                            // or after HTTP transmission/body failure.
                            if connection_attempt == 0
                                && let Some(failed) = candidate
                                && error.as_reqwest().is_some_and(|e| e.is_connect())
                                && retryable(&error)
                            {
                                if fallback.is_none() {
                                    let late = self.reads.resolve_other_family(request.url(), failed, attempt_deadline.saturating_duration_since(Instant::now())).await;
                                    fallback = self.reads.select(&origin, &late, &tried);
                                }
                                if fallback.is_some() {
                                    self.reads.retire(endpoint.as_ref().unwrap(), &transport);
                                    tracing::debug!(attempt, %read_id, %trace_id, selected_address = ?candidate, fallback_address = ?fallback, "sandbox metadata connection trying alternate address family");
                                    candidate = fallback;
                                    continue;
                                }
                            }
                            return Err(error);
                        }
                    }
                }
                let response = response.expect(
                    "connection attempts either return an error or receive a response",
                );
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
                    if !matches!(error, SdkError::ServerError { .. })
                        && retryable(&error)
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
                    if transport_failure
                        && let (Some(endpoint), Some(pool)) = (&endpoint, &attempted_pool)
                    {
                        self.reads.retire(endpoint, pool);
                    }
                    let failure = match error.transport_failure() {
                        Some(crate::error::TransportFailure::Timeout) => "timeout",
                        Some(crate::error::TransportFailure::Connect) => "connect",
                        None if response_status.is_some_and(|s| !s.is_success()) => "http_status",
                        None => "connection_or_body",
                    };
                    tracing::debug!(attempt, %read_id, %trace_id, stage, failure, status = ?response_status.map(|s| s.as_u16()), selected_address = ?endpoint.as_ref().and_then(|e| e.address), elapsed_ms = started.elapsed().as_millis() as u64, "sandbox metadata read attempt failed");
                    last_error = Some(error);
                    if attempt == max_attempts {
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
