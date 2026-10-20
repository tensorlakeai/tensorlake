use super::*;
use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response as HyperResponse, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::{
    convert::Infallible,
    sync::atomic::{AtomicUsize, Ordering},
};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};
use tokio_rustls::TlsAcceptor;

#[derive(Clone, Copy)]
enum Behavior {
    Healthy,
    Stall,
    ResetAfterFirst,
    Truncated,
    Status(u16),
    RetryAfter,
    UndeliveredOnce,
    UndeliveredAlways,
}

struct Pki {
    acceptor: TlsAcceptor,
    certificate: reqwest::tls::Certificate,
}

impl Pki {
    fn new(h2: bool) -> Self {
        let _ = crate::http_transport::https_builder();
        let key = rcgen::KeyPair::generate().unwrap();
        let certificate = rcgen::CertificateParams::new(vec!["metadata.test".into()])
            .unwrap()
            .self_signed(&key)
            .unwrap();
        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certificate.der().clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
            )
            .unwrap();
        config.alpn_protocols = vec![if h2 {
            b"h2".to_vec()
        } else {
            b"http/1.1".to_vec()
        }];
        Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            certificate: reqwest::tls::Certificate::from_der(certificate.der()).unwrap(),
        }
    }
}

struct Server {
    address: SocketAddr,
    requests: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn server(pki: &Pki, behavior: Behavior) -> Server {
    server_on(pki, behavior, "127.0.0.1:0").await
}

async fn server_on(pki: &Pki, behavior: Behavior, bind: &str) -> Server {
    let listener = TcpListener::bind(bind).await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests = Arc::new(AtomicUsize::new(0));
    let connections = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    let connection_count = connections.clone();
    let acceptor = pki.acceptor.clone();
    let task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                connection = listener.accept() => {
                    let (socket, _) = connection.unwrap();
                    connection_count.fetch_add(1, Ordering::SeqCst);
                    let acceptor = acceptor.clone();
                    let count = count.clone();
                    tasks.spawn(async move {
                        let Ok(stream) = acceptor.accept(socket).await else { return; };
                        assert_eq!(stream.get_ref().1.server_name(), Some("metadata.test"));
                        let reset = Arc::new(Notify::new());
                        let notify = reset.clone();
                        let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                            let count = count.clone();
                            let notify = notify.clone();
                            async move {
                                let n = count.fetch_add(1, Ordering::SeqCst);
                                let authority = request.uri().authority().map(|a| a.as_str()).or_else(|| request.headers().get("host").and_then(|h| h.to_str().ok()));
                                assert_eq!(authority.and_then(|a| a.split(':').next()), Some("metadata.test"));
                                assert!(request.headers().contains_key("traceparent"));
                                if matches!(behavior, Behavior::Stall) {
                                    std::future::pending::<()>().await;
                                }
                                if matches!(behavior, Behavior::ResetAfterFirst) && n > 0 {
                                    notify.notify_one();
                                    std::future::pending::<()>().await;
                                }
                                let undelivered = matches!(behavior, Behavior::UndeliveredAlways)
                                    || matches!(behavior, Behavior::UndeliveredOnce) && n == 0;
                                let status = if undelivered { 500 } else { match behavior { Behavior::Status(s) => s, Behavior::RetryAfter => 503, _ => 200 } };
                                let body = Bytes::from_static(if undelivered {
                                    br#"{"error":"route lookup unavailable","code":"SANDBOX_UPSTREAM_ERROR"}"#
                                } else { br#"{"ok":true}"# });
                                let mut response = HyperResponse::builder().status(status);
                                if matches!(behavior, Behavior::Truncated) {
                                    response = response.header("content-length", 1000);
                                }
                                if matches!(behavior, Behavior::RetryAfter) {
                                    response = response.header("retry-after", "60");
                                }
                                Ok::<_, Infallible>(response.body(Full::new(body)).unwrap())
                            }
                        });
                        let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
                        tokio::select! {
                            _ = builder.serve_connection(TokioIo::new(stream), service) => {},
                            _ = reset.notified() => {}, // drop an established HTTP/2 connection
                        }
                    });
                }
                _ = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
    });
    Server {
        address,
        requests,
        connections,
        task,
    }
}

fn client(pki: &Pki, addresses: Vec<SocketAddr>) -> Client {
    let client = ClientBuilder::new("https://metadata.test")
        .read_retry(ReadRetryConfig {
            max_attempts: 3,
            attempt_timeout: Duration::from_millis(150),
            total_timeout: Duration::from_secs(2),
        })
        .build()
        .unwrap();
    *client.reads.test_options.lock().unwrap() = TestOptions {
        addresses: Some(addresses),
        certificate: Some(pki.certificate.clone()),
    };
    client
}

#[derive(Debug, serde::Deserialize)]
struct Metadata {
    ok: bool,
}

async fn read(client: &Client) -> Result<Traced<Metadata>, SdkError> {
    client
        .execute_read_json(
            client
                .request(Method::GET, "/sandboxes/example")
                .build()
                .unwrap(),
        )
        .await
}

#[tokio::test]
async fn tls_stall_and_truncated_body_fail_over_to_third_address() {
    for h2 in [false, true] {
        let pki = Pki::new(h2);
        let stalled = server(&pki, Behavior::Stall).await;
        let truncated = server(&pki, Behavior::Truncated).await;
        let healthy = server(&pki, Behavior::Healthy).await;
        let client = client(
            &pki,
            vec![stalled.address, truncated.address, healthy.address],
        );
        let started = Instant::now();
        assert!(read(&client).await.unwrap().ok);
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(stalled.requests.load(Ordering::SeqCst), 1);
        assert_eq!(truncated.requests.load(Ordering::SeqCst), 1);
        assert_eq!(healthy.requests.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn broken_established_http2_connection_moves_to_another_pool() {
    let pki = Pki::new(true);
    let reset = server(&pki, Behavior::ResetAfterFirst).await;
    let healthy = server(&pki, Behavior::Healthy).await;
    let client = client(&pki, vec![reset.address, healthy.address]);
    assert!(read(&client).await.unwrap().ok);
    assert!(read(&client).await.unwrap().ok);
    assert_eq!(
        reset.connections.load(Ordering::SeqCst),
        1,
        "second request must reuse the established h2 connection"
    );
    assert_eq!(reset.requests.load(Ordering::SeqCst), 2);
    assert_eq!(healthy.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn caller_deadline_includes_all_attempts_and_outer_retry_does_not_restart_it() {
    let pki = Pki::new(true);
    let stalled = server(&pki, Behavior::Stall).await;
    let client = client(&pki, vec![stalled.address])
        .with_base_url_and_timeout("https://metadata.test", Some(Duration::from_millis(75)))
        .unwrap();
    let started = Instant::now();
    let result = crate::retry::retry(
        client,
        crate::retry::RetryPolicy::idempotent(5),
        |c| async move { read(&c).await },
    )
    .await;
    let error = result.unwrap_err();
    assert!(matches!(error, SdkError::ReadRetryExhausted(_)));
    assert_eq!(
        error.transport_failure(),
        Some(crate::error::TransportFailure::Timeout)
    );
    assert!(!error.never_reached_server());
    assert!(started.elapsed() < Duration::from_millis(300));
    assert_eq!(stalled.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn definitive_errors_and_retry_after_do_not_trigger_more_requests() {
    for behavior in [
        Behavior::Status(401),
        Behavior::Status(403),
        Behavior::Status(404),
        Behavior::RetryAfter,
    ] {
        let pki = Pki::new(true);
        let target = server(&pki, behavior).await;
        let client = client(&pki, vec![target.address]);
        let error = crate::retry::retry(
            client,
            crate::retry::RetryPolicy::idempotent(5),
            |c| async move { read(&c).await },
        )
        .await
        .unwrap_err()
        .into_original();
        match behavior {
            Behavior::Status(401) => assert!(matches!(error, SdkError::Authentication(_))),
            Behavior::Status(403) => assert!(matches!(error, SdkError::Authorization(_))),
            _ => assert!(matches!(error, SdkError::ServerError { .. })),
        }
        assert_eq!(target.requests.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn failed_tls_verification_is_not_retried() {
    let pki = Pki::new(true);
    let target = server(&pki, Behavior::Healthy).await;
    let client = client(&pki, vec![target.address]);
    client.reads.test_options.lock().unwrap().certificate = None;
    assert!(
        crate::retry::retry(
            client,
            crate::retry::RetryPolicy::idempotent(5),
            |c| async move { read(&c).await }
        )
        .await
        .is_err()
    );
    assert_eq!(target.connections.load(Ordering::SeqCst), 1);
    assert_eq!(target.requests.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn dns_changes_are_respected_even_with_an_existing_pool() {
    let pki = Pki::new(true);
    let first = server(&pki, Behavior::Healthy).await;
    let second = server(&pki, Behavior::Healthy).await;
    let client = client(&pki, vec![first.address]);
    assert!(read(&client).await.unwrap().ok);
    assert!(read(&client).await.unwrap().ok);
    assert_eq!(first.connections.load(Ordering::SeqCst), 1);
    client.reads.test_options.lock().unwrap().addresses = Some(vec![second.address]);
    assert!(read(&client).await.unwrap().ok);
    assert_eq!(first.requests.load(Ordering::SeqCst), 2);
    assert_eq!(second.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancellation_stops_retries() {
    let pki = Pki::new(true);
    let target = server(&pki, Behavior::Stall).await;
    let client = client(&pki, vec![target.address]);
    let clone = client.clone();
    let task = tokio::spawn(async move { read(&clone).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while target.requests.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(target.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn retry_budget_is_shared_bounded_and_recovers() {
    let client = ClientBuilder::new("https://metadata.test").build().unwrap();
    let clone = client.with_base_url("https://other.test");
    for _ in 0..20 {
        assert!(client.reads.take_retry_token());
    }
    assert!(!clone.reads.take_retry_token());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(clone.reads.take_retry_token());
    assert!(!client.reads.take_retry_token());
    clone.reads.success();
    assert!(client.reads.take_retry_token());
}

#[tokio::test]
async fn mutation_is_never_sent_through_the_read_retry_path() {
    let pki = Pki::new(true);
    let target = server(&pki, Behavior::Healthy).await;
    let client = client(&pki, vec![target.address]);
    let request = client
        .request(Method::POST, "/api/v1/processes")
        .json(&serde_json::json!({"command":"charge-card"}))
        .build()
        .unwrap();
    assert!(matches!(
        client.execute_read_json::<Metadata>(request).await,
        Err(SdkError::ClientError(_))
    ));
    assert_eq!(target.requests.load(Ordering::SeqCst), 0);
}

#[test]
fn zero_or_excessive_limits_are_rejected() {
    let config = ReadRetryConfig {
        max_attempts: 0,
        ..ReadRetryConfig::default()
    };
    assert!(
        ClientBuilder::new("https://metadata.test")
            .read_retry(config)
            .build()
            .is_err()
    );
}

#[tokio::test]
async fn retryable_http_errors_have_one_shared_attempt_limit() {
    let pki = Pki::new(true);
    let target = server(&pki, Behavior::Status(503)).await;
    let client = client(&pki, vec![target.address]);
    let error = crate::retry::retry(
        client,
        crate::retry::RetryPolicy::idempotent(5),
        |c| async move { read(&c).await },
    )
    .await
    .unwrap_err();
    assert!(matches!(error, SdkError::ReadRetryExhausted(_)));
    assert!(matches!(
        error.into_original(),
        SdkError::ServerError {
            status: StatusCode::SERVICE_UNAVAILABLE,
            ..
        }
    ));
    assert_eq!(target.requests.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn exhausted_retry_budget_still_allows_the_initial_request() {
    let pki = Pki::new(true);
    let target = server(&pki, Behavior::Status(503)).await;
    let client = client(&pki, vec![target.address]);
    client.reads.state.lock().unwrap().tokens = 0.0;
    assert!(read(&client).await.is_err());
    assert_eq!(target.requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn successful_pool_is_not_closed_when_another_address_fails() {
    let pki = Pki::new(true);
    let target = server(&pki, Behavior::Healthy).await;
    let stalled = server(&pki, Behavior::Stall).await;
    let client = client(&pki, vec![target.address]);
    assert!(read(&client).await.unwrap().ok);
    client.reads.test_options.lock().unwrap().addresses =
        Some(vec![stalled.address, target.address]);
    assert!(read(&client).await.unwrap().ok);
    assert_eq!(target.connections.load(Ordering::SeqCst), 1);
    assert_eq!(target.requests.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn late_old_pool_failure_preserves_replacement_pool() {
    let pki = Pki::new(true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(AtomicUsize::new(0));
    let release = Arc::new([Notify::new(), Notify::new()]);
    let (seen_tx, mut seen_rx) = tokio::sync::mpsc::unbounded_channel();
    let acceptor = pki.acceptor.clone();
    let server_connections = connections.clone();
    let server_requests = requests.clone();
    let server_release = release.clone();
    let task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                connection = listener.accept() => {
                    let (socket, _) = connection.unwrap();
                    server_connections.fetch_add(1, Ordering::SeqCst);
                    let acceptor = acceptor.clone();
                    let requests = server_requests.clone();
                    let release = server_release.clone();
                    let seen_tx = seen_tx.clone();
                    tasks.spawn(async move {
                        let stream = acceptor.accept(socket).await.unwrap();
                        let service = service_fn(move |_: hyper::Request<hyper::body::Incoming>| {
                            let requests = requests.clone();
                            let release = release.clone();
                            let seen_tx = seen_tx.clone();
                            async move {
                                let n = requests.fetch_add(1, Ordering::SeqCst);
                                seen_tx.send(n).unwrap();
                                let mut response = HyperResponse::builder();
                                if let Some(gate) = release.get(n) {
                                    // Both requests share the first HTTP/2 connection.
                                    // Release their failures independently so the second
                                    // arrives only after a healthy pool has replaced it.
                                    gate.notified().await;
                                    response = response.header("content-length", 1000);
                                }
                                Ok::<_, Infallible>(response.body(Full::new(Bytes::from_static(br#"{"ok":true}"#))).unwrap())
                            }
                        });
                        let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                            .serve_connection(TokioIo::new(stream), service).await;
                    });
                }
                _ = tasks.join_next(), if !tasks.is_empty() => {}
            }
        }
    });
    let target = Server {
        address,
        requests,
        connections,
        task,
    };
    let mut client = client(&pki, vec![address]);
    Arc::get_mut(&mut client.reads).unwrap().config = ReadRetryConfig {
        max_attempts: 1,
        attempt_timeout: Duration::from_secs(10),
        total_timeout: Duration::from_secs(10),
    };
    let first_client = client.clone();
    let first = tokio::spawn(async move { read(&first_client).await });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), seen_rx.recv())
            .await
            .unwrap(),
        Some(0),
    );
    let second_client = client.clone();
    let second = tokio::spawn(async move { read(&second_client).await });
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), seen_rx.recv())
            .await
            .unwrap(),
        Some(1),
    );
    assert_eq!(target.connections.load(Ordering::SeqCst), 1);

    release[0].notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), first)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(read(&client).await.unwrap().ok);
    assert_eq!(target.connections.load(Ordering::SeqCst), 2);
    assert!(
        !second.is_finished(),
        "the old pool still has an in-flight request"
    );
    let suspect_until = client.reads.state.lock().unwrap().suspect[0].1;

    release[1].notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_eq!(
        client.reads.state.lock().unwrap().suspect[0].1,
        suspect_until,
        "a late failure must not extend the replacement pool's suspect interval"
    );
    assert!(read(&client).await.unwrap().ok);
    assert_eq!(
        target.connections.load(Ordering::SeqCst),
        2,
        "a late failure must not evict the healthy replacement pool"
    );
}

#[tokio::test]
async fn unscheduled_retries_do_not_exhaust_shared_budget() {
    let pki = Pki::new(true);
    let throttled = server(&pki, Behavior::RetryAfter).await;
    let client = client(&pki, vec![throttled.address]);
    for _ in 0..20 {
        assert!(read(&client).await.is_err());
    }
    assert_eq!(
        throttled.requests.load(Ordering::SeqCst),
        20,
        "Retry-After exceeds the deadline, so no retries should be sent"
    );
    assert_eq!(
        client.reads.state.lock().unwrap().tokens,
        RETRY_CAPACITY,
        "unsent retries must leave the shared quota intact"
    );

    let recovering = server(&pki, Behavior::UndeliveredOnce).await;
    client.reads.test_options.lock().unwrap().addresses = Some(vec![recovering.address]);
    assert!(read(&client).await.unwrap().ok);
    assert_eq!(recovering.requests.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn mutation_with_an_ambiguous_timeout_is_sent_once() {
    use tokio::io::AsyncReadExt;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let server = tokio::spawn(async move {
        let mut sockets = Vec::new();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(std::str::from_utf8(&buf[..n]).unwrap().starts_with("POST "));
            count.fetch_add(1, Ordering::SeqCst);
            sockets.push(socket); // accept the mutation, then lose its response
        }
    });
    let client = ClientBuilder::new(&url)
        .timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    let result = crate::retry::retry(
        client,
        crate::retry::RetryPolicy::non_idempotent(),
        |c| async move {
            let request = c
                .request(Method::POST, "/sandboxes")
                .json(&serde_json::json!({"name":"one-sandbox"}))
                .build()
                .unwrap();
            c.execute_json::<Metadata>(request).await
        },
    )
    .await;
    server.abort();
    assert!(result.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct Marker;
#[async_trait::async_trait]
impl Middleware for Marker {
    async fn handle(
        &self,
        mut request: Request,
        extensions: &mut http::Extensions,
        next: reqwest_middleware::Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        request
            .headers_mut()
            .insert("x-test-middleware", HeaderValue::from_static("preserved"));
        let response = next.run(request, extensions).await?;
        Ok(response)
    }
}

#[tokio::test]
async fn address_pools_preserve_credentials_scope_and_middleware() {
    let pki = Pki::new(true);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = pki.acceptor.clone();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let stream = acceptor.accept(socket).await.unwrap();
        let service = service_fn(|r: hyper::Request<hyper::body::Incoming>| async move {
            assert_eq!(r.headers()["authorization"], "Bearer test-token");
            assert_eq!(r.headers()["x-forwarded-project-id"], "project");
            assert_eq!(r.headers()["x-forwarded-organization-id"], "organization");
            assert_eq!(r.headers()["x-test-middleware"], "preserved");
            assert_eq!(r.headers()["user-agent"], "test-sdk");
            Ok::<_, Infallible>(HyperResponse::new(Full::new(Bytes::from_static(
                br#"{"ok":true}"#,
            ))))
        });
        hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(stream), service)
            .await
            .unwrap();
    });
    let client = ClientBuilder::new("https://metadata.test")
        .bearer_token("test-token")
        .scope("organization", "project")
        .user_agent("test-sdk")
        .middleware(Marker)
        .build()
        .unwrap();
    *client.reads.test_options.lock().unwrap() = TestOptions {
        addresses: Some(vec![address]),
        certificate: Some(pki.certificate),
    };
    assert!(
        read(&client.with_base_url("https://metadata.test"))
            .await
            .unwrap()
            .ok
    );
    task.abort();
}

#[tokio::test]
#[ignore = "local latency benchmark; run just bench-sdk-resilience"]
async fn benchmark_healthy_read_transport() {
    let pki = Pki::new(true);
    let target = server(&pki, Behavior::Healthy).await;
    let mut client = client(&pki, vec![target.address]);
    client.base_client = crate::http_transport::https_builder()
        .no_proxy()
        .resolve("metadata.test", target.address)
        .tls_certs_merge([pki.certificate])
        .build()
        .unwrap();
    client.client = ReqwestClientBuilder::new(client.base_client.clone()).build();
    // Warm both pools before timing. Alternating rounds avoids measuring only
    // one path on a cold CPU. This fixture excludes DNS; measure that separately.
    for _ in 0..10 {
        read(&client).await.unwrap();
        client
            .execute_json::<Metadata>(
                client
                    .request(Method::GET, "/sandboxes/example")
                    .build()
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    const N: usize = 1000;
    for round in 0..5 {
        let started = Instant::now();
        for _ in 0..N {
            client
                .execute_json::<Metadata>(
                    client
                        .request(Method::GET, "/sandboxes/example")
                        .build()
                        .unwrap(),
                )
                .await
                .unwrap();
        }
        let baseline = started.elapsed();
        let started = Instant::now();
        for _ in 0..N {
            read(&client).await.unwrap();
        }
        let resilient = started.elapsed();
        println!(
            "round={round} requests={N} baseline_ms={:.3} resilient_ms={:.3}",
            baseline.as_secs_f64() * 1000.0,
            resilient.as_secs_f64() * 1000.0
        );
    }
    let started = Instant::now();
    for _ in 0..N {
        let _ = tokio::net::lookup_host(("localhost", 443))
            .await
            .unwrap()
            .count();
    }
    println!(
        "system_dns_localhost requests={N} total_ms={:.3}",
        started.elapsed().as_secs_f64() * 1000.0
    );
    client.reads.test_options.lock().unwrap().addresses = None;
    let local = url::Url::parse("https://localhost").unwrap();
    assert!(
        !client
            .reads
            .addresses(&local, Duration::from_secs(3))
            .await
            .is_empty()
    );
    let started = Instant::now();
    for _ in 0..N {
        assert!(
            !client
                .reads
                .addresses(&local, Duration::from_secs(3))
                .await
                .is_empty()
        );
    }
    println!(
        "cached_resolver_localhost requests={N} total_ms={:.3}",
        started.elapsed().as_secs_f64() * 1000.0
    );
    println!(
        "pooled_connections={}",
        target.connections.load(Ordering::SeqCst)
    );
    assert_eq!(target.connections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn dns_answers_are_cached_until_their_ttl_and_then_refreshed() {
    use hickory_resolver::{
        config::{LookupIpStrategy, NameServerConfigGroup, ResolverConfig},
        name_server::TokioConnectionProvider,
        proto::{
            op::{Message, MessageType},
            rr::{RData, Record, RecordType, rdata::A},
        },
    };
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    let queries = Arc::new(AtomicUsize::new(0));
    let count = queries.clone();
    let server = tokio::spawn(async move {
        let mut buf = [0; 2048];
        loop {
            let (n, peer) = socket.recv_from(&mut buf).await.unwrap();
            let request = Message::from_vec(&buf[..n]).unwrap();
            let query = request.queries()[0].clone();
            let mut response = Message::new();
            response
                .set_id(request.id())
                .set_message_type(MessageType::Response)
                .set_recursion_desired(true)
                .set_recursion_available(true)
                .add_query(query.clone());
            if query.query_type() == RecordType::A {
                let n = count.fetch_add(1, Ordering::SeqCst);
                response.add_answer(Record::from_rdata(
                    query.name().clone(),
                    1,
                    RData::A(A::new(127, 0, 0, if n == 0 { 1 } else { 2 })),
                ));
            }
            socket
                .send_to(&response.to_vec().unwrap(), peer)
                .await
                .unwrap();
        }
    });
    let nameservers = NameServerConfigGroup::from_ips_clear(&[address.ip()], address.port(), true);
    let mut builder = hickory_resolver::TokioResolver::builder_with_config(
        ResolverConfig::from_parts(None, vec![], nameservers),
        TokioConnectionProvider::default(),
    );
    builder.options_mut().ip_strategy = LookupIpStrategy::Ipv4Only;
    let client = ClientBuilder::new("https://metadata.test").build().unwrap();
    client
        .reads
        .resolver
        .set(Some(ReadResolvers::new(builder)))
        .unwrap();
    let url = url::Url::parse("https://metadata.test").unwrap();
    let first = client.reads.addresses(&url, Duration::from_secs(3)).await;
    assert_eq!(first, vec!["127.0.0.1:443".parse::<SocketAddr>().unwrap()]);
    assert_eq!(
        client.reads.addresses(&url, Duration::from_secs(3)).await,
        first
    );
    assert_eq!(queries.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        client.reads.addresses(&url, Duration::from_secs(3)).await,
        vec!["127.0.0.2:443".parse::<SocketAddr>().unwrap()]
    );
    assert_eq!(queries.load(Ordering::SeqCst), 2);
    server.abort();
}

#[tokio::test]
async fn subsequent_reads_prefer_a_peer_that_has_not_just_failed() {
    let pki = Pki::new(true);
    let stalled = server(&pki, Behavior::Stall).await;
    let healthy = server(&pki, Behavior::Healthy).await;
    let client = client(&pki, vec![stalled.address, healthy.address]);
    assert!(read(&client).await.unwrap().ok);
    assert!(read(&client).await.unwrap().ok);
    assert_eq!(stalled.requests.load(Ordering::SeqCst), 1);
    assert_eq!(healthy.requests.load(Ordering::SeqCst), 2);
    assert_eq!(healthy.connections.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn forwarding_proxy_does_not_require_local_origin_dns() {
    const CHILD: &str = "TENSORLAKE_READ_PROXY_TEST";
    if std::env::var_os(CHILD).is_some() {
        let client = ClientBuilder::new("http://not-resolvable.invalid")
            .build()
            .unwrap();
        assert!(read(&client).await.unwrap().ok);
        assert!(
            client.reads.resolver.get().is_none(),
            "proxy requests must not resolve the origin locally"
        );
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let service = service_fn(
            |request: hyper::Request<hyper::body::Incoming>| async move {
                assert_eq!(request.uri().host(), Some("not-resolvable.invalid"));
                Ok::<_, Infallible>(HyperResponse::new(Full::new(Bytes::from_static(
                    br#"{"ok":true}"#,
                ))))
            },
        );
        hyper::server::conn::http1::Builder::new()
            .serve_connection(TokioIo::new(socket), service)
            .await
            .unwrap();
    });
    let proxy = format!("http://{address}");
    let child = tokio::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "client::read_retry::tests::forwarding_proxy_does_not_require_local_origin_dns",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("HTTP_PROXY", &proxy)
        .env("http_proxy", &proxy)
        .env("ALL_PROXY", &proxy)
        .env("all_proxy", &proxy)
        .env_remove("NO_PROXY")
        .env_remove("no_proxy")
        .kill_on_drop(true)
        .output();
    let output = tokio::time::timeout(Duration::from_secs(5), child)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn definitive_status_with_a_stalled_body_is_not_retried() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let server = tokio::spawn(async move {
        let mut sockets = Vec::new();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            let received = socket.read(&mut buf).await.unwrap();
            assert!(received > 0);
            count.fetch_add(1, Ordering::SeqCst);
            socket
                .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 1000\r\n\r\nx")
                .await
                .unwrap();
            sockets.push(socket);
        }
    });
    let client = ClientBuilder::new(&url)
        .timeout(Duration::from_millis(75))
        .build()
        .unwrap();
    assert!(matches!(
        read(&client).await,
        Err(SdkError::Authentication(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn known_undelivered_500_retries_are_bounded_but_generic_500_is_terminal() {
    for (behavior, expected_requests, succeeds) in [
        (Behavior::UndeliveredOnce, 2, true),
        (Behavior::UndeliveredAlways, 3, false),
        (Behavior::Status(500), 1, false),
    ] {
        let pki = Pki::new(true);
        let target = server(&pki, behavior).await;
        let client = client(&pki, vec![target.address]);
        let result = crate::retry::retry(
            client,
            crate::retry::RetryPolicy::idempotent(5),
            |c| async move { read(&c).await },
        )
        .await;
        assert_eq!(result.is_ok(), succeeds, "{result:?}");
        assert_eq!(target.requests.load(Ordering::SeqCst), expected_requests);
        if !succeeds {
            assert!(matches!(
                result.unwrap_err().into_original(),
                SdkError::ServerError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    ..
                }
            ));
        }
    }
}

#[tokio::test]
async fn unreachable_addresses_use_the_other_family_in_the_same_http_attempt() {
    for ipv6_first in [true, false] {
        let pki = Pki::new(true);
        let healthy = server_on(
            &pki,
            Behavior::Healthy,
            if ipv6_first { "127.0.0.1:0" } else { "[::1]:0" },
        )
        .await;
        let closed = (0..3)
            .map(|_| {
                std::net::TcpListener::bind(if ipv6_first { "[::1]:0" } else { "127.0.0.1:0" })
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut addresses = closed
            .iter()
            .map(|l| l.local_addr().unwrap())
            .collect::<Vec<_>>();
        addresses.push(healthy.address);
        drop(closed);
        let mut client = client(&pki, addresses);
        Arc::get_mut(&mut client.reads).unwrap().config.max_attempts = 1;
        assert!(read(&client).await.unwrap().ok);
        assert_eq!(healthy.requests.load(Ordering::SeqCst), 1);
        let state = client.reads.state.lock().unwrap();
        assert_eq!(
            state.suspect.len(),
            1,
            "only the first failed connection was tried"
        );
        assert_eq!(
            state.tokens, RETRY_CAPACITY,
            "family fallback is not an HTTP replay"
        );
    }
}

#[tokio::test]
async fn tls_policy_failure_does_not_fall_back_to_another_family() {
    let pki = Pki::new(true);
    let first = server(&pki, Behavior::Healthy).await;
    let other = server_on(&pki, Behavior::Healthy, "[::1]:0").await;
    let client = client(&pki, vec![first.address, other.address]);
    client.reads.test_options.lock().unwrap().certificate = None;
    assert!(
        crate::retry::retry(
            client,
            crate::retry::RetryPolicy::idempotent(5),
            |c| async move { read(&c).await }
        )
        .await
        .is_err()
    );
    assert_eq!(first.connections.load(Ordering::SeqCst), 1);
    assert_eq!(other.connections.load(Ordering::SeqCst), 0);
}

async fn partial_dns(
    ipv4_answers: bool,
    ipv6_answers: bool,
    ipv4_delay: Duration,
) -> (ReadResolvers, JoinHandle<()>) {
    use hickory_resolver::{
        config::{NameServerConfigGroup, ResolverConfig},
        name_server::TokioConnectionProvider,
        proto::{
            op::{Message, MessageType},
            rr::{
                RData, Record, RecordType,
                rdata::{A, AAAA},
            },
        },
    };
    let socket = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let address = socket.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        let mut buf = [0; 2048];
        loop {
            let (n, peer) = socket.recv_from(&mut buf).await.unwrap();
            let request = Message::from_vec(&buf[..n]).unwrap();
            let query = request.queries()[0].clone();
            let answer = match query.query_type() {
                RecordType::A if ipv4_answers => RData::A(A::new(127, 0, 0, 1)),
                RecordType::AAAA if ipv6_answers => {
                    RData::AAAA(AAAA(std::net::Ipv6Addr::LOCALHOST))
                }
                _ => continue, // silently drop the unhealthy family's queries
            };
            let mut response = Message::new();
            response
                .set_id(request.id())
                .set_message_type(MessageType::Response)
                .set_recursion_desired(true)
                .set_recursion_available(true)
                .add_query(query.clone())
                .add_answer(Record::from_rdata(query.name().clone(), 60, answer));
            let bytes = response.to_vec().unwrap();
            let socket = socket.clone();
            tasks.spawn(async move {
                if query.query_type() == RecordType::A {
                    tokio::time::sleep(ipv4_delay).await;
                }
                socket.send_to(&bytes, peer).await.unwrap();
            });
            while tasks.try_join_next().is_some() {}
        }
    });
    let nameservers = NameServerConfigGroup::from_ips_clear(&[address.ip()], address.port(), true);
    let builder = hickory_resolver::TokioResolver::builder_with_config(
        ResolverConfig::from_parts(None, vec![], nameservers),
        TokioConnectionProvider::default(),
    );
    (ReadResolvers::new(builder), server)
}

#[tokio::test]
async fn stalled_dns_family_does_not_discard_available_answers() {
    for (ipv4, ipv6) in [(true, false), (false, true), (true, true)] {
        let (resolver, dns) = partial_dns(ipv4, ipv6, Duration::ZERO).await;
        let client = ClientBuilder::new("https://metadata.test").build().unwrap();
        client.reads.resolver.set(Some(resolver)).unwrap();
        let url = url::Url::parse("https://metadata.test").unwrap();
        // Include a caller deadline shorter than the normal 50ms family grace:
        // the DNS sub-deadline must still return an already received answer.
        for budget in [Duration::from_secs(3), Duration::from_millis(100)] {
            let addresses = tokio::time::timeout(
                Duration::from_millis(500),
                client.reads.addresses(&url, budget),
            )
            .await
            .unwrap();
            assert_eq!(addresses.iter().any(SocketAddr::is_ipv4), ipv4);
            assert_eq!(addresses.iter().any(SocketAddr::is_ipv6), ipv6);
        }
        dns.abort();
    }
}

#[tokio::test]
async fn unavailable_dns_leaves_time_for_platform_resolution_and_connection() {
    let (resolver, dns) = partial_dns(false, false, Duration::ZERO).await;
    let started = Instant::now();
    let answers = resolver
        .lookup("metadata.test", started + Duration::from_millis(50))
        .await;
    assert!(answers.is_empty());
    assert!(started.elapsed() < Duration::from_millis(250));
    dns.abort();
}

#[tokio::test]
async fn metadata_read_reaches_healthy_a_address_while_aaaa_is_stalled() {
    let pki = Pki::new(true);
    let target = server(&pki, Behavior::Healthy).await;
    let (resolver, dns) = partial_dns(true, false, Duration::ZERO).await;
    let client = ClientBuilder::new(&format!("https://metadata.test:{}", target.address.port()))
        .read_retry(ReadRetryConfig {
            max_attempts: 1,
            ..ReadRetryConfig::default()
        })
        .build()
        .unwrap();
    client.reads.resolver.set(Some(resolver)).unwrap();
    client.reads.test_options.lock().unwrap().certificate = Some(pki.certificate);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read(&client))
            .await
            .unwrap()
            .unwrap()
            .ok
    );
    assert_eq!(target.requests.load(Ordering::SeqCst), 1);
    dns.abort();
}

#[tokio::test]
async fn connect_timeout_leaves_time_for_the_other_family() {
    let pki = Pki::new(true);
    let healthy = server(&pki, Behavior::Healthy).await;
    let listener = TcpListener::bind("[::1]:0").await.unwrap();
    let stalled = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (_socket, _) = listener.accept().await.unwrap();
        std::future::pending::<()>().await; // TCP succeeds, TLS never completes
    });
    let mut client = client(&pki, vec![stalled, healthy.address]);
    Arc::get_mut(&mut client.reads).unwrap().config = ReadRetryConfig {
        max_attempts: 1,
        ..ReadRetryConfig::default()
    };
    assert!(read(&client).await.unwrap().ok);
    assert_eq!(healthy.requests.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test]
async fn slow_other_family_dns_can_recover_after_a_fast_family_cannot_connect() {
    let pki = Pki::new(true);
    let target = server(&pki, Behavior::Healthy).await;
    // AAAA answers immediately, but there is no IPv6 listener at this port.
    // A is healthy and arrives after the initial family-resolution grace.
    let (resolver, dns) = partial_dns(true, true, Duration::from_millis(100)).await;
    let client = ClientBuilder::new(&format!("https://metadata.test:{}", target.address.port()))
        .read_retry(ReadRetryConfig {
            max_attempts: 1,
            ..ReadRetryConfig::default()
        })
        .build()
        .unwrap();
    client.reads.resolver.set(Some(resolver)).unwrap();
    client.reads.test_options.lock().unwrap().certificate = Some(pki.certificate);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), read(&client))
            .await
            .unwrap()
            .unwrap()
            .ok
    );
    assert_eq!(target.requests.load(Ordering::SeqCst), 1);
    dns.abort();
}
