use super::*;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio_tungstenite::{accept_async, accept_hdr_async};

fn options(addr: SocketAddr) -> TcpTunnelOptions {
    TcpTunnelOptions {
        base_url: format!("http://{addr}"),
        headers: HeaderMap::new(),
        remote_port: 9000,
        local_host: "127.0.0.1".into(),
        local_port: 0,
        connect_timeout: Duration::from_secs(2),
    }
}

async fn echo<S: AsyncRead + AsyncWrite + Unpin>(stream: S) {
    let mut ws = accept_async(stream).await.unwrap();
    while let Some(Ok(message)) = ws.next().await {
        match message {
            Message::Binary(data) => {
                if ws.send(Message::Binary(data)).await.is_err() {
                    break;
                }
            }
            Message::Close(_) => {
                let _ = ws.flush().await;
                break;
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_immediate_writes_over_verified_tls() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let key = rcgen::KeyPair::generate().unwrap();
    let cert = rcgen::CertificateParams::new(vec!["127.0.0.1".into()])
        .unwrap()
        .self_signed(&key)
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.der().clone()).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        )
        .unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut opts = options(listener.local_addr().unwrap());
    opts.base_url = opts.base_url.replace("http:", "https:");
    let server = tokio::spawn(async move {
        let mut clients = JoinSet::new();
        for _ in 0..100 {
            let (stream, _) = listener.accept().await.unwrap();
            let acceptor = acceptor.clone();
            clients.spawn(async move {
                echo(acceptor.accept(stream).await.unwrap()).await;
            });
        }
        while let Some(result) = clients.join_next().await {
            result.unwrap();
        }
    });
    let tunnel = TcpTunnel::listen_with_connector(opts, Some(Connector::Rustls(Arc::new(client))))
        .await
        .unwrap();
    let concurrency = Arc::new(tokio::sync::Semaphore::new(8));
    let mut clients = JoinSet::new();
    for n in 0..100 {
        let permit = concurrency.clone().acquire_owned().await.unwrap();
        let addr = tunnel.local_addr();
        clients.spawn(async move {
            let _permit = permit;
            tokio::time::timeout(Duration::from_secs(10), async {
                let mut socket = TcpStream::connect(addr).await.unwrap();
                let data = vec![n as u8; if n % 10 == 0 { 256 * 1024 } else { 128 }];
                socket.write_all(&data).await.unwrap();
                let mut received = vec![0; data.len()];
                socket.read_exact(&mut received).await.unwrap();
                assert_eq!(received, data);
                socket.shutdown().await.unwrap();
                let mut tail = Vec::new();
                socket.read_to_end(&mut tail).await.unwrap();
                assert!(tail.is_empty());
            })
            .await
            .unwrap();
        });
    }
    while let Some(result) = clients.join_next().await {
        result.unwrap();
    }
    tunnel.close().await;
    server.await.unwrap();
}

#[tokio::test]
#[allow(clippy::result_large_err)] // Tungstenite fixes the handshake callback's error type.
async fn preserves_routing_headers_and_bidirectional_binary_data() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut opts = options(listener.local_addr().unwrap());
    for (name, value) in [
        ("host", "sbx.example.test"),
        ("authorization", "Bearer test-key"),
        ("x-tensorlake-sandbox-id", "sbx-123"),
        ("x-tensorlake-route-hint", "route-7"),
    ] {
        opts.headers.insert(
            reqwest::header::HeaderName::from_static(name),
            value.parse().unwrap(),
        );
    }
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_hdr_async(
            stream,
            |request: &tungstenite::handshake::server::Request, response| {
                assert_eq!(request.uri(), "/api/v1/tunnels/tcp?port=9000");
                assert_eq!(request.headers()["host"], "sbx.example.test");
                assert_eq!(request.headers()["authorization"], "Bearer test-key");
                assert_eq!(request.headers()["x-tensorlake-sandbox-id"], "sbx-123");
                assert_eq!(request.headers()["x-tensorlake-route-hint"], "route-7");
                Ok(response)
            },
        )
        .await
        .unwrap();
        ws.send(Message::Binary(vec![0, 255, 1, 128].into()))
            .await
            .unwrap();
        assert_eq!(ws.next().await.unwrap().unwrap().into_data(), b"hello"[..]);
        ws.send(Message::Ping(b"probe"[..].into())).await.unwrap();
        assert_eq!(
            ws.next().await.unwrap().unwrap(),
            Message::Pong(b"probe"[..].into())
        );
        ws.close(None).await.unwrap();
    });
    let tunnel = TcpTunnel::listen(opts).await.unwrap();
    let mut socket = TcpStream::connect(tunnel.local_addr()).await.unwrap();
    socket.write_all(b"hello").await.unwrap();
    let mut data = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut data))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data, [0, 255, 1, 128]);
    server.await.unwrap();
    tunnel.close().await;
}

#[tokio::test]
async fn close_cancels_stalled_handshake_and_releases_listener() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tunnel = TcpTunnel::listen(options(listener.local_addr().unwrap()))
        .await
        .unwrap();
    let addr = tunnel.local_addr();
    let mut socket = TcpStream::connect(addr).await.unwrap();
    let (mut remote, _) = listener.accept().await.unwrap();
    let mut request = [0; 1024];
    assert!(remote.read(&mut request).await.unwrap() > 0);
    tokio::time::timeout(Duration::from_secs(1), tunnel.close())
        .await
        .unwrap();
    assert!(tunnel.is_closed());
    assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
    assert_eq!(remote.read(&mut [0]).await.unwrap(), 0);
    assert!(TcpStream::connect(addr).await.is_err());
    tunnel.close().await;
}

#[tokio::test]
async fn handshake_timeout_does_not_poison_next_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut opts = options(listener.local_addr().unwrap());
    opts.connect_timeout = Duration::from_millis(100);
    let tunnel = TcpTunnel::listen(opts).await.unwrap();
    let mut first = TcpStream::connect(tunnel.local_addr()).await.unwrap();
    let (_stalled, _) = listener.accept().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), first.read(&mut [0]))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        echo(stream).await;
    });
    let mut second = TcpStream::connect(tunnel.local_addr()).await.unwrap();
    second.write_all(b"ok").await.unwrap();
    let mut data = [0; 2];
    second.read_exact(&mut data).await.unwrap();
    assert_eq!(&data, b"ok");
    tunnel.close().await;
    server.await.unwrap();
}

#[tokio::test]
async fn local_half_close_delivers_all_tail_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let opts = options(listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        assert_eq!(ws.next().await.unwrap().unwrap().into_data(), b"go"[..]);
        // The peer can send frames concurrently with our close handshake.
        for n in 0..8u8 {
            ws.send(Message::Binary(vec![n; 1024 * 1024].into()))
                .await
                .unwrap();
        }
        ws.close(None).await.unwrap();
    });
    let tunnel = TcpTunnel::listen(opts).await.unwrap();
    let mut socket = TcpStream::connect(tunnel.local_addr()).await.unwrap();
    socket.write_all(b"go").await.unwrap();
    socket.shutdown().await.unwrap();
    let mut data = Vec::new();
    tokio::time::timeout(Duration::from_secs(4), socket.read_to_end(&mut data))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(data.len(), 8 * 1024 * 1024);
    for n in 0..8 {
        assert!(
            data[n * 1024 * 1024..(n + 1) * 1024 * 1024]
                .iter()
                .all(|b| *b == n as u8)
        );
    }
    server.await.unwrap();
    tunnel.close().await;
}

#[tokio::test]
async fn close_cancels_backpressure_from_nonreading_local_client() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let opts = options(listener.local_addr().unwrap());
    let (started, ready) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        started.send(()).unwrap();
        let data = bytes::Bytes::from(vec![42; 1024 * 1024]);
        while ws.send(Message::Binary(data.clone())).await.is_ok() {}
    });
    let tunnel = TcpTunnel::listen(opts).await.unwrap();
    let _socket = TcpStream::connect(tunnel.local_addr()).await.unwrap();
    ready.await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    tokio::time::timeout(Duration::from_secs(1), tunnel.close())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn close_cancels_backpressure_from_nonreading_proxy() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tunnel = TcpTunnel::listen(options(listener.local_addr().unwrap()))
        .await
        .unwrap();
    let mut socket = TcpStream::connect(tunnel.local_addr()).await.unwrap();
    let (remote, _) = listener.accept().await.unwrap();
    let _ws = accept_async(remote).await.unwrap();
    let writer = tokio::spawn(async move {
        let data = vec![42; 1024 * 1024];
        while socket.write_all(&data).await.is_ok() {}
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    tokio::time::timeout(Duration::from_secs(1), tunnel.close())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), writer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn drop_cancels_connections_and_listener() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tunnel = TcpTunnel::listen(options(listener.local_addr().unwrap()))
        .await
        .unwrap();
    let done = tunnel.done.clone();
    let addr = tunnel.local_addr();
    let mut socket = TcpStream::connect(addr).await.unwrap();
    let (_remote, _) = listener.accept().await.unwrap();
    drop(tunnel);
    tokio::time::timeout(Duration::from_secs(1), done.cancelled())
        .await
        .unwrap();
    assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
    assert!(TcpStream::connect(addr).await.is_err());
}

#[test]
fn reset_without_close_is_only_expected_after_shutdown_starts() {
    let error = tungstenite::Error::Protocol(
        tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
    );
    assert!(!expected_shutdown(&error, false));
    assert!(expected_shutdown(&error, true));
}

#[tokio::test]
async fn local_eof_without_close_reply_releases_the_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tunnel = TcpTunnel::listen(options(listener.local_addr().unwrap()))
        .await
        .unwrap();
    let mut socket = TcpStream::connect(tunnel.local_addr()).await.unwrap();
    let (remote, _) = listener.accept().await.unwrap();
    let mut ws = accept_async(remote).await.unwrap();
    socket.shutdown().await.unwrap();
    assert!(matches!(
        ws.next().await.unwrap().unwrap(),
        Message::Close(_)
    ));
    // Never poll the WebSocket again: leave its queued close reply unflushed.
    let mut tail = Vec::new();
    tokio::time::timeout(Duration::from_secs(7), ws.get_mut().read_to_end(&mut tail))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
    assert!(!tunnel.is_closed());
    tunnel.close().await;
}

#[tokio::test]
async fn handshake_errors_are_reported_without_exposing_credentials() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut opts = options(listener.local_addr().unwrap());
    opts.headers.insert(
        "authorization",
        "Bearer sensitive-test-token".parse().unwrap(),
    );
    let tunnel = TcpTunnel::listen(opts).await.unwrap();
    let mut errors = tunnel.errors();
    let mut socket = TcpStream::connect(tunnel.local_addr()).await.unwrap();
    let (mut remote, _) = listener.accept().await.unwrap();
    let mut request = [0; 2048];
    assert!(remote.read(&mut request).await.unwrap() > 0);
    remote
        .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n")
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(1), errors.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(error.message.contains("401"));
    assert!(!error.message.contains("sensitive-test-token"));
    assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
    assert!(!tunnel.is_closed());
    tunnel.close().await;
}
