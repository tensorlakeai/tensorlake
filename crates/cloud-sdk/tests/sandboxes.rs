use tensorlake::{
    ClientBuilder,
    sandboxes::{SandboxProxyClient, SandboxesClient},
};
use tokio::net::TcpListener;

mod common;

use common::http_mock::{read_http_request, write_empty_response, write_json_response};

#[tokio::test]
async fn sandbox_proxy_raw_and_empty_posts_send_content_length_and_routing_headers() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept write_stdin");
        let write_stdin = read_http_request(&mut socket).await;
        write_empty_response(&mut socket)
            .await
            .expect("write response");

        let (mut socket, _) = listener.accept().await.expect("accept close_stdin");
        let close_stdin = read_http_request(&mut socket).await;
        write_empty_response(&mut socket)
            .await
            .expect("write response");

        let (mut socket, _) = listener.accept().await.expect("accept restart");
        let restart = read_http_request(&mut socket).await;
        let body = r#"{"pid":101,"status":"running","command":"bash","args":[],"started_at":0}"#;
        write_json_response(&mut socket, body)
            .await
            .expect("write response");

        (write_stdin, close_stdin, restart)
    });

    let client = ClientBuilder::new(&format!("http://{address}"))
        .build()
        .expect("build client");
    let sandbox = SandboxProxyClient::new(client, Some("sandbox-host.test".to_string()))
        .with_sandbox_id(Some("sbx-1".to_string()))
        .with_routing_hint(Some("route-a".to_string()));

    sandbox
        .write_stdin(101_i64, b"hello".to_vec())
        .await
        .expect("write stdin");
    sandbox.close_stdin(101_i64).await.expect("close stdin");
    sandbox
        .restart_process(101_i64)
        .await
        .expect("restart process");

    let (write_stdin, close_stdin, restart) = server.await.expect("server join");
    let write_text = String::from_utf8_lossy(&write_stdin);
    let close_text = String::from_utf8_lossy(&close_stdin);
    let restart_text = String::from_utf8_lossy(&restart);

    assert!(write_text.starts_with("POST /api/v1/processes/101/stdin HTTP/1.1\r\n"));
    assert!(write_text.contains("\r\nhost: sandbox-host.test\r\n"));
    assert!(write_text.contains("\r\nx-tensorlake-sandbox-id: sbx-1\r\n"));
    assert!(write_text.contains("\r\nx-tensorlake-route-hint: route-a\r\n"));
    assert!(write_text.contains("\r\ncontent-length: 5\r\n"));
    assert!(write_stdin.ends_with(b"\r\n\r\nhello"));

    assert!(close_text.starts_with("POST /api/v1/processes/101/stdin/close HTTP/1.1\r\n"));
    assert!(close_text.contains("\r\ncontent-length: 0\r\n"));
    assert!(close_text.contains("\r\nx-tensorlake-sandbox-id: sbx-1\r\n"));

    assert!(restart_text.starts_with("POST /api/v1/processes/101/restart HTTP/1.1\r\n"));
    assert!(restart_text.contains("\r\ncontent-length: 0\r\n"));
    assert!(restart_text.contains("\r\nx-tensorlake-route-hint: route-a\r\n"));
}

#[tokio::test]
async fn direct_empty_post_helper_sends_content_length_zero() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept request");
        let request = read_http_request(&mut socket).await;
        let body = r#"{"sandbox_id":"sbx-1","status":"running"}"#;
        write_json_response(&mut socket, body)
            .await
            .expect("write response");
        request
    });

    let client = ClientBuilder::new(&format!("http://{address}"))
        .build()
        .expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", false);

    sandboxes.claim("pool-1").await.expect("claim sandbox");

    let request = server.await.expect("server join");
    let request_text = String::from_utf8_lossy(&request);
    assert!(request_text.starts_with("POST /sandbox-pools/pool-1/sandboxes HTTP/1.1\r\n"));
    assert!(request_text.contains("\r\ncontent-length: 0\r\n"));
}
