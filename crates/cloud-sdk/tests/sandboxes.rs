use tensorlake::{
    ClientBuilder,
    sandboxes::{
        SandboxProxyClient, SandboxesClient,
        models::{
            ContainerResourcesInfo, CreateSandboxPoolRequest, NetworkConfig, NetworkPolicyUpdate,
            SandboxPoolRequest, UpdateSandboxPoolRequest,
        },
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::test]
async fn sandbox_proxy_raw_and_empty_posts_send_content_length_and_routing_headers() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept write_stdin");
        let write_stdin = read_http_request(&mut socket).await;
        write_empty_response(&mut socket).await;

        let (mut socket, _) = listener.accept().await.expect("accept close_stdin");
        let close_stdin = read_http_request(&mut socket).await;
        write_empty_response(&mut socket).await;

        let (mut socket, _) = listener.accept().await.expect("accept restart");
        let restart = read_http_request(&mut socket).await;
        let body = r#"{"pid":101,"status":"running","command":"bash","args":[],"started_at":0}"#;
        write_json_response(&mut socket, body).await;

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
        write_json_response(&mut socket, body).await;
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

#[tokio::test]
async fn pool_network_policy_is_sent_and_preserved_on_update() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept create");
        let create = read_http_request(&mut socket).await;
        write_json_response(&mut socket, r#"{"pool_id":"pool-1","namespace":"default"}"#).await;

        let (mut socket, _) = listener.accept().await.expect("accept get");
        let get = read_http_request(&mut socket).await;
        write_json_response(
            &mut socket,
            r#"{
                "pool_id":"pool-1",
                "namespace":"default",
                "image":"alpine",
                "resources":{"cpus":1.0,"memory_mb":1024,"ephemeral_disk_mb":1024},
                "network_policy":{
                    "allow_internet_access":false,
                    "allow_out":["10.0.0.0/8"],
                    "deny_out":["192.0.2.0/24"]
                }
            }"#,
        )
        .await;

        let (mut socket, _) = listener.accept().await.expect("accept update");
        let update = read_http_request(&mut socket).await;
        write_json_response(
            &mut socket,
            r#"{
                "pool_id":"pool-1",
                "namespace":"default",
                "image":"alpine",
                "resources":{"cpus":1.0,"memory_mb":2048,"ephemeral_disk_mb":1024},
                "network_policy":{
                    "allow_internet_access":false,
                    "allow_out":["10.0.0.0/8"],
                    "deny_out":["192.0.2.0/24"]
                }
            }"#,
        )
        .await;

        (create, get, update)
    });

    let client = ClientBuilder::new(&format!("http://{address}"))
        .build()
        .expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", false);
    let policy = NetworkConfig {
        allow_internet_access: false,
        allow_out: vec!["10.0.0.0/8".to_string()],
        deny_out: vec!["192.0.2.0/24".to_string()],
    };

    sandboxes
        .create_pool_with_network(&CreateSandboxPoolRequest {
            pool: SandboxPoolRequest {
                image: Some("alpine".to_string()),
                resources: ContainerResourcesInfo {
                    cpus: 1.0,
                    memory_mb: 1024,
                    ephemeral_disk_mb: 1024,
                },
                timeout_secs: 0,
                entrypoint: None,
                max_containers: None,
                warm_containers: Some(1),
            },
            network: Some(policy.clone()),
        })
        .await
        .expect("create pool");

    sandboxes
        .update_pool(
            "pool-1",
            &SandboxPoolRequest {
                image: Some("alpine".to_string()),
                resources: ContainerResourcesInfo {
                    cpus: 1.0,
                    memory_mb: 2048,
                    ephemeral_disk_mb: 1024,
                },
                timeout_secs: 0,
                entrypoint: None,
                max_containers: None,
                warm_containers: Some(1),
            },
        )
        .await
        .expect("update pool");

    let (create, get, update) = server.await.expect("server join");
    let create_text = String::from_utf8_lossy(&create);
    let get_text = String::from_utf8_lossy(&get);
    let update_text = String::from_utf8_lossy(&update);
    assert!(create_text.contains(r#""network":{"allow_internet_access":false"#));
    assert!(get_text.starts_with("GET /sandbox-pools/pool-1 HTTP/1.1\r\n"));
    assert!(update_text.contains(r#""network":{"allow_internet_access":false"#));
}

#[tokio::test]
async fn update_pool_with_network_replaces_policy_without_get() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    // Exactly one request is served: an explicit replacement policy must be
    // sent as-is, with no current-policy GET beforehand.
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept update");
        let update = read_http_request(&mut socket).await;
        write_json_response(
            &mut socket,
            r#"{
                "pool_id":"pool-1",
                "namespace":"default",
                "image":"alpine",
                "resources":{"cpus":1.0,"memory_mb":1024,"ephemeral_disk_mb":1024},
                "network_policy":{
                    "allow_internet_access":true,
                    "allow_out":[],
                    "deny_out":["198.51.100.0/24"]
                }
            }"#,
        )
        .await;
        update
    });

    let client = ClientBuilder::new(&format!("http://{address}"))
        .build()
        .expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", false);

    let info = sandboxes
        .update_pool_with_network(
            "pool-1",
            &UpdateSandboxPoolRequest {
                pool: SandboxPoolRequest {
                    image: Some("alpine".to_string()),
                    resources: ContainerResourcesInfo {
                        cpus: 1.0,
                        memory_mb: 1024,
                        ephemeral_disk_mb: 1024,
                    },
                    timeout_secs: 0,
                    entrypoint: None,
                    max_containers: None,
                    warm_containers: Some(1),
                },
                network: NetworkPolicyUpdate::Set(NetworkConfig {
                    allow_internet_access: true,
                    allow_out: vec![],
                    deny_out: vec!["198.51.100.0/24".to_string()],
                }),
            },
        )
        .await
        .expect("update pool with network");

    let update = server.await.expect("server join");
    let update_text = String::from_utf8_lossy(&update);
    assert!(update_text.starts_with("PUT /sandbox-pools/pool-1 HTTP/1.1\r\n"));
    assert!(update_text.contains(
        r#""network":{"allow_internet_access":true,"allow_out":[],"deny_out":["198.51.100.0/24"]}"#
    ));
    assert_eq!(
        info.network_policy,
        Some(NetworkConfig {
            allow_internet_access: true,
            allow_out: vec![],
            deny_out: vec!["198.51.100.0/24".to_string()],
        })
    );
}

#[tokio::test]
async fn update_pool_clear_sends_explicit_null_network() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    // One request only: clearing must not read the current policy first.
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept update");
        let update = read_http_request(&mut socket).await;
        write_json_response(
            &mut socket,
            r#"{
                "pool_id":"pool-1",
                "namespace":"default",
                "image":"alpine",
                "resources":{"cpus":1.0,"memory_mb":1024,"ephemeral_disk_mb":1024}
            }"#,
        )
        .await;
        update
    });

    let client = ClientBuilder::new(&format!("http://{address}"))
        .build()
        .expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", false);

    let info = sandboxes
        .update_pool_with_network(
            "pool-1",
            &UpdateSandboxPoolRequest {
                pool: SandboxPoolRequest {
                    image: Some("alpine".to_string()),
                    resources: ContainerResourcesInfo {
                        cpus: 1.0,
                        memory_mb: 1024,
                        ephemeral_disk_mb: 1024,
                    },
                    timeout_secs: 0,
                    entrypoint: None,
                    max_containers: None,
                    warm_containers: Some(1),
                },
                network: NetworkPolicyUpdate::Clear,
            },
        )
        .await
        .expect("clear pool network policy");

    let update = server.await.expect("server join");
    let update_text = String::from_utf8_lossy(&update);
    assert!(
        update_text.contains(r#""network":null"#),
        "clear must send an explicit null so the service removes the policy: {update_text}"
    );
    assert_eq!(info.network_policy, None);
}

#[test]
fn network_policy_update_wire_shapes() {
    // Keep is omitted entirely, Clear is an explicit null, Set is an object.
    let pool = SandboxPoolRequest {
        image: Some("alpine".to_string()),
        resources: ContainerResourcesInfo {
            cpus: 1.0,
            memory_mb: 1024,
            ephemeral_disk_mb: 1024,
        },
        timeout_secs: 0,
        entrypoint: None,
        max_containers: None,
        warm_containers: None,
    };
    let encode = |network| {
        serde_json::to_string(&UpdateSandboxPoolRequest {
            pool: pool.clone(),
            network,
        })
        .expect("serialize")
    };

    assert!(!encode(NetworkPolicyUpdate::Keep).contains("network"));
    assert!(encode(NetworkPolicyUpdate::Clear).contains(r#""network":null"#));
    assert!(
        encode(NetworkPolicyUpdate::Set(NetworkConfig {
            allow_internet_access: false,
            allow_out: vec![],
            deny_out: vec![],
        }))
        .contains(r#""network":{"allow_internet_access":false"#)
    );

    // Round-trip: an absent key must decode back to Keep, null to Clear.
    let keep: UpdateSandboxPoolRequest =
        serde_json::from_str(&encode(NetworkPolicyUpdate::Keep)).expect("decode keep");
    assert_eq!(keep.network, NetworkPolicyUpdate::Keep);
    let clear: UpdateSandboxPoolRequest =
        serde_json::from_str(&encode(NetworkPolicyUpdate::Clear)).expect("decode clear");
    assert_eq!(clear.network, NetworkPolicyUpdate::Clear);
}

async fn read_http_request(socket: &mut TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut buf = [0_u8; 4096];

    loop {
        let read = socket.read(&mut buf).await.expect("read request");
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buf[..read]);

        if let Some(headers_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&request[..headers_end + 4]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if name.eq_ignore_ascii_case("content-length") {
                        value.trim().parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);

            if request.len() >= headers_end + 4 + content_length {
                break;
            }
        }
    }

    request
}

async fn write_empty_response(socket: &mut TcpStream) {
    socket
        .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await
        .expect("write response");
}

async fn write_json_response(socket: &mut TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write response");
}
