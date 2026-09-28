use std::time::Duration;

use tensorlake::{
    ClientBuilder,
    sandboxes::{
        SandboxProxyClient, SandboxesClient, TERMINATION_REASON_NO_CAPACITY,
        models::{
            ClaimSandboxRequest, CreateSandboxPoolRequest, CreateSandboxRequest,
            CreateSandboxResources, FileSystemMount, NetworkConfig, NetworkPolicyUpdate,
            SandboxPoolRequest, UpdateSandboxPoolRequest, UpdateSandboxRequest,
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
async fn pool_claim_with_file_systems_sends_json_body() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept request");
        let request = read_http_request(&mut socket).await;
        let body =
            r#"{"sandbox_id":"sbx-1","status":"running","claim_configuration_applied":true}"#;
        write_json_response(&mut socket, body).await;
        request
    });

    let client = ClientBuilder::new(&format!("http://{address}"))
        .build()
        .expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", false);
    sandboxes
        .claim_with_request(
            "pool-1",
            &ClaimSandboxRequest {
                file_systems: vec![FileSystemMount {
                    file_system_id: "file_system_abc".to_string(),
                    mount_path: "/mnt/skills".to_string(),
                    ..Default::default()
                }],
            },
        )
        .await
        .expect("claim sandbox with file systems");

    let request = server.await.expect("server join");
    let request_text = String::from_utf8_lossy(&request);
    assert!(request_text.starts_with("POST /sandbox-pools/pool-1/sandboxes HTTP/1.1\r\n"));
    assert!(request_text.contains("\r\ncontent-type: application/json\r\n"));
    assert!(request.ends_with(
        br#"{"file_systems":[{"file_system_id":"file_system_abc","mount_path":"/mnt/skills"}]}"#
    ));
}

#[tokio::test]
async fn pool_claim_with_file_systems_rejects_server_without_acknowledgment() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept request");
        let claim_request = read_http_request(&mut socket).await;
        let body = r#"{"sandbox_id":"sbx-1","status":"running"}"#;
        write_json_response(&mut socket, body).await;

        let (mut socket, _) = listener.accept().await.expect("accept cleanup request");
        let cleanup_request = read_http_request(&mut socket).await;
        write_json_response(&mut socket, "{}").await;
        (claim_request, cleanup_request)
    });

    let client = ClientBuilder::new(&format!("http://{address}"))
        .build()
        .expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", false);
    let error = sandboxes
        .claim_with_request(
            "pool-1",
            &ClaimSandboxRequest {
                file_systems: vec![FileSystemMount {
                    file_system_id: "file_system_abc".to_string(),
                    mount_path: "/mnt/skills".to_string(),
                    ..Default::default()
                }],
            },
        )
        .await
        .expect_err("an old server must not silently ignore claim-time mounts");

    assert!(error.to_string().contains("did not acknowledge"));
    assert!(
        error
            .to_string()
            .contains("termination was requested for sandbox \"sbx-1\"")
    );
    let (_, cleanup_request) = server.await.expect("server join");
    assert!(
        String::from_utf8_lossy(&cleanup_request)
            .starts_with("DELETE /sandboxes/sbx-1 HTTP/1.1\r\n")
    );
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
                "resources":{"cpus":1.0,"memory_mb":1024,"disk_mb":1024},
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
                "resources":{"cpus":1.0,"memory_mb":2048,"disk_mb":1024},
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
                resources: CreateSandboxResources {
                    cpus: 1.0,
                    memory_mb: 1024,
                    disk_mb: None,
                    gpu_configs: None,
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
                resources: CreateSandboxResources {
                    cpus: 1.0,
                    memory_mb: 2048,
                    disk_mb: Some(2048),
                    gpu_configs: None,
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
    assert!(!create_text.contains("disk_mb"));
    assert!(!create_text.contains("ephemeral_disk_mb"));
    assert!(get_text.starts_with("GET /sandbox-pools/pool-1 HTTP/1.1\r\n"));
    assert!(update_text.contains(r#""network":{"allow_internet_access":false"#));
    assert!(update_text.contains(r#""disk_mb":2048"#));
    assert!(!update_text.contains("ephemeral_disk_mb"));
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
                "resources":{"cpus":1.0,"memory_mb":1024,"disk_mb":1024},
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
                    resources: CreateSandboxResources {
                        cpus: 1.0,
                        memory_mb: 1024,
                        disk_mb: None,
                        gpu_configs: None,
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
                "resources":{"cpus":1.0,"memory_mb":1024,"disk_mb":1024}
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
                    resources: CreateSandboxResources {
                        cpus: 1.0,
                        memory_mb: 1024,
                        disk_mb: None,
                        gpu_configs: None,
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
        resources: CreateSandboxResources {
            cpus: 1.0,
            memory_mb: 1024,
            disk_mb: None,
            gpu_configs: None,
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

    let keep_json = encode(NetworkPolicyUpdate::Keep);
    assert!(!keep_json.contains("network"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&keep_json).expect("decode keep JSON value")["resources"]
            ["cpus"],
        serde_json::json!(1.0),
        "floating-point resources must remain JSON numbers when arbitrary precision is enabled"
    );
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
    let keep: UpdateSandboxPoolRequest = serde_json::from_str(&keep_json).expect("decode keep");
    assert_eq!(keep.network, NetworkPolicyUpdate::Keep);
    let clear: UpdateSandboxPoolRequest =
        serde_json::from_str(&encode(NetworkPolicyUpdate::Clear)).expect("decode clear");
    assert_eq!(clear.network, NetworkPolicyUpdate::Clear);
}

#[test]
fn create_pool_wire_round_trips_with_arbitrary_precision() {
    let request = CreateSandboxPoolRequest {
        pool: SandboxPoolRequest {
            image: Some("alpine".to_string()),
            resources: CreateSandboxResources {
                cpus: 1.5,
                memory_mb: 1024,
                disk_mb: Some(2048),
                gpu_configs: None,
            },
            timeout_secs: 60,
            entrypoint: Some(vec!["sleep".to_string(), "60".to_string()]),
            max_containers: Some(5),
            warm_containers: Some(1),
        },
        network: Some(NetworkConfig {
            allow_internet_access: false,
            allow_out: vec!["example.com".to_string()],
            deny_out: vec![],
        }),
    };

    let encoded = serde_json::to_string(&request).expect("encode create pool");
    let value: serde_json::Value =
        serde_json::from_str(&encoded).expect("decode create pool JSON value");
    assert_eq!(value["resources"]["cpus"], serde_json::json!(1.5));
    assert_eq!(value["resources"]["disk_mb"], serde_json::json!(2048));
    assert!(value["resources"].get("ephemeral_disk_mb").is_none());
    assert_eq!(
        serde_json::from_str::<CreateSandboxPoolRequest>(&encoded).expect("round-trip create pool"),
        request
    );
}

const SANDBOX_INFO_JSON: &str = r#"{
    "sandbox_id":"sb-1",
    "namespace":"default",
    "status":"running",
    "resources":{"cpus":1.0,"memory_mb":1024,"disk_mb":1024}
}"#;

#[tokio::test]
async fn update_sandbox_replaces_network_policy() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept update");
        let update = read_http_request(&mut socket).await;
        write_json_response(&mut socket, SANDBOX_INFO_JSON).await;
        update
    });

    let client = ClientBuilder::new(&format!("http://{address}"))
        .build()
        .expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", false);

    sandboxes
        .update(
            "sb-1",
            &UpdateSandboxRequest {
                name: None,
                allow_unauthenticated_access: None,
                exposed_ports: None,
                network: NetworkPolicyUpdate::Set(NetworkConfig {
                    allow_internet_access: true,
                    allow_out: vec!["example.com".to_string()],
                    deny_out: vec![],
                }),
            },
        )
        .await
        .expect("update sandbox network policy");

    let update = server.await.expect("server join");
    let update_text = String::from_utf8_lossy(&update);
    assert!(
        update_text.starts_with("PATCH "),
        "sandbox network updates must use PATCH: {update_text}"
    );
    assert!(
        update_text.contains(r#""network":{"allow_internet_access":true"#),
        "the replacement policy must be sent as an object: {update_text}"
    );
    assert!(
        update_text.contains(r#""allow_out":["example.com"]"#),
        "allow_out entries must be sent: {update_text}"
    );
}

#[tokio::test]
async fn update_sandbox_clear_sends_explicit_null_network() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept update");
        let update = read_http_request(&mut socket).await;
        write_json_response(&mut socket, SANDBOX_INFO_JSON).await;
        update
    });

    let client = ClientBuilder::new(&format!("http://{address}"))
        .build()
        .expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", false);

    sandboxes
        .update(
            "sb-1",
            &UpdateSandboxRequest {
                name: None,
                allow_unauthenticated_access: None,
                exposed_ports: None,
                network: NetworkPolicyUpdate::Clear,
            },
        )
        .await
        .expect("clear sandbox network policy");

    let update = server.await.expect("server join");
    let update_text = String::from_utf8_lossy(&update);
    assert!(
        update_text.contains(r#""network":null"#),
        "clear must send an explicit null so the service removes the policy: {update_text}"
    );
}

#[test]
fn sandbox_network_policy_update_wire_shapes() {
    // Keep is omitted entirely, Clear is an explicit null, Set is an object —
    // the tri-state PATCH semantics the running-sandbox update relies on.
    let encode = |network| {
        serde_json::to_string(&UpdateSandboxRequest {
            name: None,
            allow_unauthenticated_access: None,
            exposed_ports: None,
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

    // Round-trip: an absent key decodes back to Keep, null to Clear.
    let keep: UpdateSandboxRequest =
        serde_json::from_str(&encode(NetworkPolicyUpdate::Keep)).expect("decode keep");
    assert_eq!(keep.network, NetworkPolicyUpdate::Keep);
    let clear: UpdateSandboxRequest =
        serde_json::from_str(&encode(NetworkPolicyUpdate::Clear)).expect("decode clear");
    assert_eq!(clear.network, NetworkPolicyUpdate::Clear);
}

// ---- ADR 0086: wait-free create and polling readiness ------------------------

/// A scripted lifecycle server: each entry answers one connection with the
/// given status and JSON body, and the request that arrived is recorded.
async fn scripted_server(
    responses: Vec<(u16, &'static str)>,
) -> (String, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, body) in responses {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let request = read_http_request(&mut socket).await;
            requests.push(String::from_utf8_lossy(&request).into_owned());
            write_status_json_response(&mut socket, status, body).await;
        }
        requests
    });
    (format!("http://{address}"), server)
}

fn request_line(request: &str) -> &str {
    request.lines().next().unwrap_or_default()
}

fn request_body(request: &str) -> &str {
    request
        .split_once("\r\n\r\n")
        .map(|(_, body)| body)
        .unwrap_or_default()
}

fn create_request() -> CreateSandboxRequest {
    CreateSandboxRequest {
        image: Some("tensorlake/ubuntu-minimal".to_string()),
        resources: CreateSandboxResources::default(),
        timeout_secs: None,
        entrypoint: None,
        network: None,
        snapshot_id: None,
        name: Some("coreauto-run-17".to_string()),
        file_systems: Vec::new(),
        wait: None,
        max_pending_secs: Some(1800),
    }
}

fn info(status: &str) -> &'static str {
    Box::leak(
        format!(
            r#"{{"id":"sbx-1","namespace":"default","status":"{status}","resources":{{"cpus":1.0,"memory_mb":512,"disk_mb":1024}},"pending_reason":"no_resources_available","sandbox_url":"https://sbx-1.sandbox.tensorlake.ai","routing_hint":"hint-1"}}"#
        )
        .into_boxed_str(),
    )
}

#[tokio::test]
async fn create_no_wait_sends_wait_false_and_returns_the_acknowledgement() {
    let accepted = r#"{"sandbox_id":"sbx-1","name":"coreauto-run-17","state":"pending","pending_reason":"scheduling"}"#;
    let (url, server) = scripted_server(vec![(202, accepted)]).await;
    let client = ClientBuilder::new(&url).build().expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", true);

    let created = sandboxes
        .create_no_wait(&create_request())
        .await
        .expect("create is acknowledged");
    assert_eq!(created.sandbox_id, "sbx-1");
    assert_eq!(created.name.as_deref(), Some("coreauto-run-17"));
    assert_eq!(created.state, "pending");
    assert_eq!(created.pending_reason.as_deref(), Some("scheduling"));

    let requests = server.await.expect("server join");
    assert_eq!(requests.len(), 1);
    assert_eq!(
        request_line(&requests[0]),
        "POST /v1/namespaces/default/sandboxes HTTP/1.1"
    );
    let body: serde_json::Value = serde_json::from_str(request_body(&requests[0])).unwrap();
    assert_eq!(body["wait"], false);
    assert_eq!(body["max_pending_secs"], 1800);
    assert_eq!(body["name"], "coreauto-run-17");
}

#[tokio::test]
async fn blocking_create_omits_wait_and_an_unset_bound() {
    let (url, server) =
        scripted_server(vec![(200, r#"{"sandbox_id":"sbx-1","status":"running"}"#)]).await;
    let client = ClientBuilder::new(&url).build().expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", false);
    let request = CreateSandboxRequest {
        max_pending_secs: None,
        ..create_request()
    };
    sandboxes.create(&request).await.expect("create");
    let requests = server.await.expect("server join");
    let body = request_body(&requests[0]);
    assert!(!body.contains("\"wait\""), "{body}");
    assert!(!body.contains("max_pending_secs"), "{body}");
}

#[tokio::test]
async fn create_no_wait_accepts_the_legacy_blocking_shape_from_older_servers() {
    // A server that predates `wait: false` ignores it and answers the
    // blocking create's shape, possibly already running or timed out.
    let (url, server) = scripted_server(vec![
        (200, r#"{"sandbox_id":"sbx-1","status":"running","sandbox_url":"https://sbx-1.sandbox.tensorlake.ai"}"#),
        (504, r#"{"sandbox_id":"sbx-2","status":"timeout"}"#),
    ])
    .await;
    let client = ClientBuilder::new(&url).build().expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", true);
    let running = sandboxes
        .create_no_wait(&create_request())
        .await
        .expect("running");
    assert_eq!(running.state, "running");
    assert_eq!(
        running.sandbox_url.as_deref(),
        Some("https://sbx-1.sandbox.tensorlake.ai")
    );
    let timed_out = sandboxes
        .create_no_wait(&create_request())
        .await
        .expect("timeout");
    assert_eq!(timed_out.sandbox_id, "sbx-2");
    assert_eq!(timed_out.state, "timeout");
    server.await.expect("server join");
}

#[tokio::test]
async fn wait_until_settled_polls_until_the_sandbox_is_running() {
    let (url, server) = scripted_server(vec![(200, info("pending")), (200, info("running"))]).await;
    let client = ClientBuilder::new(&url).build().expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", true);

    let observed = sandboxes
        .wait_until_settled("sbx-1", Duration::from_secs(30), Duration::from_millis(10))
        .await
        .expect("wait");
    assert_eq!(observed.status, "running");
    assert_eq!(observed.routing_hint.as_deref(), Some("hint-1"));
    let requests = server.await.expect("server join");
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_eq!(
            request_line(request),
            "GET /v1/namespaces/default/sandboxes/sbx-1 HTTP/1.1"
        );
    }
}

/// Like [`scripted_server`] but each answer can be delayed before it is
/// written, to emulate a slow poll.
async fn delayed_server(
    responses: Vec<(u16, &'static str, Duration)>,
) -> (String, tokio::task::JoinHandle<usize>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let server = tokio::spawn(async move {
        let mut served = 0;
        for (status, body, delay) in responses {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            read_http_request(&mut socket).await;
            tokio::time::sleep(delay).await;
            write_status_json_response(&mut socket, status, body).await;
            served += 1;
        }
        served
    });
    (format!("http://{address}"), server)
}

#[tokio::test]
async fn wait_until_settled_does_not_overshoot_its_budget() {
    // The first poll answers at once (pending); the second would take two
    // seconds. A 50 ms budget must return the pending state within the
    // budget plus a small margin, without waiting for that second answer.
    let (url, server) = delayed_server(vec![
        (200, info("pending"), Duration::ZERO),
        (200, info("running"), Duration::from_secs(2)),
    ])
    .await;
    let client = ClientBuilder::new(&url)
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", true);

    let budget = Duration::from_millis(50);
    let started = std::time::Instant::now();
    let observed = sandboxes
        .wait_until_settled("sbx-1", budget, Duration::from_millis(10))
        .await
        .expect("a timed-out wait is not an error");
    let elapsed = started.elapsed();
    assert_eq!(observed.status, "pending");
    assert!(
        elapsed < budget + Duration::from_millis(300),
        "wait overshot its budget: {elapsed:?}"
    );
    server.abort();
}

#[tokio::test]
async fn wait_until_settled_stops_at_the_deadline_after_a_failed_first_poll() {
    // A 503 on the first poll, then a poll that stalls. The retry backoff
    // consumes the 50 ms budget; the next poll must not be issued with the
    // first-poll floor: the wait returns the 503 within the budget plus a
    // small margin instead of waiting a second for the stalled answer.
    let (url, server) = delayed_server(vec![
        (503, r#"{"message":"busy"}"#, Duration::ZERO),
        (200, info("pending"), Duration::from_secs(30)),
    ])
    .await;
    let client = ClientBuilder::new(&url)
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", true);

    let budget = Duration::from_millis(50);
    let started = std::time::Instant::now();
    let error = sandboxes
        .wait_until_settled("sbx-1", budget, Duration::from_millis(10))
        .await
        .expect_err("nothing was observed before the budget ran out");
    let elapsed = started.elapsed();
    assert!(
        matches!(&error, tensorlake::error::SdkError::ServerError { status, .. } if status.as_u16() == 503),
        "the last error is surfaced: {error}"
    );
    assert!(
        elapsed < Duration::from_millis(150),
        "wait overshot its budget after a failed poll: {elapsed:?}"
    );
    server.abort();
}

#[tokio::test]
async fn wait_until_settled_caps_a_slow_first_poll_at_the_budget_floor() {
    // With nothing observed yet the first poll is allowed the floor, not the
    // client's full 30 s timeout: a poll that never answers fails within it.
    let (url, server) = delayed_server(vec![(200, info("pending"), Duration::from_secs(30))]).await;
    let client = ClientBuilder::new(&url)
        .timeout(Duration::from_secs(30))
        .build()
        .expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", true);

    let started = std::time::Instant::now();
    let result = sandboxes
        .wait_until_settled(
            "sbx-1",
            Duration::from_millis(50),
            Duration::from_millis(10),
        )
        .await;
    let elapsed = started.elapsed();
    assert!(result.is_err(), "a poll that never answers is an error");
    assert!(
        elapsed < tensorlake::sandboxes::WAIT_POLL_MIN_REQUEST_TIMEOUT + Duration::from_secs(2),
        "poll outlived the floor: {elapsed:?}"
    );
    server.abort();
}

#[tokio::test]
async fn wait_until_settled_returns_the_pending_state_when_the_budget_runs_out() {
    let (url, server) = scripted_server(vec![(200, info("pending"))]).await;
    let client = ClientBuilder::new(&url).build().expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", true);
    let observed = sandboxes
        .wait_until_settled("sbx-1", Duration::ZERO, Duration::from_secs(2))
        .await
        .expect("a timed-out wait is not an error");
    // The sandbox keeps its place in the queue: the caller sees its state and
    // decides. Nothing here deletes it (the server saw exactly one GET).
    assert_eq!(observed.status, "pending");
    assert_eq!(
        observed.pending_reason.as_deref(),
        Some("no_resources_available")
    );
    assert_eq!(server.await.expect("server join").len(), 1);
}

#[tokio::test]
async fn wait_until_settled_surfaces_a_no_capacity_failure() {
    let failed = r#"{"id":"sbx-1","namespace":"default","status":"terminated","resources":{"cpus":1.0,"memory_mb":512,"disk_mb":1024},"termination_reason":"no_capacity","pending_reason":"no_resources_available","error_details":"no host could place 4 CPUs within 1800s"}"#;
    let (url, server) = scripted_server(vec![(200, failed)]).await;
    let client = ClientBuilder::new(&url).build().expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", true);
    let observed = sandboxes
        .wait_until_settled("sbx-1", Duration::from_secs(30), Duration::from_secs(2))
        .await
        .expect("a terminal state ends the wait");
    assert_eq!(observed.status, "terminated");
    assert_eq!(
        observed.termination_reason.as_deref(),
        Some(TERMINATION_REASON_NO_CAPACITY)
    );
    server.await.expect("server join");
}

#[tokio::test]
async fn wait_until_settled_repeats_a_poll_cut_short_by_an_intermediary() {
    let (url, server) = scripted_server(vec![
        (502, r#"{"message":"Failed to proxy request"}"#),
        (200, info("running")),
    ])
    .await;
    let client = ClientBuilder::new(&url).build().expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", true);
    let observed = sandboxes
        .wait_until_settled("sbx-1", Duration::from_secs(30), Duration::from_millis(10))
        .await
        .expect("a gateway error mid-wait is repeated");
    assert_eq!(observed.status, "running");
    assert_eq!(server.await.expect("server join").len(), 2);
}

#[tokio::test]
async fn wait_until_settled_reports_a_missing_sandbox() {
    let (url, server) = scripted_server(vec![(404, r#"{"message":"not found"}"#)]).await;
    let client = ClientBuilder::new(&url).build().expect("build client");
    let sandboxes = SandboxesClient::new(client, "default", true);
    let error = sandboxes
        .wait_until_settled(
            "sbx-missing",
            Duration::from_secs(30),
            Duration::from_secs(2),
        )
        .await
        .expect_err("a sandbox that does not exist is an error, not a wait");
    assert!(
        matches!(error, tensorlake::error::SdkError::ServerError { status, .. } if status.as_u16() == 404),
        "unexpected error: {error}"
    );
    server.await.expect("server join");
}

async fn write_status_json_response(socket: &mut TcpStream, status: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write response");
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
