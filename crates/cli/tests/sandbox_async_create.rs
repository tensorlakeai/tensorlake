//! `tl sbx create` / `tl sbx wait` against a scripted lifecycle server:
//! the blocking create, the queued create (`--queue`, `wait: false`) and
//! readiness by polling (ADR 0087).
//! Every request the CLI sends is captured so the tests can assert on the
//! body, the paths and, above all, on the absence of a DELETE.

use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Command,
    time::{Duration, timeout},
};

struct CliRun {
    output: std::process::Output,
    /// Raw HTTP requests in arrival order, excluding the API key introspection.
    requests: Vec<String>,
}

async fn run_cli(args: &[&str], responses: Vec<(u16, Value)>) -> CliRun {
    timeout(Duration::from_secs(30), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            // API key introspection precedes the sandbox request.
            let responses = std::iter::once((200, json!({
                "organizationId": "org-test", "projectId": "project-test",
            }))).chain(responses);
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut byte = [0];
                while !request.ends_with(b"\r\n\r\n") {
                    assert!(request.len() < 65536, "bounded request headers");
                    stream.read_exact(&mut byte).await.unwrap();
                    request.push(byte[0]);
                }
                let headers = String::from_utf8(request).unwrap();
                let length = headers.lines().find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                }).unwrap_or(0);
                assert!(length < 65536, "bounded request body");
                let mut body_bytes = vec![0; length];
                stream.read_exact(&mut body_bytes).await.unwrap();
                requests.push(format!("{headers}{}", String::from_utf8_lossy(&body_bytes)));
                let body = body.to_string();
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len(),
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });
        let temp = tempfile::tempdir().unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_tl"))
            .args(["--api-url", &format!("http://{address}"), "--api-key", "test-key", "sbx"])
            .args(args)
            .current_dir(temp.path())
            .env_remove("TENSORLAKE_GIT_TOKEN")
            .env("NO_COLOR", "1")
            .env("TZ", "UTC")
            .kill_on_drop(true)
            .output().await.unwrap();
        let mut requests = server.await.unwrap();
        requests.remove(0);
        CliRun { output, requests }
    }).await.expect("CLI and fixture must finish within 30 seconds")
}

fn request_line(request: &str) -> &str {
    request.lines().next().unwrap_or_default()
}

fn body(request: &str) -> Value {
    let (_, body) = request.split_once("\r\n\r\n").unwrap();
    serde_json::from_str(body).unwrap_or(Value::Null)
}

fn accepted(sandbox_id: &str) -> Value {
    json!({
        "sandbox_id": sandbox_id, "state": "pending", "pending_reason": "scheduling",
    })
}

fn info(status: &str, extra: Value) -> Value {
    let mut value = json!({
        "id": "sbx-1", "namespace": "default", "status": status,
        "resources": {"cpus": 1.0, "memory_mb": 1024, "disk_mb": 10240},
    });
    if let (Some(map), Some(more)) = (value.as_object_mut(), extra.as_object()) {
        map.extend(more.clone());
    }
    value
}

#[tokio::test]
async fn create_queue_sends_wait_false_and_prints_the_id() {
    let run = run_cli(
        &["create", "--queue", "--max-pending-secs", "1800"],
        vec![(202, accepted("sbx-queued"))],
    )
    .await;
    let stdout = String::from_utf8_lossy(&run.output.stdout);
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(run.output.status.success(), "{stderr}");
    assert_eq!(stdout.trim(), "sbx-queued");

    assert_eq!(run.requests.len(), 1, "no wait, no poll: one request");
    let create = &run.requests[0];
    assert!(
        request_line(create).starts_with("POST /v1/namespaces/"),
        "{create}"
    );
    let sent = body(create);
    assert_eq!(sent["wait"], false);
    assert_eq!(sent["max_pending_secs"], 1800);
}

#[tokio::test]
async fn create_no_wait_is_an_alias_for_queue() {
    let run = run_cli(&["create", "--no-wait"], vec![(202, accepted("sbx-1"))]).await;
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(run.output.status.success(), "{stderr}");
    assert_eq!(run.requests.len(), 1, "queued: one request, no poll");
    assert_eq!(body(&run.requests[0])["wait"], false);
}

#[tokio::test]
async fn create_omits_an_unset_bound() {
    let run = run_cli(&["create", "--queue"], vec![(202, accepted("sbx-1"))]).await;
    assert!(run.output.status.success());
    let sent = body(&run.requests[0]);
    assert_eq!(sent["wait"], false);
    assert!(sent.get("max_pending_secs").is_none(), "{sent}");
}

#[tokio::test]
async fn create_blocks_on_the_server_in_one_request() {
    let run = run_cli(
        &["create"],
        vec![(
            200,
            json!({
                "sandbox_id": "sbx-1", "status": "running",
                "sandbox_url": "https://sbx-1.sandbox.tensorlake.ai",
            }),
        )],
    )
    .await;
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(run.output.status.success(), "{stderr}");
    assert_eq!(String::from_utf8_lossy(&run.output.stdout).trim(), "sbx-1");
    assert_eq!(
        run.requests.len(),
        1,
        "the server held the request: no poll"
    );
    let sent = body(&run.requests[0]);
    assert!(
        sent.get("wait").is_none(),
        "a blocking create leaves the server default in place: {sent}"
    );
    assert!(sent.get("max_pending_secs").is_none());
}

#[tokio::test]
async fn create_timeout_leaves_the_sandbox_queued() {
    // The server's blocking wait ran out: 504 with the sandbox still pending.
    let run = run_cli(
        &["create"],
        vec![(
            504,
            json!({"sandbox_id": "sbx-1", "status": "timeout", "pending_reason": "no_resources_available"}),
        )],
    )
    .await;
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(!run.output.status.success(), "{stderr}");
    assert!(stderr.contains("sbx-1"), "{stderr}");
    assert!(stderr.contains("still pending"), "{stderr}");
    assert!(stderr.contains("no_resources_available"), "{stderr}");
    assert!(stderr.contains("tl sbx wait sbx-1"), "{stderr}");
    assert_eq!(run.requests.len(), 1);
    assert!(
        !run.requests
            .iter()
            .any(|r| request_line(r).starts_with("DELETE ")),
        "never cancels"
    );
}

#[tokio::test]
async fn create_polls_when_the_server_answers_early() {
    // A server that acknowledges before the sandbox runs still owes a
    // running sandbox: the CLI finishes the wait by polling.
    let run = run_cli(
        &["create"],
        vec![
            (202, accepted("sbx-1")),
            (
                200,
                info(
                    "pending",
                    json!({"pending_reason": "no_resources_available"}),
                ),
            ),
            (
                200,
                info(
                    "running",
                    json!({"sandbox_url": "https://sbx-1.sandbox.tensorlake.ai"}),
                ),
            ),
        ],
    )
    .await;
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(run.output.status.success(), "{stderr}");
    assert_eq!(String::from_utf8_lossy(&run.output.stdout).trim(), "sbx-1");

    assert_eq!(run.requests.len(), 3);
    assert!(body(&run.requests[0]).get("wait").is_none());
    for poll in &run.requests[1..] {
        assert_eq!(
            request_line(poll),
            "GET /v1/namespaces/default/sandboxes/sbx-1 HTTP/1.1"
        );
    }
    assert!(
        !run.requests
            .iter()
            .any(|r| request_line(r).starts_with("DELETE "))
    );
}

#[tokio::test]
async fn wait_that_runs_out_leaves_the_sandbox_queued() {
    let run = run_cli(
        &["wait", "sbx-1", "--timeout", "0"],
        vec![(
            200,
            info(
                "pending",
                json!({"pending_reason": "no_executors_available"}),
            ),
        )],
    )
    .await;
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(!run.output.status.success(), "{stderr}");
    assert!(stderr.contains("sbx-1"), "{stderr}");
    assert!(stderr.contains("still pending"), "{stderr}");
    assert!(stderr.contains("no_executors_available"), "{stderr}");
    assert!(stderr.contains("tl sbx wait sbx-1"), "{stderr}");
    assert_eq!(run.requests.len(), 1);
    assert_eq!(
        request_line(&run.requests[0]),
        "GET /v1/namespaces/default/sandboxes/sbx-1 HTTP/1.1"
    );
    assert!(
        !run.requests
            .iter()
            .any(|r| request_line(r).starts_with("DELETE ")),
        "never cancels"
    );
}

#[tokio::test]
async fn wait_prints_the_id_once_running() {
    let run = run_cli(
        &["wait", "sbx-1"],
        vec![(
            200,
            info(
                "running",
                json!({"sandbox_url": "https://sbx-1.sandbox.tensorlake.ai"}),
            ),
        )],
    )
    .await;
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(run.output.status.success(), "{stderr}");
    assert_eq!(String::from_utf8_lossy(&run.output.stdout).trim(), "sbx-1");
}

#[tokio::test]
async fn wait_surfaces_a_no_capacity_failure() {
    let run = run_cli(
        &["wait", "sbx-1"],
        vec![(
            200,
            info(
                "terminated",
                json!({
                    "termination_reason": "no_capacity", "pending_reason": "no_resources_available",
                    "error_details": "no host could place 4 CPUs within 1800s",
                }),
            ),
        )],
    )
    .await;
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(!run.output.status.success(), "{stderr}");
    assert!(stderr.contains("no_capacity"), "{stderr}");
    assert!(stderr.contains("no host could place"), "{stderr}");
}
