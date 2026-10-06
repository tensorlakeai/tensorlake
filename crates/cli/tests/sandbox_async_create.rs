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

fn resize_info(status: &str, memory: u64, error: Option<&str>) -> Value {
    let mut result = info(
        "running",
        json!({"runtime":"cloud-hypervisor", "sandbox_url":"https://sbx-1.sandbox.tensorlake.ai"}),
    );
    result["resources"]["memory_mb"] = memory.into();
    result["resource_resize"] = json!({
        "generation": 7, "status": status, "error_message": error,
        "requested": {"cpus":1.0,"memory_mb":2048,"disk_mb":10240},
    });
    result
}

#[tokio::test]
async fn resize_waits_by_default_and_only_prints_success_after_confirmation() {
    let current = info("running", json!({}));
    let run = run_cli(
        &["update", "sbx-1", "-m", "2048", "-c", "1.0"],
        vec![
            (200, current),
            (200, resize_info("pending", 1024, None)),
            (200, resize_info("succeeded", 2048, None)),
        ],
    )
    .await;
    let stdout = String::from_utf8_lossy(&run.output.stdout);
    assert!(
        run.output.status.success(),
        "{}",
        String::from_utf8_lossy(&run.output.stderr)
    );
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert_eq!(stdout.trim(), "sbx-1");
    assert!(stderr.contains("generation 7: pending"), "{stderr}");
    assert!(
        stderr.contains("Requested: 1 CPUs, 2048 MiB memory"),
        "{stderr}"
    );
    assert!(stderr.contains("generation 7: succeeded"), "{stderr}");
    assert!(
        stderr.contains("Confirmed allocation: 1 CPUs, 2048 MiB memory"),
        "{stderr}"
    );
    assert!(!stderr.contains("1024 MiB memory"), "{stderr}");
    assert!(!stderr.contains("Inspect with:"), "{stderr}");
    assert_eq!(
        run.requests.len(),
        3,
        "one preflight GET, PATCH, completion GET"
    );
    assert_eq!(
        body(&run.requests[1]),
        json!({"resources":{"memory_mb":2048}})
    );
    assert_eq!(
        run.requests
            .iter()
            .filter(|r| request_line(r).starts_with("PATCH "))
            .count(),
        1
    );
}

#[tokio::test]
async fn resize_no_wait_prints_admission_without_claiming_completion() {
    let current = info("running", json!({}));
    let run = run_cli(
        &["update", "sbx-1", "--memory", "2048", "--no-wait"],
        vec![(200, current), (200, resize_info("pending", 1024, None))],
    )
    .await;
    let stdout = String::from_utf8_lossy(&run.output.stdout);
    assert!(
        run.output.status.success(),
        "{}",
        String::from_utf8_lossy(&run.output.stderr)
    );
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert_eq!(stdout.trim(), "sbx-1");
    assert!(stderr.contains("generation 7: pending"));
    assert!(!stderr.contains("succeeded"));
    assert!(stderr.contains("Requested: 1 CPUs, 2048 MiB memory"));
    assert!(!stderr.contains("Confirmed allocation"));
    assert!(
        stderr
            .lines()
            .any(|line| line == "Wait with: tl sbx wait sbx-1 --resize 7 --timeout 300")
    );
    assert!(stderr.contains("tl sbx describe sbx-1"));
    assert_eq!(run.requests.len(), 2);
}

#[tokio::test]
async fn resize_driver_failure_and_timeout_exit_nonzero_without_cancellation() {
    for failed in [true, false] {
        let current = info("running", json!({}));
        let mut responses = vec![(200, current), (200, resize_info("pending", 1024, None))];
        let args = if failed {
            responses.push((
                200,
                resize_info(
                    "failed",
                    1536,
                    Some("ConfigurationError: pinned guest memory"),
                ),
            ));
            vec!["update", "sbx-1", "--memory", "2048"]
        } else {
            responses.push((200, resize_info("pending", 1024, None)));
            vec!["update", "sbx-1", "--memory", "2048", "--wait-timeout", "0"]
        };
        let run = run_cli(&args, responses).await;
        let stderr = String::from_utf8_lossy(&run.output.stderr);
        assert!(!run.output.status.success());
        assert!(stderr.contains("generation 7"), "{stderr}");
        assert!(
            stderr.contains(if failed {
                "pinned guest memory"
            } else {
                "not cancelled"
            }),
            "{stderr}"
        );
        assert!(run.output.stdout.is_empty());
        if failed {
            assert!(stderr.contains("1536 MiB memory"), "{stderr}");
        } else {
            assert!(stderr.contains("1024 MiB memory"), "{stderr}");
            let lines: Vec<_> = stderr.lines().collect();
            assert_eq!(
                lines[lines.len() - 2],
                "Wait with: tl sbx wait sbx-1 --resize 7 --timeout 300"
            );
            assert_eq!(
                lines[lines.len() - 1],
                "Inspect with: tl sbx describe sbx-1"
            );
            assert!(lines[lines.len() - 3].contains("last confirmed allocation"));
        }
        assert!(
            !run.requests
                .iter()
                .any(|r| request_line(r).starts_with("DELETE "))
        );
    }
}

#[tokio::test]
async fn resize_rejects_fractional_cpu_and_disk_shrink_without_patch() {
    for (args, responses, diagnostic) in [
        (vec!["update", "sbx-1", "--cpus", "1.5"], vec![], "1.5"),
        (
            vec!["update", "sbx-1", "--disk_mb", "100"],
            vec![(200, info("running", json!({})))],
            "current 10240",
        ),
    ] {
        let run = run_cli(&args, responses).await;
        assert!(!run.output.status.success());
        assert!(String::from_utf8_lossy(&run.output.stderr).contains(diagnostic));
        assert!(
            !run.requests
                .iter()
                .any(|r| request_line(r).starts_with("PATCH "))
        );
    }
}

#[tokio::test]
async fn describe_displays_resize_generation_and_driver_error() {
    let run = run_cli(
        &["describe", "sbx-1"],
        vec![(
            200,
            resize_info("failed", 1536, Some("below boot-memory floor")),
        )],
    )
    .await;
    let stdout = String::from_utf8_lossy(&run.output.stdout);
    assert!(
        run.output.status.success(),
        "{}",
        String::from_utf8_lossy(&run.output.stderr)
    );
    assert!(
        stdout.contains("Runtime:         cloud-hypervisor"),
        "{stdout}"
    );
    assert!(stdout.contains("Memory:          1536 MiB"), "{stdout}");
    assert!(stdout.contains("Disk:            10240 MiB"), "{stdout}");
    assert!(stdout.contains("generation 7: failed"), "{stdout}");
    assert!(stdout.contains("failed"), "{stdout}");
    assert!(stdout.contains("below boot-memory floor"), "{stdout}");
}

#[tokio::test]
async fn resize_noop_uses_one_get_and_never_reports_a_previous_failure_as_current() {
    for previous in [
        Value::Null,
        resize_info("failed", 1024, Some("old failure"))["resource_resize"].clone(),
    ] {
        let run = run_cli(
            &["update", "named", "--memory", "1024"],
            vec![(200, info("running", json!({"resource_resize":previous})))],
        )
        .await;
        let stderr = String::from_utf8_lossy(&run.output.stderr);
        assert!(run.output.status.success(), "{stderr}");
        assert_eq!(String::from_utf8_lossy(&run.output.stdout).trim(), "sbx-1");
        assert!(
            stderr.contains("No resource changes for sandbox sbx-1"),
            "{stderr}"
        );
        assert!(stderr.contains("Confirmed allocation"), "{stderr}");
        assert!(!stderr.contains("failed"), "{stderr}");
        assert_eq!(run.requests.len(), 1);
        assert!(request_line(&run.requests[0]).starts_with("GET "));
    }
}

#[tokio::test]
async fn wait_resize_observes_exact_generation_without_another_update() {
    let run = run_cli(
        &["wait", "named", "--resize", "7"],
        vec![
            (200, resize_info("pending", 1024, None)),
            (200, resize_info("succeeded", 2048, None)),
        ],
    )
    .await;
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(run.output.status.success(), "{stderr}");
    assert_eq!(String::from_utf8_lossy(&run.output.stdout).trim(), "sbx-1");
    assert!(stderr.contains("generation 7: succeeded"), "{stderr}");
    assert!(
        stderr.contains("Confirmed allocation: 1 CPUs, 2048 MiB memory"),
        "{stderr}"
    );
    assert_eq!(run.requests.len(), 2);
    assert!(
        run.requests
            .iter()
            .all(|r| request_line(r).starts_with("GET "))
    );
}

#[tokio::test]
async fn wait_resize_reports_failure_superseding_and_resumable_timeout() {
    let mut superseded = resize_info("succeeded", 2048, None);
    superseded["resource_resize"]["generation"] = 8.into();
    for (responses, timeout, diagnostic) in [
        (
            vec![(
                200,
                resize_info("failed", 1536, Some("pinned guest memory")),
            )],
            "10",
            "pinned guest memory",
        ),
        (vec![(200, superseded)], "10", "superseded"),
        (
            vec![(200, resize_info("pending", 1024, None))],
            "0",
            "tl sbx wait sbx-1 --resize 7 --timeout 300",
        ),
    ] {
        let run = run_cli(
            &["wait", "named", "--resize", "7", "--timeout", timeout],
            responses,
        )
        .await;
        let stderr = String::from_utf8_lossy(&run.output.stderr);
        assert!(!run.output.status.success());
        assert!(run.output.stdout.is_empty());
        assert_eq!(run.requests.len(), 1, "a zero budget still checks once");
        assert!(stderr.contains(diagnostic), "{stderr}");
        assert!(
            run.requests
                .iter()
                .all(|r| request_line(r).starts_with("GET "))
        );
    }
}

#[tokio::test]
async fn wait_resize_zero_timeout_observes_an_already_completed_resize() {
    let run = run_cli(
        &["wait", "named", "--resize", "7", "--timeout", "0"],
        vec![(200, resize_info("succeeded", 2048, None))],
    )
    .await;
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    assert!(run.output.status.success(), "{stderr}");
    assert_eq!(String::from_utf8_lossy(&run.output.stdout).trim(), "sbx-1");
    assert!(stderr.contains("generation 7: succeeded"), "{stderr}");
    assert_eq!(run.requests.len(), 1);
    assert!(request_line(&run.requests[0]).starts_with("GET "));
}
