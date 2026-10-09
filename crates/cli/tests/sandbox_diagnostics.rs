use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Command,
    time::{Duration, timeout},
};

const DIAGNOSIS: &str = "Cannot mount /tools: overlaps the registered mount /tools. Remove the conflicting registration.";

async fn run_cli(args: &[&str], responses: Vec<(u16, Value)>) -> std::process::Output {
    timeout(Duration::from_secs(20), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            // API key introspection precedes the sandbox request.
            let responses = std::iter::once((200, json!({
                "organizationId": "org-test", "projectId": "project-test",
            }))).chain(responses);
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
                stream.read_exact(&mut vec![0; length]).await.unwrap();
                let body = body.to_string();
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len(),
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
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
        server.await.unwrap();
        output
    }).await.expect("CLI and fixture must finish within 20 seconds")
}

#[tokio::test]
async fn create_and_copy_print_the_reason_and_diagnostic() {
    let failure = json!({
        "sandbox_id": "sbx-config", "status": "failed",
        "reason": "ConfigurationError", "error_details": DIAGNOSIS,
    });
    for (args, status, body) in [
        (vec!["create"], 422, failure.clone()),
        (
            vec!["copy", "sbx-source"],
            422,
            json!({
                "source_sandbox_id": "sbx-source", "sandboxes": [failure.clone()],
            }),
        ),
        (
            vec!["copy", "sbx-source"],
            207,
            json!({
                "source_sandbox_id": "sbx-source", "sandboxes": [
                    {"sandbox_id": "sbx-running", "status": "running"}, failure,
                ],
            }),
        ),
    ] {
        let output = run_cli(&args, vec![(status, body)]).await;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{stderr}");
        assert!(stderr.contains("sbx-config"), "{stderr}");
        assert!(stderr.contains("ConfigurationError"), "{stderr}");
        assert!(stderr.contains(DIAGNOSIS), "{stderr}");
        assert!(!stderr.contains("error_details"), "{stderr}");
    }
}

#[tokio::test]
async fn legacy_create_errors_keep_their_diagnosis() {
    let output = run_cli(
        &["create"],
        vec![(
            422,
            json!({
                "sandbox_id": "sbx-legacy", "status": "failed", "message": "legacy diagnosis",
            }),
        )],
    )
    .await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains("legacy diagnosis"), "{stderr}");
}

#[tokio::test]
async fn describe_prints_live_and_archived_diagnostics() {
    let sandbox = json!({
        "id": "sbx-config", "status": "terminated",
        "termination_reason": "ConfigurationError", "error_details": DIAGNOSIS,
    });
    for responses in [
        vec![(200, sandbox.clone())],
        vec![(404, json!({})), (200, sandbox)],
    ] {
        let output = run_cli(&["describe", "sbx-config"], responses).await;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stdout.contains("Reason:          ConfigurationError"),
            "{stdout}"
        );
        assert!(
            stdout.contains(&format!("Error details:   {DIAGNOSIS}")),
            "{stdout}"
        );
    }
}

#[tokio::test]
async fn create_wait_reports_a_typed_termination_with_legacy_details() {
    let output = run_cli(
        &["create"],
        vec![
            (
                200,
                json!({"sandbox_id": "sbx-config", "status": "pending"}),
            ),
            (
                200,
                json!({
                    "sandbox_id": "sbx-config", "status": "terminated",
                    "termination_reason": "ConfigurationError",
                    "error_details": [{"message": DIAGNOSIS}, "Rebuild the snapshot."],
                    "future_field": {"ignored": true},
                }),
            ),
        ],
    )
    .await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(
        stderr.contains(&format!("Sandbox failed to reach 'running': ConfigurationError: {DIAGNOSIS}; Rebuild the snapshot.")),
        "{stderr}"
    );
}

#[tokio::test]
async fn describe_preserves_legacy_fields_and_timestamp_formats() {
    for created_at in [
        json!("2020-01-02T12:00:00Z"),
        json!(1577966400),
        json!(1577966400000_u64),
        json!(1577966400000000_u64),
    ] {
        let output = run_cli(
            &["describe", "sbx-legacy"],
            vec![(200, json!({
                "id": "sbx-legacy", "status": "terminated", "image": "example-image",
                "namespace": "example-namespace", "name": "example-name",
                "resources": {"cpus": 0.5, "memory_mb": 1024, "ephemeral_disk_mb": 2048},
                "allow_unauthenticated_proxy_access": true,
                "network_policy": {"allow_internet_access": false, "allow_out": ["allowed.example"], "deny_out": ["denied.example"]},
                "created_at": created_at, "terminated_at": null,
                "timeout_secs": 120, "sandboxUrl": "https://sbx-legacy.example.com",
                "entrypoint": ["/bin/sh", "-l"], "exposedPorts": [8080, 9090],
                "termination_reason": "FutureReason",
                "error_details": {"message": DIAGNOSIS, "phase": "mount"},
                "outcome": "failure", "future_field": {"ignored": true},
            }))],
        ).await;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        for line in [
            "ID:              sbx-legacy",
            "Name:            example-name",
            "Namespace:       example-namespace",
            "Image:           example-image",
            "CPUs:            0.5",
            "Memory:          1024 MiB",
            "Disk:            2048 MiB",
            "Proxy auth:      unauthenticated",
            "Internet:        blocked",
            "Created:         2020-01-02",
            "Timeout:         120s",
            "URL:             https://sbx-legacy.example.com",
            "Entrypoint:      /bin/sh -l",
            "Ports:           8080, 9090",
            "Allow out:       allowed.example",
            "Deny out:        denied.example",
            "Reason:          FutureReason",
            "Outcome:         failure",
        ] {
            assert!(stdout.contains(line), "Missing {line:?}: {stdout}");
        }
        assert!(
            stdout.contains(&format!("Error details:   {DIAGNOSIS}")),
            "{stdout}"
        );
    }
}

#[tokio::test]
async fn describe_running_sandbox_still_prints_ssh_config() {
    let output = run_cli(
        &["describe", "sbx-running"],
        vec![(
            200,
            json!({
                "sandbox_id": "sbx-running", "status": "running",
                "sandbox_url": "https://sbx-running.example.com",
            }),
        )],
    )
    .await;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stdout.contains("SSH Config:"), "{stdout}");
    assert!(
        stdout.contains("HostName sbx-running.example.com"),
        "{stdout}"
    );
    assert!(stdout.contains("User sbx-running"), "{stdout}");
    assert!(!stdout.contains("Error details:"), "{stdout}");
}

#[tokio::test]
async fn describe_prints_gpu_allocations_for_live_and_archived_sandboxes() {
    for (field, count, model) in [("gpus", 2, "H100-PCIe-80GB"), ("gpu_configs", 1, "A10")] {
        for archived in [false, true] {
            let mut sandbox = json!({
                "sandbox_id": "sbx-gpu", "pool_id": "pool-gpu",
                "status": if archived { "terminated" } else { "running" },
                "sandbox_url": "https://sbx-gpu.example.com",
                "resources": {"cpus": 2, "memory_mb": 4096, "disk_mb": 20480},
            });
            sandbox["resources"][field] = json!([{"count": count, "model": model}]);
            let responses = if archived {
                vec![(404, json!({})), (200, sandbox)]
            } else {
                vec![(200, sandbox)]
            };
            let output = run_cli(&["describe", "sbx-gpu"], responses).await;
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "{stdout}\n{stderr}");
            assert!(
                stdout
                    .lines()
                    .any(|line| line == format!("GPUs:            {count} x {model}")),
                "{stdout}"
            );
            for line in [
                "CPUs:            2",
                "Memory:          4096 MiB",
                "Disk:            20480 MiB",
            ] {
                assert!(stdout.contains(line), "{stdout}");
            }
        }
    }
}

#[tokio::test]
async fn describe_handles_cpu_and_legacy_resources_without_gpus() {
    for resources in [
        json!({"cpus": 1, "memory_mb": 1024, "disk_mb": 10240}),
        json!({"gpus": []}),
        json!({"gpu_configs": null}),
        Value::Null,
    ] {
        let output = run_cli(
            &["describe", "sbx-cpu"],
            vec![(
                200,
                json!({
                    "sandbox_id": "sbx-cpu", "status": "terminated", "resources": resources,
                }),
            )],
        )
        .await;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert!(
            stdout.lines().any(|line| line == "GPUs:            -"),
            "{stdout}"
        );
    }
}

#[tokio::test]
async fn ls_prints_gpu_count_and_model_in_live_and_archived_columns() {
    for field in ["gpus", "gpu_configs"] {
        for archived in [false, true] {
            let mut gpu_sandbox = json!({
                "sandbox_id": "sbx-gpu", "pool_id": "pool-gpu",
                "status": "running", "image": "gpu-image",
                "resources": {"cpus": 2, "memory_mb": 4096, "disk_mb": 20480},
            });
            gpu_sandbox["resources"][field] = json!([{"count": 2, "model": "H100-PCIe-80GB"}]);
            let body = json!({"sandboxes": [
                gpu_sandbox,
                {"sandbox_id": "sbx-cpu", "status": "running", "resources": {"gpus": []}},
                {"sandbox_id": "sbx-legacy", "status": "running", "resources": {}},
                {"sandbox_id": "sbx-null", "status": "running", "resources": {"gpu_configs": null}},
            ]});
            let args = if archived {
                vec!["ls", "--archived"]
            } else {
                vec!["ls"]
            };
            let output = run_cli(&args, vec![(200, body)]).await;
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(output.status.success(), "{stdout}\n{stderr}");
            let mut lines = stdout.lines();
            let header: Vec<_> = lines.next().unwrap().split('\t').collect();
            let gpu_column = header
                .iter()
                .position(|name| *name == "GPUs")
                .expect("GPUs column");
            for (id, allocation) in [
                ("sbx-gpu", "2 x H100-PCIe-80GB"),
                ("sbx-cpu", "-"),
                ("sbx-legacy", "-"),
                ("sbx-null", "-"),
            ] {
                let row: Vec<_> = lines.next().unwrap().split('\t').collect();
                assert_eq!(row.len(), header.len(), "{stdout}");
                assert_eq!(row[0], id, "{stdout}");
                assert_eq!(row[gpu_column], allocation, "{stdout}");
                if id == "sbx-gpu" {
                    for (name, value) in
                        [("CPUs", "2"), ("Memory", "4096 MiB"), ("Disk", "20480 MiB")]
                    {
                        let column = header.iter().position(|field| *field == name).unwrap();
                        assert_eq!(row[column], value, "{stdout}");
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn ls_quiet_keeps_ids_only_for_gpu_and_cpu_sandboxes() {
    for archived in [false, true] {
        let args = if archived {
            vec!["ls", "--archived", "--quiet"]
        } else {
            vec!["ls", "--quiet"]
        };
        let output = run_cli(&args, vec![(200, json!({"sandboxes": [
            {"sandbox_id": "sbx-gpu", "status": "running", "resources": {"gpus": [{"count": 1, "model": "A10"}]}},
            {"sandbox_id": "sbx-cpu", "status": "running"},
        ]}))]).await;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{stdout}\n{stderr}");
        assert_eq!(stdout, "sbx-gpu\nsbx-cpu\n");
    }
}
