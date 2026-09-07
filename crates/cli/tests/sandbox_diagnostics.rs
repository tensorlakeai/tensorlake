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
    for (args, body) in [
        (vec!["create"], failure.clone()),
        (
            vec!["copy", "sbx-source"],
            json!({
                "source_sandbox_id": "sbx-source", "sandboxes": [failure],
            }),
        ),
    ] {
        let output = run_cli(&args, vec![(422, body)]).await;
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(!output.status.success(), "{stderr}");
        assert!(stderr.contains("sbx-config"), "{stderr}");
        assert!(stderr.contains("ConfigurationError"), "{stderr}");
        assert!(stderr.contains(DIAGNOSIS), "{stderr}");
        assert!(!stderr.contains("error_details"), "{stderr}");
    }
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
