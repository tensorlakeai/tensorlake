//! A scripted, recording HTTP fixture for `tl sbx image ...` tests.
//!
//! Replies are matched by method and path prefix (first unused match), so
//! requests the CLI issues concurrently (the merged `ls`) and requests whose
//! order the test does not care about are both handled. Every request is
//! recorded — including unscripted ones, which are answered 404 — so a test
//! can assert on what was sent and, above all, on what was never sent.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Command,
    time::{Duration, timeout},
};

#[derive(Clone)]
pub enum Mode {
    Respond,
    /// Read the request, then drop the connection without answering.
    Reset,
    /// Read the request, hold the connection for the duration, then drop it.
    Hang(Duration),
}

#[derive(Clone)]
pub struct Reply {
    pub method: &'static str,
    pub path_prefix: &'static str,
    pub status: u16,
    pub body: Value,
    pub headers: Vec<(String, String)>,
    pub mode: Mode,
}

pub fn reply(method: &'static str, path_prefix: &'static str, status: u16, body: Value) -> Reply {
    Reply {
        method,
        path_prefix,
        status,
        body,
        headers: Vec::new(),
        mode: Mode::Respond,
    }
}

pub fn reset(method: &'static str, path_prefix: &'static str) -> Reply {
    Reply {
        mode: Mode::Reset,
        ..reply(method, path_prefix, 0, Value::Null)
    }
}

pub fn hang(method: &'static str, path_prefix: &'static str, duration: Duration) -> Reply {
    Reply {
        mode: Mode::Hang(duration),
        ..reply(method, path_prefix, 0, Value::Null)
    }
}

/// API key introspection precedes every authenticated command.
pub fn introspection() -> Reply {
    reply(
        "POST",
        "/platform/v1/keys/introspect",
        200,
        json!({"organizationId": "org-test", "projectId": "project-test"}),
    )
}

#[derive(Debug, Clone)]
pub struct Recorded {
    pub line: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Recorded {
    pub fn method(&self) -> &str {
        self.line.split(' ').next().unwrap_or_default()
    }

    pub fn path(&self) -> &str {
        self.line.split(' ').nth(1).unwrap_or_default()
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    pub fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap_or(Value::Null)
    }
}

pub struct Server {
    pub url: String,
    requests: Arc<Mutex<Vec<Recorded>>>,
    pending: Arc<Mutex<Vec<Option<Reply>>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    /// Add scripted replies after the server listens (for replies that
    /// must embed the server's own address).
    pub fn script(&self, replies: Vec<Reply>) {
        self.pending
            .lock()
            .unwrap()
            .extend(replies.into_iter().map(Some));
    }

    /// Wait until a request whose path starts with `path_prefix` has been
    /// recorded (the fixture records before replying or hanging), so a test
    /// can act at a known point of the CLI's progress.
    pub async fn wait_for(&self, method: &str, path_prefix: &str) {
        timeout(Duration::from_secs(30), async {
            loop {
                if self.requests.lock().unwrap().iter().any(|request| {
                    request.method() == method && request.path().starts_with(path_prefix)
                }) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no {method} {path_prefix} request within 30 seconds"));
    }

    /// Stop listening and return every request seen, in arrival order,
    /// without the API key introspection.
    pub async fn finish(self) -> Vec<Recorded> {
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.task.abort();
        let _ = self.task.await;
        let mut requests = std::mem::take(&mut *self.requests.lock().unwrap());
        requests.retain(|request| !request.path().starts_with("/platform/v1/keys/introspect"));
        requests
    }
}

pub async fn start(replies: Vec<Reply>) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let requests: Arc<Mutex<Vec<Recorded>>> = Arc::default();
    let record = requests.clone();
    let pending: Arc<Mutex<Vec<Option<Reply>>>> =
        Arc::new(Mutex::new(replies.into_iter().map(Some).collect()));
    let pending_handle = pending.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let record = record.clone();
            let pending = pending.clone();
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut byte = [0_u8];
                while !raw.ends_with(b"\r\n\r\n") {
                    if raw.len() > 65536 || stream.read_exact(&mut byte).await.is_err() {
                        return;
                    }
                    raw.push(byte[0]);
                }
                let head = String::from_utf8_lossy(&raw).to_string();
                let mut lines = head.lines();
                let line = lines.next().unwrap_or_default().to_string();
                let headers: Vec<(String, String)> = lines
                    .filter_map(|header| header.split_once(':'))
                    .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
                    .collect();
                let length = headers
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                    .and_then(|(_, value)| value.parse::<usize>().ok())
                    .unwrap_or(0);
                let mut body = vec![0_u8; length];
                if length > 0 && stream.read_exact(&mut body).await.is_err() {
                    return;
                }
                let recorded = Recorded {
                    line: line.clone(),
                    headers,
                    body: String::from_utf8_lossy(&body).to_string(),
                };
                record.lock().unwrap().push(recorded.clone());
                let matched = {
                    let mut pending = pending.lock().unwrap();
                    pending
                        .iter_mut()
                        .find(|slot| {
                            slot.as_ref().is_some_and(|reply| {
                                recorded.method() == reply.method
                                    && recorded.path().starts_with(reply.path_prefix)
                            })
                        })
                        .and_then(Option::take)
                };
                let reply = matched.unwrap_or_else(|| {
                    reply(
                        "",
                        "",
                        404,
                        json!({"error": format!("unscripted request: {line}")}),
                    )
                });
                match reply.mode {
                    Mode::Reset => return,
                    Mode::Hang(duration) => {
                        tokio::time::sleep(duration).await;
                        return;
                    }
                    Mode::Respond => {}
                }
                let body = reply.body.to_string();
                let mut response = format!(
                    "HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
                    reply.status,
                    body.len(),
                );
                for (key, value) in &reply.headers {
                    response.push_str(&format!("{key}: {value}\r\n"));
                }
                response.push_str("\r\n");
                response.push_str(&body);
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    Server {
        url: format!("http://{address}"),
        requests,
        pending: pending_handle,
        task,
    }
}

/// `tl --api-url <server> --api-key test-key sbx <args>` in `cwd`, with the
/// Image Service URL override and docker config isolated.
pub fn command(server_url: &str, args: &[&str], cwd: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tl"));
    command
        .args(["--api-url", server_url, "--api-key", "test-key", "sbx"])
        .args(args)
        .current_dir(cwd)
        .env_remove("TENSORLAKE_GIT_TOKEN")
        .env_remove("TENSORLAKE_IMAGE_SERVICE_URL")
        .env_remove("TENSORLAKE_SANDBOX_API_URL")
        .env("DOCKER_CONFIG", cwd)
        .env("NO_COLOR", "1")
        .env("TZ", "UTC")
        .kill_on_drop(true);
    command
}

pub struct CliRun {
    pub output: std::process::Output,
    pub requests: Vec<Recorded>,
}

impl CliRun {
    pub fn stdout(&self) -> String {
        String::from_utf8_lossy(&self.output.stdout).to_string()
    }

    pub fn stderr(&self) -> String {
        String::from_utf8_lossy(&self.output.stderr).to_string()
    }

    pub fn code(&self) -> Option<i32> {
        self.output.status.code()
    }

    /// Requests sent to the Image Service ingress path.
    pub fn image_service_requests(&self) -> Vec<&Recorded> {
        self.requests
            .iter()
            .filter(|request| request.path().starts_with("/images/v4/"))
            .collect()
    }

    pub fn lines(&self) -> Vec<String> {
        self.requests
            .iter()
            .map(|request| request.line.clone())
            .collect()
    }
}

/// Run the CLI to completion against a fresh scripted server.
pub async fn run(args: &[&str], replies: Vec<Reply>, cwd: &std::path::Path) -> CliRun {
    timeout(Duration::from_secs(60), async {
        let server = start(replies).await;
        let output = command(&server.url, args, cwd).output().await.unwrap();
        let requests = server.finish().await;
        CliRun { output, requests }
    })
    .await
    .expect("CLI and fixture must finish within 60 seconds")
}

/// A build record as the Image Service reports it.
pub fn build_json(build_id: &str, status: &str, result: Option<Value>) -> Value {
    json!({
        "build_id": build_id,
        "client_request_id": "rid-1",
        "project": "project-test",
        "image_scope": "executor_fleet",
        "kind": "import",
        "name": "img",
        "status": status,
        "phase": if status == "succeeded" { "finished" } else { "executing" },
        "attempt_no": 1,
        "builder_sandbox_id": null,
        "builder_resources": {"cpus": 2.0, "memory_mb": 4096},
        "result": result,
        "cleanup_pending": false,
        "phase_deadline_ms": null,
        "operation_deadline_ms": null,
        "created_at_ms": 1_760_000_000_000_u64,
        "updated_at_ms": 1_760_000_100_000_u64,
    })
}

/// The Image Service's tenant record for a published image.
pub fn image_record(public_id: &str, name: &str) -> Value {
    json!({
        "id": public_id,
        "name": name,
        "snapshot_id": "snap-cas",
        "snapshot_uri": "snapshot://snap-cas",
        "snapshot_sandbox_id": null,
        "snapshot_format_version": "content_addressed_streaming_v1",
        "snapshot_size_bytes": 42,
        "rootfs_disk_bytes": 10_737_418_240_u64,
        "rootfs_node_kind": "base",
        "public": false,
        "dockerfile": "",
        "created_by_user_id": null,
        "created_at": "2026-10-15T00:00:00.000Z",
        "updated_at": "2026-10-15T00:00:00.000Z",
        "image_id": "a".repeat(64),
        "build_id": "build-1",
    })
}

/// A legacy platform sandbox template.
pub fn template_record(id: &str, name: &str) -> Value {
    json!({
        "id": id,
        "name": name,
        "snapshotId": "snap-legacy",
        "snapshotSandboxId": "sbx-legacy",
        "snapshotUri": "s3://snapshots/snap-legacy.tar.zst",
        "snapshotFormatVersion": "durable_archive_v1",
        "snapshotSizeBytes": 123,
        "rootfsDiskBytes": 10_737_418_240_u64,
        "rootfsNodeKind": "base",
        "public": false,
        "dockerfile": "FROM ubuntu:24.04\n",
        "createdAt": "2026-10-01T00:00:00Z",
        "updatedAt": "2026-10-01T00:00:00Z",
    })
}

pub fn write_dockerfile(dir: &std::path::Path, text: &str) -> std::path::PathBuf {
    let path = dir.join("Dockerfile");
    std::fs::write(&path, text).unwrap();
    path
}
