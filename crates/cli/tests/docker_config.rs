#![cfg(unix)]

use std::{os::unix::fs::PermissionsExt, path::Path, process::Command};

fn cli(directory: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_tl"));
    command
        .env("HOME", directory)
        .env("DOCKER_CONFIG", directory)
        .env("TENSORLAKE_CONTEXT", "")
        .env("TENSORLAKE_API_KEY", "test-key")
        .env("TENSORLAKE_ONPREM", "1")
        .env_remove("TENSORLAKE_PAT")
        .env("TENSORLAKE_TOKEN_STORAGE", "file")
        .env("PATH", format!("{}:/usr/bin:/bin", directory.display()));
    command
}

fn deployer(directory: &Path, contents: &str) {
    let path = directory.join("tensorlake-deploy");
    std::fs::write(&path, contents).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}

fn run_deployment(directory: &Path) -> std::process::Output {
    use std::{
        io::Write,
        process::Stdio,
        time::{Duration, Instant},
    };

    let temporary = directory.join("temporary");
    std::fs::create_dir(&temporary).unwrap();
    let mut child = cli(directory)
        .args(["deploy", "application.py"])
        .env("TMPDIR", &temporary)
        .env("TMP", &temporary)
        .env("TEMP", &temporary)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"application stdin")
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            panic!(
                "deployer timed out: {:?}",
                child.wait_with_output().unwrap()
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let output = child.wait_with_output().unwrap();
    assert!(std::fs::read_dir(temporary).unwrap().next().is_none());
    output
}

#[test]
fn deployment_transfers_snapshot_without_temporary_files_or_changing_stdin() {
    // Include a snapshot larger than pipe capacity to exercise concurrent writing.
    for repetitions in [0, 1, 10_000] {
        let directory = tempfile::tempdir().unwrap();
        if repetitions != 0 {
            let config = serde_json::json!({"auths": {"registry": {
                "identitytoken": "private-token-".repeat(repetitions)
            }}});
            std::fs::write(directory.path().join("config.json"), config.to_string()).unwrap();
        }
        deployer(
            directory.path(),
            &r#"#!/usr/bin/env python3
import json, os, stat, sys
from pathlib import Path
fd = int(os.environ.pop("TENSORLAKE_DOCKER_CONFIG_PIPE"))
assert stat.S_ISFIFO(os.fstat(fd).st_mode)
assert "TENSORLAKE_DOCKER_CONFIG_SNAPSHOT" not in os.environ
assert not any("private-token-" in value for value in [*os.environ.values(), *sys.argv])
assert not list(Path(os.environ["TMPDIR"]).iterdir())
os.set_blocking(fd, False)
try:
    os.read(fd, 1)
    raise AssertionError("pipe received data or EOF before negotiation")
except BlockingIOError:
    pass
os.set_blocking(fd, True)
Path(os.environ["DOCKER_CONFIG"], "config.json").write_text("invalid replacement")
print(json.dumps({"type": "protocol", "version": 1, "docker_config_pipe": True}), flush=True)
with os.fdopen(fd) as pipe:
    captured = json.load(pipe)
expected = {"auths": {"registry": {"identitytoken": "private-token-" * REPETITIONS}}} if REPETITIONS else {}
assert captured == expected
assert sys.stdin.read() == "application stdin"
assert not list(Path(os.environ["TMPDIR"]).iterdir())
"#.replace("REPETITIONS", &repetitions.to_string()),
        );
        let output = run_deployment(directory.path());
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn legacy_deployers_discover_credentials_without_receiving_a_snapshot() {
    for capability in ["", r#", "docker_config_pipe": false"#] {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("config.json"),
            r#"{"auths":{"registry":{"identitytoken":"private-token"}}}"#,
        )
        .unwrap();
        deployer(
            directory.path(),
            &r#"#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
fd = int(os.environ["TENSORLAKE_DOCKER_CONFIG_PIPE"])
print('{"type":"protocol","version":1CAPABILITY}', flush=True)
with os.fdopen(fd) as pipe:
    assert pipe.read() == "", "legacy deployers must never receive credentials"
config = Path(os.environ["DOCKER_CONFIG"], "config.json")
config.write_text('{"auths":{"registry":{"identitytoken":"replacement-token"}}}')
assert json.loads(config.read_text())["auths"]["registry"]["identitytoken"] == "replacement-token"
assert sys.stdin.read() == "application stdin"
assert not list(Path(os.environ["TMPDIR"]).iterdir())
"#
            .replace("CAPABILITY", capability),
        );
        let output = run_deployment(directory.path());
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn incompatible_protocol_receives_no_credentials() {
    let directory = tempfile::tempdir().unwrap();
    deployer(
        directory.path(),
        r#"#!/usr/bin/env python3
import json, os
fd = int(os.environ["TENSORLAKE_DOCKER_CONFIG_PIPE"])
os.set_blocking(fd, False)
try:
    os.read(fd, 1)
    raise AssertionError("credentials sent before handshake")
except BlockingIOError:
    pass
print(json.dumps({"type":"protocol", "version":2, "docker_config_pipe":True}), flush=True)
"#,
    );
    let output = run_deployment(directory.path());
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("incompatible with this CLI"));
}

#[test]
fn closed_snapshot_pipe_stops_deployment_without_leaking_credentials() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("config.json"),
        r#"{"auths":{"registry":{"identitytoken":"private-token"}}}"#,
    )
    .unwrap();
    deployer(
        directory.path(),
        r#"#!/usr/bin/env python3
import json, os, time
os.close(int(os.environ["TENSORLAKE_DOCKER_CONFIG_PIPE"]))
print(json.dumps({"type":"protocol", "version":1, "docker_config_pipe":True}), flush=True)
time.sleep(30)
"#,
    );
    let output = run_deployment(directory.path());
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("Failed to send Docker config"), "{error}");
    assert!(!error.contains("private-token"));
}

#[test]
fn malformed_config_fails_at_image_command_startup_but_does_not_affect_version() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(
        directory.path().join("config.json"),
        r#"{"auths":"secret"}"#,
    )
    .unwrap();
    for args in [
        vec!["sbx", "image", "create", "/nonexistent/Dockerfile"],
        vec!["sbx", "image", "import", "registry.example.com/app"],
    ] {
        let output = cli(directory.path()).args(args).output().unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains("Invalid Docker config JSON"), "{error}");
        assert!(!error.contains("secret"));
    }
    assert!(
        cli(directory.path())
            .arg("--version")
            .output()
            .unwrap()
            .status
            .success()
    );
}
