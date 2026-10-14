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

#[test]
fn deployment_uses_startup_contents_even_if_original_config_changes() {
    for config in [
        None,
        Some(r#"{"auths":{"registry":{"identitytoken":"test-token"}}}"#),
    ] {
        let directory = tempfile::tempdir().unwrap();
        if let Some(config) = config {
            std::fs::write(directory.path().join("config.json"), config).unwrap();
        }
        deployer(
            directory.path(),
            r#"#!/bin/sh
printf 'invalid replacement' > "$DOCKER_CONFIG/config.json"
test "$(cat "$TENSORLAKE_DOCKER_CONFIG_SNAPSHOT")" = "$EXPECTED_CONFIG" || exit 3
printf '%s' "$TENSORLAKE_DOCKER_CONFIG_SNAPSHOT" > "$SNAPSHOT_PATH_RECORD"
printf '%s\n' '{"type":"protocol","version":1,"docker_config_snapshot":true}'
"#,
        );
        let record = directory.path().join("snapshot-path");
        let output = cli(directory.path())
            .args(["deploy", "application.py"])
            .env("EXPECTED_CONFIG", config.unwrap_or("{}"))
            .env("SNAPSHOT_PATH_RECORD", &record)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let snapshot_path = std::fs::read_to_string(record).unwrap();
        assert!(
            !Path::new(&snapshot_path).exists(),
            "snapshot must be cleaned up"
        );
    }
}

#[test]
fn old_python_deployers_cannot_silently_rediscover_credentials() {
    let directory = tempfile::tempdir().unwrap();
    deployer(
        directory.path(),
        "#!/bin/sh\nprintf '%s\\n' '{\"type\":\"protocol\",\"version\":1}'\n",
    );
    let output = cli(directory.path())
        .args(["deploy", "application.py"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("incompatible with this CLI"));
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
