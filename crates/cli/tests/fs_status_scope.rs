#![cfg(feature = "mount")]

use std::collections::BTreeMap;
use std::fs;
use std::process::Command;

#[test]
fn human_fs_status_hydrates_mount_scope_and_remains_offline_safe() {
    let temp = tempfile::tempdir().expect("temporary test root");
    let home = temp.path().join("home");
    let mountpoint = temp.path().join("mounted-drive");
    let state_dir = temp.path().join("mount-state");
    fs::create_dir_all(home.join(".config/tensorlake")).expect("config directory");
    fs::create_dir_all(&mountpoint).expect("mountpoint");
    fs::create_dir_all(state_dir.join("upper")).expect("upper directory");
    fs::create_dir_all(state_dir.join("wh")).expect("whiteout directory");

    let state = serde_json::json!({
        "project_id": "project-from-mount",
        "organization_id": "organization-from-mount",
        "repo": "drive-from-mount",
        "native_filesystem": true,
        "workspace_id": "workspace-from-mount",
        "ref_name": "refs/workspaces/workspace-from-mount",
        "mountpoint": mountpoint,
        "created_at_secs": 1_700_000_000_u64,
    });
    fs::write(
        state_dir.join("state.json"),
        serde_json::to_vec_pretty(&state).expect("serialize mount state"),
    )
    .expect("write mount state");

    let canonical_mountpoint = mountpoint.canonicalize().expect("canonical mountpoint");
    let mut registry = BTreeMap::new();
    registry.insert(
        canonical_mountpoint.to_string_lossy().into_owned(),
        state_dir.to_string_lossy().into_owned(),
    );
    fs::write(
        home.join(".config/tensorlake/mounts.toml"),
        toml::to_string_pretty(&registry).expect("serialize mount registry"),
    )
    .expect("write mount registry");

    let output = Command::new(env!("CARGO_BIN_EXE_tl"))
        .args([
            "--api-url",
            "http://127.0.0.1:9",
            "--api-key",
            "invalid-test-key",
            "fs",
            "status",
        ])
        .arg(&canonical_mountpoint)
        .current_dir(temp.path())
        .env("HOME", &home)
        .env("NO_COLOR", "1")
        .env_remove("TENSORLAKE_API_KEY")
        .env_remove("TENSORLAKE_PAT")
        .env_remove("TENSORLAKE_ORGANIZATION_ID")
        .env_remove("TENSORLAKE_PROJECT_ID")
        .output()
        .expect("run tl fs status");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "human status must remain an offline-safe local diagnostic\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("filesystem: drive-from-mount"),
        "local mount facts must still render\nstdout:\n{stdout}"
    );
    assert!(
        !stdout.contains("missing project ID"),
        "status must hydrate the project ID from mount state before reading the server head\n\
         stdout:\n{stdout}"
    );
    assert!(
        stdout.contains("last autosave: unknown ("),
        "the unreachable test server should degrade the server summary without failing local status\n\
         stdout:\n{stdout}"
    );
}
