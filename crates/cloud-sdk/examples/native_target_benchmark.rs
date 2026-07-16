//! Profile the native filesystem path with a real Rust `target/` directory against a local
//! artifact-storage server. The server should run in open mode; this harness uses a fixed dummy
//! Basic credential and accepts only the server URL and source path as explicit arguments.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use tensorlake::artifact_storage::ArtifactStorageClient;
use tensorlake::artifact_storage::native_fs::{
    NativeChangeSet, NativeLocalUpsert, NativePushEvent, NativePushOptions,
};

fn collect_files(root: &Path, directory: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(directory)
        .with_context(|| format!("reading source directory {}", directory.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() {
            collect_files(root, &path, files)?;
        } else if metadata.is_file() {
            files.push(path.strip_prefix(root)?.to_path_buf());
        }
    }
    Ok(())
}

fn native_path(path: &Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn prepare_one_percent(root: &Path) -> Result<(tempfile::TempDir, Vec<NativeLocalUpsert>, u64)> {
    let mut files = Vec::new();
    collect_files(root, root, &mut files)?;
    files.sort();
    if files.is_empty() {
        bail!("source target directory contains no regular files");
    }

    let changed = tempfile::tempdir()?;
    let sample_count = files.len().div_ceil(100);
    let stride = (files.len() / sample_count).max(1);
    let mut upserts = Vec::with_capacity(sample_count);
    let mut logical_bytes = 0u64;
    for relative in files.iter().step_by(stride).take(sample_count) {
        let source = root.join(relative);
        let destination = changed.path().join(relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(&source, &destination).with_context(|| {
            format!(
                "copying sampled target file {} to {}",
                source.display(),
                destination.display()
            )
        })?;
        OpenOptions::new()
            .append(true)
            .open(&destination)?
            .write_all(b"\ntensorlake-native-target-benchmark\n")?;
        logical_bytes = logical_bytes.saturating_add(fs::metadata(&destination)?.len());
        upserts.push(NativeLocalUpsert {
            path: native_path(relative),
            source: destination,
        });
    }
    Ok((changed, upserts, logical_bytes))
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let base_url = args
        .next()
        .context("usage: native_target_benchmark <artifact-storage-url> <target-directory>")?;
    let source = PathBuf::from(
        args.next()
            .context("usage: native_target_benchmark <artifact-storage-url> <target-directory>")?,
    )
    .canonicalize()?;
    if args.next().is_some() || !source.is_dir() {
        bail!("expected exactly one existing target directory");
    }

    let run = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let project = format!("native-target-bench-{run}");
    let repo = "target";
    let fork = "target-fork";
    let username = "benchmark";
    let token = "open-mode";
    let api = tensorlake::ClientBuilder::new(&base_url)
        .bearer_token("unused")
        .build()?;
    let client = ArtifactStorageClient::new(api, &base_url)?;

    client
        .create_repo_with_credential(
            &project,
            repo,
            Some("main"),
            Some("filesystem"),
            username,
            token,
        )
        .await?;

    let cold_started = Instant::now();
    let cold_progress_started = cold_started;
    let cold = client
        .push_native_directory_with_credential(
            &project,
            repo,
            &source,
            username,
            token,
            NativePushOptions {
                message: "Cold Rust target snapshot".into(),
                progress: Some(Arc::new(move |event: NativePushEvent| {
                    eprintln!(
                        "cold phase elapsed_ms={} event={event:?}",
                        cold_progress_started.elapsed().as_millis()
                    );
                })),
                ..Default::default()
            },
        )
        .await?;
    let cold_wall = cold_started.elapsed();
    let workspace = client
        .create_native_workspace_with_credential(
            &project,
            repo,
            Some(&cold.snapshot_id),
            false,
            username,
            token,
        )
        .await?;

    let no_op_started = Instant::now();
    let no_op = client
        .push_native_changes_with_credential(
            &project,
            repo,
            NativeChangeSet::default(),
            username,
            token,
            NativePushOptions {
                expected_snapshot_id: Some(cold.snapshot_id.clone()),
                workspace_id: Some(workspace.workspace_id.clone()),
                ..Default::default()
            },
        )
        .await;
    let no_op_wall = no_op_started.elapsed();
    if !no_op
        .expect_err("empty mounted delta must short-circuit")
        .to_string()
        .contains("change set is empty")
    {
        bail!("empty mounted delta returned an unexpected error");
    }

    let fixture_started = Instant::now();
    let (_changed, upserts, changed_logical_bytes) = prepare_one_percent(&source)?;
    let fixture_wall = fixture_started.elapsed();
    let changed_files = upserts.len();
    let incremental_started = Instant::now();
    let preparation_started = Instant::now();
    let candidate = client
        .prepare_native_snapshot_candidate_with_credential(
            &project,
            repo,
            &cold.snapshot_id,
            NativeChangeSet {
                upserts,
                ..Default::default()
            },
            username,
            token,
        )
        .await?;
    let preparation_wall = preparation_started.elapsed();
    let seal_started = Instant::now();
    let incremental = client
        .publish_native_snapshot_candidate_with_credential(
            &project,
            repo,
            candidate,
            username,
            token,
            NativePushOptions {
                message: "One-percent Rust target update".into(),
                expected_snapshot_id: Some(cold.snapshot_id.clone()),
                workspace_id: Some(workspace.workspace_id.clone()),
                ..Default::default()
            },
        )
        .await?;
    let seal_wall = seal_started.elapsed();
    let incremental_wall = incremental_started.elapsed();

    let fork_started = Instant::now();
    client
        .fork_repo_with_credential(&project, fork, repo, username, token)
        .await?;
    let fork_wall = fork_started.elapsed();

    let restore_started = Instant::now();
    let restored = client
        .restore_native_snapshot_with_credential(
            &project,
            repo,
            &workspace.workspace_id,
            &cold.snapshot_id,
            &incremental.snapshot_id,
            &uuid::Uuid::new_v4().to_string(),
            chrono::Utc::now().timestamp_millis().max(0) as u64,
            username,
            token,
        )
        .await?;
    let restore_wall = restore_started.elapsed();

    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "project": project,
            "source": source,
            "cold": {
                "wall_ms": cold_wall.as_millis(),
                "snapshot_id": cold.snapshot_id,
                "files": cold.files,
                "directories": cold.directories,
                "logical_bytes": cold.logical_bytes,
                "stored_bytes": cold.stored_bytes,
                "uploaded_bytes": cold.uploaded_bytes,
                "segments": cold.total_segments,
                "transport": cold.transport,
            },
            "no_op": {
                "wall_us": no_op_wall.as_micros(),
                "network_requests": 0,
            },
            "one_percent_fixture": {
                "files": changed_files,
                "logical_bytes": changed_logical_bytes,
                "preparation_ms_excluded_from_incremental": fixture_wall.as_millis(),
            },
            "incremental": {
                "wall_ms": incremental_wall.as_millis(),
                "background_preparation_ms": preparation_wall.as_millis(),
                "snapshot_seal_ms": seal_wall.as_millis(),
                "snapshot_id": incremental.snapshot_id,
                "logical_bytes": incremental.logical_bytes,
                "stored_bytes": incremental.stored_bytes,
                "uploaded_bytes": incremental.uploaded_bytes,
                "segments": incremental.total_segments,
                "transport": incremental.transport,
            },
            "fork": {
                "wall_us": fork_wall.as_micros(),
                "repo": fork,
            },
            "restore_by_reference": {
                "wall_ms": restore_wall.as_millis(),
                "snapshot_id": restored,
                "file_bytes_moved": 0,
            },
        }))?
    );
    Ok(())
}
