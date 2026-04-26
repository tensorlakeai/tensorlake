use crate::auth::context::CliContext;
use crate::commands::sbx::snapshot::fetch_snapshot_info;
use crate::error::{CliError, Result};

#[derive(Debug)]
struct SnapshotRegistrationMetadata {
    sandbox_id: String,
    snapshot_uri: String,
    snapshot_size_bytes: u64,
    rootfs_disk_bytes: u64,
}

fn parse_snapshot_registration_metadata(
    snapshot_id: &str,
    info: &serde_json::Value,
) -> Result<SnapshotRegistrationMetadata> {
    let status = info.get("status").and_then(|value| value.as_str()).ok_or_else(|| {
        CliError::Other(anyhow::anyhow!(
            "snapshot {} is missing status",
            snapshot_id
        ))
    })?;
    if status != "completed" {
        return Err(CliError::Other(anyhow::anyhow!(
            "snapshot {} is not completed (status: {})",
            snapshot_id,
            status
        )));
    }

    let sandbox_id = info
        .get("sandbox_id")
        .or_else(|| info.get("sandboxId"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            CliError::Other(anyhow::anyhow!(
                "snapshot {} is missing sandbox_id",
                snapshot_id
            ))
        })?
        .to_string();

    let snapshot_uri = info
        .get("snapshot_uri")
        .or_else(|| info.get("snapshotUri"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            CliError::Other(anyhow::anyhow!(
                "snapshot {} is missing snapshot_uri",
                snapshot_id
            ))
        })?
        .to_string();

    let snapshot_size_bytes = info
        .get("size_bytes")
        .or_else(|| info.get("sizeBytes"))
        .and_then(|value| value.as_u64())
        .ok_or_else(|| {
            CliError::Other(anyhow::anyhow!(
                "snapshot {} is missing size_bytes",
                snapshot_id
            ))
        })?;

    let rootfs_disk_bytes = info
        .get("rootfs_disk_bytes")
        .or_else(|| info.get("rootfsDiskBytes"))
        .and_then(|value| value.as_u64())
        .ok_or_else(|| {
            CliError::Other(anyhow::anyhow!(
                "snapshot {} is missing rootfs_disk_bytes",
                snapshot_id
            ))
        })?;

    Ok(SnapshotRegistrationMetadata {
        sandbox_id,
        snapshot_uri,
        snapshot_size_bytes,
        rootfs_disk_bytes,
    })
}

pub async fn run(
    ctx: &CliContext,
    image_name: &str,
    snapshot_id: &str,
    dockerfile_path: &str,
    is_public: bool,
) -> Result<()> {
    let client = ctx.client()?;
    let (url, _, _) = super::templates_base_url(ctx)?;
    let dockerfile = tokio::fs::read_to_string(dockerfile_path)
        .await
        .map_err(CliError::Io)?;
    let snapshot_info = fetch_snapshot_info(ctx, &client, snapshot_id).await?;
    let snapshot = parse_snapshot_registration_metadata(snapshot_id, &snapshot_info)?;

    let body = serde_json::json!({
        "name": image_name,
        "dockerfile": dockerfile,
        "snapshotId": snapshot_id,
        "snapshotSandboxId": snapshot.sandbox_id,
        "snapshotUri": snapshot.snapshot_uri,
        "snapshotSizeBytes": snapshot.snapshot_size_bytes,
        "rootfsDiskBytes": snapshot.rootfs_disk_bytes,
        "public": is_public,
    });

    let resp = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to register sandbox image '{}' (HTTP {}): {}",
            image_name,
            status,
            body
        )));
    }

    let registered: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
    let rootfs_disk_bytes = registered
        .get("rootfsDiskBytes")
        .or_else(|| registered.get("rootfs_disk_bytes"))
        .and_then(|value| value.as_u64())
        .unwrap_or(snapshot.rootfs_disk_bytes);
    let rootfs_disk_gib = rootfs_disk_bytes / (1024 * 1024 * 1024);
    let snapshot_uri = registered
        .get("snapshotUri")
        .or_else(|| registered.get("snapshot_uri"))
        .and_then(|value| value.as_str())
        .unwrap_or(&snapshot.snapshot_uri);
    let template_id = registered
        .get("id")
        .and_then(|value| value.as_str())
        .unwrap_or("-");

    println!(
        "Registered image '{}' -> snapshot {} (rootfs {} GiB)\nTemplate: {}\n{}",
        image_name, snapshot_id, rootfs_disk_gib, template_id, snapshot_uri
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_snapshot_registration_metadata;
    use serde_json::json;

    #[test]
    fn parse_snapshot_registration_metadata_requires_completed_snapshot() {
        let err = parse_snapshot_registration_metadata(
            "snap-1",
            &json!({
                "status": "in_progress",
                "sandbox_id": "sbx-1",
                "snapshot_uri": "s3://snapshots/snap-1.tar.zst",
                "size_bytes": 123,
                "rootfs_disk_bytes": 10 * 1024 * 1024 * 1024_u64,
            }),
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("snapshot snap-1 is not completed (status: in_progress)")
        );
    }

    #[test]
    fn parse_snapshot_registration_metadata_accepts_completed_snapshot() {
        let metadata = parse_snapshot_registration_metadata(
            "snap-1",
            &json!({
                "status": "completed",
                "sandbox_id": "sbx-1",
                "snapshot_uri": "s3://snapshots/snap-1.tar.zst",
                "size_bytes": 123,
                "rootfs_disk_bytes": 10 * 1024 * 1024 * 1024_u64,
            }),
        )
        .unwrap();

        assert_eq!(metadata.sandbox_id, "sbx-1");
        assert_eq!(metadata.snapshot_uri, "s3://snapshots/snap-1.tar.zst");
        assert_eq!(metadata.snapshot_size_bytes, 123);
        assert_eq!(metadata.rootfs_disk_bytes, 10 * 1024 * 1024 * 1024_u64);
    }
}
