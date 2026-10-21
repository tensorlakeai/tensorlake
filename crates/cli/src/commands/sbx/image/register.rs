use crate::auth::context::CliContext;
use crate::commands::sbx::snapshot::fetch_snapshot_info;
use crate::error::{CliError, Result};
use tensorlake::{
    sandbox_image_catalog::{
        CAS_SNAPSHOT_FORMAT_VERSION, canonical_cas_image_name, validate_registered_name,
    },
    sandbox_templates::models::{CreateSandboxTemplateRequest, SandboxTemplate},
};

/// The Dockerfile text sent for a legacy registration without `--dockerfile`
/// (ADR 0107 D11): the platform stores it verbatim and nothing reads it.
const OMITTED_DOCKERFILE: &str = "<omitted>";

#[derive(Debug)]
struct SnapshotRegistrationMetadata {
    sandbox_id: String,
    snapshot_uri: String,
    snapshot_format_version: Option<String>,
    snapshot_size_bytes: u64,
    rootfs_disk_bytes: u64,
}

/// The snapshot must be `completed` for either family. A CAS snapshot that
/// is `local_ready` (or still publishing) is resumable but not yet durable,
/// which is what registration needs (ADR 0107 D39).
fn require_completed(snapshot_id: &str, info: &serde_json::Value) -> Result<()> {
    let status = info
        .get("status")
        .and_then(|value| value.as_str())
        .ok_or_else(|| {
            CliError::Other(anyhow::anyhow!(
                "snapshot {} is missing status",
                snapshot_id
            ))
        })?;
    match status {
        "completed" => Ok(()),
        "local_ready" | "in_progress" | "pending" | "replicating" => {
            Err(CliError::Other(anyhow::anyhow!(
                "snapshot {} is not completed (status: {}): its publication is still in \
                 progress; retry once it is completed",
                snapshot_id,
                status
            )))
        }
        other => Err(CliError::Other(anyhow::anyhow!(
            "snapshot {} is not completed (status: {})",
            snapshot_id,
            other
        ))),
    }
}

fn snapshot_format_version(info: &serde_json::Value) -> Option<String> {
    info.get("snapshot_format_version")
        .or_else(|| info.get("snapshotFormatVersion"))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn parse_snapshot_registration_metadata(
    snapshot_id: &str,
    info: &serde_json::Value,
) -> Result<SnapshotRegistrationMetadata> {
    require_completed(snapshot_id, info)?;

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
        snapshot_format_version: snapshot_format_version(info),
        snapshot_size_bytes,
        rootfs_disk_bytes,
    })
}

/// `tl sbx image register <name> <snapshot-id>`: a completed CAS snapshot is
/// registered through the Image Service (adoption, ADR 0107 D9-D12); a
/// completed legacy snapshot through the platform template registry, with
/// the Dockerfile stored when one is given.
///
/// A CAS registration the service answers with `503 publication_pending`
/// is still running as a build operation: the CLI reports its id, retries
/// the identical request under the same key until it publishes (Ctrl+C
/// cancels it) and prints the same final output. Legacy registration is
/// unchanged and never touches the Image Service.
pub async fn run(
    ctx: &CliContext,
    image_name: &str,
    snapshot_id: &str,
    dockerfile_path: Option<&str>,
    is_public: bool,
) -> Result<()> {
    validate_registered_name(image_name).map_err(CliError::usage)?;
    let client = ctx.client()?;
    let snapshot_info = fetch_snapshot_info(ctx, &client, snapshot_id).await?;

    if snapshot_format_version(&snapshot_info).as_deref() == Some(CAS_SNAPSHOT_FORMAT_VERSION) {
        require_completed(snapshot_id, &snapshot_info)?;
        if dockerfile_path.is_some() {
            eprintln!("⚠️  --dockerfile is ignored for CAS snapshots.");
        }
        // Shown and sent in the CAS image registry's spelling.
        let image_name = &canonical_cas_image_name(image_name);
        let catalog = super::sandbox_image_catalog(ctx)?;
        let guard = super::build::CasInterruptGuard::install();
        let mut renderer = super::ImageBuildEventRenderer::new();
        let registered = catalog
            .image_service()
            .adopt_snapshot_and_wait(
                image_name,
                snapshot_id,
                is_public,
                guard.interrupt(),
                |event| {
                    guard.observe(&event);
                    renderer.render(event)
                },
            )
            .await
            .map_err(super::build::build_error)?;
        drop(renderer);
        let rootfs_disk_bytes = registered.rootfs_disk_bytes.unwrap_or(0);
        let snapshot_uri = registered.snapshot_uri.clone().unwrap_or_default();
        print_registered(
            "CAS",
            image_name,
            snapshot_id,
            rootfs_disk_bytes,
            &registered,
            &snapshot_uri,
        );
        return Ok(());
    }

    let templates_client = super::sandbox_templates_client(ctx)?;
    let dockerfile = match dockerfile_path {
        Some(path) => tokio::fs::read_to_string(path)
            .await
            .map_err(CliError::Io)?,
        None => OMITTED_DOCKERFILE.to_string(),
    };
    let snapshot = parse_snapshot_registration_metadata(snapshot_id, &snapshot_info)?;

    let registered = templates_client
        .create(&CreateSandboxTemplateRequest {
            name: image_name.to_string(),
            dockerfile,
            snapshot_id: snapshot_id.to_string(),
            snapshot_sandbox_id: snapshot.sandbox_id.clone(),
            snapshot_uri: snapshot.snapshot_uri.clone(),
            snapshot_format_version: snapshot.snapshot_format_version.clone(),
            snapshot_size_bytes: snapshot.snapshot_size_bytes,
            rootfs_disk_bytes: snapshot.rootfs_disk_bytes,
            public: is_public,
            // The CLI register flow targets base snapshots; the server
            // defaults to "base" when these fields are absent. Registering
            // a diff snapshot via the CLI would require the user to
            // supply parent metadata, which is out of scope here.
            rootfs_node_kind: None,
            parent_template: None,
            additional_local_images: None,
        })
        .await?;

    let rootfs_disk_bytes = registered
        .rootfs_disk_bytes
        .unwrap_or(snapshot.rootfs_disk_bytes);
    let snapshot_uri = registered
        .snapshot_uri
        .clone()
        .unwrap_or_else(|| snapshot.snapshot_uri.clone());
    print_registered(
        "legacy",
        image_name,
        snapshot_id,
        rootfs_disk_bytes,
        &registered,
        &snapshot_uri,
    );
    Ok(())
}

fn print_registered(
    family: &str,
    image_name: &str,
    snapshot_id: &str,
    rootfs_disk_bytes: u64,
    registered: &SandboxTemplate,
    snapshot_uri: &str,
) {
    let rootfs_disk_gib = rootfs_disk_bytes / (1024 * 1024 * 1024);
    let template_id = registered.id.as_deref().unwrap_or("-");
    println!(
        "Registered {} image '{}' -> snapshot {} (rootfs {} GiB)\nTemplate: {}\n{}",
        family, image_name, snapshot_id, rootfs_disk_gib, template_id, snapshot_uri
    );
}

#[cfg(test)]
mod tests {
    use super::{parse_snapshot_registration_metadata, require_completed};
    use serde_json::json;

    #[test]
    fn parse_snapshot_registration_metadata_requires_completed_snapshot() {
        let err = parse_snapshot_registration_metadata(
            "snap-1",
            &json!({
                "status": "in_progress",
                "sandbox_id": "sbx-1",
                "snapshot_uri": "s3://snapshots/snap-1.tar.zst",
                "snapshot_format_version": "durable_archive_v1",
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
    fn local_ready_snapshots_are_still_publishing() {
        let err = require_completed(
            "snap-1",
            &json!({"status": "local_ready", "snapshot_format_version": "content_addressed_streaming_v1"}),
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("snapshot snap-1 is not completed (status: local_ready)")
        );
        assert!(err.to_string().contains("publication is still in progress"));
        let err = require_completed("snap-1", &json!({"status": "failed"})).unwrap_err();
        assert!(
            err.to_string()
                .contains("snapshot snap-1 is not completed (status: failed)")
        );
        assert!(!err.to_string().contains("still in progress"));
        assert!(require_completed("snap-1", &json!({"status": "completed"})).is_ok());
    }

    #[test]
    fn parse_snapshot_registration_metadata_accepts_completed_snapshot() {
        let metadata = parse_snapshot_registration_metadata(
            "snap-1",
            &json!({
                "status": "completed",
                "sandbox_id": "sbx-1",
                "snapshot_uri": "s3://snapshots/snap-1.tar.zst",
                "snapshot_format_version": "durable_archive_v1",
                "size_bytes": 123,
                "rootfs_disk_bytes": 10 * 1024 * 1024 * 1024_u64,
            }),
        )
        .unwrap();

        assert_eq!(metadata.sandbox_id, "sbx-1");
        assert_eq!(metadata.snapshot_uri, "s3://snapshots/snap-1.tar.zst");
        assert_eq!(
            metadata.snapshot_format_version.as_deref(),
            Some("durable_archive_v1")
        );
        assert_eq!(metadata.snapshot_size_bytes, 123);
        assert_eq!(metadata.rootfs_disk_bytes, 10 * 1024 * 1024 * 1024_u64);
    }
}
