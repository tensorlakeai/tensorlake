use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootfsNodeKind {
    Base,
    Diff,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSandboxTemplateRequest {
    pub name: String,
    pub dockerfile: String,
    pub snapshot_id: String,
    pub snapshot_sandbox_id: String,
    pub snapshot_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_format_version: Option<String>,
    pub snapshot_size_bytes: u64,
    pub rootfs_disk_bytes: u64,
    pub public: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PrepareSandboxTemplateBuildRequest {
    pub name: String,
    pub dockerfile: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_image: Option<String>,
    pub public: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxTemplateBuildPrepared {
    pub build_id: String,
    pub snapshot_id: String,
    pub snapshot_uri: String,
    pub rootfs_node_kind: RootfsNodeKind,
    pub builder: SandboxTemplateBuildBuilder,
    pub upload: SandboxTemplateBuildUpload,
    pub runtime_contract: SandboxTemplateBuildRuntimeContract,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<SandboxTemplateBuildParent>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxTemplateBuildBuilder {
    pub image: String,
    pub command: String,
    pub build_spec_version: String,
    pub cpus: f64,
    pub memory_mb: i64,
    pub disk_mb: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxTemplateBuildUpload {
    pub kind: String,
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
    pub expires_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxTemplateBuildRuntimeContract {
    pub guest_runtime_layout: String,
    pub guest_runtime_drive_format: String,
    pub guest_boot_contract: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxTemplateBuildParent {
    pub template_id: String,
    pub name: String,
    pub visibility: String,
    pub snapshot_id: String,
    pub snapshot_uri: String,
    pub parent_manifest_uri: String,
    pub rootfs_node_kind: RootfsNodeKind,
    pub download: SandboxTemplateBuildDownload,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxTemplateBuildDownload {
    pub kind: String,
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
    pub expires_at: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompleteSandboxTemplateBuildRequest {
    pub snapshot_id: String,
    pub snapshot_uri: String,
    pub snapshot_format_version: String,
    pub snapshot_size_bytes: u64,
    pub rootfs_disk_bytes: u64,
    pub rootfs_node_kind: RootfsNodeKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_manifest_uri: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SandboxTemplate {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub snapshot_id: Option<String>,
    #[serde(default)]
    pub snapshot_sandbox_id: Option<String>,
    #[serde(default)]
    pub snapshot_uri: Option<String>,
    #[serde(default)]
    pub snapshot_format_version: Option<String>,
    #[serde(default)]
    pub snapshot_size_bytes: Option<u64>,
    #[serde(default)]
    pub rootfs_disk_bytes: Option<u64>,
    #[serde(default)]
    pub public: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::{
        CompleteSandboxTemplateBuildRequest, CreateSandboxTemplateRequest,
        PrepareSandboxTemplateBuildRequest, RootfsNodeKind,
    };

    #[test]
    fn create_sandbox_template_request_serializes_as_camel_case() {
        let request = CreateSandboxTemplateRequest {
            name: "image1".to_string(),
            dockerfile: "FROM ubuntu:24.04\n".to_string(),
            snapshot_id: "snap-1".to_string(),
            snapshot_sandbox_id: "sbx-1".to_string(),
            snapshot_uri: "s3://snapshots/snap-1".to_string(),
            snapshot_format_version: Some("durable_archive_v1".to_string()),
            snapshot_size_bytes: 123,
            rootfs_disk_bytes: 456,
            public: true,
        };

        let json = serde_json::to_string(&request).unwrap();
        assert_eq!(
            json,
            r#"{"name":"image1","dockerfile":"FROM ubuntu:24.04\n","snapshotId":"snap-1","snapshotSandboxId":"sbx-1","snapshotUri":"s3://snapshots/snap-1","snapshotFormatVersion":"durable_archive_v1","snapshotSizeBytes":123,"rootfsDiskBytes":456,"public":true}"#
        );
    }

    #[test]
    fn prepare_sandbox_template_build_request_serializes_as_camel_case() {
        let request = PrepareSandboxTemplateBuildRequest {
            name: "image1".to_string(),
            dockerfile: "FROM ubuntu:24.04\n".to_string(),
            base_image: Some("ubuntu:24.04".to_string()),
            public: false,
        };

        let json = serde_json::to_string(&request).unwrap();
        assert_eq!(
            json,
            r#"{"name":"image1","dockerfile":"FROM ubuntu:24.04\n","baseImage":"ubuntu:24.04","public":false}"#
        );
    }

    #[test]
    fn complete_sandbox_template_build_request_serializes_as_camel_case() {
        let request = CompleteSandboxTemplateBuildRequest {
            snapshot_id: "snap-1".to_string(),
            snapshot_uri: "s3://snapshots/snap-1".to_string(),
            snapshot_format_version: "durable_archive_v1".to_string(),
            snapshot_size_bytes: 123,
            rootfs_disk_bytes: 456,
            rootfs_node_kind: RootfsNodeKind::Diff,
            parent_manifest_uri: Some("s3://snapshots/parent".to_string()),
        };

        let json = serde_json::to_string(&request).unwrap();
        assert_eq!(
            json,
            r#"{"snapshotId":"snap-1","snapshotUri":"s3://snapshots/snap-1","snapshotFormatVersion":"durable_archive_v1","snapshotSizeBytes":123,"rootfsDiskBytes":456,"rootfsNodeKind":"diff","parentManifestUri":"s3://snapshots/parent"}"#
        );
    }
}
