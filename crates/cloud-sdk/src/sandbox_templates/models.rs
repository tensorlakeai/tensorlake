use serde::{Deserialize, Serialize};

/// One resolved sandbox template entry sent in a create/prepare request body.
///
/// `templateId` identifies the row; `name` and `reference` are the
/// registered name and the exact string the user wrote in the Dockerfile;
/// `snapshotId` and `public` describe the snapshot view the SDK observed
/// when it looked the template up.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalImageInput {
    pub template_id: String,
    pub name: String,
    pub reference: String,
    pub snapshot_id: String,
    pub public: bool,
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
    /// Forwarded from the snapshot metadata. The server defaults to `"base"`
    /// when omitted; diff snapshots must set this explicitly.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rootfs_node_kind: Option<String>,
    /// Optional resolved-template fields. When supplied, the server skips
    /// parsing the Dockerfile to determine the parent / multi-stage local
    /// images and uses these entries directly.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_template: Option<Option<LocalImageInput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_local_images: Option<Vec<LocalImageInput>>,
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
    pub rootfs_node_kind: Option<String>,
    #[serde(default)]
    pub public: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::CreateSandboxTemplateRequest;

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
            rootfs_node_kind: None,
            parent_template: None,
            additional_local_images: None,
        };

        let json = serde_json::to_string(&request).unwrap();
        assert_eq!(
            json,
            r#"{"name":"image1","dockerfile":"FROM ubuntu:24.04\n","snapshotId":"snap-1","snapshotSandboxId":"sbx-1","snapshotUri":"s3://snapshots/snap-1","snapshotFormatVersion":"durable_archive_v1","snapshotSizeBytes":123,"rootfsDiskBytes":456,"public":true}"#
        );
    }

    #[test]
    fn create_sandbox_template_request_includes_rootfs_node_kind_when_set() {
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
            rootfs_node_kind: Some("diff".to_string()),
            parent_template: None,
            additional_local_images: None,
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains(r#""rootfsNodeKind":"diff""#), "got {json}");
    }
}
