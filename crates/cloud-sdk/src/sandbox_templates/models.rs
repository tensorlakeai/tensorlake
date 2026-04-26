use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSandboxTemplateRequest {
    pub name: String,
    pub dockerfile: String,
    pub snapshot_id: String,
    pub snapshot_sandbox_id: String,
    pub snapshot_uri: String,
    pub snapshot_size_bytes: u64,
    pub rootfs_disk_bytes: u64,
    pub public: bool,
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
    pub snapshot_size_bytes: Option<u64>,
    #[serde(default)]
    pub rootfs_disk_bytes: Option<u64>,
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
            snapshot_size_bytes: 123,
            rootfs_disk_bytes: 456,
            public: true,
        };

        let json = serde_json::to_string(&request).unwrap();
        assert_eq!(
            json,
            r#"{"name":"image1","dockerfile":"FROM ubuntu:24.04\n","snapshotId":"snap-1","snapshotSandboxId":"sbx-1","snapshotUri":"s3://snapshots/snap-1","snapshotSizeBytes":123,"rootfsDiskBytes":456,"public":true}"#
        );
    }
}
