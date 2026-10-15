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

/// One registered sandbox image in the platform template shape. Both image
/// families answer it: legacy templates verbatim from the platform API, and
/// CAS images mapped from the Image Service record by
/// [`crate::sandbox_image_catalog`]. Fields the server did not send stay
/// `None`; keys the model does not know are kept verbatim in `extra` so
/// machine consumers see the complete record (the CAS provenance fields
/// `imageId` and `buildId` travel there).
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dockerfile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_by_user_id: Option<String>,
    /// RFC 3339 creation time (or an epoch number rendered as a string).
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "string_or_number"
    )]
    pub created_at: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "string_or_number"
    )]
    pub updated_at: Option<String>,
    #[serde(flatten)]
    pub extra: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Accept a timestamp written as a string or as a JSON number.
fn string_or_number<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(match value {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(text)) => Some(text),
        Some(serde_json::Value::Number(number)) => Some(number.to_string()),
        Some(other) => {
            return Err(serde::de::Error::custom(format!(
                "expected a timestamp string or number, got {other}"
            )));
        }
    })
}

#[cfg(test)]
mod tests {
    use super::{CreateSandboxTemplateRequest, SandboxTemplate};

    #[test]
    fn sandbox_template_keeps_unknown_keys_and_timestamps() {
        let template: SandboxTemplate = serde_json::from_value(serde_json::json!({
            "id": "sandbox_template_1",
            "name": "python",
            "snapshotId": "snap-1",
            "createdAt": "2026-10-15T00:00:00.000Z",
            "updatedAt": 1_792_022_400_000_u64,
            "dockerfile": "FROM python\n",
            "createdByUserId": "user-1",
            "customField": {"nested": true},
        }))
        .unwrap();
        assert_eq!(
            template.created_at.as_deref(),
            Some("2026-10-15T00:00:00.000Z")
        );
        assert_eq!(template.updated_at.as_deref(), Some("1792022400000"));
        assert_eq!(template.dockerfile.as_deref(), Some("FROM python\n"));
        assert_eq!(
            template.extra["customField"],
            serde_json::json!({"nested": true})
        );
        let json = serde_json::to_value(&template).unwrap();
        assert_eq!(json["customField"]["nested"], true);
        assert_eq!(json["createdByUserId"], "user-1");
        // Fields the platform did not send are not invented.
        let bare: SandboxTemplate =
            serde_json::from_value(serde_json::json!({"id": "sandbox_template_2"})).unwrap();
        let json = serde_json::to_value(&bare).unwrap();
        assert!(json.get("createdAt").is_none());
        assert!(json.get("dockerfile").is_none());
    }

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
