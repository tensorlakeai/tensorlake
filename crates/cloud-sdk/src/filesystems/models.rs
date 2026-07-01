use serde::{Deserialize, Serialize};

/// Request body for registering a new filesystem with a project.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateFilesystemRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// A registered filesystem as returned by the Platform API.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Filesystem {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::{CreateFilesystemRequest, Filesystem};

    #[test]
    fn create_filesystem_request_serializes_as_camel_case() {
        let request = CreateFilesystemRequest {
            name: "skills".to_string(),
            description: Some("shared skills volume".to_string()),
        };
        let json = serde_json::to_string(&request).unwrap();
        assert_eq!(
            json,
            r#"{"name":"skills","description":"shared skills volume"}"#
        );
    }

    #[test]
    fn create_filesystem_request_omits_absent_description() {
        let request = CreateFilesystemRequest {
            name: "skills".to_string(),
            description: None,
        };
        let json = serde_json::to_string(&request).unwrap();
        assert_eq!(json, r#"{"name":"skills"}"#);
    }

    #[test]
    fn filesystem_deserializes_camel_case_response() {
        let body = r#"{
            "id": "file_system_bKtRcMWrzcRTRGfmMhgDc",
            "name": "skills",
            "region": "us-east-1",
            "status": "ready",
            "createdAt": "2026-06-25T00:00:00Z",
            "updatedAt": "2026-06-25T00:00:00Z"
        }"#;
        let fs: Filesystem = serde_json::from_str(body).unwrap();
        assert_eq!(fs.id.as_deref(), Some("file_system_bKtRcMWrzcRTRGfmMhgDc"));
        assert_eq!(fs.name.as_deref(), Some("skills"));
        assert_eq!(fs.region.as_deref(), Some("us-east-1"));
        assert_eq!(fs.created_at.as_deref(), Some("2026-06-25T00:00:00Z"));
    }
}
