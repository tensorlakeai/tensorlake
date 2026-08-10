use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::model::BlobReference;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrepareWriteRequest {
    pub incarnation: String,
    pub fence_token: u64,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommitWriteRequest {
    pub incarnation: String,
    pub fence_token: u64,
    #[serde(default)]
    pub part_etags: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PrepareWriteResponse {
    pub write_id: String,
    pub blob: BlobReference,
    pub upload: AgentUploadPlan,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentUploadPlan {
    Committed,
    SinglePut {
        url: String,
        method: String,
        #[serde(default)]
        headers: BTreeMap<String, String>,
        expires_at_ms: u64,
    },
    Multipart {
        part_size_bytes: u64,
        parts: Vec<AgentUploadPart>,
        expires_at_ms: u64,
    },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AgentUploadPart {
    pub part_number: u32,
    pub url: String,
}
