use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContainerResourcesInfo {
    pub cpus: f64,
    pub memory_mb: i64,
    pub ephemeral_disk_mb: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NetworkConfig {
    #[serde(default = "default_allow_internet_access")]
    pub allow_internet_access: bool,
    #[serde(default)]
    pub allow_out: Vec<String>,
    #[serde(default)]
    pub deny_out: Vec<String>,
}

fn default_allow_internet_access() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateSandboxRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    pub resources: ContainerResourcesInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_names: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxPoolRequest {
    pub image: String,
    pub resources: ContainerResourcesInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_names: Option<Vec<String>>,
    #[serde(default)]
    pub timeout_secs: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_containers: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warm_containers: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateSandboxResponse {
    pub sandbox_id: String,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxInfo {
    #[serde(alias = "id", alias = "sandbox_id")]
    pub sandbox_id: String,
    pub namespace: String,
    pub status: String,
    #[serde(default)]
    pub image: Option<String>,
    pub resources: ContainerResourcesInfo,
    #[serde(default)]
    pub secret_names: Vec<String>,
    #[serde(default)]
    pub timeout_secs: Option<i64>,
    #[serde(default)]
    pub entrypoint: Option<Vec<String>>,
    #[serde(default)]
    pub network: Option<NetworkConfig>,
    #[serde(default)]
    pub pool_id: Option<String>,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub created_at: Option<serde_json::Value>,
    #[serde(default)]
    pub terminated_at: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListSandboxesResponse {
    pub sandboxes: Vec<SandboxInfo>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateSandboxPoolResponse {
    pub pool_id: String,
    pub namespace: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PoolContainerInfo {
    pub id: String,
    pub state: String,
    #[serde(default)]
    pub sandbox_id: Option<String>,
    pub executor_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxPoolInfo {
    #[serde(alias = "id", alias = "pool_id")]
    pub pool_id: String,
    pub namespace: String,
    pub image: String,
    pub resources: ContainerResourcesInfo,
    #[serde(default)]
    pub secret_names: Vec<String>,
    #[serde(default)]
    pub timeout_secs: i64,
    #[serde(default)]
    pub entrypoint: Option<Vec<String>>,
    #[serde(default)]
    pub max_containers: Option<i64>,
    #[serde(default)]
    pub warm_containers: Option<i64>,
    #[serde(default)]
    pub containers: Option<Vec<PoolContainerInfo>>,
    #[serde(default)]
    pub created_at: Option<serde_json::Value>,
    #[serde(default)]
    pub updated_at: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListSandboxPoolsResponse {
    pub pools: Vec<SandboxPoolInfo>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateSnapshotResponse {
    pub snapshot_id: String,
    pub status: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SnapshotInfo {
    #[serde(alias = "id", alias = "snapshot_id")]
    pub snapshot_id: String,
    pub namespace: String,
    pub sandbox_id: String,
    pub base_image: String,
    pub status: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub snapshot_uri: Option<String>,
    #[serde(default)]
    pub size_bytes: Option<i64>,
    #[serde(default)]
    pub created_at: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListSnapshotsResponse {
    pub snapshots: Vec<SnapshotInfo>,
}
