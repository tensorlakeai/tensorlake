use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContainerResourcesInfo {
    pub cpus: f64,
    pub memory_mb: i64,
    #[serde(default)]
    pub ephemeral_disk_mb: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateSandboxResources {
    pub cpus: f64,
    pub memory_mb: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_mb: Option<u64>,
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
    pub resources: CreateSandboxResources,
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
    /// Optional name for the sandbox. Named sandboxes support suspend/resume.
    /// When absent the sandbox is ephemeral.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateSandboxRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_unauthenticated_access: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exposed_ports: Option<Vec<u16>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxPoolRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_details: Option<serde_json::Value>,
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
    pub termination_reason: Option<String>,
    #[serde(default)]
    pub error_details: Option<serde_json::Value>,
    #[serde(default)]
    pub created_at: Option<serde_json::Value>,
    #[serde(default)]
    pub terminated_at: Option<serde_json::Value>,
    /// User-provided name. Present only on named (non-ephemeral) sandboxes.
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub allow_unauthenticated_access: bool,
    #[serde(default)]
    pub exposed_ports: Option<Vec<u16>>,
    #[serde(default)]
    pub sandbox_url: Option<String>,
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotType {
    Memory,
    Filesystem,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateSnapshotRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_type: Option<SnapshotType>,
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
    #[serde(default)]
    pub base_image: Option<String>,
    pub status: String,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub snapshot_uri: Option<String>,
    #[serde(default)]
    pub size_bytes: Option<i64>,
    #[serde(default)]
    pub rootfs_disk_bytes: Option<u64>,
    #[serde(default)]
    pub snapshot_type: Option<SnapshotType>,
    #[serde(default)]
    pub created_at: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListSnapshotsResponse {
    pub snapshots: Vec<SnapshotInfo>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcessInfo {
    pub pid: i64,
    pub status: String,
    #[serde(default)]
    pub exit_code: Option<i64>,
    #[serde(default)]
    pub signal: Option<i64>,
    #[serde(default)]
    pub stdin_writable: bool,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub started_at: serde_json::Value,
    #[serde(default)]
    pub ended_at: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListProcessesResponse {
    pub processes: Vec<ProcessInfo>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SendSignalResponse {
    pub success: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OutputResponse {
    pub pid: i64,
    pub lines: Vec<String>,
    pub line_count: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OutputEvent {
    pub line: String,
    pub timestamp: serde_json::Value,
    #[serde(default)]
    pub stream: Option<String>,
}

/// Events returned by the streaming `POST /api/v1/processes/run` endpoint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RunProcessEvent {
    /// First event: process was created.
    Started {
        pid: i64,
        started_at: serde_json::Value,
    },
    /// Intermediate events: output lines (stdout/stderr).
    Output(OutputEvent),
    /// Final event: process exited.
    Exited {
        #[serde(default)]
        exit_code: Option<i64>,
        #[serde(default)]
        signal: Option<i64>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DaemonInfo {
    pub version: String,
    pub uptime_secs: i64,
    pub running_processes: i64,
    pub total_processes: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub healthy: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DirectoryEntry {
    pub name: String,
    pub is_dir: bool,
    #[serde(default)]
    pub size: Option<i64>,
    #[serde(default)]
    pub modified_at: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListDirectoryResponse {
    pub path: String,
    pub entries: Vec<DirectoryEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_type_serializes_as_snake_case() {
        assert_eq!(
            serde_json::to_string(&SnapshotType::Memory).unwrap(),
            "\"memory\""
        );
        assert_eq!(
            serde_json::to_string(&SnapshotType::Filesystem).unwrap(),
            "\"filesystem\""
        );
    }

    #[test]
    fn create_snapshot_request_skips_none_snapshot_type() {
        let body = CreateSnapshotRequest {
            snapshot_type: None,
        };
        assert_eq!(serde_json::to_string(&body).unwrap(), "{}");
    }

    #[test]
    fn run_process_event_deserializes_started() {
        let json = r#"{"pid": 42, "started_at": 1234567890.123}"#;
        let event: RunProcessEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(event, RunProcessEvent::Started { pid: 42, .. }));
    }

    #[test]
    fn run_process_event_deserializes_output() {
        let json = r#"{"line": "hello", "timestamp": 1234567890.456, "stream": "stdout"}"#;
        let event: RunProcessEvent = serde_json::from_str(json).unwrap();
        match event {
            RunProcessEvent::Output(evt) => {
                assert_eq!(evt.line, "hello");
                assert_eq!(evt.stream.as_deref(), Some("stdout"));
            }
            _ => panic!("expected Output variant"),
        }
    }

    #[test]
    fn run_process_event_deserializes_exited() {
        let json = r#"{"exit_code": 0}"#;
        let event: RunProcessEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(
            event,
            RunProcessEvent::Exited {
                exit_code: Some(0),
                signal: None,
            }
        ));
    }

    #[test]
    fn run_process_event_deserializes_signaled() {
        let json = r#"{"signal": 9}"#;
        let event: RunProcessEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(
            event,
            RunProcessEvent::Exited {
                exit_code: None,
                signal: Some(9),
            }
        ));
    }

    #[test]
    fn create_snapshot_request_serializes_filesystem() {
        let body = CreateSnapshotRequest {
            snapshot_type: Some(SnapshotType::Filesystem),
        };
        assert_eq!(
            serde_json::to_string(&body).unwrap(),
            r#"{"snapshot_type":"filesystem"}"#
        );
    }

    #[test]
    fn snapshot_info_deserializes_snapshot_type() {
        let json = r#"{
            "id":"snap-1",
            "namespace":"default",
            "sandbox_id":"sbx-1",
            "status":"completed",
            "snapshot_type":"filesystem"
        }"#;
        let info: SnapshotInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.snapshot_type, Some(SnapshotType::Filesystem));
    }
}
