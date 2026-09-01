use derive_builder::Builder;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SandboxLogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

impl SandboxLogLevel {
    pub fn as_i8(self) -> i8 {
        match self {
            SandboxLogLevel::Trace => 1,
            SandboxLogLevel::Debug => 2,
            SandboxLogLevel::Info => 3,
            SandboxLogLevel::Warn => 4,
            SandboxLogLevel::Error => 5,
            SandboxLogLevel::Fatal => 6,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxLogSignal {
    pub timestamp: u64,
    pub uuid: uuid::Uuid,
    pub namespace: String,
    pub application: String,
    #[serde(default, rename = "sandboxId")]
    pub sandbox_id: Option<String>,
    #[serde(default, rename = "resourceAttributes")]
    pub resource_attributes: Vec<(String, String)>,
    pub body: String,
    #[serde(rename = "logAttributes")]
    pub log_attributes: String,
    #[serde(default)]
    pub allocations: Vec<String>,
    #[serde(default, rename = "functionRuns")]
    pub function_runs: Vec<String>,
    #[serde(default)]
    pub level: Option<i8>,
    #[serde(default)]
    pub retention: Option<i8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxLogsResponse {
    pub logs: Vec<SandboxLogSignal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxProcessLogFilter {
    #[serde(rename = "processId")]
    pub process_id: String,
    #[serde(rename = "processPid")]
    pub process_pid: String,
    #[serde(rename = "processCommand")]
    pub process_command: String,
    #[serde(rename = "processManagedId")]
    pub process_managed_id: String,
    #[serde(rename = "processManagedName")]
    pub process_managed_name: String,
    #[serde(rename = "firstSeen")]
    pub first_seen: i64,
    #[serde(rename = "lastSeen")]
    pub last_seen: i64,
    #[serde(rename = "logCount")]
    pub log_count: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxProcessLogFiltersResponse {
    pub processes: Vec<SandboxProcessLogFilter>,
}

#[derive(Builder, Clone, Debug, Default, Deserialize)]
pub struct GetSandboxLogsRequest {
    #[builder(setter(into))]
    pub sandbox_id: String,
    #[builder(default, setter(into))]
    pub levels: Vec<SandboxLogLevel>,
    #[builder(default, setter(into))]
    pub process_ids: Vec<String>,
    #[builder(default, setter(into, strip_option))]
    pub next_token: Option<String>,
    #[builder(default, setter(strip_option))]
    pub head: Option<usize>,
    #[builder(default, setter(strip_option))]
    pub tail: Option<usize>,
    #[builder(default, setter(into, strip_option))]
    pub body: Option<String>,
}

impl GetSandboxLogsRequest {
    pub fn builder() -> GetSandboxLogsRequestBuilder {
        GetSandboxLogsRequestBuilder::default()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContainerResourcesInfo {
    pub cpus: f64,
    pub memory_mb: i64,
    #[serde(default)]
    pub ephemeral_disk_mb: i64,
}

/// GPU models supported by the sandbox scheduler.
///
/// The serialized values are the canonical names used by Server and the
/// executor API.
#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum GpuModel {
    #[serde(rename = "A100-40GB")]
    A10040Gb,
    #[serde(rename = "A100-80GB")]
    A10080Gb,
    #[serde(rename = "H100")]
    H100,
    #[serde(rename = "T4")]
    T4,
    #[serde(rename = "A6000")]
    A6000,
    #[serde(rename = "A10")]
    A10,
}

impl GpuModel {
    pub const ALL: [Self; 6] = [
        Self::A10040Gb,
        Self::A10080Gb,
        Self::H100,
        Self::T4,
        Self::A6000,
        Self::A10,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::A10040Gb => "A100-40GB",
            Self::A10080Gb => "A100-80GB",
            Self::H100 => "H100",
            Self::T4 => "T4",
            Self::A6000 => "A6000",
            Self::A10 => "A10",
        }
    }
}

impl fmt::Display for GpuModel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for GpuModel {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|model| model.as_str() == value)
            .ok_or_else(|| format!("unsupported GPU model: {value}"))
    }
}

/// A homogeneous GPU allocation for a sandbox.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GpuRequest {
    pub count: u32,
    pub model: GpuModel,
}

impl GpuRequest {
    pub fn new(model: GpuModel, count: u32) -> Result<Self, String> {
        if count == 0 {
            return Err("GPU count must be positive".to_string());
        }
        Ok(Self { count, model })
    }
}

/// GPU resource request used by the existing sandbox request model.
///
/// `model` remains a string for Rust source compatibility. New code can build
/// a typed [`GpuRequest`] and convert it with [`From`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GPUResources {
    pub count: u32,
    pub model: String,
}

impl From<GpuRequest> for GPUResources {
    fn from(request: GpuRequest) -> Self {
        Self {
            count: request.count,
            model: request.model.to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateSandboxResources {
    pub cpus: f64,
    pub memory_mb: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disk_mb: Option<u64>,
    /// GPU allocation. The server accepts GPU sandboxes only with a CAS
    /// (`content_addressed_streaming_v1`) image; omitting `image` asks the
    /// server to select its configured GPU default.
    #[serde(rename = "gpus", skip_serializing_if = "Option::is_none")]
    pub gpu_configs: Option<Vec<GPUResources>>,
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

/// One file system mounted into a sandbox at an absolute guest path.
///
/// `file_system_id` is the Artifact Storage file-system name created with
/// `tl fs create <name>` and `mount_path` is an absolute, unique guest path
/// (e.g. `/mnt/skills`).
///
/// `read_only` and `prefetch` are optional mount modes. They serialize only
/// when `true`: older servers deserialize mount bodies with
/// `deny_unknown_fields` and would reject an explicit `false`.
///
/// `snapshot_id` optionally pins the mount to a specific filesystem
/// snapshot. A pinned mount must also be `read_only` (the server rejects a
/// writable pin with HTTP 400). The field serializes only when set: older
/// servers reject mount bodies carrying unknown fields, so "unpinned" is
/// expressed by omission.
///
/// `owner` optionally presents the mounted files as owned by a guest user:
/// `NAME`, `UID`, `NAME:GROUP`, or `UID:GID` (e.g. `agent` or `1001:1001`),
/// resolved against the sandbox image's own user database when the mount
/// attaches. Unset means the image default (the baked `tl-user` account when
/// the image has one, otherwise root). The field serializes only when set,
/// for the same compatibility reason as the pin.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FileSystemMount {
    pub file_system_id: String,
    pub mount_path: String,
    /// Mount the file system read-only inside the guest.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
    /// Eagerly download the complete file system into the guest cache.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub prefetch: bool,
    /// Pin the mount to a specific filesystem snapshot (requires
    /// `read_only`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    /// Present the mounted files as owned by this guest user spec.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateSandboxRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    pub resources: CreateSandboxResources,
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
    /// File systems to mount into the sandbox at boot, each at its own
    /// absolute, unique guest mount path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub file_systems: Vec<FileSystemMount>,
}

/// Sandbox-specific state to apply while claiming a warm pool container.
///
/// Pool templates stay storage-neutral; these mounts belong only to the
/// sandbox created by the claim.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ClaimSandboxRequest {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub file_systems: Vec<FileSystemMount>,
}

/// Request body for detaching a file system from a running sandbox. The
/// mount path is sent in the body (rather than the URL) so its slashes don't
/// need URL-encoding.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DetachFileSystemRequest {
    pub mount_path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateSandboxRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_unauthenticated_access: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exposed_ports: Option<Vec<u16>>,
    /// Tri-state update of the running sandbox's egress network policy: omitted
    /// leaves it unchanged, `null` clears it to unrestricted egress, an object
    /// replaces the whole policy. The dataplane re-renders the live VM's
    /// firewall in one atomic swap with no enforcement gap.
    #[serde(default, skip_serializing_if = "NetworkPolicyUpdate::is_keep")]
    pub network: NetworkPolicyUpdate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SandboxPoolRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    pub resources: ContainerResourcesInfo,
    #[serde(default)]
    pub timeout_secs: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_containers: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warm_containers: Option<i64>,
}

/// A pool creation request with an optional container network policy.
#[derive(Clone, Debug, PartialEq)]
pub struct CreateSandboxPoolRequest {
    pub pool: SandboxPoolRequest,
    pub network: Option<NetworkConfig>,
}

// These pool request wrappers intentionally implement their flat wire shape
// without `serde(flatten)`. When another workspace dependency enables
// serde_json's `arbitrary_precision` feature, flatten's intermediate content
// representation serializes nested floating-point values as private Number
// maps instead of JSON numbers.
impl Serialize for CreateSandboxPoolRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            image: Option<&'a str>,
            resources: &'a ContainerResourcesInfo,
            timeout_secs: i64,
            #[serde(skip_serializing_if = "Option::is_none")]
            entrypoint: Option<&'a Vec<String>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            max_containers: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            warm_containers: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            network: Option<&'a NetworkConfig>,
        }

        Wire {
            image: self.pool.image.as_deref(),
            resources: &self.pool.resources,
            timeout_secs: self.pool.timeout_secs,
            entrypoint: self.pool.entrypoint.as_ref(),
            max_containers: self.pool.max_containers,
            warm_containers: self.pool.warm_containers,
            network: self.network.as_ref(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for CreateSandboxPoolRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            image: Option<String>,
            resources: ContainerResourcesInfo,
            #[serde(default)]
            timeout_secs: i64,
            entrypoint: Option<Vec<String>>,
            max_containers: Option<i64>,
            warm_containers: Option<i64>,
            network: Option<NetworkConfig>,
        }

        let wire = Wire::deserialize(deserializer)?;
        Ok(Self {
            pool: SandboxPoolRequest {
                image: wire.image,
                resources: wire.resources,
                timeout_secs: wire.timeout_secs,
                entrypoint: wire.entrypoint,
                max_containers: wire.max_containers,
                warm_containers: wire.warm_containers,
            },
            network: wire.network,
        })
    }
}

/// What an update should do with a pool's or sandbox's network policy.
///
/// The three states are distinct on the wire: the field is omitted to keep the
/// current policy, sent as `null` to clear it, or sent as an object to replace
/// it. Shared by [`UpdateSandboxPoolRequest`] and [`UpdateSandboxRequest`].
#[derive(Clone, Debug, Default, PartialEq)]
pub enum NetworkPolicyUpdate {
    /// Leave the current network policy in place.
    #[default]
    Keep,
    /// Remove the network policy, leaving egress unrestricted.
    Clear,
    /// Replace the network policy.
    Set(NetworkConfig),
}

impl NetworkPolicyUpdate {
    pub fn is_keep(&self) -> bool {
        matches!(self, Self::Keep)
    }
}

impl Serialize for NetworkPolicyUpdate {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            // `Keep` is normally skipped by `skip_serializing_if`; if a caller
            // serializes it directly, `null` is the closest wire meaning.
            Self::Keep | Self::Clear => serializer.serialize_none(),
            Self::Set(policy) => serializer.serialize_some(policy),
        }
    }
}

impl<'de> Deserialize<'de> for NetworkPolicyUpdate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Only reached when the key is present, so an absent field falls back
        // to `Default` (`Keep`) via `serde(default)` on the field.
        Ok(match Option::<NetworkConfig>::deserialize(deserializer)? {
            None => Self::Clear,
            Some(policy) => Self::Set(policy),
        })
    }
}

/// A pool update request carrying a tri-state network policy instruction.
#[derive(Clone, Debug, PartialEq)]
pub struct UpdateSandboxPoolRequest {
    pub pool: SandboxPoolRequest,
    pub network: NetworkPolicyUpdate,
}

impl Serialize for UpdateSandboxPoolRequest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct Wire<'a> {
            #[serde(skip_serializing_if = "Option::is_none")]
            image: Option<&'a str>,
            resources: &'a ContainerResourcesInfo,
            timeout_secs: i64,
            #[serde(skip_serializing_if = "Option::is_none")]
            entrypoint: Option<&'a Vec<String>>,
            #[serde(skip_serializing_if = "Option::is_none")]
            max_containers: Option<i64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            warm_containers: Option<i64>,
            #[serde(skip_serializing_if = "network_policy_update_ref_is_keep")]
            network: &'a NetworkPolicyUpdate,
        }

        fn network_policy_update_ref_is_keep(value: &&NetworkPolicyUpdate) -> bool {
            value.is_keep()
        }

        Wire {
            image: self.pool.image.as_deref(),
            resources: &self.pool.resources,
            timeout_secs: self.pool.timeout_secs,
            entrypoint: self.pool.entrypoint.as_ref(),
            max_containers: self.pool.max_containers,
            warm_containers: self.pool.warm_containers,
            network: &self.network,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for UpdateSandboxPoolRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            image: Option<String>,
            resources: ContainerResourcesInfo,
            #[serde(default)]
            timeout_secs: i64,
            entrypoint: Option<Vec<String>>,
            max_containers: Option<i64>,
            warm_containers: Option<i64>,
            #[serde(default)]
            network: NetworkPolicyUpdate,
        }

        let wire = Wire::deserialize(deserializer)?;
        Ok(Self {
            pool: SandboxPoolRequest {
                image: wire.image,
                resources: wire.resources,
                timeout_secs: wire.timeout_secs,
                entrypoint: wire.entrypoint,
                max_containers: wire.max_containers,
                warm_containers: wire.warm_containers,
            },
            network: wire.network,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateSandboxResponse {
    pub sandbox_id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing_hint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress_endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub termination_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_details: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CopySandboxResponse {
    pub source_sandbox_id: String,
    pub sandboxes: Vec<CreateSandboxResponse>,
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
    pub routing_hint: Option<String>,
    #[serde(default)]
    pub allow_unauthenticated_access: bool,
    #[serde(default)]
    pub exposed_ports: Option<Vec<u16>>,
    #[serde(default)]
    pub ingress_endpoint: Option<String>,
    #[serde(default)]
    pub sandbox_url: Option<String>,
    /// File systems currently mounted into the sandbox, each at its own
    /// guest mount path. Empty when no file systems are mounted.
    #[serde(default)]
    pub file_systems: Vec<FileSystemMount>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListSandboxesResponse {
    pub sandboxes: Vec<SandboxInfo>,
}

/// Sandbox information plus the archival timestamp. Returned by
/// `list_archived_sandboxes` and `get_archived_sandbox`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ArchivedSandboxInfo {
    #[serde(flatten)]
    pub sandbox: SandboxInfo,
    /// Wall-clock milliseconds when the sandbox was archived.
    pub archived_at: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListArchivedSandboxesResponse {
    pub sandboxes: Vec<ArchivedSandboxInfo>,
    #[serde(default)]
    pub prev_cursor: Option<String>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchivedSandboxesPaginationDirection {
    Forward,
    Backward,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ListArchivedSandboxesParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<ArchivedSandboxesPaginationDirection>,
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
    pub timeout_secs: i64,
    #[serde(default)]
    pub entrypoint: Option<Vec<String>>,
    #[serde(default)]
    pub max_containers: Option<i64>,
    #[serde(default)]
    pub warm_containers: Option<i64>,
    #[serde(default)]
    pub network_policy: Option<NetworkConfig>,
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
    pub snapshot_format_version: Option<String>,
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
    #[serde(default)]
    pub handle: Option<i64>,
    pub pid: i64,
    pub status: String,
    #[serde(default)]
    pub exit_code: Option<i64>,
    #[serde(default)]
    pub signal: Option<i64>,
    #[serde(default)]
    pub oom_killed: bool,
    #[serde(default)]
    pub stdin_writable: bool,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub started_at: serde_json::Value,
    #[serde(default)]
    pub ended_at: Option<serde_json::Value>,
    #[serde(default)]
    pub managed: Option<ProcessManagedInfo>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListProcessesResponse {
    pub processes: Vec<ProcessInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestartPolicy {
    Never,
    OnFailure,
    Always,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestartPolicyConfig {
    #[serde(default = "default_restart_policy")]
    pub policy: RestartPolicy,
    #[serde(default)]
    pub max_restarts: Option<u32>,
    #[serde(default = "default_restart_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "default_restart_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

fn default_restart_policy() -> RestartPolicy {
    RestartPolicy::OnFailure
}

fn default_restart_initial_backoff_ms() -> u64 {
    500
}

fn default_restart_max_backoff_ms() -> u64 {
    30_000
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessHealthCheckKind {
    Http,
    Tcp,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessHealthCheck {
    #[serde(rename = "type")]
    pub kind: ProcessHealthCheckKind,
    pub port: u16,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default = "default_health_initial_delay_ms")]
    pub initial_delay_ms: u64,
    #[serde(default = "default_health_interval_ms")]
    pub interval_ms: u64,
    #[serde(default = "default_health_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_health_failure_threshold")]
    pub failure_threshold: u32,
}

fn default_health_initial_delay_ms() -> u64 {
    5_000
}

fn default_health_interval_ms() -> u64 {
    1_000
}

fn default_health_timeout_ms() -> u64 {
    500
}

fn default_health_failure_threshold() -> u32 {
    3
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisedProcessStatus {
    Starting,
    Running,
    BackingOff,
    Stopped,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisedProcessHealthStatus {
    Disabled,
    Starting,
    Healthy,
    Unhealthy,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SupervisedProcessExit {
    #[serde(default)]
    pub exit_code: Option<i64>,
    #[serde(default)]
    pub signal: Option<i64>,
    #[serde(default)]
    pub oom_killed: bool,
    pub ended_at: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcessManagedInfo {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub status: SupervisedProcessStatus,
    pub restart_count: u32,
    pub restart: RestartPolicyConfig,
    #[serde(default)]
    pub health_check: Option<ProcessHealthCheck>,
    pub health_status: SupervisedProcessHealthStatus,
    pub consecutive_health_failures: u32,
    #[serde(default)]
    pub last_exit: Option<SupervisedProcessExit>,
    #[serde(default)]
    pub last_error: Option<String>,
    #[serde(default)]
    pub next_restart_at: Option<serde_json::Value>,
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
        #[serde(default)]
        oom_killed: bool,
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

/// Request body for `POST /api/v1/blob/sign`. The proxy converts either an
/// artifact path returned by platform-api or a full parent snapshot blob URI
/// into a concrete builder upload/download spec.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SignBlobRequest {
    pub target: SignBlobTarget,
    pub op: SignBlobOp,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignBlobTarget {
    Artifact { rel_path: String },
    Blob { uri: String },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SignBlobOp {
    PutArtifact {
        #[serde(skip_serializing_if = "Option::is_none")]
        multipart_hint: Option<MultipartHint>,
    },
    GetBlob,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MultipartHint {
    pub max_parts: u32,
    pub part_size_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_pool_request_deserializes_flat_network_policy() {
        let request: CreateSandboxPoolRequest = serde_json::from_value(serde_json::json!({
            "image": "alpine",
            "resources": {
                "cpus": 1.0,
                "memory_mb": 1024,
                "ephemeral_disk_mb": 1024
            },
            "timeout_secs": 0,
            "network": {
                "allow_internet_access": false,
                "allow_out": [],
                "deny_out": []
            }
        }))
        .unwrap();

        assert_eq!(request.pool.image.as_deref(), Some("alpine"));
        assert_eq!(
            request.network,
            Some(NetworkConfig {
                allow_internet_access: false,
                allow_out: vec![],
                deny_out: vec![],
            })
        );
    }

    #[test]
    fn file_system_mount_omits_false_mount_modes() {
        let mount = FileSystemMount {
            file_system_id: "skills".to_string(),
            mount_path: "/mnt/skills".to_string(),
            read_only: false,
            prefetch: false,
            snapshot_id: None,
            owner: None,
        };
        assert_eq!(
            serde_json::to_value(&mount).unwrap(),
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills"
            })
        );
    }

    #[test]
    fn file_system_mount_serializes_true_mount_modes() {
        let mount = FileSystemMount {
            file_system_id: "skills".to_string(),
            mount_path: "/mnt/skills".to_string(),
            read_only: true,
            prefetch: true,
            snapshot_id: None,
            owner: None,
        };
        assert_eq!(
            serde_json::to_value(&mount).unwrap(),
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
                "read_only": true,
                "prefetch": true
            })
        );
    }

    #[test]
    fn file_system_mount_serializes_snapshot_pin_only_when_set() {
        let pinned = FileSystemMount {
            file_system_id: "skills".to_string(),
            mount_path: "/mnt/skills".to_string(),
            read_only: true,
            prefetch: false,
            snapshot_id: Some("0abc123def".to_string()),
            owner: None,
        };
        assert_eq!(
            serde_json::to_value(&pinned).unwrap(),
            serde_json::json!({
                "file_system_id": "skills",
                "mount_path": "/mnt/skills",
                "read_only": true,
                "snapshot_id": "0abc123def"
            })
        );
    }

    #[test]
    fn file_system_mount_deserializes_absent_and_present_mount_modes() {
        let absent: FileSystemMount = serde_json::from_value(serde_json::json!({
            "file_system_id": "skills",
            "mount_path": "/mnt/skills"
        }))
        .unwrap();
        assert!(!absent.read_only);
        assert!(!absent.prefetch);
        assert_eq!(absent.snapshot_id, None);

        let present: FileSystemMount = serde_json::from_value(serde_json::json!({
            "file_system_id": "skills",
            "mount_path": "/mnt/skills",
            "read_only": true,
            "prefetch": true,
            "snapshot_id": "0abc123def"
        }))
        .unwrap();
        assert!(present.read_only);
        assert!(present.prefetch);
        assert_eq!(present.snapshot_id.as_deref(), Some("0abc123def"));
    }

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
    fn sign_blob_put_artifact_serializes_as_adt() {
        let body = SignBlobRequest {
            target: SignBlobTarget::Artifact {
                rel_path: "projects/p/sandbox-template-builds/b/s.tlsnap".to_string(),
            },
            op: SignBlobOp::PutArtifact {
                multipart_hint: Some(MultipartHint {
                    max_parts: 160,
                    part_size_bytes: 67_108_864,
                }),
            },
        };

        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            serde_json::json!({
                "target": {
                    "kind": "artifact",
                    "rel_path": "projects/p/sandbox-template-builds/b/s.tlsnap"
                },
                "op": {
                    "kind": "put_artifact",
                    "multipart_hint": {
                        "max_parts": 160,
                        "part_size_bytes": 67_108_864
                    }
                }
            })
        );
    }

    #[test]
    fn sign_blob_get_blob_serializes_as_adt() {
        let body = SignBlobRequest {
            target: SignBlobTarget::Blob {
                uri: "gs://bucket/path/parent-manifest.json".to_string(),
            },
            op: SignBlobOp::GetBlob,
        };

        assert_eq!(
            serde_json::to_value(&body).unwrap(),
            serde_json::json!({
                "target": {
                    "kind": "blob",
                    "uri": "gs://bucket/path/parent-manifest.json"
                },
                "op": { "kind": "get_blob" }
            })
        );
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
                oom_killed: false,
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
                oom_killed: false,
            }
        ));
    }

    #[test]
    fn run_process_event_deserializes_oom_killed() {
        let json = r#"{"signal": 9, "oom_killed": true}"#;
        let event: RunProcessEvent = serde_json::from_str(json).unwrap();
        assert!(matches!(
            event,
            RunProcessEvent::Exited {
                exit_code: None,
                signal: Some(9),
                oom_killed: true,
            }
        ));
    }

    #[test]
    fn process_info_deserializes_oom_killed() {
        let json = r#"{
            "pid": 42,
            "status": "oom_killed",
            "signal": 9,
            "oom_killed": true,
            "command": "/usr/local/bin/tl-rootfs-build",
            "args": [],
            "started_at": 123,
            "ended_at": 456
        }"#;
        let info: ProcessInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.status, "oom_killed");
        assert_eq!(info.signal, Some(9));
        assert!(info.oom_killed);
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
    fn copy_sandbox_response_deserializes() {
        let json = r#"{
            "source_sandbox_id":"source-1",
            "sandboxes":[{"sandbox_id":"copy-1","status":"running"}]
        }"#;
        let response: CopySandboxResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.source_sandbox_id, "source-1");
        assert_eq!(response.sandboxes[0].sandbox_id, "copy-1");
        assert_eq!(response.sandboxes[0].status, "running");
    }

    #[test]
    fn sandbox_info_deserializes_routing_hint_and_sandbox_url() {
        let json = r#"{
            "id":"sbx-1",
            "namespace":"default",
            "status":"running",
            "resources":{"cpus":1.0,"memory_mb":512,"ephemeral_disk_mb":1024},
            "routing_hint":"hint-1",
            "sandbox_url":"https://sbx-1.sandbox.tensorlake.ai"
        }"#;
        let info: SandboxInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.sandbox_id, "sbx-1");
        assert_eq!(info.routing_hint.as_deref(), Some("hint-1"));
        assert_eq!(
            info.sandbox_url.as_deref(),
            Some("https://sbx-1.sandbox.tensorlake.ai")
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

    #[test]
    fn gpu_models_use_server_wire_values() {
        let wire_values = ["A100-40GB", "A100-80GB", "H100", "T4", "A6000", "A10"];
        for (model, wire_value) in GpuModel::ALL.into_iter().zip(wire_values) {
            assert_eq!(model.to_string(), wire_value);
            assert_eq!(wire_value.parse::<GpuModel>().unwrap(), model);
            assert_eq!(
                serde_json::to_string(&model).unwrap(),
                format!(r#""{wire_value}""#)
            );
        }
    }

    #[test]
    fn gpu_request_requires_positive_count() {
        assert!(GpuRequest::new(GpuModel::H100, 0).is_err());
        assert_eq!(
            GpuRequest::new(GpuModel::H100, 2).unwrap(),
            GpuRequest {
                count: 2,
                model: GpuModel::H100,
            }
        );
        assert_eq!(
            GPUResources::from(GpuRequest::new(GpuModel::H100, 2).unwrap()),
            GPUResources {
                count: 2,
                model: "H100".to_string(),
            }
        );
    }
}
