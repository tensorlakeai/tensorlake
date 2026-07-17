use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MintGitTokenRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreateRepoRequest {
    #[serde(rename = "default_branch")]
    pub default_branch: String,
    /// "repository" (default) or "filesystem". Omitted (not sent) when `None` so the request
    /// stays valid against servers that predate repo kinds (`deny_unknown_fields`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

impl Default for CreateRepoRequest {
    fn default() -> Self {
        Self {
            default_branch: "main".to_string(),
            kind: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GitCredential {
    pub token: String,
    pub token_type: String,
    pub expires_at: String,
    pub git_username: String,
    pub repo_pattern: String,
    pub scopes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Repo {
    pub name: String,
    pub full_name: String,
    pub default_branch: String,
    pub status: String,
    /// "repository" or "filesystem". Defaults to "repository" when the server predates repo
    /// kinds and omits the field.
    #[serde(default = "default_repo_kind")]
    pub kind: String,
}

fn default_repo_kind() -> String {
    REPO_KIND_REPOSITORY.to_string()
}

/// The wire names for repo kinds — defined once; every construction and comparison goes
/// through these (a typo'd bare literal would compile and silently misroute).
pub const REPO_KIND_REPOSITORY: &str = "repository";
pub const REPO_KIND_FILESYSTEM: &str = "filesystem";

impl Repo {
    pub fn is_filesystem(&self) -> bool {
        self.kind == REPO_KIND_FILESYSTEM
    }
}

/// `GET /project/{p}/repos/{r}/meta` — the authoritative point-read of one repo's product
/// identity. Never served from the listing cache (read-your-writes after create) and
/// answerable with a repo-scoped credential.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RepoMetaInfo {
    pub name: String,
    pub full_name: String,
    pub default_branch: String,
    pub status: String,
    #[serde(default = "default_repo_kind")]
    pub kind: String,
}

impl RepoMetaInfo {
    pub fn is_filesystem(&self) -> bool {
        self.kind == REPO_KIND_FILESYSTEM
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListReposResponse {
    pub project: String,
    pub repos: Vec<Repo>,
    #[serde(default)]
    pub next_after: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitRef {
    pub name: String,
    pub oid: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListRefsResponse {
    pub repo: String,
    pub refs: Vec<GitRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Branch {
    pub name: String,
    pub ref_name: String,
    pub oid: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListBranchesResponse {
    pub repo: String,
    pub branches: Vec<Branch>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RepoInfo {
    pub repo: String,
    pub url: String,
    pub branches: Vec<Branch>,
    pub refs: Vec<GitRef>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OperationRef {
    pub name: String,
    pub old: Option<String>,
    pub new: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Operation {
    pub op_id: String,
    pub repo: String,
    pub network: Option<String>,
    pub parent_op_id: Option<String>,
    pub actor: String,
    pub at_secs: u64,
    pub kind: String,
    pub result: String,
    pub refs: Vec<OperationRef>,
    pub pack_ids: Vec<String>,
    pub old_pack_ids: Vec<String>,
    pub related_repo: Option<String>,
    pub status: Option<String>,
    pub old_pack_count: u32,
    pub object_count: u32,
    pub pack_bytes: u64,
    /// The operation materialized merge conflicts (its commit carries a conflict record).
    /// Elided by the server when false; absent from pre-visibility servers.
    #[serde(default)]
    pub conflicted: bool,
    /// Native snapshot attributed to a verification or head-promotion operation. Absent on Git
    /// operations and on servers predating the native engine.
    #[serde(default)]
    pub native_snapshot: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListOperationsResponse {
    pub repo: String,
    pub operations: Vec<Operation>,
    #[serde(default)]
    pub next_after: Option<String>,
}
