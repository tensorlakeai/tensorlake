use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CreateRepoRequest {
    #[serde(rename = "default_branch")]
    pub default_branch: String,
}

impl Default for CreateRepoRequest {
    fn default() -> Self {
        Self {
            default_branch: "main".to_string(),
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
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListReposResponse {
    pub project: String,
    pub repos: Vec<Repo>,
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
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ListOperationsResponse {
    pub repo: String,
    pub operations: Vec<Operation>,
}
