pub mod models;

use reqwest::Method;
use serde::de::DeserializeOwned;

use crate::{client::Client, error::SdkError};

use models::{
    CreateSandboxPoolResponse, CreateSandboxRequest, CreateSandboxResponse, CreateSnapshotResponse,
    ListSandboxPoolsResponse, ListSandboxesResponse, ListSnapshotsResponse, SandboxInfo,
    SandboxPoolInfo, SandboxPoolRequest, SnapshotInfo,
};

/// A client for managing sandbox lifecycle, pool, and snapshot APIs.
#[derive(Clone)]
pub struct SandboxesClient {
    client: Client,
    namespace: String,
    use_namespaced_endpoints: bool,
}

impl SandboxesClient {
    /// Create a new sandboxes client.
    ///
    /// If `use_namespaced_endpoints` is true, requests are sent to
    /// `/v1/namespaces/{namespace}/...`; otherwise to `/{endpoint}`.
    pub fn new(
        client: Client,
        namespace: impl Into<String>,
        use_namespaced_endpoints: bool,
    ) -> Self {
        Self {
            client,
            namespace: namespace.into(),
            use_namespaced_endpoints,
        }
    }

    fn endpoint(&self, endpoint: &str) -> String {
        if self.use_namespaced_endpoints {
            format!("/v1/namespaces/{}/{}", self.namespace, endpoint)
        } else {
            format!("/{endpoint}")
        }
    }

    async fn parse_json<T: DeserializeOwned>(response: reqwest::Response) -> Result<T, SdkError> {
        let bytes = response.bytes().await?;
        let jd = &mut serde_json::Deserializer::from_slice(bytes.as_ref());
        let parsed = serde_path_to_error::deserialize(jd)?;
        Ok(parsed)
    }

    pub async fn create(
        &self,
        request: &CreateSandboxRequest,
    ) -> Result<CreateSandboxResponse, SdkError> {
        let uri = self.endpoint("sandboxes");
        let req = self
            .client
            .build_post_json_request(Method::POST, &uri, request)?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn claim(&self, pool_id: &str) -> Result<CreateSandboxResponse, SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}/sandboxes"));
        let req = self.client.request(Method::POST, &uri).build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn get(&self, sandbox_id: &str) -> Result<SandboxInfo, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}"));
        let req = self.client.request(Method::GET, &uri).build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn list(&self) -> Result<Vec<SandboxInfo>, SdkError> {
        let uri = self.endpoint("sandboxes");
        let req = self.client.request(Method::GET, &uri).build()?;
        let resp = self.client.execute(req).await?;
        let list: ListSandboxesResponse = Self::parse_json(resp).await?;
        Ok(list.sandboxes)
    }

    pub async fn delete(&self, sandbox_id: &str) -> Result<(), SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}"));
        let req = self.client.request(Method::DELETE, &uri).build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
    }

    pub async fn snapshot(&self, sandbox_id: &str) -> Result<CreateSnapshotResponse, SdkError> {
        let uri = self.endpoint(&format!("sandboxes/{sandbox_id}/snapshot"));
        let req = self.client.request(Method::POST, &uri).build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn get_snapshot(&self, snapshot_id: &str) -> Result<SnapshotInfo, SdkError> {
        let uri = self.endpoint(&format!("snapshots/{snapshot_id}"));
        let req = self.client.request(Method::GET, &uri).build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotInfo>, SdkError> {
        let uri = self.endpoint("snapshots");
        let req = self.client.request(Method::GET, &uri).build()?;
        let resp = self.client.execute(req).await?;
        let list: ListSnapshotsResponse = Self::parse_json(resp).await?;
        Ok(list.snapshots)
    }

    pub async fn delete_snapshot(&self, snapshot_id: &str) -> Result<(), SdkError> {
        let uri = self.endpoint(&format!("snapshots/{snapshot_id}"));
        let req = self.client.request(Method::DELETE, &uri).build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
    }

    pub async fn create_pool(
        &self,
        request: &SandboxPoolRequest,
    ) -> Result<CreateSandboxPoolResponse, SdkError> {
        let uri = self.endpoint("sandbox-pools");
        let req = self
            .client
            .build_post_json_request(Method::POST, &uri, request)?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn get_pool(&self, pool_id: &str) -> Result<SandboxPoolInfo, SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}"));
        let req = self.client.request(Method::GET, &uri).build()?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn list_pools(&self) -> Result<Vec<SandboxPoolInfo>, SdkError> {
        let uri = self.endpoint("sandbox-pools");
        let req = self.client.request(Method::GET, &uri).build()?;
        let resp = self.client.execute(req).await?;
        let list: ListSandboxPoolsResponse = Self::parse_json(resp).await?;
        Ok(list.pools)
    }

    pub async fn update_pool(
        &self,
        pool_id: &str,
        request: &SandboxPoolRequest,
    ) -> Result<SandboxPoolInfo, SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}"));
        let req = self
            .client
            .build_post_json_request(Method::PUT, &uri, request)?;
        let resp = self.client.execute(req).await?;
        Self::parse_json(resp).await
    }

    pub async fn delete_pool(&self, pool_id: &str) -> Result<(), SdkError> {
        let uri = self.endpoint(&format!("sandbox-pools/{pool_id}"));
        let req = self.client.request(Method::DELETE, &uri).build()?;
        let _resp = self.client.execute(req).await?;
        Ok(())
    }
}
