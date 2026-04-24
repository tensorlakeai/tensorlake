//! # Tensorlake Cloud SDK - Secrets
//!
//! This module provides functionality for managing secrets in the Tensorlake Cloud platform.
//!
//! ## Usage
//!
//! ```rust
//! use tensorlake_cloud_sdk::{Sdk, secrets::models::{UpsertSecretRequest, ListSecretsRequest}};
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     let sdk = Sdk::new("https://api.tensorlake.ai", "your-api-key")?;
//!     let secrets_client = sdk.secrets();
//!
//!     // Create a secret
//!     let create_req = UpsertSecretRequest::builder()
//!         .organization_id("org-id")
//!         .project_id("project-id")
//!         .secrets(("my-secret", "secret-value"))
//!         .build()?;
//!     secrets_client.upsert(create_req).await?;
//!
//!     // List secrets
//!     let list_req = ListSecretsRequest::builder()
//!         .organization_id("org-id")
//!         .project_id("project-id")
//!         .build()?;
//!     secrets_client.list(&list_req).await?;
//!     Ok(())
//! }
//! ```

pub mod error;
pub mod models;

use crate::{
    client::{Client, Traced},
    error::SdkError,
};

use models::*;
use reqwest::Method;

/// A client for managing secrets in Tensorlake Cloud.
#[derive(Clone)]
pub struct SecretsClient {
    client: Client,
}

impl SecretsClient {
    /// Create a new secrets client.
    ///
    /// # Arguments
    ///
    /// * `client` - The base HTTP client configured with authentication
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use tensorlake_cloud_sdk::{ClientBuilder, secrets::SecretsClient};
    ///
    /// fn example() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = ClientBuilder::new("https://api.tensorlake.ai")
    ///         .bearer_token("your-api-key")
    ///         .build()?;
    ///     let secrets_client = SecretsClient::new(client);
    ///     Ok(())
    /// }
    /// ```
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Upsert secrets (create or update).
    pub async fn upsert(
        &self,
        request: UpsertSecretRequest,
    ) -> Result<Traced<UpsertSecretResponse>, SdkError> {
        let uri_str = format!(
            "/platform/v1/organizations/{}/projects/{}/secrets",
            request.organization_id, request.project_id
        );
        let req = self
            .client
            .build_post_json_request(Method::PUT, &uri_str, &request.secrets)?;
        self.client.execute_json(req).await
    }

    /// List secrets in a project.
    pub async fn list(
        &self,
        request: &models::ListSecretsRequest,
    ) -> Result<Traced<SecretsList>, SdkError> {
        let uri_str = format!(
            "/platform/v1/organizations/{}/projects/{}/secrets",
            request.organization_id, request.project_id
        );

        let mut req_builder = self.client.request(Method::GET, &uri_str);

        if let Some(param_value) = &request.next {
            req_builder = req_builder.query(&[("next", param_value)]);
        }
        if let Some(param_value) = &request.prev {
            req_builder = req_builder.query(&[("prev", param_value)]);
        }
        if let Some(param_value) = request.page_size {
            req_builder = req_builder.query(&[("pageSize", param_value)]);
        }

        let req = req_builder.build()?;
        self.client.execute_json(req).await
    }

    /// Get a specific secret by ID.
    pub async fn get(
        &self,
        request: &models::GetSecretRequest,
    ) -> Result<Traced<Secret>, SdkError> {
        let uri_str = format!(
            "/platform/v1/organizations/{}/projects/{}/secrets/{}",
            request.organization_id, request.project_id, request.secret_id
        );
        let req = self.client.request(Method::GET, &uri_str).build()?;
        self.client.execute_json(req).await
    }

    /// Delete a secret.
    pub async fn delete(
        &self,
        request: &models::DeleteSecretRequest,
    ) -> Result<Traced<()>, SdkError> {
        let uri_str = format!(
            "/platform/v1/organizations/{}/projects/{}/secrets/{}",
            request.organization_id, request.project_id, request.secret_id
        );
        let req = self
            .client
            .request(reqwest::Method::DELETE, &uri_str)
            .build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }
}
