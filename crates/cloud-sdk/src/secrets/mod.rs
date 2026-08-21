//! # Tensorlake Cloud SDK - Secrets
//!
//! This module provides functionality for managing secrets in the Tensorlake Cloud platform.
//!
//! ## Usage
//!
//! ```rust
//! use tensorlake::{Sdk, secrets::models::UpsertSecret};
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     let sdk = Sdk::new("https://api.tensorlake.ai", "your-api-key")?;
//!     let secrets_client = sdk.secrets();
//!
//!     // API keys derive the project from the bearer credential, so no
//!     // introspection or local org/project IDs are needed.
//!     secrets_client
//!         .upsert_api_key_scoped(UpsertSecret::from(("my-secret", "secret-value")))
//!         .await?;
//!     secrets_client.list_api_key_scoped(Some(100)).await?;
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
    /// use tensorlake::{ClientBuilder, secrets::SecretsClient};
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
        let path = format!(
            "/platform/v1/organizations/{}/projects/{}/secrets",
            request.organization_id, request.project_id
        );
        self.upsert_at(&path, request.secrets).await
    }

    /// Upsert secrets in the project bound to the authenticated API key.
    pub async fn upsert_api_key_scoped(
        &self,
        secrets: UpsertSecret,
    ) -> Result<Traced<UpsertSecretResponse>, SdkError> {
        self.upsert_at("/platform/v1/secrets", secrets).await
    }

    async fn upsert_at(
        &self,
        path: &str,
        secrets: UpsertSecret,
    ) -> Result<Traced<UpsertSecretResponse>, SdkError> {
        let req = self
            .client
            .build_post_json_request(Method::PUT, path, &secrets)?;
        self.client.execute_json(req).await
    }

    /// List secrets in a project.
    pub async fn list(
        &self,
        request: &models::ListSecretsRequest,
    ) -> Result<Traced<SecretsList>, SdkError> {
        let path = format!(
            "/platform/v1/organizations/{}/projects/{}/secrets",
            request.organization_id, request.project_id
        );
        self.list_at(
            &path,
            request.next.as_deref(),
            request.prev.as_deref(),
            request.page_size,
        )
        .await
    }

    /// List secrets in the project bound to the authenticated API key.
    pub async fn list_api_key_scoped(
        &self,
        page_size: Option<i32>,
    ) -> Result<Traced<SecretsList>, SdkError> {
        self.list_at("/platform/v1/secrets", None, None, page_size)
            .await
    }

    async fn list_at(
        &self,
        path: &str,
        next: Option<&str>,
        prev: Option<&str>,
        page_size: Option<i32>,
    ) -> Result<Traced<SecretsList>, SdkError> {
        let mut req_builder = self.client.request(Method::GET, path);

        if let Some(param_value) = next {
            req_builder = req_builder.query(&[("next", param_value)]);
        }
        if let Some(param_value) = prev {
            req_builder = req_builder.query(&[("prev", param_value)]);
        }
        if let Some(param_value) = page_size {
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

    /// Get a secret in the project bound to the authenticated API key.
    pub async fn get_api_key_scoped(&self, secret_id: &str) -> Result<Traced<Secret>, SdkError> {
        let path = format!("/platform/v1/secrets/{}", urlencoding::encode(secret_id));
        let req = self.client.request(Method::GET, &path).build()?;
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

    /// Delete a secret in the project bound to the authenticated API key.
    pub async fn delete_api_key_scoped(&self, secret_id: &str) -> Result<Traced<()>, SdkError> {
        let path = format!("/platform/v1/secrets/{}", urlencoding::encode(secret_id));
        let req = self.client.request(Method::DELETE, &path).build()?;
        Ok(self.client.execute_traced(req).await?.map(|_| ()))
    }
}
