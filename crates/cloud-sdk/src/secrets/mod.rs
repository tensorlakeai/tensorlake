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

use crate::{client::Client, error::SdkError};

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
    ///
    /// # Arguments
    ///
    /// * `organization_id` - The ID of the organization
    /// * `project_id` - The ID of the project
    /// * `upsert_secret` - The secret upsert request (single or multiple)
    ///
    /// # Returns
    ///
    /// Returns the upserted secret(s).
    ///
    /// # Example
    ///
    /// ```rust
    /// use tensorlake_cloud_sdk::{ClientBuilder, secrets::{SecretsClient, models::UpsertSecretRequest}};
    ///
    /// async fn example() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = ClientBuilder::new("https://api.tensorlake.ai")
    ///         .bearer_token("your-api-key")
    ///         .build()?;
    ///     let secrets_client = SecretsClient::new(client);
    ///     let req = UpsertSecretRequest::builder()
    ///         .organization_id("org-123")
    ///         .project_id("proj-456")
    ///         .secrets(("api-key", "secret123"))
    ///         .build()?;
    ///     secrets_client.upsert(req).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn upsert(
        &self,
        request: UpsertSecretRequest,
    ) -> Result<UpsertSecretResponse, SdkError> {
        let uri_str = format!(
            "/platform/v1/organizations/{}/projects/{}/secrets",
            request.organization_id, request.project_id
        );

        let req = self
            .client
            .build_post_json_request(Method::PUT, &uri_str, &request.secrets)?;
        let resp = self.client.execute(req).await?;

        let bytes = resp.bytes().await?;
        let jd = &mut serde_json::Deserializer::from_reader(bytes.as_ref());
        let response = serde_path_to_error::deserialize(jd)?;

        Ok(response)
    }

    /// List secrets in a project.
    ///
    /// # Arguments
    ///
    /// * `request` - The list secrets request
    ///
    /// # Returns
    ///
    /// Returns a list of secrets with pagination information.
    ///
    /// # Example
    ///
    /// ```rust
    /// use tensorlake_cloud_sdk::{ClientBuilder, secrets::{SecretsClient, models::ListSecretsRequest}};
    ///
    /// async fn example() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = ClientBuilder::new("https://api.tensorlake.ai")
    ///         .bearer_token("your-api-key")
    ///         .build()?;
    ///     let secrets_client = SecretsClient::new(client);
    ///     let request = ListSecretsRequest::builder()
    ///         .organization_id("org-123")
    ///         .project_id("proj-456")
    ///         .page_size(20)
    ///         .build()?;
    ///     secrets_client.list(&request).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn list(
        &self,
        request: &models::ListSecretsRequest,
    ) -> Result<SecretsList, SdkError> {
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
        let resp = self.client.execute(req).await?;

        let bytes = resp.bytes().await?;
        let jd = &mut serde_json::Deserializer::from_reader(bytes.as_ref());
        let list = serde_path_to_error::deserialize(jd)?;

        Ok(list)
    }

    /// Get a specific secret by ID.
    ///
    /// # Arguments
    ///
    /// * `request` - The get secret request
    ///
    /// # Returns
    ///
    /// Returns the secret details.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use tensorlake_cloud_sdk::{ClientBuilder, secrets::{SecretsClient, models::GetSecretRequest}};
    ///
    /// async fn example() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = ClientBuilder::new("https://api.tensorlake.ai")
    ///         .bearer_token("your-api-key")
    ///         .build()?;
    ///     let secrets_client = SecretsClient::new(client);
    ///     let request = GetSecretRequest::builder()
    ///         .organization_id("org-123")
    ///         .project_id("proj-456")
    ///         .secret_id("secret-789")
    ///         .build()?;
    ///     secrets_client.get(&request).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn get(&self, request: &models::GetSecretRequest) -> Result<Secret, SdkError> {
        let uri_str = format!(
            "/platform/v1/organizations/{}/projects/{}/secrets/{}",
            request.organization_id, request.project_id, request.secret_id
        );

        let req_builder = self.client.request(Method::GET, &uri_str);

        let req = req_builder.build()?;
        let resp = self.client.execute(req).await?;

        let bytes = resp.bytes().await?;
        let jd = &mut serde_json::Deserializer::from_reader(bytes.as_ref());
        let secret = serde_path_to_error::deserialize(jd)?;

        Ok(secret)
    }

    /// Delete a secret.
    ///
    /// # Arguments
    ///
    /// * `request` - The delete secret request
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use tensorlake_cloud_sdk::{ClientBuilder, secrets::{SecretsClient, models::DeleteSecretRequest}};
    ///
    /// async fn example() -> Result<(), Box<dyn std::error::Error>> {
    ///     let client = ClientBuilder::new("https://api.tensorlake.ai")
    ///         .bearer_token("your-api-key")
    ///         .build()?;
    ///     let secrets_client = SecretsClient::new(client);
    ///     let request = DeleteSecretRequest::builder()
    ///         .organization_id("org-123")
    ///         .project_id("proj-456")
    ///         .secret_id("secret-789")
    ///         .build()?;
    ///     secrets_client.delete(&request).await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn delete(&self, request: &models::DeleteSecretRequest) -> Result<(), SdkError> {
        let uri_str = format!(
            "/platform/v1/organizations/{}/projects/{}/secrets/{}",
            request.organization_id, request.project_id, request.secret_id
        );

        let req_builder = self.client.request(reqwest::Method::DELETE, &uri_str);

        let req = req_builder.build()?;
        let _resp = self.client.execute(req).await?;

        Ok(())
    }
}
