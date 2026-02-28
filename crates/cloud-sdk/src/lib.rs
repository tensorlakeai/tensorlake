//! # Tensorlake Cloud SDK
//!
//! A Rust SDK for interacting with Tensorlake Cloud APIs.
//! This SDK provides a high-level, ergonomic interface for managing applications,
//! functions, and execution requests in the Tensorlake Cloud platform.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use tensorlake_cloud_sdk::{Sdk, applications::models::ListApplicationsRequest};
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create the SDK client
//!     let sdk = Sdk::new("https://api.tensorlake.ai", "your-api-key")?;
//!
//!     // Get the applications client
//!     let apps_client = sdk.applications();
//!
//!     // List applications in the default namespace
//!     let request = ListApplicationsRequest::builder()
//!         .namespace("default".to_string())
//!         .build()?;
//!     apps_client.list(&request).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Authentication
//!
//! The SDK uses Bearer token authentication, either a Personal Access Token (PAT) or a Project API key.
//! Provide your token when creating the SDK:
//!
//! ```rust,no_run
//! use tensorlake_cloud_sdk::Sdk;
//!
//! let sdk = Sdk::new("https://api.tensorlake.ai", "your-token").unwrap();
//! ```
//!
//! ## Available Clients
//!
//! - [`ApplicationsClient`](applications::ApplicationsClient): Manage applications, functions, and requests
//! - [`ImagesClient`](images::ImagesClient): Build and manage container images
//! - [`SecretsClient`](secrets::SecretsClient): Manage secrets for secure configuration
//!
//! ## Error Handling
//!
//! The SDK provides detailed error types for different scenarios:
//!
//! ```rust,no_run
//! use tensorlake_cloud_sdk::{Sdk, applications::models::ListApplicationsRequest};
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     let sdk = Sdk::new("https://api.tensorlake.ai", "your-api-key")?;
//!     let apps_client = sdk.applications();
//!
//!     let request = ListApplicationsRequest::builder()
//!         .namespace("default".to_string())
//!         .build()?;
//!     match apps_client.list(&request).await {
//!         Ok(apps) => println!("Success: {:?}", apps.applications.len()),
//!         Err(e) => eprintln!("Error: {}", e),
//!     }
//!     Ok(())
//! }
//! ```

pub mod applications;
pub mod error;
pub mod images;
pub mod secrets;
use applications::*;
use images::*;
use secrets::*;

mod client;
pub use client::{Client, ClientBuilder};

/// The main entry point for the Tensorlake Cloud SDK.
///
/// The `Sdk` struct provides a unified interface to all Tensorlake Cloud services.
/// It manages authentication and provides access to various service clients.
///
/// ## Example
///
/// ```rust
/// use tensorlake_cloud_sdk::Sdk;
///
/// let sdk = Sdk::new("https://api.tensorlake.ai", "your-api-key").unwrap();
///
/// // Access different service clients
/// let apps_client = sdk.applications();
/// let secrets_client = sdk.secrets();
/// ```
#[derive(Clone)]
pub struct Sdk {
    client: Client,
}

impl Sdk {
    /// Create a new SDK instance with the specified base URL and bearer token.
    ///
    /// # Arguments
    ///
    /// * `base_url` - The base URL of the Tensorlake Cloud API (e.g., "https://api.tensorlake.ai")
    /// * `bearer_token` - Your API key for authentication
    ///
    /// # Returns
    ///
    /// Returns a new `Sdk` instance configured with the provided credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created or configured.
    ///
    /// # Example
    ///
    /// ```rust
    /// use tensorlake_cloud_sdk::Sdk;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let sdk = Sdk::new("https://api.tensorlake.ai", "your-api-key").unwrap();
    /// Ok(())
    /// # }
    /// ```
    pub fn new(base_url: &str, bearer_token: &str) -> Result<Self, error::SdkError> {
        let client = ClientBuilder::new(base_url)
            .bearer_token(bearer_token)
            .build()?;
        Ok(Self { client })
    }

    /// Create a new SDK instance using a client builder.
    ///
    /// This method allows for more flexible configuration of the SDK client,
    /// including custom middleware, bearer tokens, and organization/project scopes.
    ///
    /// # Arguments
    ///
    /// * `builder` - A configured [`ClientBuilder`]
    ///
    /// # Returns
    ///
    /// Returns a new `Sdk` instance configured with the builder's settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be created or configured.
    ///
    /// # Example
    ///
    /// ```rust
    /// use tensorlake_cloud_sdk::{Sdk, ClientBuilder};
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let builder = ClientBuilder::new("https://api.tensorlake.ai")
    ///     .bearer_token("your-api-key")
    ///     .scope("org-id", "project-id");
    /// let sdk = Sdk::with_client_builder(builder)?;
    /// Ok(())
    /// # }
    /// ```
    pub fn with_client_builder(builder: ClientBuilder) -> Result<Self, error::SdkError> {
        let client = builder.build()?;
        Ok(Self { client })
    }

    /// Get a client for managing applications and requests.
    ///
    /// This method returns an [`ApplicationsClient`] that provides methods for:
    /// - Listing, creating, updating, and deleting applications
    /// - Invoking applications with data
    /// - Managing execution requests
    ///
    /// # Returns
    ///
    /// Returns an [`ApplicationsClient`] instance configured with the SDK's authentication.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use tensorlake_cloud_sdk::{Sdk, applications::models::ListApplicationsRequest};
    ///
    /// async fn example() -> Result<(), Box<dyn std::error::Error>> {
    ///     let sdk = Sdk::new("https://api.tensorlake.ai", "your-api-key")?;
    ///     let apps_client = sdk.applications();
    ///
    ///     // Use the applications client
    ///     let request = ListApplicationsRequest::builder()
    ///         .namespace("default".to_string())
    ///         .build()?;
    ///     apps_client.list(&request).await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn applications(&self) -> ApplicationsClient {
        ApplicationsClient::new(self.client.clone())
    }

    /// Get a client for building and managing container images.
    ///
    /// This method returns an [`ImagesClient`] that provides methods for:
    /// - Building container images from source code and Dockerfiles
    /// - Monitoring build progress and status
    ///
    /// # Returns
    ///
    /// Returns an [`ImagesClient`] instance configured with the SDK's authentication.
    ///
    /// # Example
    ///
    /// ```rust
    /// use tensorlake_cloud_sdk::Sdk;
    ///
    /// let sdk = Sdk::new("https://api.tensorlake.ai", "your-api-key").unwrap();
    /// let images_client = sdk.images();
    ///
    /// // Use the images client
    /// // let result = images_client.build_image(request).await?;
    /// ```
    pub fn images(&self) -> ImagesClient {
        ImagesClient::new(self.client.clone())
    }

    /// Get a client for managing secrets.
    ///
    /// This method returns a [`SecretsClient`] that provides methods for:
    /// - Creating, updating, and deleting secrets
    /// - Listing secrets in a project
    /// - Retrieving individual secret details
    ///
    /// # Returns
    ///
    /// Returns a [`SecretsClient`] instance configured with the SDK's authentication.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use tensorlake_cloud_sdk::{Sdk, secrets::models::ListSecretsRequest};
    ///
    /// async fn example() -> Result<(), Box<dyn std::error::Error>> {
    ///     let sdk = Sdk::new("https://api.tensorlake.ai", "your-api-key")?;
    ///     let secrets_client = sdk.secrets();
    ///
    ///     // Use the secrets client
    ///     let request = ListSecretsRequest::builder()
    ///         .organization_id("org-id".to_string())
    ///         .project_id("project-id".to_string())
    ///         .build()?;
    ///     secrets_client.list(&request).await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn secrets(&self) -> SecretsClient {
        SecretsClient::new(self.client.clone())
    }
}
