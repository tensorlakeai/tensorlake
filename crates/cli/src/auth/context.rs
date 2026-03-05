use reqwest::header::{HeaderMap, HeaderValue};

use crate::config::resolver::ResolvedConfig;
use crate::error::{CliError, Result};
use crate::http;

/// CLI context holding resolved configuration and providing authenticated HTTP clients.
#[derive(Debug, Clone)]
pub struct CliContext {
    pub api_url: String,
    pub cloud_url: String,
    pub namespace: String,
    pub api_key: Option<String>,
    pub personal_access_token: Option<String>,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub debug: bool,
    introspect_cache: Option<IntrospectResult>,
}

#[derive(Debug, Clone)]
struct IntrospectResult {
    id: Option<String>,
    organization_id: Option<String>,
    project_id: Option<String>,
}

impl CliContext {
    pub fn from_resolved(config: ResolvedConfig) -> Self {
        Self {
            api_url: config.api_url,
            cloud_url: config.cloud_url,
            namespace: config.namespace,
            api_key: config.api_key,
            personal_access_token: config.personal_access_token,
            organization_id: config.organization_id,
            project_id: config.project_id,
            debug: config.debug,
            introspect_cache: None,
        }
    }

    /// Build an authenticated reqwest client with proper headers.
    pub fn client(&self) -> Result<reqwest::Client> {
        let mut headers = HeaderMap::new();
        headers.insert("Accept", HeaderValue::from_static("application/json"));
        headers.insert(
            "User-Agent",
            HeaderValue::from_str(&format!(
                "Tensorlake CLI (rust/{})",
                env!("CARGO_PKG_VERSION")
            ))
            .unwrap_or_else(|_| HeaderValue::from_static("Tensorlake CLI")),
        );

        if let Some(key) = &self.api_key {
            headers.insert(
                "Authorization",
                HeaderValue::from_str(&format!("Bearer {}", key))
                    .map_err(|e| CliError::auth(e.to_string()))?,
            );
        } else if let Some(pat) = &self.personal_access_token {
            headers.insert(
                "Authorization",
                HeaderValue::from_str(&format!("Bearer {}", pat))
                    .map_err(|e| CliError::auth(e.to_string()))?,
            );
            if let Some(org_id) = self.effective_organization_id() {
                headers.insert(
                    "X-Forwarded-Organization-Id",
                    HeaderValue::from_str(&org_id).map_err(|e| CliError::auth(e.to_string()))?,
                );
            }
            if let Some(proj_id) = self.effective_project_id() {
                headers.insert(
                    "X-Forwarded-Project-Id",
                    HeaderValue::from_str(&proj_id).map_err(|e| CliError::auth(e.to_string()))?,
                );
            }
        } else {
            return Err(CliError::auth(
                "Missing API key or personal access token. Please run `tensorlake login` to authenticate.",
            ));
        }

        http::client_builder()
            .default_headers(headers)
            .build()
            .map_err(CliError::Http)
    }

    pub fn bearer_token(&self) -> Result<String> {
        self.api_key
            .as_ref()
            .or(self.personal_access_token.as_ref())
            .cloned()
            .ok_or_else(|| CliError::auth("No authentication configured"))
    }

    pub fn has_authentication(&self) -> bool {
        self.api_key.is_some() || self.personal_access_token.is_some()
    }

    pub fn has_org_and_project(&self) -> bool {
        self.effective_organization_id().is_some() && self.effective_project_id().is_some()
    }

    pub fn effective_organization_id(&self) -> Option<String> {
        // For API key, org comes from introspection (handled at call site)
        // For PAT, comes from config
        self.organization_id.clone()
    }

    pub fn effective_project_id(&self) -> Option<String> {
        self.project_id.clone()
    }

    /// Introspect API key to get org/project IDs.
    pub async fn introspect(&mut self) -> Result<()> {
        if self.api_key.is_none() {
            return Ok(());
        }
        if self.introspect_cache.is_some() {
            return Ok(());
        }

        let client = self.client()?;
        let resp = client
            .post(format!("{}/platform/v1/keys/introspect", self.api_url))
            .send()
            .await
            .map_err(CliError::Http)?;

        if resp.status().as_u16() == 401 {
            return Err(CliError::auth(
                "The TensorLake API key is not valid. Please supply a valid API key with --api-key, or run `tensorlake login`.",
            ));
        }
        if resp.status().as_u16() == 404 {
            return Err(CliError::auth(format!(
                "The server at {} doesn't support TensorLake API introspection.",
                self.api_url
            )));
        }
        if !resp.status().is_success() {
            return Err(CliError::auth(format!(
                "API key validation failed with status {}",
                resp.status()
            )));
        }

        let body: serde_json::Value = resp.json().await.map_err(CliError::Http)?;
        let result = IntrospectResult {
            id: body
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            organization_id: body
                .get("organizationId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            project_id: body
                .get("projectId")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        };

        // For API keys, set org/project from introspection
        if self.organization_id.is_none() {
            self.organization_id = result.organization_id.clone();
        }
        if self.project_id.is_none() {
            self.project_id = result.project_id.clone();
        }

        self.introspect_cache = Some(result);
        Ok(())
    }

    pub fn api_key_id(&self) -> Option<String> {
        self.introspect_cache.as_ref().and_then(|r| r.id.clone())
    }
}
