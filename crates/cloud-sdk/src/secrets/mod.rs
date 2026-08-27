//! Secret management through the regional Secret Service.

pub mod error;
pub mod models;

use chrono::{SecondsFormat, TimeZone, Utc};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    client::{Client, Traced},
    error::SdkError,
};

use models::*;

#[derive(Clone)]
pub struct SecretsClient {
    client: Client,
}

#[derive(Debug, Deserialize)]
struct SecretMetadata {
    id: String,
    name: String,
    created_at_ms: i64,
}

#[derive(Debug, Deserialize)]
struct ResolvedSecretName {
    id: String,
}

#[derive(Serialize)]
struct CreateSecret<'a> {
    name: &'a str,
    value: &'a str,
}

#[derive(Serialize)]
struct RotateSecret<'a> {
    value: &'a str,
}

impl From<SecretMetadata> for Secret {
    fn from(value: SecretMetadata) -> Self {
        let created_at = Utc
            .timestamp_millis_opt(value.created_at_ms)
            .single()
            .map(|time| time.to_rfc3339_opts(SecondsFormat::Millis, true))
            .unwrap_or_else(|| value.created_at_ms.to_string());
        Self {
            id: value.id,
            name: value.name,
            created_at,
        }
    }
}

impl SecretsClient {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Create new secrets or rotate existing secrets in a namespace.
    /// `organization_id` remains in the request type for source compatibility;
    /// Secret Service isolation is keyed exclusively by the project public ID.
    pub async fn upsert(
        &self,
        request: UpsertSecretRequest,
    ) -> Result<Traced<UpsertSecretResponse>, SdkError> {
        let path = namespace_secrets_path(&request.project_id);
        self.upsert_at(&path, request.secrets).await
    }

    /// Upsert in the project bound to the authenticated API key. Ingress
    /// derives the namespace from verified authentication context.
    pub async fn upsert_api_key_scoped(
        &self,
        secrets: UpsertSecret,
    ) -> Result<Traced<UpsertSecretResponse>, SdkError> {
        self.upsert_at("/v1/secrets", secrets).await
    }

    async fn upsert_at(
        &self,
        collection_path: &str,
        secrets: UpsertSecret,
    ) -> Result<Traced<UpsertSecretResponse>, SdkError> {
        let (items, was_multiple) = match secrets {
            UpsertSecret::Single(secret) => (vec![secret], false),
            UpsertSecret::Multiple(secrets) => (secrets, true),
        };
        if items.is_empty() {
            return Ok(Traced::new(
                String::new(),
                UpsertSecretResponse::Multiple(Vec::new()),
            ));
        }

        let mut results = Vec::with_capacity(items.len());
        let mut last_trace_id = String::new();
        for secret in &items {
            let traced = self.upsert_one(collection_path, secret).await?;
            last_trace_id = traced.trace_id.clone();
            results.push(traced.into_inner());
        }
        let response = if was_multiple {
            UpsertSecretResponse::Multiple(results)
        } else {
            UpsertSecretResponse::Single(results.remove(0))
        };
        Ok(Traced::new(last_trace_id, response))
    }

    async fn upsert_one(
        &self,
        collection_path: &str,
        secret: &NewSecret,
    ) -> Result<Traced<Secret>, SdkError> {
        let request = self
            .client
            .request(Method::POST, collection_path)
            .header("Idempotency-Key", Uuid::new_v4().to_string())
            .json(&CreateSecret {
                name: &secret.name,
                value: &secret.value,
            })
            .build()?;
        match self.client.execute_json::<SecretMetadata>(request).await {
            Ok(created) => Ok(created.map(Into::into)),
            Err(SdkError::ServerError {
                status: StatusCode::CONFLICT,
                ..
            }) => {
                let base = collection_path
                    .strip_suffix("/secrets")
                    .expect("secret collection path must end in /secrets");
                let lookup_path =
                    format!("{base}/secret-names/{}", urlencoding::encode(&secret.name));
                let lookup = self.client.request(Method::GET, &lookup_path).build()?;
                let existing: Traced<ResolvedSecretName> = self.client.execute_json(lookup).await?;
                let rotate_path = format!(
                    "{collection_path}/{}/versions",
                    urlencoding::encode(&existing.id)
                );
                let rotate = self
                    .client
                    .request(Method::POST, &rotate_path)
                    .header("Idempotency-Key", Uuid::new_v4().to_string())
                    .json(&RotateSecret {
                        value: &secret.value,
                    })
                    .build()?;
                self.client
                    .execute_json::<SecretMetadata>(rotate)
                    .await
                    .map(|value| value.map(Into::into))
            }
            Err(error) => Err(error),
        }
    }

    pub async fn list(
        &self,
        request: &models::ListSecretsRequest,
    ) -> Result<Traced<SecretsList>, SdkError> {
        self.list_at(
            &namespace_secrets_path(&request.project_id),
            request.page_size,
        )
        .await
    }

    pub async fn list_api_key_scoped(
        &self,
        page_size: Option<i32>,
    ) -> Result<Traced<SecretsList>, SdkError> {
        self.list_at("/v1/secrets", page_size).await
    }

    async fn list_at(
        &self,
        path: &str,
        page_size: Option<i32>,
    ) -> Result<Traced<SecretsList>, SdkError> {
        let request = self.client.request(Method::GET, path).build()?;
        let response: Traced<Vec<SecretMetadata>> = self.client.execute_json(request).await?;
        Ok(response.map(|metadata| {
            let total = i32::try_from(metadata.len()).unwrap_or(i32::MAX);
            let limit = page_size.unwrap_or(i32::MAX).max(0) as usize;
            SecretsList {
                items: metadata.into_iter().take(limit).map(Into::into).collect(),
                pagination: Pagination {
                    next: None,
                    prev: None,
                    total,
                },
            }
        }))
    }

    pub async fn get(
        &self,
        request: &models::GetSecretRequest,
    ) -> Result<Traced<Secret>, SdkError> {
        self.get_at(&format!(
            "{}/{}",
            namespace_secrets_path(&request.project_id),
            urlencoding::encode(&request.secret_id)
        ))
        .await
    }

    pub async fn get_api_key_scoped(&self, secret_id: &str) -> Result<Traced<Secret>, SdkError> {
        self.get_at(&format!("/v1/secrets/{}", urlencoding::encode(secret_id)))
            .await
    }

    async fn get_at(&self, path: &str) -> Result<Traced<Secret>, SdkError> {
        let request = self.client.request(Method::GET, path).build()?;
        self.client
            .execute_json::<SecretMetadata>(request)
            .await
            .map(|value| value.map(Into::into))
    }

    pub async fn delete(
        &self,
        request: &models::DeleteSecretRequest,
    ) -> Result<Traced<()>, SdkError> {
        self.delete_at(&format!(
            "{}/{}",
            namespace_secrets_path(&request.project_id),
            urlencoding::encode(&request.secret_id)
        ))
        .await
    }

    pub async fn delete_api_key_scoped(&self, secret_id: &str) -> Result<Traced<()>, SdkError> {
        self.delete_at(&format!("/v1/secrets/{}", urlencoding::encode(secret_id)))
            .await
    }

    async fn delete_at(&self, path: &str) -> Result<Traced<()>, SdkError> {
        let request = self.client.request(Method::DELETE, path).build()?;
        Ok(self.client.execute_traced(request).await?.map(|_| ()))
    }
}

fn namespace_secrets_path(project_id: &str) -> String {
    format!("/v1/namespaces/{}/secrets", urlencoding::encode(project_id))
}
