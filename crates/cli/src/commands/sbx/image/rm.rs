use reqwest::StatusCode;
use tensorlake::error::SdkError;

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};

pub async fn run(ctx: &CliContext, name_or_id: &str) -> Result<()> {
    let templates_client = super::sandbox_templates_client(ctx)?;

    match templates_client.delete(name_or_id).await {
        Ok(_) => {
            println!("Deleted image '{}'.", name_or_id);
            Ok(())
        }
        Err(SdkError::ServerError { status, .. }) if status == StatusCode::NOT_FOUND => {
            delete_resolved_image(ctx, name_or_id).await
        }
        Err(error) => Err(error.into()),
    }
}

async fn delete_resolved_image(ctx: &CliContext, name_or_id: &str) -> Result<()> {
    let (base_url, _, _) = super::templates_base_url(ctx)?;
    let client = ctx.client()?;
    let item = super::find_image_item_in_paginated_list(ctx, &client, &base_url, name_or_id)
        .await?
        .ok_or_else(|| CliError::Other(anyhow::anyhow!("image '{}' not found", name_or_id)))?;

    let name = item
        .get("name")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            CliError::Other(anyhow::anyhow!(
                "image '{}' was found but is missing a registered name",
                name_or_id
            ))
        })?;

    let templates_client = super::sandbox_templates_client(ctx)?;
    templates_client.delete(name).await?;
    println!("Deleted image '{}'.", name);
    Ok(())
}
