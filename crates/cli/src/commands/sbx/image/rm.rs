use reqwest::StatusCode;
use tensorlake::error::SdkError;

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};

/// `tl sbx image rm <name|id>`: one delete through the merged catalog. The
/// reference is dispatched by shape (ADR 0107 D21): a `sandbox_template_` id
/// goes to the platform registry, an `image_` id to the Image Service, a
/// name to the Image Service first and then the platform registry. Not found
/// in both is an error; the request is never retried on a lost response.
pub async fn run(ctx: &CliContext, name_or_id: &str) -> Result<()> {
    let catalog = super::sandbox_image_catalog(ctx)?;
    match catalog.delete(name_or_id).await {
        Ok(()) => {
            println!("Deleted image '{}'.", name_or_id);
            Ok(())
        }
        Err(SdkError::ServerError { status, .. }) if status == StatusCode::NOT_FOUND => Err(
            CliError::Other(anyhow::anyhow!("image '{}' not found", name_or_id)),
        ),
        Err(error) => Err(error.into()),
    }
}
