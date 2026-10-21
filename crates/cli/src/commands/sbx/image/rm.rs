use reqwest::StatusCode;
use tensorlake::error::SdkError;
use tensorlake::sandbox_image_catalog::{
    DeletedImageFamily, ImageReference, LegacyImageDeletion, SandboxImageCatalog,
    classify_image_reference,
};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};

/// `tl sbx image rm <name|id>`: one delete through the merged catalog. The
/// reference is dispatched by shape (ADR 0107 D21): a `sandbox_template_` id
/// goes to the platform registry, an `image_` id to the Image Service, a
/// name to the Image Service first and then the platform registry. A name
/// whose CAS image is deleted is also deleted from the platform registry.
/// Not found in both is an error; the request is never retried on a lost
/// response.
pub async fn run(ctx: &CliContext, name_or_id: &str) -> Result<()> {
    let catalog = super::sandbox_image_catalog(ctx)?;
    let id_image_name = image_service_id_name(&catalog, name_or_id).await;
    match catalog.delete_reporting(name_or_id).await {
        Ok(deletion) => {
            println!("Deleted image '{}'.", name_or_id);
            match deletion.same_named_legacy {
                Some(LegacyImageDeletion::Deleted { id }) => println!(
                    "Also deleted legacy image '{name_or_id}'{}: it could no longer be used \
                     once the CAS image was deleted.",
                    id_label(id.as_deref())
                ),
                Some(LegacyImageDeletion::Failed { id, error }) => eprintln!(
                    "⚠️  Could not delete legacy image '{name_or_id}'{}, which can no longer be \
                     used once the CAS image was deleted: {error}. Delete it with `tl sbx image \
                     rm {}`.",
                    id_label(id.as_deref()),
                    id.as_deref().unwrap_or(name_or_id)
                ),
                None if deletion.family == DeletedImageFamily::Cas => {
                    if let Some(name) = id_image_name {
                        warn_if_legacy_image_remains(&catalog, &name).await;
                    }
                }
                None => {}
            }
            Ok(())
        }
        Err(SdkError::ServerError { status, .. }) if status == StatusCode::NOT_FOUND => Err(
            CliError::Other(anyhow::anyhow!("image '{}' not found", name_or_id)),
        ),
        Err(error) => Err(error.into()),
    }
}

fn id_label(id: Option<&str>) -> String {
    id.map(|id| format!(" ({id})")).unwrap_or_default()
}

/// The name of the CAS image an `image_` id names, looked up before the
/// delete removes the record. Best effort: it only feeds the warning.
async fn image_service_id_name(catalog: &SandboxImageCatalog, name_or_id: &str) -> Option<String> {
    let Ok(ImageReference::ImageServiceId(id)) = classify_image_reference(name_or_id) else {
        return None;
    };
    catalog.image_service().get_by_id(&id).await.ok()??.name
}

/// Deleting a CAS image by id releases its name for new sandboxes, so a
/// legacy image of the same name stays listed but can no longer start one.
/// An id deletes only the image it names, so this warns instead of deleting
/// it. Best effort: the delete already succeeded, so a failed lookup prints
/// nothing.
async fn warn_if_legacy_image_remains(catalog: &SandboxImageCatalog, name: &str) {
    let Ok(Some(legacy)) = catalog.templates().find_by_name(name).await else {
        return;
    };
    let id = legacy.into_inner().id;
    let reference = id.as_deref().unwrap_or(name);
    eprintln!(
        "⚠️  Legacy image '{name}' ({reference}) has the same name and is still listed, but \
         new sandboxes can no longer use it. Rebuild or re-import it with --no-cas to use it \
         again, or delete it with `tl sbx image rm {reference}`."
    );
}
