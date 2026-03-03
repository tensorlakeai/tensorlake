use rand::Rng;
use std::env;
use tensorlake_cloud_sdk::{Sdk, images::models::*};

fn env_var(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("{name} must be set"))
}

pub fn create_sdk() -> Result<Sdk, String> {
    let url = env_var("TENSORLAKE_API_URL")?;
    let api_key = env_var("TENSORLAKE_API_KEY")?;

    Sdk::new(&url, &api_key).map_err(|e| format!("failed to create SDK: {e}"))
}

#[allow(dead_code)]
pub async fn build_test_image(
    sdk: &Sdk,
    application_name: &str,
    application_version: &str,
    func_name: &str,
) -> Result<ImageBuildResult, String> {
    let images_client = sdk.images();

    // Create an image context
    let image = Image::builder()
        .name("test-integration-image".to_string())
        .base_image("python:3.13".to_string())
        .build_operations(vec![
            ImageBuildOperation::builder()
                .operation_type(ImageBuildOperationType::RUN)
                .args(vec!["pip install requests".to_string()])
                .build()
                .unwrap(),
        ])
        .build()
        .unwrap();

    // Build image
    let build_request = ImageBuildRequest::builder()
        .image(image)
        .image_tag(random_string())
        .application_name(application_name)
        .application_version(application_version)
        .function_name(func_name)
        .sdk_version(
            env::var("TENSORLAKE_PYTHON_SDK_VERSION").unwrap_or_else(|_| "~=0.4".to_string()),
        )
        .build()
        .map_err(|e| format!("failed to build image request: {e}"))?;

    images_client
        .build_image(build_request)
        .await
        .map_err(|e| format!("failed to build image: {e}"))
}

#[allow(dead_code)]
pub fn get_org_and_project_ids() -> Result<(String, String), String> {
    let org_id = env_var("TENSORLAKE_ORGANIZATION_ID")?;
    let project_id = env_var("TENSORLAKE_PROJECT_ID")?;

    Ok((org_id, project_id))
}

pub fn random_string() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    let mut rng = rand::rng();
    let length = rng.random_range(5..=10);

    (0..length)
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}
