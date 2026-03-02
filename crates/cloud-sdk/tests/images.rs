use tensorlake_cloud_sdk::images::models::*;

use crate::common::random_string;

mod common;

#[tokio::test]
#[cfg_attr(not(feature = "integration-tests"), ignore)]
async fn test_images_operations() {
    let sdk = match common::create_sdk() {
        Ok(sdk) => sdk,
        Err(msg) => {
            eprintln!("Skipping integration test: {msg}");
            return;
        }
    };

    let application_name = format!("integration_test_app_{}", random_string());
    let application_version = random_string();

    let result: Result<(), String> = async {
        let image =
            common::build_test_image(&sdk, &application_name, &application_version, "test_func")
                .await?;
        if image.status != BuildStatus::Succeeded {
            return Err(format!(
                "expected succeeded image build, got {:?}",
                image.status
            ));
        }

        let build_id = image.id.clone();
        let images_client = sdk.images();

        // Search across all pages to avoid missing the build due to pagination ordering.
        let mut found_in_list = false;
        let mut page = 1;
        loop {
            let list_request = ListBuildsRequest::builder()
                .page(page)
                .page_size(100)
                .build()
                .map_err(|e| format!("failed to build list request: {e}"))?;

            let list_response = images_client
                .list_builds(&list_request)
                .await
                .map_err(|e| format!("list builds failed: {e}"))?;

            if list_response.items.iter().any(|b| b.public_id == build_id) {
                found_in_list = true;
                break;
            }

            if page >= list_response.total_pages || list_response.items.is_empty() {
                break;
            }
            page += 1;
        }

        if !found_in_list {
            return Err(format!(
                "build {build_id} was not found in paginated build listing"
            ));
        }

        // Get build information
        let get_request = GetBuildInfoRequest::builder()
            .build_id(build_id.clone())
            .build()
            .map_err(|e| format!("failed to build get-build request: {e}"))?;

        let get_response = images_client
            .get_build_info(&get_request)
            .await
            .map_err(|e| format!("get build info failed: {e}"))?;

        if get_response.id != build_id {
            return Err(format!(
                "expected build id {build_id}, got {}",
                get_response.id
            ));
        }

        if get_response.status != BuildStatus::Succeeded {
            return Err(format!(
                "expected build status succeeded, got {:?}",
                get_response.status
            ));
        }

        Ok(())
    }
    .await;

    if let Err(err) = result {
        panic!("{err}");
    }
}
