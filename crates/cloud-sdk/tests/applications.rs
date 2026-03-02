use data_encoding::BASE64;
use std::{collections::HashMap, io::Write, time::Duration};
use tensorlake_cloud_sdk::{applications::models::*, images::models::BuildStatus};
use tokio::time::sleep;

use crate::common::random_string;

mod common;

const APP_CODE: &str = r#"
from tensorlake.applications import application, function

@application()
@function(description="A simple test function")
def simple_test_func(input_text: str) -> str:
    output = helper_func(input_text)
    return f"Processed: {output}"

@function()
def helper_func(value: str) -> str:
    return f"Helper processed: {value}"
"#;

fn build_code_zip() -> Result<Vec<u8>, String> {
    let mut zip_writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let options =
        zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Deflated);

    let manifest = r#"{
        "functions": {
            "simple_test_func": {
                "name": "simple_test_func",
                "module_import_name": "app"
            },
            "helper_func": {
                "name": "helper_func",
                "module_import_name": "app"
            }
        }
    }"#;

    zip_writer
        .start_file(".tensorlake_code_manifest.json", options)
        .map_err(|e| format!("failed to add manifest to zip: {e}"))?;
    zip_writer
        .write_all(manifest.as_bytes())
        .map_err(|e| format!("failed to write manifest to zip: {e}"))?;

    zip_writer
        .start_file("app.py", options)
        .map_err(|e| format!("failed to add app.py to zip: {e}"))?;
    zip_writer
        .write_all(APP_CODE.as_bytes())
        .map_err(|e| format!("failed to write app.py to zip: {e}"))?;

    let cursor = zip_writer
        .finish()
        .map_err(|e| format!("failed to finalize zip archive: {e}"))?;

    Ok(cursor.into_inner())
}

fn build_app_manifest(
    application_name: &str,
    application_version: String,
    function_entrypoint: &str,
    data_type: DataType,
) -> Result<ApplicationManifest, String> {
    let return_type = data_type
        .to_json_value()
        .map_err(|e| format!("failed to serialize return type: {e}"))?;
    let output_hint = BASE64.encode(
        data_type
            .to_json_string()
            .map_err(|e| format!("failed to encode output hint: {e}"))?
            .as_bytes(),
    );

    let function_manifest = FunctionManifest::builder()
        .name("simple_test_func")
        .description("A simple test function")
        .is_api(true)
        .initialization_timeout_sec(300)
        .timeout_sec(300)
        .resources(
            Resources::builder()
                .cpus(1.0)
                .memory_mb(1024)
                .ephemeral_disk_mb(2048)
                .build()
                .map_err(|e| format!("failed to build function resources: {e}"))?,
        )
        .retry_policy(
            RetryPolicy::builder()
                .max_retries(0)
                .initial_delay_sec(1.0)
                .max_delay_sec(60.0)
                .delay_multiplier(2.0)
                .build()
                .map_err(|e| format!("failed to build retry policy: {e}"))?,
        )
        .parameters(vec![
            Parameter::builder()
                .name("input_text")
                .data_type(data_type.clone())
                .build()
                .map_err(|e| format!("failed to build function parameter: {e}"))?,
        ])
        .return_type(return_type.clone())
        .placement_constraints(
            PlacementConstraintsManifest::builder()
                .build()
                .map_err(|e| format!("failed to build placement constraints: {e}"))?,
        )
        .max_concurrency(1)
        .build()
        .map_err(|e| format!("failed to build API function manifest: {e}"))?;

    let helper_function_manifest = FunctionManifest::builder()
        .name("helper_func")
        .is_api(false)
        .initialization_timeout_sec(300)
        .timeout_sec(300)
        .resources(
            Resources::builder()
                .cpus(1.0)
                .memory_mb(1024)
                .ephemeral_disk_mb(2048)
                .build()
                .map_err(|e| format!("failed to build helper resources: {e}"))?,
        )
        .retry_policy(
            RetryPolicy::builder()
                .max_retries(0)
                .initial_delay_sec(1.0)
                .max_delay_sec(60.0)
                .delay_multiplier(2.0)
                .build()
                .map_err(|e| format!("failed to build helper retry policy: {e}"))?,
        )
        .parameters(vec![
            Parameter::builder()
                .name("value")
                .data_type(data_type)
                .build()
                .map_err(|e| format!("failed to build helper parameter: {e}"))?,
        ])
        .return_type(return_type)
        .placement_constraints(
            PlacementConstraintsManifest::builder()
                .build()
                .map_err(|e| format!("failed to build helper placement constraints: {e}"))?,
        )
        .max_concurrency(1)
        .build()
        .map_err(|e| format!("failed to build helper function manifest: {e}"))?;

    let mut functions = HashMap::new();
    functions.insert("simple_test_func".to_string(), function_manifest);
    functions.insert("helper_func".to_string(), helper_function_manifest);

    ApplicationManifest::builder()
        .name(application_name)
        .description("Test application")
        .tags(HashMap::new())
        .version(application_version)
        .functions(functions)
        .entrypoint(
            Entrypoint::builder()
                .function_name(function_entrypoint)
                .input_serializer("json")
                .output_serializer("json")
                .output_type_hints_base64(output_hint)
                .build()
                .map_err(|e| format!("failed to build app entrypoint: {e}"))?,
        )
        .build()
        .map_err(|e| format!("failed to build app manifest: {e}"))
}

#[tokio::test]
#[cfg_attr(not(feature = "integration-tests"), ignore)]
async fn test_applications_operations() {
    let sdk = match common::create_sdk() {
        Ok(sdk) => sdk,
        Err(msg) => {
            eprintln!("Skipping integration test: {msg}");
            return;
        }
    };
    let (_, project_id) = match common::get_org_and_project_ids() {
        Ok(ids) => ids,
        Err(msg) => {
            eprintln!("Skipping integration test: {msg}");
            return;
        }
    };

    let application_name = format!("integration_test_app_{}", random_string());
    let application_version = random_string();
    let function_entrypoint = "simple_test_func";

    let apps_client = sdk.applications();
    let mut app_created = false;
    let mut request_id_for_cleanup: Option<String> = None;

    let result: Result<(), String> = async {
        let data_type = DataType::builder()
            .typ("string")
            .build()
            .map_err(|e| format!("failed to build data type: {e}"))?;

        // Build both function images.
        let image = common::build_test_image(
            &sdk,
            &application_name,
            &application_version,
            function_entrypoint,
        )
        .await?;
        if image.status != BuildStatus::Succeeded {
            return Err(format!(
                "entrypoint image build did not succeed: {:?}",
                image.status
            ));
        }

        let image =
            common::build_test_image(&sdk, &application_name, &application_version, "helper_func")
                .await?;
        if image.status != BuildStatus::Succeeded {
            return Err(format!(
                "helper image build did not succeed: {:?}",
                image.status
            ));
        }

        let zip_data = build_code_zip()?;
        let app_manifest = build_app_manifest(
            &application_name,
            application_version.clone(),
            function_entrypoint,
            data_type,
        )?;

        let upsert_request = UpsertApplicationRequest::builder()
            .namespace(&project_id)
            .application_manifest(app_manifest)
            .code_zip(zip_data)
            .build()
            .map_err(|e| format!("failed to build upsert request: {e}"))?;

        apps_client
            .upsert(&upsert_request)
            .await
            .map_err(|e| format!("upsert application failed: {e}"))?;
        app_created = true;

        let list_request = ListApplicationsRequest::builder()
            .namespace(&project_id)
            .limit(100)
            .build()
            .map_err(|e| format!("failed to build list apps request: {e}"))?;

        let list_response = apps_client
            .list(&list_request)
            .await
            .map_err(|e| format!("list applications failed: {e}"))?;

        if !list_response
            .applications
            .iter()
            .any(|a| a.name == application_name)
        {
            return Err(format!(
                "application {application_name} not found in application list"
            ));
        }

        let get_request = GetApplicationRequest::builder()
            .namespace(&project_id)
            .application(&application_name)
            .build()
            .map_err(|e| format!("failed to build get app request: {e}"))?;

        let get_response = apps_client
            .get(&get_request)
            .await
            .map_err(|e| format!("get application failed: {e}"))?;

        if get_response.name != application_name {
            return Err(format!(
                "expected app name {application_name}, got {}",
                get_response.name
            ));
        }

        let invoke_request = InvokeApplicationRequest::builder()
            .namespace(&project_id)
            .application(&application_name)
            .body(serde_json::json!({"input_text": "hello world"}))
            .build()
            .map_err(|e| format!("failed to build invoke request: {e}"))?;

        let invoke_response = apps_client
            .invoke(&invoke_request)
            .await
            .map_err(|e| format!("invoke failed: {e}"))?;

        let request_id = match invoke_response {
            InvokeResponse::RequestId(id) if !id.is_empty() => id,
            _ => return Err("invoke response did not contain a non-empty request id".to_string()),
        };
        request_id_for_cleanup = Some(request_id.clone());

        let list_requests_request = ListRequestsRequest::builder()
            .namespace(&project_id)
            .application(&application_name)
            .limit(50)
            .build()
            .map_err(|e| format!("failed to build list requests request: {e}"))?;

        let requests = apps_client
            .list_requests(&list_requests_request)
            .await
            .map_err(|e| format!("list requests failed: {e}"))?;

        if !requests.requests.iter().any(|r| r.id == request_id) {
            return Err(format!(
                "request {request_id} not found in request listing for {application_name}"
            ));
        }

        // Poll request status for a terminal outcome and verify success.
        let mut request_succeeded = false;
        for _ in 0..30 {
            let get_request = GetRequestRequest::builder()
                .namespace(&project_id)
                .application(&application_name)
                .request_id(&request_id)
                .build()
                .map_err(|e| format!("failed to build get request request: {e}"))?;

            let request = apps_client
                .get_request(&get_request)
                .await
                .map_err(|e| format!("get request failed: {e}"))?;

            if request.id != request_id {
                return Err(format!(
                    "expected request id {request_id}, got {}",
                    request.id
                ));
            }

            match request.outcome {
                Some(RequestOutcome::Success) => {
                    request_succeeded = true;
                    break;
                }
                Some(RequestOutcome::Failure(reason)) => {
                    return Err(format!("request finished with failure: {:?}", reason));
                }
                _ => {}
            }

            sleep(Duration::from_secs(2)).await;
        }

        if !request_succeeded {
            return Err(
                "request did not reach successful terminal state within timeout".to_string(),
            );
        }

        let check_output_request = CheckFunctionOutputRequest::builder()
            .namespace(&project_id)
            .application(&application_name)
            .request_id(&request_id)
            .build()
            .map_err(|e| format!("failed to build check output request: {e}"))?;

        let output_metadata = apps_client
            .check_function_output(&check_output_request)
            .await
            .map_err(|e| format!("check output failed: {e}"))?;

        if output_metadata.is_none() {
            return Err("request output metadata was not available".to_string());
        }

        let download_output_request = DownloadRequestOutputRequest::builder()
            .namespace(&project_id)
            .application(&application_name)
            .request_id(&request_id)
            .build()
            .map_err(|e| format!("failed to build download output request: {e}"))?;

        let output = apps_client
            .download_request_output(&download_output_request)
            .await
            .map_err(|e| format!("download output failed: {e}"))?;

        if output.content.is_empty() {
            return Err("downloaded request output was empty".to_string());
        }

        let output_text = String::from_utf8_lossy(&output.content);
        if !output_text.contains("Processed") || !output_text.contains("hello world") {
            return Err(format!("unexpected output payload: {output_text}"));
        }

        Ok(())
    }
    .await;

    // Best-effort cleanup of request/app runs regardless of test pass/fail.
    if let Some(request_id) = request_id_for_cleanup {
        match DeleteRequestRequest::builder()
            .namespace(&project_id)
            .application(&application_name)
            .request_id(request_id.clone())
            .build()
        {
            Ok(delete_request) => {
                if let Err(e) = apps_client.delete_request(&delete_request).await {
                    eprintln!("Cleanup failed for request {request_id}: {e}");
                }
            }
            Err(e) => eprintln!("Cleanup skipped for request {request_id}: {e}"),
        }
    }

    if app_created {
        match DeleteApplicationRequest::builder()
            .namespace(&project_id)
            .application(&application_name)
            .build()
        {
            Ok(delete_app) => {
                if let Err(e) = apps_client.delete(&delete_app).await {
                    eprintln!("Cleanup failed for application {application_name}: {e}");
                }
            }
            Err(e) => eprintln!("Cleanup skipped for application {application_name}: {e}"),
        }
    }

    if let Err(err) = result {
        panic!("{err}");
    }
}
