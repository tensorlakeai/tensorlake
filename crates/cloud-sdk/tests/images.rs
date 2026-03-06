use tensorlake_cloud_sdk::images::models::*;
use tensorlake_cloud_sdk::{ClientBuilder, images::ImagesClient};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::common::random_string;

mod common;

#[tokio::test]
async fn test_create_application_build_sends_application_json_and_context_parts() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept request");
        let request_bytes = read_http_request(&mut socket).await;

        let response_body = r#"{"id":"app-build-1","organization_id":"org-1","project_id":"proj-1","name":"app_fn","version":"v1","status":"building","image_builds":[{"id":"img-build-1","app_version_id":"app-version-1","key":"img-1","name":"image-a","status":"pending","function_names":["fn-1","fn-2"],"created_at":"2026-03-07T10:00:00Z","updated_at":"2026-03-07T10:01:00Z"}]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write response");

        request_bytes
    });

    let client = ClientBuilder::new(&format!("http://{}", address))
        .scope("org-1", "proj-1")
        .build()
        .expect("build client");
    let images_client = ImagesClient::new(client);
    let request = CreateApplicationBuildRequest::builder()
        .name("app_fn")
        .version("v1")
        .images(vec![
            CreateApplicationBuildImageRequest::builder()
                .key("img-1")
                .name("image-a")
                .context_tar_part_name("img-1")
                .context_sha256("sha-a")
                .function_names(vec!["fn-1".to_string(), "fn-2".to_string()])
                .build()
                .expect("build image 1 request"),
            CreateApplicationBuildImageRequest::builder()
                .key("img-2")
                .name("image-b")
                .context_tar_part_name("img-2")
                .context_sha256("sha-b")
                .function_names(vec!["fn-3".to_string()])
                .build()
                .expect("build image 2 request"),
        ])
        .build()
        .expect("build app request");
    let image_contexts = vec![
        ApplicationBuildContext {
            context_tar_part_name: "img-1".to_string(),
            context_tar_gz: b"context-a".to_vec(),
        },
        ApplicationBuildContext {
            context_tar_part_name: "img-2".to_string(),
            context_tar_gz: b"context-b".to_vec(),
        },
    ];

    let response = images_client
        .create_application_build("/images/v3/applications", &request, &image_contexts)
        .await
        .expect("create application build");

    let request_bytes = server.await.expect("server join");
    let request_text = String::from_utf8_lossy(&request_bytes);

    assert!(request_text.starts_with("POST /images/v3/applications HTTP/1.1\r\n"));
    assert!(request_text.contains("\r\nx-forwarded-organization-id: org-1\r\n"));
    assert!(request_text.contains("\r\nx-forwarded-project-id: proj-1\r\n"));
    assert!(request_text.contains("\r\nx-tensorlake-organization-id: org-1\r\n"));
    assert!(request_text.contains("\r\nx-tensorlake-project-id: proj-1\r\n"));
    assert!(request_text.contains("name=\"app_version\"; filename=\"app_version\""));
    assert!(request_text.contains("Content-Type: application/json"));
    assert!(request_text.contains("\"context_tar_part_name\":\"img-1\""));
    assert!(request_text.contains("\"context_tar_part_name\":\"img-2\""));
    assert!(request_text.contains("\"context_sha256\":\"sha-a\""));
    assert!(request_text.contains("\"context_sha256\":\"sha-b\""));
    assert!(request_text.contains("name=\"img-1\""));
    assert!(request_text.contains("name=\"img-2\""));
    assert!(request_text.contains("context-a"));
    assert!(request_text.contains("context-b"));

    assert_eq!(response.id, "app-build-1");
    assert_eq!(response.status.as_deref(), Some("building"));
    assert_eq!(response.image_builds.len(), 1);
    assert_eq!(response.image_builds[0].status, "pending");
}

async fn read_http_request(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut request = Vec::new();
    let mut buf = [0_u8; 4096];

    loop {
        let read = socket.read(&mut buf).await.expect("read request");
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buf[..read]);

        if let Some(headers_end) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&request[..headers_end + 4]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    if name.eq_ignore_ascii_case("content-length") {
                        value.trim().parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);

            if request.len() >= headers_end + 4 + content_length {
                break;
            }
        }
    }

    request
}

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
