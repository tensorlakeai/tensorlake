use tensorlake::{ClientBuilder, sandbox_templates::SandboxTemplatesClient};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod common;

#[tokio::test]
async fn delete_sandbox_template_sends_encoded_delete_request() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept request");
        let request_bytes = read_http_request(&mut socket).await;
        let response = "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n";
        socket
            .write_all(response.as_bytes())
            .await
            .expect("write response");
        request_bytes
    });

    let client = ClientBuilder::new(&format!("http://{}", address))
        .build()
        .expect("build client");
    let templates = SandboxTemplatesClient::new(client, "org-1", "proj-1");

    templates
        .delete("tensorlake/python:3.12-slim")
        .await
        .expect("delete sandbox template");

    let request_bytes = server.await.expect("server join");
    let request_text = String::from_utf8_lossy(&request_bytes);

    assert!(request_text.starts_with(
        "DELETE /platform/v1/organizations/org-1/projects/proj-1/sandbox-templates/tensorlake%2Fpython%3A3.12-slim HTTP/1.1\r\n"
    ));
}

#[tokio::test]
async fn list_sandbox_templates_follows_pagination() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");
    let base_path = "/platform/v1/organizations/org-1/projects/proj-1/sandbox-templates";

    let server = tokio::spawn(async move {
        // Page 1: two templates and a `next` link pointing at page 2.
        let (mut socket, _) = listener.accept().await.expect("accept page 1");
        let request_1 = read_http_request(&mut socket).await;
        let page_1 = serde_json::json!({
            "items": [
                { "id": "tpl-1", "name": "image-a", "snapshotId": "snap-a" },
                { "id": "tpl-2", "name": "image-b", "snapshotId": "snap-b" }
            ],
            "pagination": {
                "next": "/platform/v1/organizations/org-1/projects/proj-1/sandbox-templates?pageSize=100&cursor=abc"
            }
        })
        .to_string();
        write_json_response(&mut socket, &page_1).await;

        // Page 2: one template and no further pages.
        let (mut socket, _) = listener.accept().await.expect("accept page 2");
        let request_2 = read_http_request(&mut socket).await;
        let page_2 = serde_json::json!({
            "items": [
                { "id": "tpl-3", "name": "image-c", "snapshotId": "snap-c" }
            ],
            "pagination": { "next": serde_json::Value::Null }
        })
        .to_string();
        write_json_response(&mut socket, &page_2).await;

        (request_1, request_2)
    });

    let client = ClientBuilder::new(&format!("http://{}", address))
        .build()
        .expect("build client");
    let templates = SandboxTemplatesClient::new(client, "org-1", "proj-1");

    let listed = templates.list().await.expect("list sandbox templates");
    let names: Vec<String> = listed
        .iter()
        .filter_map(|template| template.name.clone())
        .collect();
    assert_eq!(names, vec!["image-a", "image-b", "image-c"]);

    let (request_1, request_2) = server.await.expect("server join");
    let request_1_text = String::from_utf8_lossy(&request_1);
    let request_2_text = String::from_utf8_lossy(&request_2);
    assert!(request_1_text.starts_with(&format!("GET {base_path}?pageSize=100 HTTP/1.1\r\n")));
    assert!(request_2_text.starts_with(&format!(
        "GET {base_path}?pageSize=100&cursor=abc HTTP/1.1\r\n"
    )));
}

#[tokio::test]
#[cfg_attr(not(feature = "integration-tests"), ignore)]
async fn test_create_then_delete_sandbox_image() {
    use std::env;
    use tensorlake::sandbox_images::{
        CommonBuildOptions, SandboxImageBuildOptions, build_sandbox_image,
    };

    let sdk = match common::create_sdk() {
        Ok(sdk) => sdk,
        Err(msg) => {
            eprintln!("Skipping integration test: {msg}");
            return;
        }
    };
    let (organization_id, project_id) = match common::get_org_and_project_ids() {
        Ok(scope) => scope,
        Err(msg) => {
            eprintln!("Skipping integration test: {msg}");
            return;
        }
    };
    let api_url =
        env::var("TENSORLAKE_API_URL").unwrap_or_else(|_| "https://api.tensorlake.ai".to_string());
    let bearer_token = match env::var("TENSORLAKE_API_KEY") {
        Ok(token) => token,
        Err(_) => {
            eprintln!("Skipping integration test: TENSORLAKE_API_KEY must be set");
            return;
        }
    };
    let namespace = env::var("INDEXIFY_NAMESPACE").unwrap_or_else(|_| "default".to_string());
    let image_name = format!(
        "sdk-rust-delete-test-{}",
        common::random_string().to_lowercase()
    );
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let dockerfile_path = temp_dir.path().join("Dockerfile");
    std::fs::write(
        &dockerfile_path,
        "FROM tensorlake/ubuntu-minimal\nRUN printf 'rust delete acceptance\\n' > /tmp/rust-delete-acceptance\n",
    )
    .expect("write Dockerfile");

    let templates = sdk.sandbox_templates(&organization_id, &project_id);
    let result: Result<(), String> = async {
        build_sandbox_image(
            SandboxImageBuildOptions {
                common: CommonBuildOptions {
                    api_url,
                    bearer_token,
                    use_scope_headers: false,
                    organization_id: Some(organization_id),
                    project_id: Some(project_id),
                    namespace,
                    registered_name: Some(image_name.clone()),
                    disk_mb: None,
                    builder_disk_mb: None,
                    cpus: Some(1.0),
                    memory_mb: Some(1024),
                    is_public: false,
                    streaming: false,
                    user_agent: None,
                    docker_compat: false,
                },
                dockerfile_path,
                dockerfile_text: None,
                context_dir: None,
            },
            |_| {},
        )
        .await
        .map_err(|error| format!("build sandbox image failed: {error}"))?;

        templates
            .delete(&image_name)
            .await
            .map_err(|error| format!("delete sandbox image failed: {error}"))?;
        Ok(())
    }
    .await;

    if result.is_err() {
        let _ = templates.delete(&image_name).await;
    }

    if let Err(err) = result {
        panic!("{err}");
    }
}

async fn write_json_response(socket: &mut tokio::net::TcpStream, body: &str) {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    socket
        .write_all(response.as_bytes())
        .await
        .expect("write response");
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

        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    request
}
