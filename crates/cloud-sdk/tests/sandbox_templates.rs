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
#[cfg_attr(not(feature = "integration-tests"), ignore)]
async fn test_create_then_delete_sandbox_image() {
    use std::env;
    use tensorlake::sandbox_images::{SandboxImageBuildOptions, build_sandbox_image};

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
                api_url,
                bearer_token,
                use_scope_headers: false,
                organization_id: Some(organization_id),
                project_id: Some(project_id),
                namespace,
                dockerfile_path,
                dockerfile_text: None,
                context_dir: None,
                registered_name: Some(image_name.clone()),
                disk_mb: None,
                builder_disk_mb: None,
                cpus: Some(1.0),
                memory_mb: Some(1024),
                is_public: false,
                user_agent: None,
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
