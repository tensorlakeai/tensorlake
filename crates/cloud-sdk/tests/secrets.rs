use tensorlake::secrets::models::*;
use tensorlake::{ClientBuilder, secrets::SecretsClient};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::common::random_string;

mod common;

#[tokio::test]
async fn api_key_scoped_secrets_use_root_routes_without_introspection() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("listener address");

    let server = tokio::spawn(async move {
        let mut requests = Vec::new();

        let (mut socket, _) = listener.accept().await.expect("accept list request");
        requests.push(read_http_request(&mut socket).await);
        write_json_response(
            &mut socket,
            r#"{"items":[],"pagination":{"next":null,"prev":null,"total":0}}"#,
        )
        .await;

        let (mut socket, _) = listener.accept().await.expect("accept upsert request");
        requests.push(read_http_request(&mut socket).await);
        write_json_response(
            &mut socket,
            r#"{"id":"secret/1","name":"TOKEN","createdAt":"2026-08-20T00:00:00Z"}"#,
        )
        .await;

        let (mut socket, _) = listener.accept().await.expect("accept delete request");
        requests.push(read_http_request(&mut socket).await);
        socket
            .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
            .await
            .expect("write delete response");

        requests
    });

    let client = ClientBuilder::new(&format!("http://{address}"))
        .bearer_token("tl_apiKey_test")
        .build()
        .expect("build client");
    let secrets = SecretsClient::new(client);

    secrets
        .list_api_key_scoped(Some(100))
        .await
        .expect("list secrets");
    secrets
        .upsert_api_key_scoped(UpsertSecret::from(("TOKEN", "value")))
        .await
        .expect("upsert secret");
    secrets
        .delete_api_key_scoped("secret/1")
        .await
        .expect("delete secret");

    let requests = server.await.expect("server join");
    let requests: Vec<String> = requests
        .iter()
        .map(|request| String::from_utf8_lossy(request).to_string())
        .collect();
    assert!(requests[0].starts_with("GET /platform/v1/secrets?pageSize=100 HTTP/1.1\r\n"));
    assert!(requests[1].starts_with("PUT /platform/v1/secrets HTTP/1.1\r\n"));
    assert!(requests[2].starts_with("DELETE /platform/v1/secrets/secret%2F1 HTTP/1.1\r\n"));
    for request in requests {
        let request = request.to_lowercase();
        assert!(request.contains("authorization: bearer tl_apikey_test"));
        assert!(!request.contains("/platform/v1/keys/introspect"));
        assert!(!request.contains("x-forwarded-organization-id:"));
        assert!(!request.contains("x-forwarded-project-id:"));
    }
}

#[tokio::test]
#[cfg_attr(not(feature = "integration-tests"), ignore)]
async fn test_secrets_operations() {
    let sdk = match common::create_sdk() {
        Ok(sdk) => sdk,
        Err(msg) => {
            eprintln!("Skipping integration test: {msg}");
            return;
        }
    };
    let (org_id, project_id) = match common::get_org_and_project_ids() {
        Ok(ids) => ids,
        Err(msg) => {
            eprintln!("Skipping integration test: {msg}");
            return;
        }
    };

    let secrets_client = sdk.secrets();

    let secret_1_name = format!("integration_test_secret_1_{}", random_string());
    let secret_2_name = format!("integration_test_secret_2_{}", random_string());

    let mut cleanup_secret_ids: Vec<String> = Vec::new();

    let result: Result<(), String> = async {
        // Create new secrets
        let upsert_request = UpsertSecretRequest::builder()
            .organization_id(&org_id)
            .project_id(&project_id)
            .secrets(vec![
                (secret_1_name.as_str(), "initial_value"),
                (secret_2_name.as_str(), "initial_value"),
            ])
            .build()
            .map_err(|e| format!("failed to build upsert request: {e}"))?;

        let upsert_response = secrets_client
            .upsert(upsert_request)
            .await
            .map_err(|e| format!("upsert failed: {e}"))?;

        match upsert_response.into_inner() {
            UpsertSecretResponse::Single(secret) => cleanup_secret_ids.push(secret.id),
            UpsertSecretResponse::Multiple(secrets) => {
                cleanup_secret_ids.extend(secrets.into_iter().map(|s| s.id))
            }
        }

        let list_request = ListSecretsRequest::builder()
            .organization_id(&org_id)
            .project_id(&project_id)
            .page_size(100)
            .build()
            .map_err(|e| format!("failed to build list request: {e}"))?;

        let list_response = secrets_client
            .list(&list_request)
            .await
            .map_err(|e| format!("list failed: {e}"))?;

        let secret_1 = list_response
            .items
            .iter()
            .find(|item| item.name == secret_1_name)
            .cloned()
            .ok_or_else(|| format!("secret {secret_1_name} not found after create"))?;
        let secret_2 = list_response
            .items
            .iter()
            .find(|item| item.name == secret_2_name)
            .cloned()
            .ok_or_else(|| format!("secret {secret_2_name} not found after create"))?;

        if secret_1.id == secret_2.id {
            return Err("two created secrets have the same ID".to_string());
        }

        cleanup_secret_ids = vec![secret_1.id.clone(), secret_2.id.clone()];

        // Get one secret and verify identity fields.
        let get_request = GetSecretRequest::builder()
            .organization_id(&org_id)
            .project_id(&project_id)
            .secret_id(secret_1.id.clone())
            .build()
            .map_err(|e| format!("failed to build get request: {e}"))?;

        let get_response = secrets_client
            .get(&get_request)
            .await
            .map_err(|e| format!("get failed: {e}"))?;

        if get_response.id != secret_1.id {
            return Err(format!(
                "expected id {}, got {}",
                secret_1.id, get_response.id
            ));
        }
        if get_response.name != secret_1.name {
            return Err(format!(
                "expected name {}, got {}",
                secret_1.name, get_response.name
            ));
        }
        if get_response.created_at != secret_1.created_at {
            return Err("created_at mismatch on fetched secret".to_string());
        }

        // Update both secrets.
        let update_request = UpsertSecretRequest::builder()
            .organization_id(&org_id)
            .project_id(&project_id)
            .secrets(vec![
                (secret_1_name.as_str(), "updated_value"),
                (secret_2_name.as_str(), "updated_value"),
            ])
            .build()
            .map_err(|e| format!("failed to build update request: {e}"))?;

        secrets_client
            .upsert(update_request)
            .await
            .map_err(|e| format!("update failed: {e}"))?;

        let updated_list = secrets_client
            .list(&list_request)
            .await
            .map_err(|e| format!("list after update failed: {e}"))?;

        let updated_1 = updated_list
            .items
            .iter()
            .find(|item| item.name == secret_1_name)
            .ok_or_else(|| format!("secret {secret_1_name} not found after update"))?;
        let updated_2 = updated_list
            .items
            .iter()
            .find(|item| item.name == secret_2_name)
            .ok_or_else(|| format!("secret {secret_2_name} not found after update"))?;

        if updated_1.id != secret_1.id {
            return Err(format!(
                "secret {} ID changed unexpectedly ({} -> {})",
                secret_1_name, secret_1.id, updated_1.id
            ));
        }
        if updated_2.id != secret_2.id {
            return Err(format!(
                "secret {} ID changed unexpectedly ({} -> {})",
                secret_2_name, secret_2.id, updated_2.id
            ));
        }

        Ok(())
    }
    .await;

    // Best-effort cleanup runs regardless of test pass/fail.
    cleanup_secret_ids.sort();
    cleanup_secret_ids.dedup();
    for secret_id in cleanup_secret_ids {
        let delete_request = match DeleteSecretRequest::builder()
            .organization_id(&org_id)
            .project_id(&project_id)
            .secret_id(secret_id.clone())
            .build()
        {
            Ok(request) => request,
            Err(e) => {
                eprintln!("Cleanup skipped for secret {secret_id}: failed to build request: {e}");
                continue;
            }
        };

        if let Err(e) = secrets_client.delete(&delete_request).await {
            eprintln!("Cleanup failed for secret {secret_id}: {e}");
        }
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
