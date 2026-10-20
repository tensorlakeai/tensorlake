//! The merged sandbox-image catalog against scripted servers: dispatch by
//! reference shape, fall-through only on a clean 404, failures reported
//! rather than mistaken for misses, not-found only when both catalogs miss,
//! and a listing that fails whole when either catalog fails.
//!
//! One scripted listener plays both the platform API (`/platform/v1/...`)
//! and the Image Service (`/images/v4/...`): the catalog derives the Image
//! Service URL from the API URL, and every request opens a new connection,
//! so the recorded request lines show exactly which catalog was consulted.

use reqwest::StatusCode;
use tensorlake::{ClientBuilder, error::SdkError, sandbox_image_catalog::SandboxImageCatalog};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const CAS_RECORD: &str = r#"{
    "id": "image_abc123",
    "name": "team/app",
    "snapshot_id": "snap123",
    "global": false,
    "physical_size_bytes": 42,
    "logical_size_bytes": 10737418240,
    "created_at": "2026-10-15T00:00:00.000Z",
    "updated_at": "2026-10-15T00:00:00.000Z"
}"#;

const LEGACY_RECORD: &str = r#"{
    "id": "sandbox_template_1",
    "name": "team/app",
    "snapshotId": "snap-legacy",
    "snapshotFormatVersion": "durable_archive_v1",
    "createdAt": "2026-10-14T00:00:00.000Z",
    "public": true
}"#;

/// Each entry answers one connection, in order, and the request line that
/// arrived is recorded.
async fn scripted_server(
    responses: Vec<(u16, String)>,
) -> (SandboxImageCatalog, tokio::task::JoinHandle<Vec<String>>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let api_url = format!(
        "http://{}",
        listener.local_addr().expect("listener address")
    );
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for (status, body) in responses {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let request = read_http_request(&mut socket).await;
            let text = String::from_utf8_lossy(&request);
            requests.push(text.lines().next().unwrap_or_default().to_string());
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write response");
        }
        requests
    });
    let client = ClientBuilder::new(&api_url)
        .bearer_token("tl_apiKey_test")
        .build()
        .expect("build client");
    (
        SandboxImageCatalog::new(client, &api_url, Some(("org-1".into(), "proj-1".into()))),
        server,
    )
}

async fn read_http_request(socket: &mut TcpStream) -> Vec<u8> {
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
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            if request.len() >= headers_end + 4 + content_length {
                break;
            }
        }
    }
    request
}

const NOT_FOUND: &str = r#"{"error":"not found"}"#;

#[tokio::test]
async fn names_resolve_in_the_image_service_first_and_fall_through_on_404() {
    let (catalog, server) = scripted_server(vec![(200, CAS_RECORD.to_string())]).await;
    let image = catalog.find("team/app").await.unwrap().unwrap();
    assert_eq!(image.id.as_deref(), Some("image_abc123"));
    assert_eq!(image.snapshot_id.as_deref(), Some("snap123"));
    assert_eq!(
        server.await.unwrap(),
        vec!["GET /images/v4/images/name/team/app HTTP/1.1"]
    );

    let (catalog, server) = scripted_server(vec![
        (404, NOT_FOUND.to_string()),
        (200, LEGACY_RECORD.to_string()),
    ])
    .await;
    let image = catalog.find("team/app").await.unwrap().unwrap();
    assert_eq!(image.id.as_deref(), Some("sandbox_template_1"));
    assert_eq!(
        server.await.unwrap(),
        vec![
            "GET /images/v4/images/name/team/app HTTP/1.1",
            "GET /platform/v1/organizations/org-1/projects/proj-1/sandbox-templates/by-name/team%2Fapp HTTP/1.1",
        ]
    );
}

#[tokio::test]
async fn ids_go_straight_to_their_catalog() {
    let (catalog, server) = scripted_server(vec![(200, CAS_RECORD.to_string())]).await;
    assert!(catalog.find("image_abc123").await.unwrap().is_some());
    assert_eq!(
        server.await.unwrap(),
        vec!["GET /images/v4/images/image_abc123 HTTP/1.1"]
    );

    let (catalog, server) = scripted_server(vec![(200, LEGACY_RECORD.to_string())]).await;
    assert!(catalog.find("sandbox_template_1").await.unwrap().is_some());
    assert_eq!(
        server.await.unwrap(),
        vec![
            "GET /platform/v1/organizations/org-1/projects/proj-1/sandbox-templates/sandbox_template_1 HTTP/1.1"
        ]
    );
}

#[tokio::test]
async fn names_outside_the_image_service_grammar_skip_it() {
    let (catalog, server) = scripted_server(vec![(200, LEGACY_RECORD.to_string())]).await;
    assert!(catalog.find("MyImage").await.unwrap().is_some());
    assert_eq!(
        server.await.unwrap(),
        vec![
            "GET /platform/v1/organizations/org-1/projects/proj-1/sandbox-templates/by-name/MyImage HTTP/1.1"
        ]
    );
}

#[tokio::test]
async fn only_a_clean_404_falls_through() {
    for status in [400, 403, 500, 503] {
        let (catalog, server) =
            scripted_server(vec![(status, r#"{"error":"refused"}"#.to_string())]).await;
        let error = catalog.find("team/app").await.unwrap_err();
        match status {
            403 => assert!(matches!(error, SdkError::Authorization(_)), "{error}"),
            _ => assert!(matches!(error, SdkError::ServerError { .. }), "{error}"),
        }
        assert!(error.to_string().contains("refused"), "{error}");
        assert_eq!(server.await.unwrap().len(), 1, "status {status}");
    }
}

#[tokio::test]
async fn not_found_only_when_both_catalogs_miss() {
    let (catalog, server) = scripted_server(vec![
        (404, NOT_FOUND.to_string()),
        (404, r#"{"message":"not found"}"#.to_string()),
    ])
    .await;
    assert!(catalog.find("team/app").await.unwrap().is_none());
    assert_eq!(server.await.unwrap().len(), 2);

    let (catalog, server) = scripted_server(vec![
        (404, NOT_FOUND.to_string()),
        (404, r#"{"message":"not found"}"#.to_string()),
    ])
    .await;
    let error = catalog.delete("team/app").await.unwrap_err();
    assert!(
        matches!(error, SdkError::ServerError { status, .. } if status == StatusCode::NOT_FOUND)
    );
    assert!(error.to_string().contains("image 'team/app' not found"));
    assert_eq!(
        server.await.unwrap(),
        vec![
            "DELETE /images/v4/images/name/team/app HTTP/1.1",
            "DELETE /platform/v1/organizations/org-1/projects/proj-1/sandbox-templates/by-name/team%2Fapp HTTP/1.1",
        ]
    );
}

#[tokio::test]
async fn deletes_dispatch_like_lookups_and_report_refusals() {
    let (catalog, server) = scripted_server(vec![(204, String::new())]).await;
    catalog.delete("image_abc123").await.unwrap();
    assert_eq!(
        server.await.unwrap(),
        vec!["DELETE /images/v4/images/image_abc123 HTTP/1.1"]
    );

    let (catalog, server) = scripted_server(vec![(204, String::new())]).await;
    catalog.delete("sandbox_template_1").await.unwrap();
    assert_eq!(
        server.await.unwrap(),
        vec![
            "DELETE /platform/v1/organizations/org-1/projects/proj-1/sandbox-templates/sandbox_template_1 HTTP/1.1"
        ]
    );

    // A member's delete is refused by the Image Service with 403: reported,
    // never retried against the platform registry.
    let (catalog, server) = scripted_server(vec![(
        403,
        r#"{"error":"the admin role is required to delete images"}"#.to_string(),
    )])
    .await;
    let error = catalog.delete("team/app").await.unwrap_err();
    assert!(matches!(error, SdkError::Authorization(_)), "{error}");
    assert!(error.to_string().contains("admin role"), "{error}");
    assert_eq!(server.await.unwrap().len(), 1);
}

#[tokio::test]
async fn digest_references_are_refused_without_a_request() {
    let (catalog, server) = scripted_server(vec![]).await;
    for reference in [format!("cas-v1:{}", "a".repeat(64)), "a".repeat(64)] {
        let error = catalog.find(&reference).await.unwrap_err();
        assert!(matches!(error, SdkError::ClientError(_)));
        assert!(
            error
                .to_string()
                .contains("use the image name or its image_ id")
        );
        let error = catalog.delete(&reference).await.unwrap_err();
        assert!(matches!(error, SdkError::ClientError(_)));
    }
    assert!(server.await.unwrap().is_empty());
}

#[tokio::test]
async fn listing_merges_both_catalogs_newest_first_and_fails_whole() {
    // Both catalogs page: the platform list ends on its first page, the
    // Image Service hands out one cursor. Request order between the two is
    // not fixed (they run concurrently), so both orders are scripted by
    // answering on path.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let api_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for _ in 0..3 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_http_request(&mut socket).await;
            let line = String::from_utf8_lossy(&request)
                .lines()
                .next()
                .unwrap_or_default()
                .to_string();
            let body = if line.contains("/platform/v1/") {
                format!(r#"{{"items":[{LEGACY_RECORD}],"pagination":{{"next":null}}}}"#)
            } else if line.contains("cursor=") {
                format!(r#"{{"items":[{CAS_RECORD}],"pagination":{{}}}}"#)
            } else {
                let older = CAS_RECORD
                    .replace("image_abc123", "image_old")
                    .replace("\"team/app\"", "\"team/old\"")
                    .replace("2026-10-15T00:00:00.000Z", "2026-10-13T00:00:00.000Z");
                format!(r#"{{"items":[{older}],"pagination":{{"next":"c2"}}}}"#)
            };
            requests.push(line);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        }
        requests
    });
    let client = ClientBuilder::new(&api_url)
        .bearer_token("tl_apiKey_test")
        .build()
        .unwrap();
    let catalog = SandboxImageCatalog::new(client, &api_url, None);
    let images = catalog.list().await.unwrap();
    let names: Vec<(String, String)> = images
        .iter()
        .map(|image| (image.name.clone().unwrap(), image.id.clone().unwrap()))
        .collect();
    assert_eq!(
        names,
        vec![
            ("team/app".to_string(), "image_abc123".to_string()),
            ("team/app".to_string(), "sandbox_template_1".to_string()),
            ("team/old".to_string(), "image_old".to_string()),
        ]
    );
    let requests = server.await.unwrap();
    assert!(
        requests
            .iter()
            .any(|line| line.starts_with("GET /platform/v1/sandbox-templates?pageSize=100 "))
    );
    assert!(
        requests
            .iter()
            .any(|line| line.starts_with("GET /images/v4/images?page_size=100 "))
    );
    assert!(
        requests
            .iter()
            .any(|line| line.starts_with("GET /images/v4/images?page_size=100&cursor=c2 "))
    );

    // Either catalog failing fails the whole listing.
    let (catalog, server) = scripted_server(vec![
        (500, r#"{"error":"catalog unavailable"}"#.to_string()),
        (
            200,
            r#"{"items":[],"pagination":{"next":null}}"#.to_string(),
        ),
    ])
    .await;
    let error = catalog.list().await.unwrap_err();
    assert!(matches!(error, SdkError::ServerError { .. }), "{error}");
    server.abort();
}
