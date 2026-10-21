//! Legacy image and snapshot flows: recorded-call baseline.
//!
//! Baseline: the pre-workflow revision fe36caa5 of this branch ("Expose the
//! merged catalog and CAS options in the Python and TypeScript SDKs"), which
//! already carries the accepted merged-catalog differences (`describe`/`ls`
//! output, template deletion through the merged catalog, Image Service
//! lookup first for grammar-valid names). Every test below asserts the exact
//! requests (method, path, body, relevant headers), stdout/stderr and exit
//! status that revision produces; the Image Service build operations added
//! afterwards must leave them byte-for-byte unchanged. In particular a
//! positively legacy flow — `--no-cas`, a `sandbox_template_` id, a legacy
//! `.tlsnap` snapshot, `tl sbx create --image` — makes zero Image Service
//! requests, and the two existing discovery probes (merged `ls`, an untyped
//! name) are recorded explicitly rather than treated as legacy.

mod image_support;

use std::time::Duration;

use image_support::{
    hang, introspection, reply, reset, run, start, template_record, write_dockerfile,
};
use serde_json::json;

const LEGACY_BUILDS_PATH: &str = "/platform/v1/sandbox-template-builds";

/// `image create --no-cas`: the legacy builder's first request is the
/// platform template lookup of the Dockerfile base, then the rootfs build
/// preparation. A failure there is reported as it always was; no build ID
/// is printed, no `Idempotency-Key` is sent, and no Image Service request
/// of any kind is made.
#[tokio::test]
async fn legacy_create_keeps_its_requests_output_and_error() {
    let dir = tempfile::tempdir().unwrap();
    write_dockerfile(dir.path(), "FROM ubuntu:24.04\nRUN echo hi\n");
    let run = run(
        &["image", "create", "--no-cas", "-n", "img", "Dockerfile"],
        vec![
            introspection(),
            reply(
                "GET",
                "/platform/v1/sandbox-templates/by-name/ubuntu%3A24.04",
                404,
                json!({"message": "not found"}),
            ),
            reply(
                "POST",
                LEGACY_BUILDS_PATH,
                500,
                json!({"message": "baseline failure"}),
            ),
        ],
        dir.path(),
    )
    .await;
    assert_eq!(run.code(), Some(1), "{}", run.stderr());
    assert_eq!(
        run.lines(),
        vec![
            "GET /platform/v1/sandbox-templates/by-name/ubuntu%3A24.04 HTTP/1.1",
            "POST /platform/v1/sandbox-template-builds HTTP/1.1",
        ]
    );
    assert!(run.image_service_requests().is_empty());
    for request in &run.requests {
        assert!(
            request.header("idempotency-key").is_none(),
            "{}",
            request.line
        );
    }
    let prepare = run.requests[1].json();
    assert_eq!(prepare["name"], "img");
    assert_eq!(prepare["dockerfile"], "FROM ubuntu:24.04\nRUN echo hi\n");
    assert_eq!(prepare["baseImage"], "ubuntu:24.04");
    assert_eq!(prepare["rootfsNodeKind"], "base");
    assert_eq!(prepare["public"], false);
    let stderr = run.stderr();
    assert_eq!(
        stderr,
        "⚙️  Loading Dockerfile...\n⚙️  Selected image name: img\n⚙️  Preparing legacy image build...\nError: failed to prepare legacy image build (HTTP 500 Internal Server Error): {\"message\":\"baseline failure\"}\n"
    );
    assert!(!stderr.contains("Build ID"), "{stderr}");
    assert!(run.stdout().is_empty(), "{}", run.stdout());
}

/// `image import --no-cas`: the legacy import prepares straight away
/// (no template lookup) and reports the same error.
#[tokio::test]
async fn legacy_import_keeps_its_requests_output_and_error() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "import", "--no-cas", "-n", "img", "ubuntu:24.04"],
        vec![
            introspection(),
            reply(
                "POST",
                LEGACY_BUILDS_PATH,
                500,
                json!({"message": "baseline failure"}),
            ),
        ],
        dir.path(),
    )
    .await;
    assert_eq!(run.code(), Some(1), "{}", run.stderr());
    assert_eq!(
        run.lines(),
        vec!["POST /platform/v1/sandbox-template-builds HTTP/1.1"]
    );
    assert!(run.image_service_requests().is_empty());
    assert!(run.requests[0].header("idempotency-key").is_none());
    let prepare = run.requests[0].json();
    assert_eq!(prepare["name"], "img");
    assert_eq!(prepare["baseImage"], "ubuntu:24.04");
    assert_eq!(prepare["rootfsNodeKind"], "base");
    let stderr = run.stderr();
    assert!(
        stderr.starts_with("⚙️  Importing registry image ubuntu:24.04...\n"),
        "{stderr}"
    );
    assert!(
        stderr.ends_with("Error: failed to prepare legacy image build (HTTP 500 Internal Server Error): {\"message\":\"baseline failure\"}\n"),
        "{stderr}"
    );
    assert!(!stderr.contains("Build ID"), "{stderr}");
}

/// Ctrl+C during a legacy create: the process ends by the signal, as the
/// default handler always did, and sends no cancellation, deletion or
/// other request afterwards.
#[tokio::test]
async fn legacy_create_ctrl_c_keeps_the_default_interrupt_and_sends_nothing() {
    let dir = tempfile::tempdir().unwrap();
    write_dockerfile(dir.path(), "FROM ubuntu:24.04\n");
    let server = start(vec![
        introspection(),
        hang(
            "GET",
            "/platform/v1/sandbox-templates/by-name/ubuntu%3A24.04",
            Duration::from_secs(20),
        ),
    ])
    .await;
    let child = image_support::command(
        &server.url,
        &["image", "create", "--no-cas", "-n", "img", "Dockerfile"],
        dir.path(),
    )
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .spawn()
    .unwrap();
    // The legacy lookup is on the wire and unanswered.
    server
        .wait_for("GET", "/platform/v1/sandbox-templates/by-name/")
        .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    unsafe {
        libc::kill(child.id().unwrap() as i32, libc::SIGINT);
    }
    let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .expect("the default SIGINT handler ends the process at once")
        .unwrap();
    let requests = server.finish().await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), None, "killed by the signal: {stderr}");
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(output.status.signal(), Some(libc::SIGINT));
    }
    assert!(!stderr.contains("Cancelling"), "{stderr}");
    assert_eq!(
        requests
            .iter()
            .map(|request| request.line.as_str())
            .collect::<Vec<_>>(),
        vec!["GET /platform/v1/sandbox-templates/by-name/ubuntu%3A24.04 HTTP/1.1"],
    );
}

/// `describe` by a legacy template id: one platform request, zero Image
/// Service requests, the accepted detail output.
#[tokio::test]
async fn legacy_describe_by_id_queries_only_the_platform() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "describe", "sandbox_template_1"],
        vec![
            introspection(),
            reply(
                "GET",
                "/platform/v1/sandbox-templates/sandbox_template_1",
                200,
                template_record("sandbox_template_1", "legacy-img"),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(run.output.status.success(), "{}", run.stderr());
    assert_eq!(
        run.lines(),
        vec!["GET /platform/v1/sandbox-templates/sandbox_template_1 HTTP/1.1"]
    );
    assert!(run.image_service_requests().is_empty());
    let stdout = run.stdout();
    assert!(stdout.starts_with("Name:        legacy-img\nID:          sandbox_template_1\nSnapshot ID: snap-legacy\nFormat:      TLSnap\n"), "{stdout}");
    assert!(
        stdout.contains("Dockerfile:\n  FROM ubuntu:24.04\n"),
        "{stdout}"
    );
}

/// `describe` by a legacy name: the existing discovery order — Image
/// Service first, a clean 404 falls through to the platform registry.
#[tokio::test]
async fn legacy_describe_by_name_probes_the_image_service_then_the_platform() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "describe", "legacy-img"],
        vec![
            introspection(),
            reply(
                "GET",
                "/images/v4/images/name/legacy-img",
                404,
                json!({"error": "name not found"}),
            ),
            reply(
                "GET",
                "/platform/v1/sandbox-templates/by-name/legacy-img",
                200,
                template_record("sandbox_template_1", "legacy-img"),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(run.output.status.success(), "{}", run.stderr());
    assert_eq!(
        run.lines(),
        vec![
            "GET /images/v4/images/name/legacy-img HTTP/1.1",
            "GET /platform/v1/sandbox-templates/by-name/legacy-img HTTP/1.1",
        ]
    );
    assert!(
        run.stdout().contains("ID:          sandbox_template_1"),
        "{}",
        run.stdout()
    );

    // Accepted difference (section 1 item 11 b): any non-404 Image Service
    // answer fails the legacy lookup instead of falling through.
    let unavailable =
        run_describe_with_image_service(dir.path(), 503, json!({"error": "deploying"})).await;
    assert_eq!(unavailable.code(), Some(1), "{}", unavailable.stderr());
    assert_eq!(
        unavailable.lines(),
        vec!["GET /images/v4/images/name/legacy-img HTTP/1.1"]
    );
    assert!(
        unavailable
            .stderr()
            .contains("Error: Server error: 503 Service Unavailable - deploying"),
        "{}",
        unavailable.stderr()
    );
}

async fn run_describe_with_image_service(
    dir: &std::path::Path,
    status: u16,
    body: serde_json::Value,
) -> image_support::CliRun {
    run(
        &["image", "describe", "legacy-img"],
        vec![
            introspection(),
            reply("GET", "/images/v4/images/name/legacy-img", status, body),
        ],
        dir,
    )
    .await
}

/// `ls`: the merged listing reads both catalogs (recorded as the existing
/// discovery, not as a legacy-only flow) and prints legacy records with the
/// accepted table.
#[tokio::test]
async fn legacy_ls_reads_both_catalogs_and_prints_the_accepted_table() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "ls"],
        vec![
            introspection(),
            reply(
                "GET",
                "/platform/v1/sandbox-templates?pageSize=100",
                200,
                json!({"items": [template_record("sandbox_template_1", "legacy-img")]}),
            ),
            reply(
                "GET",
                "/images/v4/images?page_size=100",
                200,
                json!({"items": [], "pagination": {"next": null}}),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(run.output.status.success(), "{}", run.stderr());
    let mut lines = run.lines();
    lines.sort();
    assert_eq!(
        lines,
        vec![
            "GET /images/v4/images?page_size=100 HTTP/1.1",
            "GET /platform/v1/sandbox-templates?pageSize=100 HTTP/1.1",
        ]
    );
    let stdout = run.stdout();
    assert!(stdout.contains("legacy-img"), "{stdout}");
    assert!(stdout.contains("TLSnap"), "{stdout}");
    assert!(stdout.contains("sandbox_template_1"), "{stdout}");
    assert!(stdout.contains("snap-legacy"), "{stdout}");
    assert!(stdout.trim_end().ends_with("1 image"), "{stdout}");

    let as_json = run_ls_json(dir.path()).await;
    assert!(as_json.output.status.success(), "{}", as_json.stderr());
    let parsed: serde_json::Value = serde_json::from_str(&as_json.stdout()).unwrap();
    assert_eq!(parsed[0]["id"], "sandbox_template_1");
    assert_eq!(parsed[0]["snapshotFormatVersion"], "durable_archive_v1");
}

async fn run_ls_json(dir: &std::path::Path) -> image_support::CliRun {
    run(
        &["image", "ls", "--json"],
        vec![
            introspection(),
            reply(
                "GET",
                "/platform/v1/sandbox-templates?pageSize=100",
                200,
                json!({"items": [template_record("sandbox_template_1", "legacy-img")]}),
            ),
            reply(
                "GET",
                "/images/v4/images?page_size=100",
                200,
                json!({"items": []}),
            ),
        ],
        dir,
    )
    .await
}

/// `rm` by a legacy template id: one platform DELETE, zero Image Service
/// requests, the same output and errors.
#[tokio::test]
async fn legacy_rm_by_id_deletes_through_the_platform_only() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "rm", "sandbox_template_1"],
        vec![
            introspection(),
            reply(
                "DELETE",
                "/platform/v1/sandbox-templates/sandbox_template_1",
                204,
                json!(null),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(run.output.status.success(), "{}", run.stderr());
    assert_eq!(
        run.lines(),
        vec!["DELETE /platform/v1/sandbox-templates/sandbox_template_1 HTTP/1.1"]
    );
    assert!(run.image_service_requests().is_empty());
    assert_eq!(run.stdout().trim(), "Deleted image 'sandbox_template_1'.");
}

/// `rm` by an untyped legacy name: the Image Service is probed first as
/// today; its clean 404 falls through to exactly one platform DELETE.
#[tokio::test]
async fn legacy_rm_by_name_falls_through_only_on_a_clean_404() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "rm", "legacy-img"],
        vec![
            introspection(),
            reply(
                "DELETE",
                "/images/v4/images/name/legacy-img",
                404,
                json!({"error": "name not found"}),
            ),
            reply(
                "DELETE",
                "/platform/v1/sandbox-templates/by-name/legacy-img",
                204,
                json!(null),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(run.output.status.success(), "{}", run.stderr());
    assert_eq!(
        run.lines(),
        vec![
            "DELETE /images/v4/images/name/legacy-img HTTP/1.1",
            "DELETE /platform/v1/sandbox-templates/by-name/legacy-img HTTP/1.1",
        ]
    );
    assert_eq!(run.stdout().trim(), "Deleted image 'legacy-img'.");

    // Unknown in both catalogs: the accepted not-found error, no retry.
    let unknown = run_rm_with_image_service(
        dir.path(),
        vec![
            reply(
                "DELETE",
                "/images/v4/images/name/legacy-img",
                404,
                json!({"error": "nope"}),
            ),
            reply(
                "DELETE",
                "/platform/v1/sandbox-templates/by-name/legacy-img",
                404,
                json!({"message": "nope"}),
            ),
        ],
    )
    .await;
    assert_eq!(unknown.code(), Some(1));
    assert!(
        unknown
            .stderr()
            .contains("Error: image 'legacy-img' not found"),
        "{}",
        unknown.stderr()
    );
    assert_eq!(unknown.requests.len(), 2);
}

/// 202, 409, 5xx and a dropped connection from the Image Service are not
/// absence: nothing falls through to the platform, nothing is replayed.
#[tokio::test]
async fn legacy_rm_by_name_never_falls_through_on_other_answers() {
    let dir = tempfile::tempdir().unwrap();
    // 202: the deletion is in progress and counts as done.
    let accepted = run_rm_with_image_service(
        dir.path(),
        vec![reply(
            "DELETE",
            "/images/v4/images/name/legacy-img",
            202,
            json!({"deleting": true}),
        )],
    )
    .await;
    assert!(accepted.output.status.success(), "{}", accepted.stderr());
    // A deleted CAS image is followed by one lookup of a same-named legacy
    // image to delete as well (none here).
    assert_eq!(
        accepted.lines(),
        vec![
            "DELETE /images/v4/images/name/legacy-img HTTP/1.1",
            "GET /platform/v1/sandbox-templates/by-name/legacy-img HTTP/1.1",
        ]
    );
    assert_eq!(accepted.stdout().trim(), "Deleted image 'legacy-img'.");
    assert!(
        !accepted.stderr().contains("Legacy image"),
        "{}",
        accepted.stderr()
    );

    let conflict = run_rm_with_image_service(
        dir.path(),
        vec![reply(
            "DELETE",
            "/images/v4/images/name/legacy-img",
            409,
            json!({"error": "Image 'legacy-img' already has an unfinished build 'build-3'. Cancel it before starting another.", "code": "image_build_in_progress"}),
        )],
    )
    .await;
    assert_eq!(conflict.code(), Some(1));
    assert_eq!(
        conflict.lines(),
        vec!["DELETE /images/v4/images/name/legacy-img HTTP/1.1"]
    );
    assert!(
        conflict.stderr().contains("unfinished build 'build-3'"),
        "{}",
        conflict.stderr()
    );

    let unavailable = run_rm_with_image_service(
        dir.path(),
        vec![reply(
            "DELETE",
            "/images/v4/images/name/legacy-img",
            503,
            json!({"error": "deploying"}),
        )],
    )
    .await;
    assert_eq!(unavailable.code(), Some(1));
    assert_eq!(
        unavailable.lines(),
        vec!["DELETE /images/v4/images/name/legacy-img HTTP/1.1"]
    );
    assert!(
        unavailable
            .stderr()
            .contains("Error: Server error: 503 Service Unavailable - deploying"),
        "{}",
        unavailable.stderr()
    );

    // A lost response is never replayed across catalogs.
    let dropped = run_rm_with_image_service(
        dir.path(),
        vec![reset("DELETE", "/images/v4/images/name/legacy-img")],
    )
    .await;
    assert_eq!(dropped.code(), Some(1));
    assert_eq!(
        dropped.lines(),
        vec!["DELETE /images/v4/images/name/legacy-img HTTP/1.1"]
    );
}

async fn run_rm_with_image_service(
    dir: &std::path::Path,
    replies: Vec<image_support::Reply>,
) -> image_support::CliRun {
    let mut all = vec![introspection()];
    all.extend(replies);
    run(&["image", "rm", "legacy-img"], all, dir).await
}

/// `register` of a legacy `.tlsnap` snapshot: the snapshot lookup and the
/// platform template registration, nothing else.
#[tokio::test]
async fn legacy_register_uses_the_platform_registry_only() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "register", "legacy-img", "snap-legacy"],
        vec![
            introspection(),
            reply(
                "GET",
                "/v1/namespaces/default/snapshots/snap-legacy",
                200,
                json!({
                    "snapshot_id": "snap-legacy",
                    "status": "completed",
                    "sandbox_id": "sbx-legacy",
                    "snapshot_uri": "s3://snapshots/snap-legacy.tar.zst",
                    "snapshot_format_version": "durable_archive_v1",
                    "size_bytes": 123,
                    "rootfs_disk_bytes": 10_737_418_240_u64,
                }),
            ),
            reply(
                "POST",
                "/platform/v1/sandbox-templates",
                201,
                template_record("sandbox_template_9", "legacy-img"),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(run.output.status.success(), "{}", run.stderr());
    assert_eq!(
        run.lines(),
        vec![
            "GET /v1/namespaces/default/snapshots/snap-legacy HTTP/1.1",
            "POST /platform/v1/sandbox-templates HTTP/1.1",
        ]
    );
    assert!(run.image_service_requests().is_empty());
    let body = run.requests[1].json();
    assert_eq!(body["name"], "legacy-img");
    assert_eq!(body["snapshotId"], "snap-legacy");
    assert_eq!(body["dockerfile"], "<omitted>");
    assert_eq!(body["snapshotFormatVersion"], "durable_archive_v1");
    assert_eq!(
        run.stdout(),
        "Registered legacy image 'legacy-img' -> snapshot snap-legacy (rootfs 10 GiB)\nTemplate: sandbox_template_9\ns3://snapshots/snap-legacy.tar.zst\n"
    );
    assert!(run.stderr().is_empty(), "{}", run.stderr());
}

/// `snapshot ls` and `snapshot rm`: the sandbox API endpoints, unchanged.
#[tokio::test]
async fn legacy_snapshot_ls_and_rm_keep_their_endpoints() {
    let dir = tempfile::tempdir().unwrap();
    let listed = run(
        &["snapshot", "ls"],
        vec![
            introspection(),
            reply(
                "GET",
                "/v1/namespaces/default/snapshots",
                200,
                json!({"snapshots": [{
                    "snapshot_id": "snap-legacy", "status": "completed", "sandbox_id": "sbx-legacy",
                    "size_bytes": 123, "created_at": "2026-10-01T00:00:00Z",
                }]}),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(listed.output.status.success(), "{}", listed.stderr());
    assert_eq!(
        listed.lines(),
        vec!["GET /v1/namespaces/default/snapshots HTTP/1.1"]
    );
    assert!(
        listed.stdout().contains("snap-legacy"),
        "{}",
        listed.stdout()
    );

    let removed = run(
        &["snapshot", "rm", "snap-legacy"],
        vec![
            introspection(),
            reply(
                "DELETE",
                "/v1/namespaces/default/snapshots/snap-legacy",
                204,
                json!(null),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(removed.output.status.success(), "{}", removed.stderr());
    assert_eq!(
        removed.lines(),
        vec!["DELETE /v1/namespaces/default/snapshots/snap-legacy HTTP/1.1"]
    );
    assert!(removed.image_service_requests().is_empty());
    assert_eq!(removed.stdout().trim(), "snap-legacy");

    let failed = run(
        &["snapshot", "rm", "snap-legacy"],
        vec![
            introspection(),
            reply(
                "DELETE",
                "/v1/namespaces/default/snapshots/snap-legacy",
                500,
                json!({"error": "boom"}),
            ),
        ],
        dir.path(),
    )
    .await;
    assert_eq!(failed.code(), Some(1));
    assert!(
        failed.stderr().contains("Error: failed to delete snapshot snap-legacy (HTTP 500 Internal Server Error): {\"error\":\"boom\"}"),
        "{}",
        failed.stderr()
    );
}

/// Sandbox creation by a legacy image name: the name goes to the server as
/// is; no resolution, no Image Service admission.
#[tokio::test]
async fn legacy_sandbox_create_by_image_name_sends_only_the_create() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["create", "--image", "legacy-img"],
        vec![
            introspection(),
            reply(
                "POST",
                "/v1/namespaces/default/sandboxes",
                200,
                json!({
                    "sandbox_id": "sbx-1", "status": "running",
                    "sandbox_url": "https://sbx-1.sandbox.tensorlake.ai",
                }),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(run.output.status.success(), "{}", run.stderr());
    assert_eq!(
        run.lines(),
        vec!["POST /v1/namespaces/default/sandboxes HTTP/1.1"]
    );
    assert!(run.image_service_requests().is_empty());
    assert_eq!(run.requests[0].json()["image"], "legacy-img");
    assert_eq!(run.stdout().trim(), "sbx-1");
}
