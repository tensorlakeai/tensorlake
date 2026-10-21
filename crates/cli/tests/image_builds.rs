//! `tl sbx image build get|wait|cancel`, `--no-wait`, the CAS-only build-id
//! output and the Ctrl+C cancellation handshake, against a scripted Image
//! Service. Every request the CLI sends is recorded so the tests can assert
//! on the admission key, the cancel selector, and the absence of requests.

mod image_support;

use std::time::Duration;

use image_support::{
    build_json, hang, image_record, introspection, reply, run, start, write_dockerfile,
};
use serde_json::{Value, json};

#[tokio::test]
async fn build_get_prints_the_record_and_json() {
    let dir = tempfile::tempdir().unwrap();
    let run_table = run(
        &["image", "build", "get", "build-1"],
        vec![
            introspection(),
            reply(
                "GET",
                "/images/v4/builds/build-1",
                200,
                build_json("build-1", "running", None),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(run_table.output.status.success(), "{}", run_table.stderr());
    let stdout = run_table.stdout();
    assert!(stdout.contains("build-1"), "{stdout}");
    assert!(stdout.contains("running"), "{stdout}");
    assert!(stdout.contains("executing"), "{stdout}");
    assert!(stdout.contains("Operation deadline"), "{stdout}");
    assert!(!stdout.contains("Cleanup pending"), "{stdout}");
    assert_eq!(
        run_table.lines(),
        vec!["GET /images/v4/builds/build-1 HTTP/1.1"]
    );

    let run_json = run(
        &["image", "build", "get", "build-1", "--json"],
        vec![
            introspection(),
            reply(
                "GET",
                "/images/v4/builds/build-1",
                200,
                build_json(
                    "build-1",
                    "succeeded",
                    Some(image_record("image_ok", "img")),
                ),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(run_json.output.status.success(), "{}", run_json.stderr());
    let parsed: Value = serde_json::from_str(&run_json.stdout()).expect("valid JSON on stdout");
    assert_eq!(parsed["build_id"], "build-1");
    assert_eq!(parsed["status"], "succeeded");
    assert_eq!(parsed["result"]["id"], "image_ok");
    assert!(run_json.stderr().is_empty(), "{}", run_json.stderr());
}

#[tokio::test]
async fn build_get_reports_not_found_and_refuses_legacy_references_locally() {
    let dir = tempfile::tempdir().unwrap();
    let missing = run(
        &["image", "build", "get", "build-x"],
        vec![
            introspection(),
            reply(
                "GET",
                "/images/v4/builds/build-x",
                404,
                json!({"error": "build 'build-x' not found"}),
            ),
        ],
        dir.path(),
    )
    .await;
    assert_eq!(missing.code(), Some(1));
    assert!(
        missing.stderr().contains("build build-x not found"),
        "{}",
        missing.stderr()
    );

    // A legacy template id and an image id are refused before any request.
    for reference in ["sandbox_template_1", "image_abc123"] {
        let refused = run(
            &["image", "build", "get", reference],
            vec![introspection()],
            dir.path(),
        )
        .await;
        assert_eq!(refused.code(), Some(1), "{}", refused.stderr());
        assert!(refused.requests.is_empty(), "{:?}", refused.lines());
        assert!(
            refused.stderr().contains("not a build id") || refused.stderr().contains("legacy"),
            "{}",
            refused.stderr()
        );
    }
}

#[tokio::test]
async fn build_wait_returns_the_operations_own_result() {
    let dir = tempfile::tempdir().unwrap();
    let waited = run(
        &["image", "build", "wait", "build-1", "--json"],
        vec![
            introspection(),
            reply(
                "GET",
                "/images/v4/builds/build-1",
                200,
                build_json("build-1", "running", None),
            ),
            reply(
                "GET",
                "/images/v4/builds/build-1",
                200,
                build_json(
                    "build-1",
                    "succeeded",
                    Some(image_record("image_ok", "img")),
                ),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(waited.output.status.success(), "{}", waited.stderr());
    let parsed: Value = serde_json::from_str(&waited.stdout()).expect("valid JSON on stdout");
    // The result is the template-shaped record the catalog answers (camelCase,
    // derived from the trimmed service record): no provenance extras.
    assert_eq!(parsed["id"], "image_ok");
    assert_eq!(parsed["snapshotId"], "snap-cas");
    assert_eq!(
        parsed["snapshotFormatVersion"],
        "content_addressed_streaming_v1"
    );
    assert!(
        parsed.get("build_id").is_none() && parsed.get("buildId").is_none(),
        "{parsed}"
    );
    assert!(parsed.get("imageId").is_none(), "{parsed}");
    assert!(
        waited
            .stderr()
            .contains("CAS image 'img' registered (image_ok)"),
        "{}",
        waited.stderr()
    );
    // Only the build was polled; the mutable image record was never read.
    assert!(
        waited
            .lines()
            .iter()
            .all(|line| line.starts_with("GET /images/v4/builds/build-1 ")),
        "{:?}",
        waited.lines()
    );
}

#[tokio::test]
async fn build_wait_reports_failed_and_cancelled_distinctly() {
    let dir = tempfile::tempdir().unwrap();
    let mut failed = build_json("build-1", "failed", None);
    failed["error"] = json!("RUN apt-get exited with 100");
    let run_failed = run(
        &["image", "build", "wait", "build-1"],
        vec![
            introspection(),
            reply("GET", "/images/v4/builds/build-1", 200, failed),
        ],
        dir.path(),
    )
    .await;
    assert_eq!(run_failed.code(), Some(1));
    assert!(
        run_failed
            .stderr()
            .contains("build build-1 failed: RUN apt-get exited with 100"),
        "{}",
        run_failed.stderr()
    );

    let run_cancelled = run(
        &["image", "build", "wait", "build-1"],
        vec![
            introspection(),
            reply(
                "GET",
                "/images/v4/builds/build-1",
                200,
                build_json("build-1", "cancelled", None),
            ),
        ],
        dir.path(),
    )
    .await;
    assert_eq!(run_cancelled.code(), Some(1));
    assert!(
        run_cancelled
            .stderr()
            .contains("build build-1 was cancelled"),
        "{}",
        run_cancelled.stderr()
    );
}

#[tokio::test]
async fn build_wait_timeout_names_the_build_and_the_next_commands() {
    let dir = tempfile::tempdir().unwrap();
    let replies = std::iter::once(introspection())
        .chain(std::iter::repeat_n(
            reply(
                "GET",
                "/images/v4/builds/build-1",
                200,
                build_json("build-1", "running", None),
            ),
            20,
        ))
        .collect();
    let timed_out = run(
        &["image", "build", "wait", "build-1", "--timeout", "0.2"],
        replies,
        dir.path(),
    )
    .await;
    assert_eq!(timed_out.code(), Some(1));
    let stderr = timed_out.stderr();
    assert!(stderr.contains("build build-1 did not finish"), "{stderr}");
    assert!(stderr.contains("may still be running"), "{stderr}");
    assert!(
        stderr.contains("tl sbx image build wait build-1"),
        "{stderr}"
    );
    assert!(
        stderr.contains("tl sbx image build cancel build-1"),
        "{stderr}"
    );
    // A timeout never cancels.
    assert!(
        timed_out
            .lines()
            .iter()
            .all(|line| line.starts_with("GET ")),
        "{:?}",
        timed_out.lines()
    );
}

/// `--timeout` bounds the first request too: against a service that never
/// answers the lookup, the wait ends at the timeout (naming the build and
/// the next commands), not when the connection does, and nothing is
/// cancelled.
#[tokio::test]
async fn build_wait_timeout_bounds_a_stalled_initial_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let started = std::time::Instant::now();
    let timed_out = run(
        &["image", "build", "wait", "build-1", "--timeout", "0.2"],
        vec![
            introspection(),
            hang("GET", "/images/v4/builds/build-1", Duration::from_secs(20)),
        ],
        dir.path(),
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the stalled lookup was abandoned at the timeout: {:?}",
        started.elapsed()
    );
    assert_eq!(timed_out.code(), Some(1), "{}", timed_out.stderr());
    let stderr = timed_out.stderr();
    assert!(stderr.contains("build build-1 did not finish"), "{stderr}");
    assert!(
        stderr.contains("tl sbx image build cancel build-1"),
        "{stderr}"
    );
    assert!(
        timed_out
            .lines()
            .iter()
            .all(|line| line.starts_with("GET ")),
        "a timeout never cancels: {:?}",
        timed_out.lines()
    );
}

/// Ctrl+C while the wait's first lookup is still unanswered: the handler is
/// already installed and the lookup runs inside the interruptible wait, so
/// the build is cancelled by the id the user passed and the CLI exits with
/// the interrupt status.
#[tokio::test]
async fn build_wait_ctrl_c_during_the_initial_lookup_cancels_by_build_id() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(vec![
        introspection(),
        hang("GET", "/images/v4/builds/build-7", Duration::from_secs(20)),
        reply(
            "POST",
            "/images/v4/builds/cancel",
            200,
            json!({"outcome": "cancelled", "build": build_json("build-7", "cancelled", None)}),
        ),
    ])
    .await;
    let child = image_support::command(
        &server.url,
        &["image", "build", "wait", "build-7"],
        dir.path(),
    )
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .spawn()
    .unwrap();
    // The lookup is on the wire and unanswered.
    server.wait_for("GET", "/images/v4/builds/build-7").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    unsafe {
        libc::kill(child.id().unwrap() as i32, libc::SIGINT);
    }
    let output = tokio::time::timeout(Duration::from_secs(40), child.wait_with_output())
        .await
        .expect("interrupted CLI must exit")
        .unwrap();
    let requests = server.finish().await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(130), "{stderr}");
    assert!(stderr.contains("Cancelling build build-7..."), "{stderr}");
    assert!(stderr.contains("Build build-7 cancelled"), "{stderr}");
    let cancel = requests
        .iter()
        .find(|request| request.path() == "/images/v4/builds/cancel")
        .expect("cancel handshake by build id");
    assert_eq!(cancel.json(), json!({"build_id": "build-7"}));
}

#[tokio::test]
async fn build_cancel_by_build_id_is_sufficient_and_distinguishes_outcomes() {
    let dir = tempfile::tempdir().unwrap();
    let cancelled = run(
        &["image", "build", "cancel", "build-1"],
        vec![
            introspection(),
            reply(
                "POST",
                "/images/v4/builds/cancel",
                200,
                json!({"outcome": "cancelled", "build": build_json("build-1", "cancelled", None)}),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(cancelled.output.status.success(), "{}", cancelled.stderr());
    assert_eq!(cancelled.stdout().trim(), "Build build-1 cancelled.");
    assert_eq!(cancelled.requests.len(), 1);
    assert_eq!(cancelled.requests[0].json(), json!({"build_id": "build-1"}));
    assert!(cancelled.requests[0].header("idempotency-key").is_none());

    let completed = run(
        &["image", "build", "cancel", "build-1"],
        vec![
            introspection(),
            reply(
                "POST",
                "/images/v4/builds/cancel",
                200,
                json!({
                    "outcome": "already_completed",
                    "build": build_json("build-1", "succeeded", Some(image_record("image_ok", "img"))),
                }),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(completed.output.status.success(), "{}", completed.stderr());
    assert!(
        completed.stdout().contains("Build already completed"),
        "{}",
        completed.stdout()
    );
    assert!(
        completed
            .stdout()
            .contains("CAS image 'img' registered (image_ok)"),
        "{}",
        completed.stdout()
    );

    let as_json = run(
        &["image", "build", "cancel", "build-1", "--json"],
        vec![
            introspection(),
            reply(
                "POST",
                "/images/v4/builds/cancel",
                200,
                json!({"outcome": "already_cancelled", "build": build_json("build-1", "cancelled", None)}),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(as_json.output.status.success(), "{}", as_json.stderr());
    let parsed: Value = serde_json::from_str(&as_json.stdout()).expect("valid JSON on stdout");
    assert_eq!(parsed["outcome"], "already_cancelled");
    assert_eq!(parsed["build"]["build_id"], "build-1");

    let missing = run(
        &["image", "build", "cancel", "build-x"],
        vec![
            introspection(),
            reply(
                "POST",
                "/images/v4/builds/cancel",
                404,
                json!({"error": "build 'build-x' not found"}),
            ),
        ],
        dir.path(),
    )
    .await;
    assert_eq!(missing.code(), Some(1));
    assert!(
        missing.stderr().contains("build build-x not found"),
        "{}",
        missing.stderr()
    );
}

#[tokio::test]
async fn build_cancel_by_request_id_reports_not_admitted_as_an_observation() {
    let dir = tempfile::tempdir().unwrap();
    let not_admitted = run(
        &["image", "build", "cancel", "--request-id", "rid-lost"],
        vec![
            introspection(),
            reply(
                "POST",
                "/images/v4/builds/cancel",
                200,
                json!({"outcome": "not_admitted", "build": null}),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(
        not_admitted.output.status.success(),
        "{}",
        not_admitted.stderr()
    );
    let stdout = not_admitted.stdout();
    assert!(
        stdout.contains("No build was admitted for client request id rid-lost"),
        "{stdout}"
    );
    assert!(stdout.contains("not a cancellation"), "{stdout}");
    assert_eq!(not_admitted.requests.len(), 1);
    assert_eq!(
        not_admitted.requests[0].json(),
        json!({"client_request_id": "rid-lost"})
    );

    // The two selectors are mutually exclusive, and one is required.
    let both = run(
        &["image", "build", "cancel", "build-1", "--request-id", "rid"],
        vec![introspection()],
        dir.path(),
    )
    .await;
    assert_eq!(both.code(), Some(2), "{}", both.stderr());
    assert!(both.requests.is_empty());
    let neither = run(
        &["image", "build", "cancel"],
        vec![introspection()],
        dir.path(),
    )
    .await;
    assert_eq!(neither.code(), Some(2), "{}", neither.stderr());
    assert!(neither.requests.is_empty());
}

#[tokio::test]
async fn cas_import_no_wait_prints_the_build_id_after_admission() {
    let dir = tempfile::tempdir().unwrap();
    let admitted = run(
        &["image", "import", "--no-wait", "-n", "img", "ubuntu:24.04"],
        vec![
            introspection(),
            reply(
                "POST",
                "/images/v4/builds",
                201,
                build_json("build-7", "pending", None),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(admitted.output.status.success(), "{}", admitted.stderr());
    assert_eq!(admitted.stdout().trim(), "build-7");
    let stderr = admitted.stderr();
    assert!(stderr.contains("Build ID: build-7"), "{stderr}");
    assert!(
        stderr.contains("tl sbx image build wait build-7"),
        "{stderr}"
    );
    assert_eq!(admitted.requests.len(), 1, "{:?}", admitted.lines());
    let admission = &admitted.requests[0];
    assert_eq!(admission.line, "POST /images/v4/builds HTTP/1.1");
    let key = admission
        .header("idempotency-key")
        .expect("admission carries Idempotency-Key");
    assert_eq!(key.len(), 32);
    assert!(key.bytes().all(|b| b.is_ascii_hexdigit()));
    let body = admission.json();
    assert_eq!(body["kind"], "import");
    assert_eq!(body["image_ref"], "ubuntu:24.04");
    assert_eq!(body["name"], "img");

    let as_json = run(
        &[
            "image",
            "import",
            "--no-wait",
            "--json",
            "-n",
            "img",
            "ubuntu:24.04",
        ],
        vec![
            introspection(),
            reply(
                "POST",
                "/images/v4/builds",
                201,
                build_json("build-7", "pending", None),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(as_json.output.status.success(), "{}", as_json.stderr());
    let parsed: Value = serde_json::from_str(&as_json.stdout()).expect("valid JSON on stdout");
    assert_eq!(parsed["build_id"], "build-7");
    assert_eq!(parsed["status"], "pending");
}

#[tokio::test]
async fn cas_create_no_wait_uploads_and_seals_then_prints_the_build_id() {
    let dir = tempfile::tempdir().unwrap();
    write_dockerfile(dir.path(), "FROM scratch\nRUN echo hi\n");
    // The admission answers with an upload capability pointing back at the
    // fixture, so the context PUT and the seal are both recorded. The
    // fixture's address is known only once it listens, so the replies are
    // added after start.
    let server = start(vec![]).await;
    let mut admission = build_json("build-9", "pending", None);
    admission["kind"] = json!("dockerfile");
    admission["context_upload"] = json!({
        "url": format!("{}/upload/build-9", server.url),
        "max_bytes": 10_000_000,
    });
    server.script(vec![
        introspection(),
        reply("POST", "/images/v4/builds", 201, admission),
        reply("PUT", "/upload/build-9", 200, json!({})),
        reply(
            "POST",
            "/images/v4/builds/build-9/context/seal",
            200,
            json!({"sealed": true}),
        ),
    ]);
    let output = image_support::command(
        &server.url,
        &["image", "create", "--no-wait", "-n", "img", "Dockerfile"],
        dir.path(),
    )
    .output()
    .await
    .unwrap();
    let requests = server.finish().await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "build-9");
    // The upload capability stays inside the admission step: nothing the
    // handle reports carries it.
    assert!(!stderr.contains("/upload/build-9"), "{stderr}");
    let lines: Vec<&str> = requests
        .iter()
        .map(|request| request.line.as_str())
        .collect();
    assert_eq!(lines[0], "POST /images/v4/builds HTTP/1.1", "{lines:?}");
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("PUT /upload/build-9 ")),
        "{lines:?}"
    );
    assert_eq!(
        *lines.last().unwrap(),
        "POST /images/v4/builds/build-9/context/seal HTTP/1.1",
        "{lines:?}"
    );
    let seal = requests.last().unwrap().json();
    assert_eq!(seal["digest"].as_str().map(str::len), Some(64));
    assert!(stderr.contains("Build ID: build-9"), "{stderr}");
    assert!(stderr.contains("Sealing build context"), "{stderr}");
}

/// A CAS image takes the CAS image registry's spelling before anything is
/// shown or sent: an explicit name is lowercased, a generated default from
/// the Dockerfile's directory becomes a valid CAS image name.
#[tokio::test]
async fn cas_names_are_canonical_before_they_are_shown_or_sent() {
    let dir = tempfile::tempdir().unwrap();
    let explicit = run(
        &[
            "image",
            "import",
            "--no-wait",
            "-n",
            "MyBase",
            "ubuntu:24.04",
        ],
        vec![
            introspection(),
            reply(
                "POST",
                "/images/v4/builds",
                201,
                build_json("build-7", "pending", None),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(explicit.output.status.success(), "{}", explicit.stderr());
    assert_eq!(explicit.requests[0].json()["name"], "mybase");
    assert!(
        explicit.stderr().contains("Selected image name: mybase"),
        "{}",
        explicit.stderr()
    );

    let project = dir.path().join("My Project");
    std::fs::create_dir(&project).unwrap();
    write_dockerfile(&project, "FROM scratch\n");
    let server = start(vec![]).await;
    let mut admission = build_json("build-9", "pending", None);
    admission["kind"] = json!("dockerfile");
    admission["context_upload"] = json!({
        "url": format!("{}/upload/build-9", server.url),
        "max_bytes": 10_000_000,
    });
    server.script(vec![
        introspection(),
        reply("POST", "/images/v4/builds", 201, admission),
        reply("PUT", "/upload/build-9", 200, json!({})),
        reply(
            "POST",
            "/images/v4/builds/build-9/context/seal",
            200,
            json!({"sealed": true}),
        ),
    ]);
    let output = image_support::command(
        &server.url,
        &["image", "create", "--no-wait", "My Project/Dockerfile"],
        dir.path(),
    )
    .output()
    .await
    .unwrap();
    let requests = server.finish().await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(
        stderr.contains("Selected image name: my-project"),
        "{stderr}"
    );
    let admission = requests
        .iter()
        .find(|request| request.line == "POST /images/v4/builds HTTP/1.1")
        .expect("admission request");
    assert_eq!(admission.json()["name"], "my-project");
}

#[tokio::test]
async fn no_wait_with_no_cas_is_a_usage_error_before_any_request() {
    let dir = tempfile::tempdir().unwrap();
    write_dockerfile(dir.path(), "FROM scratch\n");
    let refused = run(
        &["image", "create", "--no-cas", "--no-wait", "Dockerfile"],
        vec![introspection()],
        dir.path(),
    )
    .await;
    assert_eq!(refused.code(), Some(2), "{}", refused.stderr());
    assert!(refused.requests.is_empty(), "{:?}", refused.lines());
    assert!(
        refused.stderr().contains("--no-cas"),
        "{}",
        refused.stderr()
    );
}

#[tokio::test]
async fn cas_import_conflict_names_the_active_build_and_its_cancel_command() {
    let dir = tempfile::tempdir().unwrap();
    let conflict = run(
        &["image", "import", "-n", "img", "ubuntu:24.04"],
        vec![
            introspection(),
            reply(
                "POST",
                "/images/v4/builds",
                409,
                json!({
                    "error": "Image 'img' already has an unfinished import 'build-3'. Cancel it before starting another.",
                    "code": "image_build_in_progress",
                    "image_name": "img",
                    "active_build_id": "build-3",
                    "active_kind": "import",
                    "active_status": "running",
                }),
            ),
        ],
        dir.path(),
    )
    .await;
    assert_eq!(conflict.code(), Some(1));
    let stderr = conflict.stderr();
    assert!(
        stderr.contains("already has an unfinished import 'build-3'"),
        "{stderr}"
    );
    assert!(
        stderr.contains("Cancel it with: tl sbx image build cancel build-3"),
        "{stderr}"
    );
    // No automatic cancellation or fresh-key retry.
    assert_eq!(conflict.requests.len(), 1, "{:?}", conflict.lines());
}

#[tokio::test]
async fn cas_import_waits_to_publication_and_prints_the_registration_line() {
    let dir = tempfile::tempdir().unwrap();
    let published = run(
        &["image", "import", "-n", "img", "--json", "ubuntu:24.04"],
        vec![
            introspection(),
            reply(
                "POST",
                "/images/v4/builds",
                201,
                build_json("build-1", "pending", None),
            ),
            reply(
                "GET",
                "/images/v4/builds/build-1",
                200,
                build_json(
                    "build-1",
                    "succeeded",
                    Some(image_record("image_ok", "img")),
                ),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(published.output.status.success(), "{}", published.stderr());
    let stderr = published.stderr();
    assert!(stderr.contains("Build ID: build-1"), "{stderr}");
    assert!(
        stderr.contains("CAS image 'img' registered (image_ok)"),
        "{stderr}"
    );
    let parsed: Value = serde_json::from_str(&published.stdout()).expect("valid JSON on stdout");
    // The result keeps the platform template shape (ADR 0107 D28).
    assert_eq!(parsed["id"], "image_ok");
    assert_eq!(
        parsed["snapshotFormatVersion"],
        "content_addressed_streaming_v1"
    );
    assert_eq!(
        published.lines(),
        vec![
            "POST /images/v4/builds HTTP/1.1",
            "GET /images/v4/builds/build-1 HTTP/1.1",
        ]
    );
}

/// Ctrl+C during a CAS wait: the CLI announces the cancellation, cancels by
/// build id, reports the confirmed outcome, and exits with the interrupt
/// status.
#[tokio::test]
async fn cas_ctrl_c_cancels_by_build_id_and_exits_with_the_interrupt_status() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(vec![
        introspection(),
        reply(
            "POST",
            "/images/v4/builds",
            201,
            build_json("build-5", "pending", None),
        ),
        reply(
            "GET",
            "/images/v4/builds/build-5",
            200,
            build_json("build-5", "running", None),
        ),
        reply(
            "POST",
            "/images/v4/builds/cancel",
            200,
            json!({"outcome": "cancelled", "build": build_json("build-5", "cancelled", None)}),
        ),
    ])
    .await;
    let child = image_support::command(
        &server.url,
        &["image", "import", "-n", "img", "ubuntu:24.04"],
        dir.path(),
    )
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .spawn()
    .unwrap();
    // Let admission and the first poll happen, then interrupt during the
    // poll interval.
    server.wait_for("GET", "/images/v4/builds/build-5").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    unsafe {
        libc::kill(child.id().unwrap() as i32, libc::SIGINT);
    }
    let output = tokio::time::timeout(Duration::from_secs(40), child.wait_with_output())
        .await
        .expect("interrupted CLI must exit")
        .unwrap();
    let requests = server.finish().await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(130), "{stderr}");
    assert!(stderr.contains("Cancelling build build-5..."), "{stderr}");
    assert!(stderr.contains("Build build-5 cancelled"), "{stderr}");
    let cancel = requests
        .iter()
        .find(|request| request.path() == "/images/v4/builds/cancel")
        .expect("cancel handshake");
    assert_eq!(cancel.json(), json!({"build_id": "build-5"}));
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.path() == "/images/v4/builds")
            .count(),
        1,
        "admission is never replayed"
    );
}

/// Ctrl+C before the admission answered: no replay of the admission; the
/// request id resolves the outcome, and `not_admitted` is printed as exactly
/// that, with the request-id cancel command.
#[tokio::test]
async fn cas_ctrl_c_before_admission_resolves_by_request_id() {
    let dir = tempfile::tempdir().unwrap();
    let server = start(vec![
        introspection(),
        hang("POST", "/images/v4/builds", Duration::from_secs(20)),
        reply(
            "POST",
            "/images/v4/builds/cancel",
            200,
            json!({"outcome": "not_admitted", "build": null}),
        ),
    ])
    .await;
    let child = image_support::command(
        &server.url,
        &["image", "import", "-n", "img", "ubuntu:24.04"],
        dir.path(),
    )
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .spawn()
    .unwrap();
    // The admission request is on the wire and unanswered.
    server.wait_for("POST", "/images/v4/builds").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    unsafe {
        libc::kill(child.id().unwrap() as i32, libc::SIGINT);
    }
    let output = tokio::time::timeout(Duration::from_secs(40), child.wait_with_output())
        .await
        .expect("interrupted CLI must exit")
        .unwrap();
    let requests = server.finish().await;
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.code(), Some(130), "{stderr}");
    assert!(
        stderr.contains("Cancelling build (admission pending)..."),
        "{stderr}"
    );
    assert!(
        stderr.contains("No build was admitted for client request id"),
        "{stderr}"
    );
    assert!(stderr.contains("not a confirmed cancellation"), "{stderr}");
    assert!(
        stderr.contains("tl sbx image build cancel --request-id "),
        "{stderr}"
    );
    let admissions: Vec<_> = requests
        .iter()
        .filter(|request| request.path() == "/images/v4/builds")
        .collect();
    assert_eq!(admissions.len(), 1, "the admission is not replayed");
    let request_id = admissions[0].header("idempotency-key").unwrap().to_string();
    let cancel = requests
        .iter()
        .find(|request| request.path() == "/images/v4/builds/cancel")
        .expect("cancel by request id");
    assert_eq!(cancel.json(), json!({"client_request_id": request_id}));
    assert!(stderr.contains(&request_id), "{stderr}");
}

#[tokio::test]
async fn cas_register_retries_a_pending_registration_and_keeps_its_final_output() {
    let dir = tempfile::tempdir().unwrap();
    let snapshot = json!({
        "snapshot_id": "snap-cas",
        "status": "completed",
        "sandbox_id": "sbx-1",
        "snapshot_uri": "snapshot://snap-cas",
        "snapshot_format_version": "content_addressed_streaming_v1",
        "size_bytes": 42,
        "rootfs_disk_bytes": 10_737_418_240_u64,
    });
    // The service never answers 202 for an adoption: a still-running
    // operation is `503 publication_pending` naming the build, and the
    // identical request under the same key joins it until it publishes.
    let pending = json!({
        "error": "registration of \"img\" is still pending (build build-2)",
        "code": "publication_pending",
        "build_id": "build-2",
    });
    let registered = run(
        &["image", "register", "img", "snap-cas"],
        vec![
            introspection(),
            reply(
                "GET",
                "/v1/namespaces/default/snapshots/snap-cas",
                200,
                snapshot.clone(),
            ),
            reply(
                "POST",
                "/images/v4/images/from-snapshot",
                503,
                pending.clone(),
            ),
            reply("POST", "/images/v4/images/from-snapshot", 503, pending),
            reply(
                "POST",
                "/images/v4/images/from-snapshot",
                201,
                image_record("image_reg", "img"),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(
        registered.output.status.success(),
        "{}",
        registered.stderr()
    );
    // The shared printer names the image family: the CAS branch prints the
    // derived `snapshot://` handle where the legacy branch prints the
    // archive URI.
    assert_eq!(
        registered.stdout(),
        "Registered CAS image 'img' -> snapshot snap-cas (rootfs 10 GiB)\nTemplate: image_reg\n\
         snapshot://snap-cas\n"
    );
    assert!(
        registered
            .stderr()
            .contains("pending (build build-2); waiting for publication"),
        "{}",
        registered.stderr()
    );
    let adoptions: Vec<_> = registered
        .requests
        .iter()
        .filter(|request| request.path() == "/images/v4/images/from-snapshot")
        .collect();
    assert_eq!(adoptions.len(), 3, "{:?}", registered.lines());
    let keys: std::collections::HashSet<_> = adoptions
        .iter()
        .map(|request| request.header("idempotency-key").map(str::to_string))
        .collect();
    assert_eq!(keys.len(), 1, "one key across every retry");
    assert_eq!(
        adoptions[0].header("idempotency-key").map(str::len),
        Some(32)
    );
    assert!(
        adoptions
            .iter()
            .all(|request| request.json()["snapshot_id"] == "snap-cas")
    );
    assert!(
        !registered
            .lines()
            .iter()
            .any(|line| line.contains("/images/v4/builds/")),
        "the pending registration is joined by replay, never polled: {:?}",
        registered.lines()
    );

    // 201 publishes directly: no retry, same output.
    let published = run(
        &["image", "register", "img", "snap-cas"],
        vec![
            introspection(),
            reply(
                "GET",
                "/v1/namespaces/default/snapshots/snap-cas",
                200,
                snapshot,
            ),
            reply(
                "POST",
                "/images/v4/images/from-snapshot",
                201,
                image_record("image_reg", "img"),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(published.output.status.success(), "{}", published.stderr());
    assert_eq!(published.stdout(), registered.stdout());
    assert!(
        !published
            .lines()
            .iter()
            .any(|line| line.contains("/images/v4/builds/")),
        "{:?}",
        published.lines()
    );
}

#[tokio::test]
async fn help_uses_the_real_tl_sbx_spelling() {
    let dir = tempfile::tempdir().unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_tl"))
        .args(["sbx", "image", "build", "--help"])
        .current_dir(dir.path())
        .env("NO_COLOR", "1")
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(
        help.contains("Usage: tl sbx image build <COMMAND>"),
        "{help}"
    );
    assert!(
        help.contains("`tl sbx image create` and `import`"),
        "{help}"
    );
    for subcommand in ["get", "wait", "cancel"] {
        assert!(help.contains(subcommand), "{help}");
    }
    let create_help = tokio::process::Command::new(env!("CARGO_BIN_EXE_tl"))
        .args(["sbx", "image", "create", "--help"])
        .current_dir(dir.path())
        .env("NO_COLOR", "1")
        .output()
        .await
        .unwrap();
    let create_help = String::from_utf8_lossy(&create_help.stdout);
    assert!(
        create_help.contains("tl sbx image build wait BUILD_ID"),
        "{create_help}"
    );
    assert!(create_help.contains("--no-wait"), "{create_help}");
    let cancel_help = tokio::process::Command::new(env!("CARGO_BIN_EXE_tl"))
        .args(["sbx", "image", "build", "cancel", "--help"])
        .current_dir(dir.path())
        .env("NO_COLOR", "1")
        .output()
        .await
        .unwrap();
    let cancel_help = String::from_utf8_lossy(&cancel_help.stdout);
    assert!(cancel_help.contains("--request-id"), "{cancel_help}");
    assert!(cancel_help.contains("not_admitted"), "{cancel_help}");
    let rm_help = tokio::process::Command::new(env!("CARGO_BIN_EXE_tl"))
        .args(["sbx", "image", "rm", "--help"])
        .current_dir(dir.path())
        .env("NO_COLOR", "1")
        .output()
        .await
        .unwrap();
    let rm_help = String::from_utf8_lossy(&rm_help.stdout);
    assert!(
        rm_help.contains("tl sbx image build cancel BUILD_ID"),
        "{rm_help}"
    );
    assert!(rm_help.contains("published"), "{rm_help}");
}
