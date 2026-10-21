//! `tl sbx image rm` of a CAS image whose name a legacy image also holds.
//! Deleting the CAS image releases the name for new sandboxes, so the legacy
//! image can no longer be used: deleting the name deletes it too, while
//! deleting an `image_` id deletes only that image and warns. Removing the
//! legacy image never fails the delete of the CAS image.

mod image_support;

use image_support::{image_record, introspection, reply, run, template_record};
use serde_json::json;

#[tokio::test]
async fn rm_cas_name_also_deletes_a_same_named_legacy_image() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "rm", "app"],
        vec![
            introspection(),
            reply("DELETE", "/images/v4/images/name/app", 204, json!(null)),
            reply(
                "GET",
                "/platform/v1/sandbox-templates/by-name/app",
                200,
                template_record("sandbox_template_1", "app"),
            ),
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
        vec![
            "DELETE /images/v4/images/name/app HTTP/1.1",
            "GET /platform/v1/sandbox-templates/by-name/app HTTP/1.1",
            "DELETE /platform/v1/sandbox-templates/sandbox_template_1 HTTP/1.1",
        ]
    );
    assert_eq!(
        run.stdout().trim(),
        "Deleted image 'app'.\nAlso deleted legacy image 'app' (sandbox_template_1): it could \
         no longer be used once the CAS image was deleted."
    );
    assert!(run.stderr().trim().is_empty(), "{}", run.stderr());
}

/// A failed legacy delete leaves the CAS delete successful and names the
/// command that finishes the job.
#[tokio::test]
async fn rm_cas_name_reports_a_failed_legacy_delete() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "rm", "app"],
        vec![
            introspection(),
            reply("DELETE", "/images/v4/images/name/app", 204, json!(null)),
            reply(
                "GET",
                "/platform/v1/sandbox-templates/by-name/app",
                200,
                template_record("sandbox_template_1", "app"),
            ),
            reply(
                "DELETE",
                "/platform/v1/sandbox-templates/sandbox_template_1",
                409,
                json!({"message": "Sandbox template is being re-registered concurrently; retry the delete"}),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(run.output.status.success(), "{}", run.stderr());
    assert_eq!(run.stdout().trim(), "Deleted image 'app'.");
    let stderr = run.stderr();
    assert!(
        stderr.contains("Could not delete legacy image 'app' (sandbox_template_1)"),
        "{stderr}"
    );
    assert!(stderr.contains("re-registered concurrently"), "{stderr}");
    assert!(
        stderr.contains("Delete it with `tl sbx image rm sandbox_template_1`."),
        "{stderr}"
    );
}

/// Without the legacy image's id, the command to finish the job names it.
#[tokio::test]
async fn rm_cas_name_reports_a_failed_legacy_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "rm", "app"],
        vec![
            introspection(),
            reply("DELETE", "/images/v4/images/name/app", 204, json!(null)),
            reply(
                "GET",
                "/platform/v1/sandbox-templates/by-name/app",
                500,
                json!({"message": "internal error"}),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(run.output.status.success(), "{}", run.stderr());
    assert_eq!(run.stdout().trim(), "Deleted image 'app'.");
    let stderr = run.stderr();
    assert!(
        stderr.contains("Could not delete legacy image 'app',"),
        "{stderr}"
    );
    assert!(
        stderr.contains("Delete it with `tl sbx image rm app`."),
        "{stderr}"
    );
}

/// An `image_` id deletes only that image. Its name comes from a lookup
/// taken before the delete removes the record, and a same-named legacy image
/// is reported, not deleted.
#[tokio::test]
async fn rm_cas_id_warns_about_a_same_named_legacy_image() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "rm", "image_abc"],
        vec![
            introspection(),
            reply(
                "GET",
                "/images/v4/images/image_abc",
                200,
                image_record("image_abc", "app"),
            ),
            reply("DELETE", "/images/v4/images/image_abc", 204, json!(null)),
            reply(
                "GET",
                "/platform/v1/sandbox-templates/by-name/app",
                200,
                template_record("sandbox_template_1", "app"),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(run.output.status.success(), "{}", run.stderr());
    assert_eq!(
        run.lines(),
        vec![
            "GET /images/v4/images/image_abc HTTP/1.1",
            "DELETE /images/v4/images/image_abc HTTP/1.1",
            "GET /platform/v1/sandbox-templates/by-name/app HTTP/1.1",
        ]
    );
    assert_eq!(run.stdout().trim(), "Deleted image 'image_abc'.");
    assert_eq!(
        run.stderr().trim(),
        "⚠️  Legacy image 'app' (sandbox_template_1) has the same name and is still listed, but \
         new sandboxes can no longer use it. Rebuild or re-import it with --no-cas to use it \
         again, or delete it with `tl sbx image rm sandbox_template_1`."
    );
}

/// Deleting the legacy image itself makes no extra request and prints
/// nothing more.
#[tokio::test]
async fn rm_legacy_name_makes_no_extra_request() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "rm", "app"],
        vec![
            introspection(),
            reply(
                "DELETE",
                "/images/v4/images/name/app",
                404,
                json!({"error": "name not found"}),
            ),
            reply(
                "DELETE",
                "/platform/v1/sandbox-templates/by-name/app",
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
            "DELETE /images/v4/images/name/app HTTP/1.1",
            "DELETE /platform/v1/sandbox-templates/by-name/app HTTP/1.1",
        ]
    );
    assert_eq!(run.stdout().trim(), "Deleted image 'app'.");
    assert!(run.stderr().trim().is_empty(), "{}", run.stderr());
}
