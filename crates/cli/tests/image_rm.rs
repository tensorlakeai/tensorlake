//! `tl sbx image rm` and `describe` by name across the CAS and legacy image
//! registries. Deleting a CAS image releases its name for new sandboxes, so
//! a legacy image of the same name can no longer be used: deleting the name
//! deletes it too, while deleting an `image_` id deletes only that image and
//! warns. Removing the legacy image never fails the delete of the CAS image.
//! The CAS image registry lowercases names, so a name is looked up there in
//! lowercase and in the legacy image registry exactly as given.

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
            reply("DELETE", "/images/v4/names/app", 204, json!(null)),
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
            "DELETE /images/v4/names/app HTTP/1.1",
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
            reply("DELETE", "/images/v4/names/app", 204, json!(null)),
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
            reply("DELETE", "/images/v4/names/app", 204, json!(null)),
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
                "/images/v4/id/image_abc",
                200,
                image_record("image_abc", "app"),
            ),
            reply("DELETE", "/images/v4/id/image_abc", 204, json!(null)),
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
            "GET /images/v4/id/image_abc HTTP/1.1",
            "DELETE /images/v4/id/image_abc HTTP/1.1",
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
                "/images/v4/names/app",
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
            "DELETE /images/v4/names/app HTTP/1.1",
            "DELETE /platform/v1/sandbox-templates/by-name/app HTTP/1.1",
        ]
    );
    assert_eq!(run.stdout().trim(), "Deleted image 'app'.");
    assert!(run.stderr().trim().is_empty(), "{}", run.stderr());
}

/// The CAS image registry stores `MyProject` as `myproject`, so the spelling
/// used when creating the image deletes it; the legacy image cleaned up
/// with it is the one named exactly like the CAS image.
#[tokio::test]
async fn rm_mixed_case_name_deletes_the_cas_image_by_its_canonical_name() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "rm", "MyProject"],
        vec![
            introspection(),
            reply(
                "DELETE",
                "/images/v4/names/myproject",
                204,
                json!(null),
            ),
            reply(
                "GET",
                "/platform/v1/sandbox-templates/by-name/myproject",
                200,
                template_record("sandbox_template_1", "myproject"),
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
            "DELETE /images/v4/names/myproject HTTP/1.1",
            "GET /platform/v1/sandbox-templates/by-name/myproject HTTP/1.1",
            "DELETE /platform/v1/sandbox-templates/sandbox_template_1 HTTP/1.1",
        ]
    );
    assert_eq!(
        run.stdout().trim(),
        "Deleted image 'MyProject'.\nAlso deleted legacy image 'myproject' (sandbox_template_1): it \
         could no longer be used once the CAS image was deleted."
    );
}

/// Only a clean CAS 404 falls through, and the legacy image registry gets
/// the exact spelling, since legacy image names are case-sensitive.
#[tokio::test]
async fn rm_mixed_case_name_falls_back_to_the_exact_legacy_spelling() {
    let dir = tempfile::tempdir().unwrap();
    let run = run(
        &["image", "rm", "MyProject"],
        vec![
            introspection(),
            reply(
                "DELETE",
                "/images/v4/names/myproject",
                404,
                json!({"error": "name not found"}),
            ),
            reply(
                "DELETE",
                "/platform/v1/sandbox-templates/by-name/MyProject",
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
            "DELETE /images/v4/names/myproject HTTP/1.1",
            "DELETE /platform/v1/sandbox-templates/by-name/MyProject HTTP/1.1",
        ]
    );
    assert_eq!(run.stdout().trim(), "Deleted image 'MyProject'.");
}

#[tokio::test]
async fn describe_mixed_case_name_finds_the_cas_image_then_the_exact_legacy_spelling() {
    let dir = tempfile::tempdir().unwrap();
    let cas = run(
        &["image", "describe", "MyProject"],
        vec![
            introspection(),
            reply(
                "GET",
                "/images/v4/names/myproject",
                200,
                image_record("image_abc", "myproject"),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(cas.output.status.success(), "{}", cas.stderr());
    assert_eq!(
        cas.lines(),
        vec!["GET /images/v4/names/myproject HTTP/1.1"]
    );
    assert!(cas.stdout().contains("image_abc"), "{}", cas.stdout());

    let legacy = run(
        &["image", "describe", "MyProject"],
        vec![
            introspection(),
            reply(
                "GET",
                "/images/v4/names/myproject",
                404,
                json!({"error": "name not found"}),
            ),
            reply(
                "GET",
                "/platform/v1/sandbox-templates/by-name/MyProject",
                200,
                template_record("sandbox_template_1", "MyProject"),
            ),
        ],
        dir.path(),
    )
    .await;
    assert!(legacy.output.status.success(), "{}", legacy.stderr());
    assert_eq!(
        legacy.lines(),
        vec![
            "GET /images/v4/names/myproject HTTP/1.1",
            "GET /platform/v1/sandbox-templates/by-name/MyProject HTTP/1.1",
        ]
    );
    assert!(
        legacy.stdout().contains("sandbox_template_1"),
        "{}",
        legacy.stdout()
    );
}
