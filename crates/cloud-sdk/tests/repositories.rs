use crate::common::random_string;

mod common;

/// Repo structural operations (create, archive/restore, operation log, delete) are gated by
/// scopes that repo-scoped credential mints deliberately omit (`repo:write`, `project:admin`).
/// This lifecycle pins that the SDK mints project-wide credentials for them — the #808
/// regression made `delete_repo`/`set_repo_status`/`list_operations` mint repo-scoped tokens
/// and 403 against a live server, which no offline test can catch.
#[tokio::test]
#[cfg_attr(not(feature = "integration-tests"), ignore)]
async fn test_repository_structural_operations() {
    let sdk = match common::create_sdk() {
        Ok(sdk) => sdk,
        Err(msg) => {
            eprintln!("Skipping integration test: {msg}");
            return;
        }
    };
    let (_org_id, project_id) = match common::get_org_and_project_ids() {
        Ok(ids) => ids,
        Err(msg) => {
            eprintln!("Skipping integration test: {msg}");
            return;
        }
    };

    let client = match sdk.artifact_storage() {
        Ok(client) => client,
        Err(e) => panic!("failed to create artifact storage client: {e}"),
    };

    let repo = format!("integration-test-repo-{}", random_string());
    // Cleared once the in-flow delete succeeds; otherwise cleanup deletes best-effort.
    let mut cleanup_repo = Some(repo.clone());

    let result: Result<(), String> = async {
        client
            .create_repo(&project_id, &repo, None)
            .await
            .map_err(|e| format!("create_repo failed: {e}"))?;

        client
            .archive_repo(&project_id, &repo)
            .await
            .map_err(|e| format!("archive_repo failed: {e}"))?;

        client
            .restore_repo(&project_id, &repo)
            .await
            .map_err(|e| format!("restore_repo failed: {e}"))?;

        let operations = client
            .list_operations(&project_id, &repo)
            .await
            .map_err(|e| format!("list_operations failed: {e}"))?
            .into_inner();
        if operations.repo != repo {
            return Err(format!(
                "operation log is for repo {}, expected {repo}",
                operations.repo
            ));
        }

        client
            .delete_repo(&project_id, &repo)
            .await
            .map_err(|e| format!("delete_repo failed: {e}"))?;
        Ok(())
    }
    .await;

    if result.is_ok() {
        cleanup_repo = None;
    }
    // Best-effort cleanup when the flow failed before its delete.
    if let Some(repo) = cleanup_repo {
        if let Err(e) = client.delete_repo(&project_id, &repo).await {
            eprintln!("Cleanup failed for repo {repo}: {e}");
        }
    }

    if let Err(err) = result {
        panic!("{err}");
    }
}
