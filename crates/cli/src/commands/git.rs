use std::path::PathBuf;

use comfy_table::Cell;
use console::style;
use tensorlake::artifact_storage::ArtifactStorageClient;
use tensorlake::artifact_storage::models::{
    ListBranchesResponse, ListOperationsResponse, ListRefsResponse, ListReposResponse,
};
use tensorlake::{ClientBuilder, Sdk};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

pub(crate) mod fastclone;

pub use fastclone::parse_cache_max_bytes;

pub fn repo_url(ctx: &CliContext, repo: &str) -> Result<String> {
    let client = artifact_storage_client(ctx)?;
    let project_id = project_id(ctx)?;
    Ok(client.git_repo_url(&project_id, repo))
}

pub async fn mint_token(ctx: &CliContext, repo: Option<&str>, output_json: bool) -> Result<()> {
    let project_id = project_id(ctx)?;
    let credential = artifact_storage_client(ctx)?
        .mint_token_for_repo(&project_id, repo)
        .await?
        .into_inner();
    if output_json {
        println!("{}", serde_json::to_string_pretty(&credential)?);
        return Ok(());
    }

    print_field("project", &project_id);
    print_field("repo", &credential.repo_pattern);
    print_field("username", &credential.git_username);
    println!(
        "{} {}",
        style("password:").dim(),
        style(&credential.token).yellow()
    );
    print_field("expires", &credential.expires_at);
    print_field("scopes", &credential.scopes.join(", "));

    if credential.repo_pattern != "*" {
        let remote_url =
            artifact_storage_client(ctx)?.git_repo_url(&project_id, &credential.repo_pattern);
        println!();
        println!("{}", style("Remote URL").bold().green());
        println!("  {}", style(&remote_url).cyan());
        println!();
        println!(
            "{}",
            style("Use this credential with Git or SDK clients")
                .bold()
                .green()
        );
        println!("  {} {}", style("username:").dim(), credential.git_username);
        println!("  {} {}", style("password:").dim(), "the token above");
    }
    Ok(())
}

pub async fn create_repo(
    ctx: &CliContext,
    repo: &str,
    default_branch: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let project_id = project_id(ctx)?;
    let client = artifact_storage_client(ctx)?;
    client
        .create_repo(&project_id, repo, default_branch)
        .await
        .map_err(map_sdk_error)?;
    if output_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "repo": repo,
                "url": client.git_repo_url(&project_id, repo),
            }))?
        );
    } else {
        println!("created {}", client.git_repo_url(&project_id, repo));
    }
    Ok(())
}

pub async fn list_repos(ctx: &CliContext, output_json: bool) -> Result<()> {
    let project_id = project_id(ctx)?;
    let response = artifact_storage_client(ctx)?
        .list_repos(&project_id)
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    if output_json {
        print_json(&response)?;
        return Ok(());
    }
    print_repos_table(&response);
    Ok(())
}

pub async fn delete_repo(ctx: &CliContext, repo: &str) -> Result<()> {
    let project_id = project_id(ctx)?;
    artifact_storage_client(ctx)?
        .delete_repo(&project_id, repo)
        .await
        .map_err(map_sdk_error)?;
    println!("deleted {repo}");
    Ok(())
}

pub async fn fork_repo(ctx: &CliContext, repo: &str, base_repo: &str) -> Result<()> {
    let project_id = project_id(ctx)?;
    let client = artifact_storage_client(ctx)?;
    client
        .fork_repo(&project_id, repo, base_repo)
        .await
        .map_err(map_sdk_error)?;
    println!(
        "forked {} from {}",
        client.git_repo_url(&project_id, repo),
        base_repo
    );
    Ok(())
}

pub async fn archive_repo(ctx: &CliContext, repo: &str) -> Result<()> {
    let project_id = project_id(ctx)?;
    artifact_storage_client(ctx)?
        .archive_repo(&project_id, repo)
        .await
        .map_err(map_sdk_error)?;
    println!("archived {repo}");
    Ok(())
}

pub async fn restore_repo(ctx: &CliContext, repo: &str) -> Result<()> {
    let project_id = project_id(ctx)?;
    artifact_storage_client(ctx)?
        .restore_repo(&project_id, repo)
        .await
        .map_err(map_sdk_error)?;
    println!("restored {repo}");
    Ok(())
}

/// Accept either a bare repo name or a full clone URL (as printed by `tl git url`), matching how
/// `git clone` itself treats its argument. A URL's last non-empty path segment (with any `.git`
/// suffix stripped) is used as the repo name.
fn normalize_repo_arg(repo: &str) -> String {
    let Ok(url) = reqwest::Url::parse(repo) else {
        return repo.to_string();
    };
    if !matches!(url.scheme(), "http" | "https") {
        return repo.to_string();
    }
    url.path_segments()
        .into_iter()
        .flatten()
        .filter(|s| !s.is_empty())
        .next_back()
        .map(|s| s.strip_suffix(".git").unwrap_or(s).to_string())
        .unwrap_or_else(|| repo.to_string())
}

pub async fn clone_repo(
    ctx: &CliContext,
    repo: &str,
    dest: Option<PathBuf>,
    cache_dir: Option<PathBuf>,
    cache_max_bytes: Option<u64>,
    no_checkout: bool,
) -> Result<()> {
    let repo = &normalize_repo_arg(repo);
    let project_id = project_id(ctx)?;
    let client = artifact_storage_client(ctx)?;
    let repo_url = client.git_repo_url(&project_id, repo);

    let spinner = fastclone::new_spinner(&format!("minting git credential for {repo}"));
    let credential = client
        .mint_token_for_repo(&project_id, Some(repo))
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    if let Some(pb) = &spinner {
        pb.set_message("fetching clone manifest");
    }

    let dest = dest.unwrap_or_else(|| fastclone::default_dest_from_url(&repo_url));
    let opts = fastclone::FastCloneOptions {
        repo_url: repo_url.clone(),
        dest,
        cache_dir,
        cache_max_bytes,
        credential: Some(fastclone::BasicAuth {
            username: credential.git_username,
            password: Some(credential.token),
        }),
        checkout: !no_checkout,
        progress: spinner,
    };
    let stats = fastclone::fast_clone(opts).await?;
    println!(
        "{}",
        fastclone::format_fast_clone_stats(&format!("cloned {repo}"), &stats)
    );
    Ok(())
}

pub async fn list_branches(ctx: &CliContext, repo: &str, output_json: bool) -> Result<()> {
    let project_id = project_id(ctx)?;
    let response = artifact_storage_client(ctx)?
        .list_branches(&project_id, repo)
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    if output_json {
        print_json(&response)?;
        return Ok(());
    }
    print_branches_table(&response);
    Ok(())
}

pub async fn delete_branch(ctx: &CliContext, repo: &str, branch: &str) -> Result<()> {
    let project_id = project_id(ctx)?;
    artifact_storage_client(ctx)?
        .delete_branch(&project_id, repo, branch)
        .await
        .map_err(map_sdk_error)?;
    println!("deleted branch {branch} from {repo}");
    Ok(())
}

pub async fn list_refs(ctx: &CliContext, repo: &str, output_json: bool) -> Result<()> {
    let project_id = project_id(ctx)?;
    let response = artifact_storage_client(ctx)?
        .list_refs(&project_id, repo)
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    if output_json {
        print_json(&response)?;
        return Ok(());
    }
    print_refs_table(&response);
    Ok(())
}

pub async fn status(ctx: &CliContext, repo: &str, output_json: bool) -> Result<()> {
    let project_id = project_id(ctx)?;
    let client = artifact_storage_client(ctx)?;
    let branches = client
        .list_branches(&project_id, repo)
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    let refs = client
        .list_refs(&project_id, repo)
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    let remote_url = client.git_repo_url(&project_id, repo);

    if output_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "repo": repo,
                "url": remote_url,
                "branches": branches.branches,
                "refs": refs.refs,
            }))?
        );
        return Ok(());
    }

    println!("repo: {repo}");
    println!("url: {remote_url}");
    println!("branches: {}", branches.branches.len());
    if branches.branches.is_empty() {
        println!("no branches found");
    } else {
        print_branches_table(&branches);
    }
    println!("refs: {}", refs.refs.len());
    Ok(())
}

pub async fn list_operations(
    ctx: &CliContext,
    repo: &str,
    git_username: &str,
    git_token: &str,
    output_json: bool,
) -> Result<()> {
    let project_id = project_id(ctx)?;
    let response = artifact_storage_client(ctx)?
        .list_operations_with_credential(&project_id, repo, git_username, git_token)
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    if output_json {
        print_json(&response)?;
        return Ok(());
    }
    print_operations_table(&response);
    Ok(())
}

pub(crate) fn artifact_storage_client(ctx: &CliContext) -> Result<ArtifactStorageClient> {
    let token = ctx.bearer_token()?;
    let mut builder = ClientBuilder::new(&ctx.api_url).bearer_token(&token);
    let use_scope_headers = ctx.personal_access_token.is_some() && ctx.api_key.is_none();
    if use_scope_headers
        && let (Some(organization_id), Some(project_id)) =
            (ctx.effective_organization_id(), ctx.effective_project_id())
    {
        builder = builder.scope(&organization_id, &project_id);
    }
    let sdk = Sdk::with_client_builder(builder)?;
    sdk.artifact_storage().map_err(Into::into)
}

pub(crate) fn project_id(ctx: &CliContext) -> Result<String> {
    ctx.effective_project_id()
        .ok_or_else(|| CliError::auth("missing project ID; run `tl init`"))
}

fn print_field(label: &str, value: &str) {
    println!("{} {}", style(format!("{label}:")).dim(), value);
}

fn print_repos_table(response: &ListReposResponse) {
    if response.repos.is_empty() {
        println!("no repos found");
        return;
    }
    let mut table = new_table(&["Name", "Default Branch", "Status"]);
    for repo in &response.repos {
        table.add_row(vec![
            Cell::new(&repo.name),
            Cell::new(&repo.default_branch),
            Cell::new(&repo.status),
        ]);
    }
    println!("{table}");
    println!("{} repos", response.repos.len());
}

fn print_branches_table(response: &ListBranchesResponse) {
    if response.branches.is_empty() {
        println!("no branches found");
        return;
    }
    let mut table = new_table(&["Branch", "OID"]);
    for branch in &response.branches {
        table.add_row(vec![Cell::new(&branch.name), Cell::new(&branch.oid)]);
    }
    println!("{table}");
}

fn print_refs_table(response: &ListRefsResponse) {
    if response.refs.is_empty() {
        println!("no refs found");
        return;
    }
    let mut table = new_table(&["Ref", "OID"]);
    for git_ref in &response.refs {
        table.add_row(vec![Cell::new(&git_ref.name), Cell::new(&git_ref.oid)]);
    }
    println!("{table}");
}

fn print_operations_table(response: &ListOperationsResponse) {
    if response.operations.is_empty() {
        println!("no operations found");
        return;
    }
    let mut table = new_table(&["Op ID", "Kind", "Actor", "Refs", "Time"]);
    for op in &response.operations {
        table.add_row(vec![
            Cell::new(&op.op_id),
            Cell::new(&op.kind),
            Cell::new(&op.actor),
            Cell::new(op.refs.len()),
            Cell::new(op.at_secs),
        ]);
    }
    println!("{table}");
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn map_sdk_error(error: tensorlake::error::SdkError) -> CliError {
    match error {
        tensorlake::error::SdkError::Authentication(_) => {
            CliError::auth("authentication failed. set TENSORLAKE_API_KEY or run 'tl login'.")
        }
        tensorlake::error::SdkError::Authorization(_) => CliError::auth(
            "permission denied. set TENSORLAKE_API_KEY with required permissions, or run 'tl init'.",
        ),
        tensorlake::error::SdkError::ServerError { status, message } => {
            if status == reqwest::StatusCode::CONFLICT {
                CliError::usage(parse_remote_message(&message))
            } else if status == reqwest::StatusCode::NOT_FOUND {
                CliError::usage(parse_remote_message(&message))
            } else {
                CliError::from(tensorlake::error::SdkError::ServerError { status, message })
            }
        }
        other => CliError::from(other),
    }
}

fn parse_remote_message(raw: &str) -> String {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|body| {
            body.get("message")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| raw.to_string())
}

/// `tl git push` — resumable chunked push of local files as one commit, no local git required.
/// Retrying after any failure is safe and cheap: the client re-negotiates and uploads only what
/// the server still lacks.
pub async fn push(
    ctx: &CliContext,
    repo: &str,
    branch: &str,
    message: &str,
    expect_oid: Option<String>,
    paths: Vec<std::path::PathBuf>,
    output_json: bool,
) -> Result<()> {
    use tensorlake::artifact_storage::ingest::{PushEvent, PushOptions};

    let project_id = project_id(ctx)?;
    let client = artifact_storage_client(ctx)?;
    // Same dev affordance as `tl fs` (FsSession::open): a pre-provisioned git credential
    // skips platform minting, e.g. against a local artifact-storage server in open-auth mode.
    let credential = if let Ok(token) = std::env::var("TENSORLAKE_GIT_TOKEN") {
        tensorlake::artifact_storage::models::GitCredential {
            token,
            token_type: "bearer".to_string(),
            expires_at: String::new(),
            git_username: std::env::var("TENSORLAKE_GIT_USERNAME")
                .unwrap_or_else(|_| "t".to_string()),
            repo_pattern: "*".to_string(),
            scopes: Vec::new(),
        }
    } else {
        client
            .mint_token_for_repo(&project_id, Some(repo))
            .await?
            .into_inner()
    };

    // Collect files: directories recurse; repo paths are relative to the CWD (or to the
    // directory itself for its children), normalized to forward slashes.
    let mut files = Vec::new();
    for path in &paths {
        collect_push_files(path, path, &mut files)?;
    }
    if files.is_empty() {
        return Err(crate::error::CliError::Usage(
            "no files found under the given paths".to_string(),
        ));
    }

    let bar = indicatif::ProgressBar::new_spinner();
    bar.enable_steady_tick(std::time::Duration::from_millis(120));
    bar.set_message(format!("hashing {} files...", files.len()));
    let bar_for_events = bar.clone();
    let opts = PushOptions {
        branch: branch.to_string(),
        message: message.to_string(),
        base: None,
        expect_oid,
        progress: Some(std::sync::Arc::new(move |ev: PushEvent| {
            use indicatif::HumanBytes;
            match ev {
                PushEvent::Chunking {
                    files_done,
                    files_total,
                    bytes_hashed,
                } => bar_for_events.set_message(format!(
                    "hashing {files_done}/{files_total} files ({})...",
                    HumanBytes(bytes_hashed)
                )),
                PushEvent::Hashed {
                    files,
                    chunks,
                    bytes,
                } => bar_for_events.set_message(format!(
                    "hashed {files} files ({chunks} chunks, {}); asking the server what it already has...",
                    HumanBytes(bytes)
                )),
                PushEvent::Negotiated { missing, total } => bar_for_events.set_message(format!(
                    "server lacks {missing} of {total} chunks; uploading..."
                )),
                PushEvent::UploadedBatch { chunks, bytes } => bar_for_events.set_message(format!(
                    "uploaded {chunks} chunks ({})...",
                    HumanBytes(bytes)
                )),
                PushEvent::Committing { files } => bar_for_events.set_message(format!(
                    "all bytes on the server; committing {files} files (server builds the tree)..."
                )),
                PushEvent::CommitProgress { phase, done, total } => {
                    if total > 0 {
                        bar_for_events.set_message(format!(
                            "committing: {phase} {done}/{total} chunks..."
                        ))
                    } else {
                        bar_for_events.set_message(format!("committing: {phase}..."))
                    }
                }
                PushEvent::Committed { ref_name, .. } => {
                    bar_for_events.set_message(format!("committed to {ref_name}"))
                }
            }
        })),
        ..Default::default()
    };
    let report = client
        .push_files(
            &project_id,
            repo,
            &credential.git_username,
            &credential.token,
            files,
            opts,
        )
        .await?
        .into_inner();
    bar.finish_and_clear();

    if output_json {
        println!(
            "{}",
            serde_json::json!({
                "commit": report.commit,
                "tree": report.tree,
                "ref": report.ref_name,
                "created": report.created,
                "files": report.files,
                "bytes_total": report.bytes_total,
                "chunks_total": report.chunks_total,
                "chunks_uploaded": report.chunks_uploaded,
                "bytes_uploaded": report.bytes_uploaded,
            })
        );
    } else {
        let deduped = report.chunks_total - report.chunks_uploaded;
        println!(
            "{} {} -> {} ({} files, {} of {} chunks uploaded, {} deduplicated, {} bytes on the wire)",
            console::style("pushed").green().bold(),
            report.commit,
            report.ref_name,
            report.files,
            report.chunks_uploaded,
            report.chunks_total,
            deduped,
            report.bytes_uploaded,
        );
    }
    Ok(())
}

fn collect_push_files(
    root: &std::path::Path,
    path: &std::path::Path,
    out: &mut Vec<tensorlake::artifact_storage::ingest::PushFile>,
) -> Result<()> {
    use tensorlake::artifact_storage::ingest::{PushFile, PushSource};
    let meta = std::fs::metadata(path)?;
    if meta.is_dir() {
        let mut entries: Vec<_> = std::fs::read_dir(path)?.collect::<std::io::Result<_>>()?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let name = entry.file_name();
            // Never traverse into VCS metadata.
            if name == ".git" {
                continue;
            }
            collect_push_files(root, &entry.path(), out)?;
        }
        return Ok(());
    }
    let repo_path = repo_path_for(root, path);
    if repo_path.is_empty() {
        return Ok(());
    }
    out.push(PushFile {
        mode: None,
        delete: false,
        repo_path,
        source: PushSource::Path(path.to_path_buf()),
    });
    Ok(())
}

/// Repo path for a file under a pushed root: relative to the root's parent (so pushing
/// `somedir` prefixes its name), built from normal components only. Pushing `.` used to
/// yield `./arch/boot.c` — the server rejects `.`/`..`/empty components at commit time,
/// after every chunk was already uploaded, so this must be clean by construction.
fn repo_path_for(root: &std::path::Path, path: &std::path::Path) -> String {
    path.strip_prefix(root.parent().unwrap_or(root))
        .unwrap_or(path)
        .components()
        .filter_map(|c| match c {
            std::path::Component::Normal(part) => Some(part.to_string_lossy()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repo_paths_from_dot_have_no_dot_components() {
        let root = std::path::Path::new(".");
        assert_eq!(
            repo_path_for(root, std::path::Path::new("./.clang-format")),
            ".clang-format"
        );
        assert_eq!(
            repo_path_for(root, std::path::Path::new("./arch/boot.c")),
            "arch/boot.c"
        );
    }

    #[test]
    fn repo_paths_from_named_dir_keep_the_dir_prefix() {
        let root = std::path::Path::new("/tmp/somedir");
        assert_eq!(
            repo_path_for(root, std::path::Path::new("/tmp/somedir/a/b.txt")),
            "somedir/a/b.txt"
        );
    }

    #[test]
    fn normalize_repo_arg_passes_through_bare_names() {
        assert_eq!(normalize_repo_arg("linux1"), "linux1");
    }

    #[test]
    fn normalize_repo_arg_extracts_repo_from_full_url() {
        assert_eq!(
            normalize_repo_arg("https://git.tensorlake.ai/project_abc/linux1"),
            "linux1"
        );
        assert_eq!(
            normalize_repo_arg("https://git.tensorlake.ai/project_abc/linux1.git"),
            "linux1"
        );
        assert_eq!(
            normalize_repo_arg("http://localhost:8080/demo/myrepo/"),
            "myrepo"
        );
    }

    #[test]
    fn normalize_repo_arg_ignores_non_http_schemes() {
        assert_eq!(
            normalize_repo_arg("git@github.com:org/repo.git"),
            "git@github.com:org/repo.git"
        );
    }
}
