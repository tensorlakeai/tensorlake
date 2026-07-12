// The fast-clone engine (`tl git clone`), backed by the private gsvc-codec packfile codec —
// compiled only into official `--features git-clone` builds. See crates/cli/Cargo.toml and
// crates/VENDORED_FROM.
#[cfg(feature = "git-clone")]
pub mod fastclone;

// `tl git setup` and the git credential helper it registers, which together make plain
// `git push`/`git pull` work against artifact-storage repos.
pub mod setup;

use std::path::PathBuf;

use comfy_table::Cell;
use console::style;
use tensorlake::artifact_storage::ArtifactStorageClient;
use tensorlake::artifact_storage::merge::MergeRequest;
use tensorlake::artifact_storage::models::{
    ListBranchesResponse, ListOperationsResponse, ListRefsResponse, ListReposResponse,
};
use tensorlake::{ClientBuilder, Sdk};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

/// Parse a human cache-size argument (`512MB`, `2GiB`, `1073741824`, ...). Lives here — not in
/// the feature-gated fast-clone module — because the CLI argument definition needs it even in
/// builds without the fast-clone engine.
pub fn parse_cache_max_bytes(value: &str) -> anyhow::Result<u64> {
    use anyhow::Context as _;
    let raw = value.trim();
    if raw.is_empty() {
        anyhow::bail!("cache size cannot be empty");
    }
    let lower = raw.to_ascii_lowercase();
    let suffixes = [
        ("tib", 1024_u64.pow(4)),
        ("tb", 1024_u64.pow(4)),
        ("t", 1024_u64.pow(4)),
        ("gib", 1024_u64.pow(3)),
        ("gb", 1024_u64.pow(3)),
        ("g", 1024_u64.pow(3)),
        ("mib", 1024_u64.pow(2)),
        ("mb", 1024_u64.pow(2)),
        ("m", 1024_u64.pow(2)),
        ("kib", 1024),
        ("kb", 1024),
        ("k", 1024),
        ("b", 1),
    ];
    let (digits, multiplier) = suffixes
        .iter()
        .find_map(|(suffix, multiplier)| {
            lower
                .strip_suffix(suffix)
                .map(|digits| (digits.trim(), *multiplier))
        })
        .unwrap_or((raw, 1));
    let bytes = digits
        .parse::<u64>()
        .with_context(|| format!("invalid cache size {value:?}"))?;
    bytes
        .checked_mul(multiplier)
        .ok_or_else(|| anyhow::anyhow!("cache size {value:?} is too large"))
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
    let remote_url = client.git_repo_url(&project_id, repo);
    let branch = default_branch.unwrap_or("main");

    // The repo exists once create_repo returns; the credential is a convenience. A mint
    // failure (env-token dev setups, PATs without token scope, transient errors) must not
    // turn the command into an error — a retry would just hit "repo already exists".
    let credential = match client.mint_token_for_repo(&project_id, Some(repo)).await {
        Ok(credential) => Some(credential.into_inner()),
        Err(err) => {
            eprintln!(
                "warning: minting a push credential failed ({err}); run `tl git token {repo}` to mint one"
            );
            None
        }
    };

    if output_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "repo": repo,
                "url": remote_url,
                "credential": credential,
            }))?
        );
        return Ok(());
    }

    println!("created {remote_url}");
    println!();
    println!("{}", style("Push to this repo").bold().green());
    println!("  {}", style(format!("tl git push {repo} {branch}")).cyan());
    println!("  (mints a fresh short-lived credential automatically on every push)");
    if let Some(credential) = credential {
        println!();
        println!(
            "{}",
            style("Credential for CI or direct HTTP access")
                .bold()
                .green()
        );
        println!("  {} {}", style("username:").dim(), credential.git_username);
        println!(
            "  {} {}",
            style("password:").dim(),
            style(&credential.token).yellow()
        );
        println!("  {} {}", style("expires:").dim(), credential.expires_at);
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
#[cfg(any(feature = "git-clone", test))]
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

/// Fast clone without the engine: this build was made from the public source tree, which
/// carries only a resolution placeholder for the private gsvc-codec. Point the user at the
/// official binary instead of failing cryptically.
#[cfg(not(feature = "git-clone"))]
pub async fn clone_repo(
    _ctx: &CliContext,
    _repo: &str,
    _dest: Option<PathBuf>,
    _cache_dir: Option<PathBuf>,
    _cache_max_bytes: Option<u64>,
    _no_checkout: bool,
) -> Result<()> {
    Err(CliError::Other(anyhow::anyhow!(
        "this build of `tl` lacks the fast-clone engine (built without the `git-clone` \
         feature). Install the official release binary, or build with `just build-cli-full` \
         from a checkout with artifact_storage access."
    )))
}

#[cfg(feature = "git-clone")]
fn new_fastclone_spinner(message: &str) -> Option<indicatif::ProgressBar> {
    if !std::io::IsTerminal::is_terminal(&std::io::stderr()) {
        return None;
    }
    let pb = indicatif::ProgressBar::new_spinner();
    pb.set_style(
        indicatif::ProgressStyle::default_spinner()
            .template("{spinner} {msg}")
            .unwrap(),
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(80));
    Some(pb)
}

#[cfg(feature = "git-clone")]
fn fastclone_byte_progress_style() -> indicatif::ProgressStyle {
    indicatif::ProgressStyle::with_template(
        "{spinner} {bytes}/{total_bytes} ({bytes_per_sec}, eta {eta}) {msg}",
    )
    .unwrap()
}

#[cfg(feature = "git-clone")]
fn fastclone_progress(
    spinner: Option<indicatif::ProgressBar>,
) -> Option<fastclone::FastCloneProgress> {
    let pb = spinner?;
    Some(std::sync::Arc::new(move |ev| {
        use self::fastclone::FastCloneEvent;
        match ev {
            FastCloneEvent::FetchingManifest => pb.set_message("fetching clone manifest"),
            FastCloneEvent::DownloadPlan { bytes } => {
                pb.set_style(fastclone_byte_progress_style());
                pb.set_length(bytes);
                pb.set_position(0);
                pb.set_message("fetching pack artifacts");
            }
            FastCloneEvent::DownloadedBytes { bytes } => pb.inc(bytes),
            FastCloneEvent::InstallingObjects { dest } => {
                pb.finish_and_clear();
                eprintln!("installing objects into {}", dest.display());
            }
        }
    }))
}

#[cfg(feature = "git-clone")]
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

    let spinner = new_fastclone_spinner(&format!("minting git credential for {repo}"));
    let credential = client
        .git_credential_for_repo(&project_id, repo)
        .await
        .map_err(map_sdk_error)?;
    if let Some(pb) = &spinner {
        pb.set_message("fetching clone manifest");
    }

    let dest = dest.unwrap_or_else(|| fastclone::default_dest_from_url(&repo_url));
    let progress = fastclone_progress(spinner.clone());
    let opts = fastclone::FastCloneOptions {
        repo_url: repo_url.clone(),
        dest: dest.clone(),
        cache_dir,
        cache_max_bytes,
        credential: Some(fastclone::BasicAuth {
            username: credential.git_username,
            password: Some(credential.token),
        }),
        checkout: !no_checkout,
        progress,
    };
    let stats = fastclone::fast_clone(opts).await?;
    if let Some(pb) = spinner {
        pb.finish_and_clear();
    }
    // Register the credential helper in the new checkout's local config (same wiring as
    // `tl git setup`) so plain `git fetch`/`git push` there authenticate automatically once the
    // clone-time token expires. The clone itself succeeded; a config failure only warrants a
    // warning.
    if let Err(err) = setup::configure_credential_helper(ctx, &dest, client.git_base_url()) {
        eprintln!("warning: could not register the git credential helper: {err}");
    }
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
    let info = client
        .repo_info(&project_id, repo)
        .await
        .map_err(map_sdk_error)?
        .into_inner();

    if output_json {
        println!("{}", serde_json::to_string_pretty(&info)?);
        return Ok(());
    }

    println!("repo: {}", info.repo);
    println!("url: {}", info.url);
    println!("branches: {}", info.branches.len());
    if info.branches.is_empty() {
        println!("no branches found");
    } else {
        print_branches_table(&ListBranchesResponse {
            repo: info.repo.clone(),
            branches: info.branches.clone(),
        });
    }
    println!("refs: {}", info.refs.len());
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

/// Server-side three-way merge of `theirs` into `ours` (gsvc merge design §9.3). Preflight
/// never writes; commit mode CAS-advances the `ours` branch. A `fail`-policy conflict prints
/// the report and exits non-zero — nothing was published.
#[allow(clippy::too_many_arguments)]
pub async fn merge(
    ctx: &CliContext,
    repo: &str,
    ours: &str,
    theirs: &str,
    preflight: bool,
    deep: bool,
    materialize: bool,
    message: Option<&str>,
    base: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let project_id = project_id(ctx)?;
    let client = artifact_storage_client(ctx)?;
    let cred = client
        .mint_token_for_repo(&project_id, Some(repo))
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    let request = MergeRequest {
        ours: ours.to_string(),
        theirs: theirs.to_string(),
        base: base.map(str::to_string),
        deep,
        mode: (!preflight).then(|| "commit".to_string()),
        policy: materialize.then(|| "materialize".to_string()),
        message: message.map(str::to_string),
        ..Default::default()
    };
    let report = client
        .repo_merge(&project_id, repo, &cred.git_username, &cred.token, &request)
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    if output_json {
        print_json(&report)?;
        // A fail-policy conflict published nothing; the exit code says so even in JSON mode.
        if !preflight && report.commit.is_none() && !report.clean {
            return Err(CliError::usage("merge conflicts; nothing was published"));
        }
        return Ok(());
    }
    let short = |oid: &str| oid[..oid.len().min(12)].to_string();
    if let Some(b) = &report.merge_base {
        print_field("merge base", &short(b));
    } else {
        print_field("merge base", "none (unrelated histories)");
    }
    print_field("changed paths", &report.changed_paths.to_string());
    if !report.conflicts.is_empty() {
        println!(
            "{} {} conflict(s):",
            style("conflicts:").dim(),
            report.conflicts.len()
        );
        for c in &report.conflicts {
            println!(
                "  {:<14} {}{}",
                style(&c.kind).yellow(),
                c.path,
                if c.potential { " (potential)" } else { "" },
            );
        }
        if report.conflicts.iter().any(|c| c.potential) {
            println!("  (run with --deep for exact content-merge answers)");
        }
    }
    if preflight {
        if report.already_merged {
            println!("{theirs} is already merged into {ours}; a merge would change nothing.");
        } else if report.clean {
            println!(
                "Clean merge{}.",
                if report.fast_forward {
                    " (fast-forward)"
                } else {
                    ""
                }
            );
        }
        return Ok(());
    }
    match report.commit {
        Some(commit) => {
            println!(
                "Merged {theirs} into {ours} at {}{}",
                short(&commit),
                if report.fast_forwarded {
                    " (fast-forward)"
                } else if !report.clean {
                    " (conflicts materialized as diff3 markers)"
                } else {
                    ""
                },
            );
            Ok(())
        }
        None if report.already_merged => {
            println!("{theirs} is already merged into {ours}; nothing to do.");
            Ok(())
        }
        None => Err(CliError::usage(format!(
            "merge conflicts; nothing was published. Rerun with --materialize to land the conflicts as diff3 markers, or resolve on a workspace forked from {ours}.",
        ))),
    }
}

/// The structured conflict record of a materialize-policy merge commit.
pub async fn commit_conflicts(
    ctx: &CliContext,
    repo: &str,
    commit: &str,
    output_json: bool,
) -> Result<()> {
    let project_id = project_id(ctx)?;
    let client = artifact_storage_client(ctx)?;
    let cred = client
        .mint_token_for_repo(&project_id, Some(repo))
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    let record = client
        .commit_conflicts(&project_id, repo, &cred.git_username, &cred.token, commit)
        .await
        .map_err(map_sdk_error)?
        .into_inner();
    let Some(record) = record else {
        println!("no conflict record: {commit} merged cleanly (or is unknown here)");
        return Ok(());
    };
    if output_json {
        return print_json(&record);
    }
    let short = |oid: &str| oid[..oid.len().min(12)].to_string();
    print_field("ours", &short(&record.ours_commit));
    print_field("theirs", &short(&record.theirs_commit));
    if let Some(base) = &record.base_commit {
        print_field("base", &short(base));
    }
    println!(
        "{} {} path(s):",
        style("conflicts:").dim(),
        record.paths.len()
    );
    for p in &record.paths {
        println!("  {:<14} {}", style(&p.kind).yellow(), p.path);
    }
    if record.truncated_paths > 0 {
        println!(
            "  … and {} more (record truncated; the commit's marker content is complete)",
            record.truncated_paths
        );
    }
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

pub(crate) fn map_sdk_error(error: tensorlake::error::SdkError) -> CliError {
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

/// `tl git push` — resumable chunked push of the current Git worktree as one commit.
/// Retrying after any failure is safe and cheap: the client re-negotiates and uploads only what
/// the server still lacks.
pub async fn push(
    ctx: &CliContext,
    repo: &str,
    branch: &str,
    message: &str,
    expect_oid: Option<String>,
    output_json: bool,
) -> Result<()> {
    use tensorlake::artifact_storage::ingest::PushOptions;

    let root = current_git_worktree_root("tl git push")?;
    let project_id = project_id(ctx)?;
    let client = artifact_storage_client(ctx)?;
    let credential = client
        .git_credential_for_repo(&project_id, repo)
        .await
        .map_err(map_sdk_error)?;

    let bar = indicatif::ProgressBar::new_spinner();
    bar.enable_steady_tick(std::time::Duration::from_millis(120));
    bar.set_message("hashing worktree files...");
    let opts = PushOptions {
        branch: branch.to_string(),
        message: message.to_string(),
        base: None,
        expect_oid,
        progress: Some(super::push_progress_spinner(&bar)),
        ..Default::default()
    };
    let report = client
        .push_worktree(
            &project_id,
            repo,
            &credential.git_username,
            &credential.token,
            &root,
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
        // Small files skip chunk negotiation, so uploads can exceed the negotiated count;
        // saturate instead of panicking on the subtraction.
        let deduped = report.chunks_total.saturating_sub(report.chunks_uploaded);
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

fn current_git_worktree_root(command: &str) -> Result<std::path::PathBuf> {
    git_worktree_root_from(&std::env::current_dir()?, command)
}

fn git_worktree_root_from(cwd: &std::path::Path, command: &str) -> Result<std::path::PathBuf> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(cwd)
        .output()
        .map_err(|err| {
            CliError::usage(format!(
                "{command} requires Git to locate the current worktree: {err}"
            ))
        })?;

    if !output.status.success() {
        let detail = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let suffix = if detail.is_empty() {
            "not a Git repository".to_string()
        } else {
            detail
        };
        return Err(CliError::usage(format!(
            "{command} must be run inside a Git worktree ({suffix})"
        )));
    }

    let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if root.is_empty() {
        return Err(CliError::usage(
            "git rev-parse returned an empty worktree root".to_string(),
        ));
    }

    Ok(std::path::PathBuf::from(root))
}

/// `tl git commit-status <repo> <job-id>` — the out-of-band view of a detached commit
/// job's state machine, from any terminal or process.
pub async fn commit_status(ctx: &CliContext, repo: &str, job_id: &str) -> Result<()> {
    let project_id = project_id(ctx)?;
    let client = artifact_storage_client(ctx)?;
    let credential = client
        .git_credential_for_repo(&project_id, repo)
        .await
        .map_err(map_sdk_error)?;
    let job = client
        .commit_job_status(
            &project_id,
            repo,
            &credential.git_username,
            &credential.token,
            job_id,
        )
        .await?
        .into_inner();
    let state = job.state.as_str();
    match state {
        "committed" => println!(
            "{} {} -> {}",
            console::style("committed").green().bold(),
            job.commit.as_deref().unwrap_or("?"),
            job.ref_name.as_deref().unwrap_or("?"),
        ),
        "failed" => println!(
            "{} {} ({}){}",
            console::style("failed").red().bold(),
            job.error
                .as_ref()
                .map(|err| err.message.as_str())
                .unwrap_or("?"),
            job.error
                .as_ref()
                .map(|err| err.kind.as_str())
                .unwrap_or("?"),
            if job.error.as_ref().map(|err| err.retryable).unwrap_or(false) {
                " — safe to re-push: uploaded chunks are deduplicated"
            } else {
                ""
            },
        ),
        _ => {
            let phase = job.phase.as_deref().unwrap_or(state);
            if let Some(read_back) = job.read_back {
                let done = read_back.done;
                let total = read_back.total;
                let pct = if total > 0 { done * 100 / total } else { 0 };
                println!(
                    "{} {phase}: {done}/{total} chunks ({pct}%)",
                    console::style(state).yellow().bold(),
                );
            } else {
                println!("{} {phase}", console::style(state).yellow().bold());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn git_worktree_root_rejects_non_git_dir() {
        if std::process::Command::new("git")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }

        let dir = tempfile::tempdir().unwrap();
        let err = git_worktree_root_from(dir.path(), "tl git push")
            .unwrap_err()
            .to_string();
        assert!(err.contains("tl git push must be run inside a Git worktree"));
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
