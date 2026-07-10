//! `tl git setup` and the credential helper it registers.
//!
//! One command makes plain `git` work against a TensorLake repo:
//!   1. points a local remote (default `tl`) at the repo's URL, so `git push tl main` works, and
//!   2. registers `tl git credential-helper` in the repo's local git config, URL-scoped to this
//!      deployment's git host, so `git push`/`git pull` in the repo mint short-lived tokens
//!      automatically — no manual `git remote add` or `git config` steps.
//!
//! The registration is deliberately repo-local, not global: the helper command bakes in an
//! `--organization`, and different repos on the same machine can belong to different orgs.

use std::collections::HashMap;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::process::Command;

use console::style;
use tensorlake::artifact_storage::{ArtifactStorageClient, resolve_artifact_storage_url};
use tensorlake::{ClientBuilder, Sdk};

use crate::auth::context::CliContext;
use crate::config::files::{
    load_git_credential, normalize_api_url, purge_git_credentials, save_git_credential,
};
use crate::error::{CliError, Result};

const DEFAULT_API_URL: &str = "https://api.tensorlake.ai";

pub async fn run(ctx: &CliContext, repo: Option<&str>, remote: &str, create: bool) -> Result<()> {
    let root = super::git_worktree_root_from(&std::env::current_dir()?, "tl git setup")?;
    let project_id = super::project_id(ctx)?;
    let client = super::artifact_storage_client(ctx)?;

    let repo_name = match repo {
        Some(name) => name.to_string(),
        None => root
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
            .ok_or_else(|| {
                CliError::usage(
                    "cannot derive a repo name from the worktree path; pass one explicitly",
                )
            })?,
    };

    ensure_repo_exists(&client, &project_id, &repo_name, create).await?;

    let url = client.git_repo_url(&project_id, &repo_name);
    let remote = configure_remote(&root, remote, &url)?;
    configure_credential_helper(ctx, &root, client.git_base_url())?;
    smoke_test(&root, &remote, &url)?;

    println!();
    println!(
        "{} plain `git` now authenticates against {url}",
        style("configured:").green().bold()
    );
    println!("  try: git push {remote} <branch>");
    Ok(())
}

async fn ensure_repo_exists(
    client: &ArtifactStorageClient,
    project_id: &str,
    repo: &str,
    create: bool,
) -> Result<()> {
    match client.repo_info(project_id, repo).await {
        Ok(_) => Ok(()),
        Err(tensorlake::error::SdkError::ServerError { status, .. })
            if status == reqwest::StatusCode::NOT_FOUND =>
        {
            if !create {
                return Err(CliError::usage(format!(
                    "repo '{repo}' does not exist in this project; rerun with --create, or run \
                     `tl git create {repo}` first"
                )));
            }
            client
                .create_repo(project_id, repo, None)
                .await
                .map_err(super::map_sdk_error)?;
            println!("created repo {repo}");
            Ok(())
        }
        Err(err) => Err(super::map_sdk_error(err)),
    }
}

/// Point a remote at the repo, returning the remote name the rest of setup should use. If any
/// existing remote already has the URL (e.g. the `origin` a `tl git clone` leaves behind), reuse
/// it instead of adding a second name for the same repo.
fn configure_remote(root: &Path, remote: &str, url: &str) -> Result<String> {
    if let Some(existing) = remote_with_url(root, url)? {
        println!("remote '{existing}' already points at {url}");
        return Ok(existing);
    }
    if git_stdout(Some(root), &["remote", "get-url", remote]).is_ok() {
        git_ok(Some(root), &["remote", "set-url", remote, url])?;
        println!("updated remote '{remote}' -> {url}");
    } else {
        git_ok(Some(root), &["remote", "add", remote, url])?;
        println!("added remote '{remote}' -> {url}");
    }
    Ok(remote.to_string())
}

fn remote_with_url(root: &Path, url: &str) -> Result<Option<String>> {
    let names = git_stdout(Some(root), &["remote"])?;
    for name in names.lines().map(str::trim).filter(|name| !name.is_empty()) {
        if let Ok(existing) = git_stdout(Some(root), &["remote", "get-url", name])
            && same_repo_url(&existing, url)
        {
            return Ok(Some(name.to_string()));
        }
    }
    Ok(None)
}

/// URL equality up to the decorations git tolerates: a trailing slash or `.git` suffix.
fn same_repo_url(a: &str, b: &str) -> bool {
    fn normalize(s: &str) -> &str {
        s.trim_end_matches('/').trim_end_matches(".git")
    }
    normalize(a) == normalize(b)
}

pub(crate) fn configure_credential_helper(
    ctx: &CliContext,
    root: &Path,
    git_base_url: &str,
) -> Result<()> {
    let helper_key = format!("credential.{git_base_url}.helper");
    let path_key = format!("credential.{git_base_url}.usehttppath");
    // With useHttpPath the helper receives the URL path (`<project>/<repo>`), so it can mint
    // repo-scoped tokens and identify the project without TensorLake config discovery.
    git_ok(Some(root), &["config", "--local", &path_key, "true"])?;
    // The leading empty value resets any helper list inherited from broader config (e.g. the
    // system-wide osxkeychain on macOS), which would otherwise store our short-lived tokens and
    // replay them after expiry.
    git_ok(
        Some(root),
        &["config", "--local", "--replace-all", &helper_key, ""],
    )?;
    let helper_value = format!("!{}", credential_helper_invocation(ctx));
    git_ok(
        Some(root),
        &["config", "--local", "--add", &helper_key, &helper_value],
    )?;
    println!("registered credential helper for {git_base_url} (repo-local git config)");
    Ok(())
}

/// The command line git runs for credential lookups. Deployment and organization are baked in so
/// the helper does not depend on cwd-relative config discovery: git invokes it wherever the user
/// happens to run git, usually nowhere near a `.tensorlake/config.toml`. Baking the org is also
/// why the registration is repo-local — each repo carries the org it belongs to. The project is
/// not baked in; it comes from the URL path.
fn credential_helper_invocation(ctx: &CliContext) -> String {
    let mut invocation = tl_command();
    if ctx.api_url != normalize_api_url(DEFAULT_API_URL) {
        invocation.push_str(&format!(" --api-url {}", ctx.api_url));
    }
    if let Some(organization_id) = ctx.effective_organization_id() {
        invocation.push_str(&format!(" --organization {organization_id}"));
    }
    invocation.push_str(" git credential-helper");
    invocation
}

fn tl_command() -> String {
    // Prefer the bare name: it survives upgrades that move the binary. Fall back to the running
    // executable's path for dev builds that aren't on PATH.
    if find_on_path("tl").is_some() {
        return "tl".to_string();
    }
    match std::env::current_exe() {
        Ok(exe) => {
            let path = exe.display().to_string();
            if path.contains(' ') {
                format!("\"{path}\"")
            } else {
                path
            }
        }
        Err(_) => "tl".to_string(),
    }
}

fn find_on_path(name: &str) -> Option<PathBuf> {
    let file = if cfg!(windows) {
        format!("{name}.exe")
    } else {
        name.to_string()
    };
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join(&file))
        .find(|candidate| candidate.is_file())
}

/// End-to-end proof that the wiring works: `git ls-remote` exercises the remote URL, the helper
/// registration, token minting, and server auth in one shot.
fn smoke_test(root: &Path, remote: &str, url: &str) -> Result<()> {
    let output = git(Some(root), &["ls-remote", "--heads", remote])?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(CliError::usage(format!(
            "setup wrote the config, but `git ls-remote {remote}` failed: {stderr}\n\
             check that `tl` is on git's PATH and that you are logged in (`tl login`)"
        )));
    }
    println!("verified: git can reach {url}");
    Ok(())
}

fn git(root: Option<&Path>, args: &[&str]) -> Result<std::process::Output> {
    let mut cmd = Command::new("git");
    if let Some(dir) = root {
        cmd.current_dir(dir);
    }
    cmd.args(args)
        .output()
        .map_err(|err| CliError::usage(format!("failed to run git: {err}")))
}

fn git_ok(root: Option<&Path>, args: &[&str]) -> Result<()> {
    let output = git(root, args)?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(CliError::usage(format!(
            "`git {}` failed: {stderr}",
            args.join(" ")
        )));
    }
    Ok(())
}

fn git_stdout(root: Option<&Path>, args: &[&str]) -> Result<String> {
    let output = git(root, args)?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(CliError::usage(format!(
            "`git {}` failed: {stderr}",
            args.join(" ")
        )));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

// ---------------------------------------------------------------------------------------------
// The credential helper: `tl git credential-helper <get|store|erase>`, invoked by git itself.
// Git speaks its credential protocol over stdio — `key=value` lines terminated by a blank line —
// so stdout must carry only protocol lines. Diagnostics go to stderr, and every soft failure
// prints nothing and exits 0 so git can fall through to its other helpers or a prompt instead of
// hard-failing the fetch.
// ---------------------------------------------------------------------------------------------

pub async fn credential_helper(ctx: &CliContext, operation: &str) -> Result<()> {
    // Always drain the request git writes, even for ops we ignore, so git never sees EPIPE.
    let attrs = read_attrs(&mut std::io::stdin().lock())?;
    match operation {
        "get" => credential_get(ctx, &attrs).await,
        // Git calls erase after the server rejects a credential; drop the cache so the next get
        // re-mints instead of replaying a revoked token (same recovery as the `tl fs` stack).
        "erase" => {
            purge_git_credentials();
            Ok(())
        }
        // store (and any future op): nothing to do — tokens are minted, never git-provided.
        _ => Ok(()),
    }
}

async fn credential_get(ctx: &CliContext, attrs: &HashMap<String, String>) -> Result<()> {
    let Some(expected_host) = git_host_for_api(&ctx.api_url) else {
        return Ok(());
    };
    match attrs.get("host") {
        Some(host) if host.eq_ignore_ascii_case(&expected_host) => {}
        // Not our host (helper misconfigured for a broader scope): stay silent so other
        // helpers can answer.
        _ => return Ok(()),
    }

    // Local/self-hosted development override, same precedence as the rest of the git stack.
    if let Some(credential) = ArtifactStorageClient::git_credential_from_env() {
        emit(&credential.git_username, &credential.token, None);
        return Ok(());
    }

    let Some((project_id, repo)) = project_and_repo(attrs.get("path").map(String::as_str), ctx)
    else {
        eprintln!(
            "tl: cannot determine the TensorLake project for this URL; run `tl git setup` in the repo"
        );
        return Ok(());
    };
    let scope = repo.as_deref().unwrap_or("*");

    if let Some((username, token, expires_at)) =
        load_git_credential(&ctx.api_url, &project_id, scope)
    {
        emit(&username, &token, Some(&expires_at));
        return Ok(());
    }

    let Ok(bearer) = ctx.bearer_token() else {
        eprintln!("tl: not logged in; run `tl login` (or set TENSORLAKE_API_KEY) and retry");
        return Ok(());
    };
    // Scope headers mirror `artifact_storage_client`, except the project comes from the URL path:
    // the same host serves every project, so the setup-time project may not be this repo's.
    let mut builder = ClientBuilder::new(&ctx.api_url).bearer_token(&bearer);
    if ctx.personal_access_token.is_some() && ctx.api_key.is_none() {
        let Some(organization_id) = ctx.effective_organization_id() else {
            eprintln!("tl: no organization configured; re-run `tl git setup` or `tl init`");
            return Ok(());
        };
        builder = builder.scope(&organization_id, &project_id);
    }
    let client = match Sdk::with_client_builder(builder).and_then(|sdk| sdk.artifact_storage()) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("tl: {err}");
            return Ok(());
        }
    };
    let credential = match client
        .mint_token_for_repo(&project_id, repo.as_deref())
        .await
    {
        Ok(credential) => credential.into_inner(),
        Err(err) => {
            eprintln!("tl: could not mint a git credential: {err}");
            return Ok(());
        }
    };
    if let Err(err) = save_git_credential(
        &ctx.api_url,
        &project_id,
        scope,
        &credential.git_username,
        &credential.token,
        &credential.expires_at,
    ) {
        eprintln!("tl: warning: could not cache the git credential: {err}");
    }
    emit(
        &credential.git_username,
        &credential.token,
        Some(&credential.expires_at),
    );
    Ok(())
}

fn read_attrs(input: &mut impl BufRead) -> Result<HashMap<String, String>> {
    let mut attrs = HashMap::new();
    let mut line = String::new();
    loop {
        line.clear();
        if input.read_line(&mut line)? == 0 {
            break;
        }
        let trimmed = line.trim_end_matches('\n');
        if trimmed.is_empty() {
            break;
        }
        if let Some((key, value)) = trimmed.split_once('=') {
            attrs.insert(key.to_string(), value.to_string());
        }
    }
    Ok(attrs)
}

fn emit(username: &str, password: &str, expires_at: Option<&str>) {
    println!("username={username}");
    println!("password={password}");
    // git >= 2.41 evicts credentials from downstream caches past this instant; older versions
    // ignore unknown attributes.
    if let Some(expiry) = expires_at.and_then(|e| chrono::DateTime::parse_from_rfc3339(e).ok()) {
        println!("password_expiry_utc={}", expiry.timestamp());
    }
}

/// The host[:port] git presents when asking for this deployment's credentials.
fn git_host_for_api(api_url: &str) -> Option<String> {
    let url = url::Url::parse(&resolve_artifact_storage_url(api_url)).ok()?;
    let host = url.host_str()?.to_string();
    Some(match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    })
}

/// Project and repo from the credential context path (`<project>/<repo>`, present because setup
/// enables useHttpPath). A bare-host context falls back to the resolved project and a
/// project-wide token.
fn project_and_repo(path: Option<&str>, ctx: &CliContext) -> Option<(String, Option<String>)> {
    if let Some(path) = path {
        let mut segments = path.trim_matches('/').split('/').filter(|s| !s.is_empty());
        if let Some(project) = segments.next() {
            let repo = segments
                .next()
                .map(|segment| decode(segment.strip_suffix(".git").unwrap_or(segment)));
            return Some((decode(project), repo));
        }
    }
    ctx.effective_project_id().map(|project| (project, None))
}

fn decode(segment: &str) -> String {
    urlencoding::decode(segment)
        .map(|decoded| decoded.into_owned())
        .unwrap_or_else(|_| segment.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::resolver::ResolvedConfig;

    fn test_ctx(project_id: Option<&str>) -> CliContext {
        CliContext::from_resolved(ResolvedConfig {
            api_url: "https://api.tensorlake.ai".to_string(),
            cloud_url: "https://cloud.tensorlake.ai".to_string(),
            namespace: "default".to_string(),
            api_key: None,
            personal_access_token: None,
            organization_id: None,
            project_id: project_id.map(str::to_string),
            debug: false,
        })
    }

    #[test]
    fn reads_credential_attrs_until_blank_line() {
        let mut input =
            "protocol=https\nhost=git.tensorlake.ai\npath=project_1/demo\n\nignored=yes\n"
                .as_bytes();
        let attrs = read_attrs(&mut input).unwrap();
        assert_eq!(attrs.get("protocol").unwrap(), "https");
        assert_eq!(attrs.get("host").unwrap(), "git.tensorlake.ai");
        assert_eq!(attrs.get("path").unwrap(), "project_1/demo");
        assert!(!attrs.contains_key("ignored"));
    }

    #[test]
    fn derives_project_and_repo_from_path() {
        let ctx = test_ctx(None);
        assert_eq!(
            project_and_repo(Some("project_1/demo"), &ctx),
            Some(("project_1".to_string(), Some("demo".to_string())))
        );
        assert_eq!(
            project_and_repo(Some("/project_1/demo.git"), &ctx),
            Some(("project_1".to_string(), Some("demo".to_string())))
        );
        assert_eq!(
            project_and_repo(Some("project_1"), &ctx),
            Some(("project_1".to_string(), None))
        );
    }

    #[test]
    fn falls_back_to_configured_project_without_path() {
        let ctx = test_ctx(Some("project_9"));
        assert_eq!(
            project_and_repo(None, &ctx),
            Some(("project_9".to_string(), None))
        );
        assert_eq!(project_and_repo(None, &test_ctx(None)), None);
    }

    #[test]
    fn same_repo_url_ignores_trailing_decorations() {
        assert!(same_repo_url(
            "https://git.tensorlake.ai/project_1/demo",
            "https://git.tensorlake.ai/project_1/demo.git"
        ));
        assert!(same_repo_url(
            "https://git.tensorlake.ai/project_1/demo/",
            "https://git.tensorlake.ai/project_1/demo"
        ));
        assert!(!same_repo_url(
            "https://git.tensorlake.ai/project_1/demo",
            "https://git.tensorlake.ai/project_1/demo2"
        ));
    }

    #[test]
    fn reuses_existing_remote_with_same_url() {
        if std::process::Command::new("git")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git_ok(Some(root), &["init", "-q"]).unwrap();
        let url = "https://git.tensorlake.ai/project_1/demo";
        git_ok(Some(root), &["remote", "add", "origin", url]).unwrap();

        // A remote with this URL already exists (e.g. from `tl git clone`): reuse it and
        // don't add a second name for the same repo.
        assert_eq!(configure_remote(root, "tl", url).unwrap(), "origin");
        assert!(git_stdout(Some(root), &["remote", "get-url", "tl"]).is_err());

        // No remote has this URL: the named remote is added.
        let other = "https://git.tensorlake.ai/project_1/other";
        assert_eq!(configure_remote(root, "tl", other).unwrap(), "tl");
        assert_eq!(
            git_stdout(Some(root), &["remote", "get-url", "tl"]).unwrap(),
            other
        );

        // The named remote exists but points elsewhere: its URL is updated in place.
        let moved = "https://git.tensorlake.ai/project_1/moved";
        assert_eq!(configure_remote(root, "tl", moved).unwrap(), "tl");
        assert_eq!(
            git_stdout(Some(root), &["remote", "get-url", "tl"]).unwrap(),
            moved
        );
    }

    #[test]
    fn registers_repo_local_credential_helper() {
        if std::process::Command::new("git")
            .arg("--version")
            .output()
            .is_err()
        {
            return;
        }
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        git_ok(Some(root), &["init", "-q"]).unwrap();

        let mut ctx = test_ctx(None);
        ctx.organization_id = Some("org_1".to_string());
        configure_credential_helper(&ctx, root, "https://git.tensorlake.ai").unwrap();

        assert_eq!(
            git_stdout(
                Some(root),
                &[
                    "config",
                    "--local",
                    "credential.https://git.tensorlake.ai.usehttppath"
                ]
            )
            .unwrap(),
            "true"
        );
        // Two helper values: the empty reset entry, then the tl invocation with the org baked in.
        let output = git(
            Some(root),
            &[
                "config",
                "--local",
                "--get-all",
                "credential.https://git.tensorlake.ai.helper",
            ],
        )
        .unwrap();
        let raw = String::from_utf8_lossy(&output.stdout);
        let values: Vec<&str> = raw.lines().collect();
        assert_eq!(values.len(), 2, "expected reset entry + helper: {values:?}");
        assert_eq!(values[0], "");
        assert!(
            values[1].starts_with('!'),
            "not a shell helper: {}",
            values[1]
        );
        assert!(values[1].contains("--organization org_1"));
        assert!(values[1].ends_with(" git credential-helper"));

        // Re-running must replace, not accumulate.
        configure_credential_helper(&ctx, root, "https://git.tensorlake.ai").unwrap();
        let output = git(
            Some(root),
            &[
                "config",
                "--local",
                "--get-all",
                "credential.https://git.tensorlake.ai.helper",
            ],
        )
        .unwrap();
        assert_eq!(String::from_utf8_lossy(&output.stdout).lines().count(), 2);
    }

    #[test]
    fn maps_api_url_to_git_host() {
        assert_eq!(
            git_host_for_api("https://api.tensorlake.ai").as_deref(),
            Some("git.tensorlake.ai")
        );
        assert_eq!(
            git_host_for_api("http://localhost:8080").as_deref(),
            Some("localhost:8080")
        );
    }
}
