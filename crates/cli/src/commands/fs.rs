//! Thin public CLI adapter for the private filesystem client engine.
//!
//! Command parsing and host authentication remain in this repository. Official builds supply the
//! implementation through the private `gsvc-fs-client` crate.

use std::path::{Path, PathBuf};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};

fn reject_background_mount_in_one_shot_exec(foreground: bool, surface: &str) -> Result<()> {
    validate_mount_process_mode(
        foreground,
        std::env::var_os(crate::commands::sbx::exec::SANDBOX_EXEC_MODE_ENV).as_deref(),
        surface,
    )
}

fn validate_mount_process_mode(
    foreground: bool,
    process_mode: Option<&std::ffi::OsStr>,
    surface: &str,
) -> Result<()> {
    if !foreground
        && process_mode
            == Some(std::ffi::OsStr::new(
                crate::commands::sbx::exec::SANDBOX_EXEC_MODE_ONE_SHOT,
            ))
    {
        return Err(CliError::usage(format!(
            "`tl {surface} mount` cannot background its daemon from inside a sandbox process \
             unit: the sandbox runtime reaps that unit's cgroup when its leader exits. Launch the \
             mount directly from the controller instead (`tl sbx exec <sandbox> -- tl {surface} \
             mount ...`); the outer CLI will keep it as a sandbox-owned process."
        )));
    }
    Ok(())
}

fn private_context(ctx: &CliContext) -> gsvc_fs_client::CliContext {
    gsvc_fs_client::CliContext {
        api_url: ctx.api_url.clone(),
        cloud_url: ctx.cloud_url.clone(),
        namespace: ctx.namespace.clone(),
        api_key: ctx.api_key.clone(),
        personal_access_token: ctx.personal_access_token.clone(),
        organization_id: ctx.organization_id.clone(),
        project_id: ctx.project_id.clone(),
        debug: ctx.debug,
        trace_id: ctx.trace_id.clone(),
    }
}

fn map<T>(result: gsvc_fs_client::Result<T>) -> Result<T> {
    result.map_err(Into::into)
}

pub mod daemon {
    use std::path::Path;

    use crate::auth::context::CliContext;
    use crate::error::Result;

    pub async fn run(ctx: &CliContext, state_dir: &Path, log_level: &str) -> Result<()> {
        gsvc_fs_client::daemon::run(&super::private_context(ctx), state_dir, log_level)
            .await
            .map_err(Into::into)
    }
}

pub async fn run_macos_kernel_refresh_helper(
    state_dir: &Path,
    through: &str,
    batch: u64,
) -> Result<()> {
    map(gsvc_fs_client::run_macos_kernel_refresh_helper(state_dir, through, batch).await)
}

pub async fn setup(from: Option<&str>, check_only: bool) -> Result<()> {
    map(gsvc_fs_client::setup(from, check_only).await)
}

pub fn require_native_filesystem_attachment(path: &Path) -> Result<()> {
    map(gsvc_fs_client::require_native_filesystem_attachment(path))
}

pub fn require_repository_mount_attachment(path: &Path) -> Result<()> {
    map(gsvc_fs_client::require_repository_mount_attachment(path))
}

pub fn resolve_mount_path(path: Option<PathBuf>) -> Result<PathBuf> {
    map(gsvc_fs_client::resolve_mount_path(path))
}

pub fn positional_is_mount_path(path: &Path) -> Result<bool> {
    map(gsvc_fs_client::positional_is_mount_path(path))
}

pub fn reject_mount_like_positional(value: &str, what: &str, usage: &str) -> Result<()> {
    map(gsvc_fs_client::reject_mount_like_positional(
        value, what, usage,
    ))
}

pub fn hydrate_scope_from_mount(ctx: &mut CliContext, path: &Path) -> Result<()> {
    let mut private = private_context(ctx);
    map(gsvc_fs_client::hydrate_scope_from_mount(&mut private, path))?;
    ctx.organization_id = private.organization_id;
    ctx.project_id = private.project_id;
    Ok(())
}

pub async fn create_filesystem(ctx: &CliContext, name: &str, json: bool) -> Result<()> {
    map(gsvc_fs_client::create_filesystem(&private_context(ctx), name, json).await)
}

pub async fn token(ctx: &CliContext, name: &str, json: bool) -> Result<()> {
    map(gsvc_fs_client::token(&private_context(ctx), name, json).await)
}

pub async fn ls_filesystems(ctx: &CliContext, json: bool) -> Result<()> {
    map(gsvc_fs_client::ls_filesystems(&private_context(ctx), json).await)
}

pub async fn ls(ctx: &CliContext, filesystem: Option<&str>, json: bool) -> Result<()> {
    map(gsvc_fs_client::ls(&private_context(ctx), filesystem, json).await)
}

pub async fn rm_filesystem(ctx: &CliContext, name: &str, force: bool) -> Result<()> {
    map(gsvc_fs_client::rm_filesystem(&private_context(ctx), name, force).await)
}

pub async fn push_dir(
    ctx: &CliContext,
    dir: &Path,
    name: &str,
    message: Option<&str>,
) -> Result<()> {
    map(gsvc_fs_client::push_dir(&private_context(ctx), dir, name, message).await)
}

pub async fn history(
    ctx: &CliContext,
    target: Option<&str>,
    limit: usize,
    json: bool,
) -> Result<()> {
    map(gsvc_fs_client::history(&private_context(ctx), target, limit, json).await)
}

pub async fn delete_snapshot(ctx: &CliContext, filesystem: &str, version: &str) -> Result<()> {
    map(gsvc_fs_client::delete_snapshot(&private_context(ctx), filesystem, version).await)
}

pub async fn mount_filesystem(
    ctx: &CliContext,
    target: &str,
    path: &Path,
    foreground: bool,
    trace_ops: bool,
    log_level: &str,
) -> Result<()> {
    reject_background_mount_in_one_shot_exec(foreground, "fs")?;
    map(gsvc_fs_client::mount_filesystem(
        &private_context(ctx),
        target,
        path,
        foreground,
        trace_ops,
        log_level,
    )
    .await)
}

#[allow(clippy::too_many_arguments)]
pub async fn mount_repo(
    ctx: &CliContext,
    target: &str,
    workspace: Option<&str>,
    path: &Path,
    publish: bool,
    foreground: bool,
    trace_ops: bool,
    log_level: &str,
) -> Result<()> {
    reject_background_mount_in_one_shot_exec(foreground, "git")?;
    map(gsvc_fs_client::mount_repo(
        &private_context(ctx),
        target,
        workspace,
        path,
        publish,
        foreground,
        trace_ops,
        log_level,
    )
    .await)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use super::validate_mount_process_mode;

    #[test]
    fn background_mount_fails_closed_inside_one_shot_sandbox_exec() {
        let error = validate_mount_process_mode(false, Some(OsStr::new("one-shot")), "fs")
            .expect_err("background daemon would be reaped with the exec cgroup");

        let message = error.to_string();
        assert!(message.contains("cannot background its daemon"));
        assert!(message.contains("tl sbx exec <sandbox>"));
    }

    #[test]
    fn foreground_or_non_sandbox_mount_keeps_existing_behavior() {
        validate_mount_process_mode(true, Some(OsStr::new("one-shot")), "fs").unwrap();
        validate_mount_process_mode(false, None, "fs").unwrap();
        validate_mount_process_mode(false, Some(OsStr::new("detached")), "fs").unwrap();
    }
}

pub async fn snapshot(ctx: &CliContext, path: &Path, message: Option<&str>) -> Result<()> {
    map(gsvc_fs_client::snapshot(&private_context(ctx), path, message).await)
}

pub async fn status(ctx: &CliContext, path: &Path, json: bool) -> Result<()> {
    map(gsvc_fs_client::status(&private_context(ctx), path, json).await)
}

pub async fn doctor(path: &Path, json: bool) -> Result<()> {
    map(gsvc_fs_client::doctor(path, json).await)
}

pub async fn unmount(ctx: &CliContext, path: &Path, delete: bool, discard: bool) -> Result<()> {
    map(gsvc_fs_client::unmount(&private_context(ctx), path, delete, discard).await)
}

pub async fn git_sync(ctx: &CliContext, path: &Path, target: Option<&str>) -> Result<()> {
    map(gsvc_fs_client::git_sync(&private_context(ctx), path, target).await)
}

pub async fn git_rebase(
    ctx: &CliContext,
    path: &Path,
    target: &str,
    fail_on_conflict: bool,
    message: Option<&str>,
) -> Result<()> {
    map(gsvc_fs_client::git_rebase(
        &private_context(ctx),
        path,
        target,
        fail_on_conflict,
        message,
    )
    .await)
}

pub async fn promote(
    ctx: &CliContext,
    path: &Path,
    branch: &str,
    full_history: bool,
    merge: bool,
    message: Option<&str>,
) -> Result<()> {
    map(gsvc_fs_client::promote(
        &private_context(ctx),
        path,
        branch,
        full_history,
        merge,
        message,
    )
    .await)
}
