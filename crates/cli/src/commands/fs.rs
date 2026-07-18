//! Thin public CLI adapter for the private filesystem client engine.
//!
//! Command parsing and host authentication remain in this repository. Official builds supply the
//! implementation through the private `gsvc-fs-client` crate.

use std::path::{Path, PathBuf};

use crate::auth::context::CliContext;
use crate::error::Result;

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

pub mod plaindir {
    use std::path::PathBuf;

    use crate::error::Result;

    pub fn binding_for_lenient(path: &std::path::Path) -> Option<(String, PathBuf)> {
        gsvc_fs_client::plaindir::binding_for_lenient(path)
    }

    pub async fn unbind(path: Option<PathBuf>) -> Result<()> {
        gsvc_fs_client::plaindir::unbind(path)
            .await
            .map_err(Into::into)
    }
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

pub async fn setup(from: Option<&str>, check_only: bool) -> Result<()> {
    map(gsvc_fs_client::setup(from, check_only).await)
}

pub fn is_tracked_directory(path: &Path) -> Result<bool> {
    map(gsvc_fs_client::is_tracked_directory(path))
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
    ro: bool,
    foreground: bool,
    trace_ops: bool,
    log_level: &str,
) -> Result<()> {
    map(gsvc_fs_client::mount_filesystem(
        &private_context(ctx),
        target,
        path,
        ro,
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
    ro: bool,
    publish: bool,
    foreground: bool,
    trace_ops: bool,
    log_level: &str,
) -> Result<()> {
    map(gsvc_fs_client::mount_repo(
        &private_context(ctx),
        target,
        workspace,
        path,
        ro,
        publish,
        foreground,
        trace_ops,
        log_level,
    )
    .await)
}

pub async fn snapshot(
    ctx: &CliContext,
    path: &Path,
    message: Option<&str>,
    clear: bool,
) -> Result<()> {
    map(gsvc_fs_client::snapshot(&private_context(ctx), path, message, clear).await)
}

pub async fn status(ctx: &CliContext, path: &Path, json: bool) -> Result<()> {
    map(gsvc_fs_client::status(&private_context(ctx), path, json).await)
}

pub async fn doctor(
    path: &Path,
    json: bool,
    repair_journal: bool,
    repair_base: Option<&str>,
) -> Result<()> {
    map(gsvc_fs_client::doctor(path, json, repair_journal, repair_base).await)
}

pub async fn restore(ctx: &CliContext, path: &Path, version: &str, discard: bool) -> Result<()> {
    map(gsvc_fs_client::restore(&private_context(ctx), path, version, discard).await)
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

pub async fn git_status(ctx: &CliContext, path: &Path, json: bool) -> Result<()> {
    map(gsvc_fs_client::git_status(&private_context(ctx), path, json).await)
}

pub async fn git_log(ctx: &CliContext, subject: Option<&str>, json: bool) -> Result<()> {
    map(gsvc_fs_client::git_log(&private_context(ctx), subject, json).await)
}

pub async fn git_smartlog(
    ctx: &CliContext,
    subject: Option<&str>,
    project: bool,
    json: bool,
) -> Result<()> {
    map(gsvc_fs_client::git_smartlog(&private_context(ctx), subject, project, json).await)
}
