use crate::config::contexts::{ContextEntry, ContextsFile, load_contexts};
use crate::config::files::{
    DEFAULT_API_URL, StoredCredentials, TomlTable, get_nested_value, load_context_token,
    load_global_config, load_local_config, load_stored_credentials, normalize_api_url,
};
use crate::error::{CliError, Result};

/// Where the selected context came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextSource {
    /// `--context` on the command line.
    Flag,
    /// `TENSORLAKE_CONTEXT`.
    Env,
    /// `context = "<name>"` in `.tensorlake/config.toml`.
    LocalConfig,
    /// `current` in `contexts.toml`.
    Current,
}

impl ContextSource {
    pub fn describe(self) -> &'static str {
        match self {
            ContextSource::Flag => "--context flag",
            ContextSource::Env => "TENSORLAKE_CONTEXT",
            ContextSource::LocalConfig => ".tensorlake/config.toml",
            ContextSource::Current => "current context",
        }
    }
}

/// Where the organization and project came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ScopeSource {
    /// `--organization` / `--project` or their env vars.
    Flags,
    /// The selected context.
    Context,
    /// `organization` / `project` keys in `.tensorlake/config.toml`.
    LocalConfig,
    /// The old per-URL login scope in `credentials.toml`.
    Credentials,
    #[default]
    None,
}

/// Resolved configuration values with source tracking.
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub api_url: String,
    pub cloud_url: String,
    pub namespace: String,
    pub api_key: Option<String>,
    pub personal_access_token: Option<String>,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    pub debug: bool,
    /// The context whose token and scope are in use, if any.
    pub context_name: Option<String>,
    pub context_source: Option<ContextSource>,
    /// Where the project came from, or the organization when there is no project.
    pub scope_source: ScopeSource,
    /// Where the organization came from.
    pub organization_source: ScopeSource,
}

/// Resolve all configuration.
///
/// Lookup order for the organization and project, from high to low:
/// 1. `--organization` / `--project` flags and their env vars.
/// 2. `--context` flag, then `TENSORLAKE_CONTEXT`.
/// 3. Local `.tensorlake/config.toml`: a `context` key, or `organization` and `project` keys.
/// 4. `current` in `contexts.toml`.
/// 5. The old saved login scope in `credentials.toml`.
///
/// CLI args and env vars are already merged by clap (via `env` attribute), except for
/// `--context`, where the caller passes the flag and `TENSORLAKE_CONTEXT` is read here.
#[allow(clippy::too_many_arguments)]
pub fn resolve(
    api_url: Option<&str>,
    cloud_url: Option<&str>,
    api_key: Option<&str>,
    pat: Option<&str>,
    namespace: Option<&str>,
    organization_id: Option<&str>,
    project_id: Option<&str>,
    context: Option<&str>,
    debug: bool,
) -> Result<ResolvedConfig> {
    resolve_inner(
        api_url,
        cloud_url,
        api_key,
        pat,
        namespace,
        organization_id,
        project_id,
        context,
        debug,
        false,
    )
}

/// Like `resolve`, but an unknown context or a missing token is a warning, not an error.
///
/// For commands that repair the configuration (`tl login`, `tl context ...`, `tl init`) and
/// for `tl whoami`, which should show the problem.
#[allow(clippy::too_many_arguments)]
pub fn resolve_lenient(
    api_url: Option<&str>,
    cloud_url: Option<&str>,
    api_key: Option<&str>,
    pat: Option<&str>,
    namespace: Option<&str>,
    organization_id: Option<&str>,
    project_id: Option<&str>,
    context: Option<&str>,
    debug: bool,
) -> ResolvedConfig {
    resolve_inner(
        api_url,
        cloud_url,
        api_key,
        pat,
        namespace,
        organization_id,
        project_id,
        context,
        debug,
        true,
    )
    .expect("lenient resolve never fails")
}

#[allow(clippy::too_many_arguments)]
fn resolve_inner(
    api_url: Option<&str>,
    cloud_url: Option<&str>,
    api_key: Option<&str>,
    pat: Option<&str>,
    namespace: Option<&str>,
    organization_id: Option<&str>,
    project_id: Option<&str>,
    context: Option<&str>,
    debug: bool,
    lenient: bool,
) -> Result<ResolvedConfig> {
    let local_config = load_local_config();
    let global_config = load_global_config();
    let contexts = load_contexts();
    let env_context = std::env::var("TENSORLAKE_CONTEXT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let selection = match select_context(context, env_context.as_deref(), &local_config, &contexts)
    {
        Ok(selection) => selection,
        Err(e) if lenient => {
            eprintln!("warning: {e}");
            None
        }
        Err(e) => return Err(e),
    };
    let selected_entry = selection.as_ref().and_then(|(name, _)| contexts.get(name));

    let final_api_url = resolve_api_url(
        api_url,
        selection.as_ref().map(|(_, source)| *source),
        selected_entry,
        &local_config,
        &global_config,
    );
    let final_cloud_url =
        resolve_cloud_url(cloud_url, &final_api_url, &local_config, &global_config);
    let final_namespace = resolve_namespace(namespace, &local_config, &global_config);
    let final_api_key = resolve_api_key(api_key, &local_config, &global_config);

    let stored_credentials = if pat.is_none() {
        load_stored_credentials(&final_api_url)
    } else {
        None
    };

    let mut scope = resolve_scope(
        organization_id,
        project_id,
        &final_api_url,
        selection.as_ref().map(|(_, source)| *source),
        selected_entry,
        &local_config,
        stored_credentials.as_ref(),
    );

    let (final_pat, token_context) = if let Some(pat) = pat {
        (Some(pat.to_string()), None)
    } else if final_api_key.is_some() {
        // An API key wins over any PAT, so do not fail on a missing token here.
        (None, None)
    } else {
        match resolve_token(
            &final_api_url,
            &mut scope,
            selection.as_ref().map(|(name, _)| name.as_str()),
            &contexts,
            stored_credentials.as_ref(),
        ) {
            Ok(found) => found,
            Err(e) if lenient => {
                eprintln!("warning: {e}");
                (None, None)
            }
            Err(e) => return Err(e),
        }
    };

    let (context_name, context_source) = match (token_context, selection) {
        (Some(name), Some((selected, source))) if name == selected => (Some(name), Some(source)),
        (Some(name), _) => (Some(name), None),
        (None, Some((selected, source))) => (Some(selected), Some(source)),
        (None, None) => (None, None),
    };

    Ok(ResolvedConfig {
        api_url: final_api_url,
        cloud_url: final_cloud_url,
        namespace: final_namespace,
        api_key: final_api_key,
        personal_access_token: final_pat,
        organization_id: scope.organization_id,
        project_id: scope.project_id,
        debug,
        context_name,
        context_source,
        scope_source: scope.source,
        organization_source: scope.organization_source,
    })
}

/// Pick the context named by the flag, the env var, the local config, or `current`.
///
/// A name that is not in `contexts.toml` is an error, except for `current`, which is
/// ignored when stale.
pub(crate) fn select_context(
    flag: Option<&str>,
    env: Option<&str>,
    local: &TomlTable,
    contexts: &ContextsFile,
) -> Result<Option<(String, ContextSource)>> {
    let local_context = get_nested_value(local, "context");
    let named = [
        (flag, ContextSource::Flag),
        (env, ContextSource::Env),
        (local_context.as_deref(), ContextSource::LocalConfig),
    ];
    for (name, source) in named {
        let Some(name) = name else { continue };
        if contexts.get(name).is_some() {
            return Ok(Some((name.to_string(), source)));
        }
        return Err(unknown_context(name, source, contexts));
    }
    Ok(contexts
        .current_entry()
        .map(|(name, _)| (name.to_string(), ContextSource::Current)))
}

fn unknown_context(name: &str, source: ContextSource, contexts: &ContextsFile) -> CliError {
    let known: Vec<&str> = contexts.contexts.keys().map(String::as_str).collect();
    let hint = if known.is_empty() {
        "no contexts are saved. run: tl login".to_string()
    } else {
        format!("saved contexts: {}", known.join(", "))
    };
    CliError::config(format!(
        "unknown context '{name}' (from {}). {hint}",
        source.describe()
    ))
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ResolvedScope {
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
    /// Where the project came from, or the organization when there is no project.
    pub source: ScopeSource,
    /// Where the organization came from.
    pub organization_source: ScopeSource,
}

/// Merge the organization and project from the sources in lookup order.
///
/// The `source` is where the project came from, or the organization when there is no project.
///
/// A context chosen by flag, env var, or local config comes before the local
/// organization/project keys. The `current` context comes after them. A context for another
/// API URL than `api_url` gives no scope: its organization and project belong to that URL.
#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_scope(
    org_flag: Option<&str>,
    proj_flag: Option<&str>,
    api_url: &str,
    selected_source: Option<ContextSource>,
    selected_entry: Option<&ContextEntry>,
    local: &TomlTable,
    stored: Option<&StoredCredentials>,
) -> ResolvedScope {
    let same_url = |entry: &&ContextEntry| entry.matches(api_url, None, None);
    let selected_entry = selected_entry.filter(same_url);
    let (early_context, late_context) = match selected_source {
        Some(ContextSource::Current) => (None, selected_entry),
        Some(_) => (selected_entry, None),
        None => (None, None),
    };

    let org_sources: [(Option<String>, ScopeSource); 5] = [
        (org_flag.map(str::to_string), ScopeSource::Flags),
        (
            early_context.and_then(|e| e.organization.clone()),
            ScopeSource::Context,
        ),
        (
            get_nested_value(local, "organization"),
            ScopeSource::LocalConfig,
        ),
        (
            late_context.and_then(|e| e.organization.clone()),
            ScopeSource::Context,
        ),
        (
            stored.and_then(|s| s.organization_id.clone()),
            ScopeSource::Credentials,
        ),
    ];
    let proj_sources: [(Option<String>, ScopeSource); 5] = [
        (proj_flag.map(str::to_string), ScopeSource::Flags),
        (
            early_context.and_then(|e| e.project.clone()),
            ScopeSource::Context,
        ),
        (get_nested_value(local, "project"), ScopeSource::LocalConfig),
        (
            late_context.and_then(|e| e.project.clone()),
            ScopeSource::Context,
        ),
        (
            stored.and_then(|s| s.project_id.clone()),
            ScopeSource::Credentials,
        ),
    ];

    let org = org_sources.into_iter().find(|(v, _)| v.is_some());
    let proj = proj_sources.into_iter().find(|(v, _)| v.is_some());
    let source = proj
        .as_ref()
        .or(org.as_ref())
        .map(|(_, s)| *s)
        .unwrap_or(ScopeSource::None);

    ResolvedScope {
        organization_source: org.as_ref().map(|(_, s)| *s).unwrap_or(ScopeSource::None),
        organization_id: org.and_then(|(v, _)| v),
        project_id: proj.and_then(|(v, _)| v),
        source,
    }
}

/// Find the token for the resolved scope: the selected context when it matches, else a
/// context that works for the scope, else the old per-URL token when no context exists.
///
/// A context with no saved token falls back to the old per-URL token when that token has the
/// same organization and project. This keeps a migrated login working when the migration
/// could not write `credentials.toml` (for example, a read-only home directory).
///
/// The organization in `scope` is aligned with the found context, so that the token, the
/// organization, and the project always belong together. See [`align_organization`].
///
/// Returns `(token, context name)`.
fn resolve_token(
    api_url: &str,
    scope: &mut ResolvedScope,
    selected: Option<&str>,
    contexts: &ContextsFile,
    stored: Option<&StoredCredentials>,
) -> Result<(Option<String>, Option<String>)> {
    let has_contexts_for_url = contexts.for_api_url(api_url).next().is_some();
    if !has_contexts_for_url {
        // No contexts (first run with no login, or a hand-written file): old behaviour.
        return Ok((stored.map(|s| s.token.clone()), None));
    }

    let found = contexts.find_for_scope(
        api_url,
        scope.organization_id.as_deref(),
        scope.project_id.as_deref(),
        selected,
    );
    match found {
        Some(name) => {
            let entry = contexts
                .get(name)
                .expect("find_for_scope returns a saved name");
            align_organization(scope, name, entry)?;
            let token = load_context_token(name)
                .map(|t| t.token)
                .or_else(|| legacy_token_for(entry, stored?));
            Ok((token, Some(name.to_string())))
        }
        None => match scope.project_id.as_deref() {
            Some(project) => Err(CliError::auth(format!(
                "no token for project {project}. run: tl context create <name> --project {project}"
            ))),
            None => Ok((None, None)),
        },
    }
}

/// Make the organization in `scope` agree with the context that supplies the token.
///
/// A project ID names one organization. When `--project` picks a context saved under another
/// organization than the one in `scope`, the organization in `scope` is stale: it came from
/// the current context, the local config, or the old login. Replace it with the one from the
/// context. An organization given on the command line is explicit, so a conflict is an error
/// instead.
fn align_organization(scope: &mut ResolvedScope, name: &str, entry: &ContextEntry) -> Result<()> {
    let Some(context_org) = entry.organization.as_deref() else {
        return Ok(());
    };
    match scope.organization_id.as_deref() {
        Some(org) if org == context_org => Ok(()),
        Some(org) if scope.organization_source == ScopeSource::Flags => {
            let project = scope.project_id.as_deref().unwrap_or_default();
            Err(CliError::usage(format!(
                "project {project} belongs to organization {context_org} (context '{name}'), \
                 not {org}. drop --organization, or run: tl context create <name> \
                 --organization {org} --project {project}"
            )))
        }
        _ => {
            scope.organization_id = Some(context_org.to_string());
            scope.organization_source = ScopeSource::Context;
            if scope.project_id.is_none() {
                scope.source = ScopeSource::Context;
            }
            Ok(())
        }
    }
}

/// The old per-URL token, when it was saved for the same organization and project as `entry`.
fn legacy_token_for(entry: &ContextEntry, stored: &StoredCredentials) -> Option<String> {
    (entry.organization == stored.organization_id && entry.project == stored.project_id)
        .then(|| stored.token.clone())
}

fn resolve_api_url(
    cli: Option<&str>,
    context_source: Option<ContextSource>,
    context: Option<&ContextEntry>,
    local: &TomlTable,
    global: &TomlTable,
) -> String {
    // A context named by flag, env var, or local config beats the local config URL.
    // The current context only beats the global config.
    let (early, late) = match context_source {
        Some(ContextSource::Current) => (None, context),
        Some(_) => (context, None),
        None => (None, None),
    };
    let api_url = cli
        .map(|s| s.to_string())
        .or_else(|| early.map(|e| e.api_url.clone()))
        .or_else(|| get_nested_value(local, "tensorlake.api_url"))
        .or_else(|| late.map(|e| e.api_url.clone()))
        .or_else(|| get_nested_value(global, "tensorlake.api_url"))
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());
    normalize_api_url(&api_url)
}

fn resolve_cloud_url(
    cli: Option<&str>,
    api_url: &str,
    local: &TomlTable,
    global: &TomlTable,
) -> String {
    cli.map(|s| s.to_string())
        .or_else(|| get_nested_value(local, "tensorlake.cloud_url"))
        .or_else(|| get_nested_value(global, "tensorlake.cloud_url"))
        .unwrap_or_else(|| cloud_url_from_api_url(api_url))
}

fn cloud_url_from_api_url(api_url: &str) -> String {
    if api_url.starts_with("https://api.tensorlake.") {
        api_url.replace("https://api.tensorlake.", "https://cloud.tensorlake.")
    } else {
        "https://cloud.tensorlake.ai".to_string()
    }
}

fn resolve_api_key(api_key: Option<&str>, local: &TomlTable, global: &TomlTable) -> Option<String> {
    api_key
        .map(|s| s.to_string())
        .or_else(|| get_nested_value(local, "tensorlake.apikey"))
        .or_else(|| get_nested_value(global, "tensorlake.apikey"))
}

fn resolve_namespace(cli: Option<&str>, local: &TomlTable, global: &TomlTable) -> String {
    cli.map(|s| s.to_string())
        .or_else(|| get_nested_value(local, "indexify.namespace"))
        .or_else(|| get_nested_value(global, "indexify.namespace"))
        .unwrap_or_else(|| "default".to_string())
}

/// Check the format of IDs given on the command line, so that a wrong value such as
/// `organizations/org_...` gives a clear error instead of a 403 from the server.
pub fn validate_organization_id(id: &str) -> Result<()> {
    validate_id(id, "org_", "organization")
}

pub fn validate_project_id(id: &str) -> Result<()> {
    validate_id(id, "project_", "project")
}

fn validate_id(id: &str, prefix: &str, what: &str) -> Result<()> {
    let trimmed = id.trim();
    let valid = trimmed.starts_with(prefix)
        && trimmed.len() > prefix.len()
        && trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if valid {
        Ok(())
    } else {
        Err(CliError::usage(format!(
            "invalid {what} ID '{id}': expected an ID that starts with '{prefix}', for example {prefix}AbC123"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::contexts::ContextEntry;

    fn entry(api_url: &str, org: &str, project: &str) -> ContextEntry {
        ContextEntry {
            api_url: api_url.to_string(),
            organization: Some(org.to_string()),
            project: Some(project.to_string()),
        }
    }

    fn contexts() -> ContextsFile {
        let mut file = ContextsFile {
            current: Some("default".into()),
            ..Default::default()
        };
        file.contexts.insert(
            "default".into(),
            entry("https://api.tensorlake.ai", "org_1", "project_default"),
        );
        file.contexts.insert(
            "staging".into(),
            entry("https://api.tensorlake.ai", "org_1", "project_staging"),
        );
        file
    }

    fn local(text: &str) -> TomlTable {
        toml::from_str(text).expect("valid toml")
    }

    #[test]
    fn select_context_in_lookup_order() {
        let c = contexts();
        let with_local = local(r#"context = "staging""#);

        assert_eq!(
            select_context(Some("default"), Some("staging"), &with_local, &c).unwrap(),
            Some(("default".into(), ContextSource::Flag))
        );
        assert_eq!(
            select_context(None, Some("staging"), &with_local, &c).unwrap(),
            Some(("staging".into(), ContextSource::Env))
        );
        assert_eq!(
            select_context(None, None, &with_local, &c).unwrap(),
            Some(("staging".into(), ContextSource::LocalConfig))
        );
        assert_eq!(
            select_context(None, None, &TomlTable::new(), &c).unwrap(),
            Some(("default".into(), ContextSource::Current))
        );
    }

    #[test]
    fn unknown_context_is_a_clear_error() {
        let c = contexts();
        let err = select_context(Some("nope"), None, &TomlTable::new(), &c).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown context 'nope'"), "{msg}");
        assert!(msg.contains("--context flag"), "{msg}");
        assert!(msg.contains("default, staging"), "{msg}");

        let err = select_context(None, Some("nope"), &TomlTable::new(), &c).unwrap_err();
        assert!(err.to_string().contains("TENSORLAKE_CONTEXT"));

        // A stale `current` is ignored, not an error.
        let mut stale = contexts();
        stale.current = Some("gone".into());
        assert_eq!(
            select_context(None, None, &TomlTable::new(), &stale).unwrap(),
            None
        );
    }

    #[test]
    fn scope_sources_win_in_order() {
        let c = contexts();
        let local_cfg = local(
            r#"
organization = "org_local"
project = "project_local"
"#,
        );
        let stored = StoredCredentials {
            token: "t".into(),
            organization_id: Some("org_stored".into()),
            project_id: Some("project_stored".into()),
        };
        let staging = c.get("staging").unwrap();
        let default = c.get("default").unwrap();
        let url = "https://api.tensorlake.ai";

        // 1. flags
        let s = resolve_scope(
            Some("org_flag"),
            Some("project_flag"),
            url,
            Some(ContextSource::Flag),
            Some(staging),
            &local_cfg,
            Some(&stored),
        );
        assert_eq!(s.project_id.as_deref(), Some("project_flag"));
        assert_eq!(s.source, ScopeSource::Flags);

        // 2. a context chosen by flag/env beats the local config
        let s = resolve_scope(
            None,
            None,
            url,
            Some(ContextSource::Env),
            Some(staging),
            &local_cfg,
            Some(&stored),
        );
        assert_eq!(s.project_id.as_deref(), Some("project_staging"));
        assert_eq!(s.source, ScopeSource::Context);

        // 3. the local config beats the current context and the stored scope
        let s = resolve_scope(
            None,
            None,
            url,
            Some(ContextSource::Current),
            Some(default),
            &local_cfg,
            Some(&stored),
        );
        assert_eq!(s.project_id.as_deref(), Some("project_local"));
        assert_eq!(s.source, ScopeSource::LocalConfig);

        // 4. the current context beats the stored scope
        let s = resolve_scope(
            None,
            None,
            url,
            Some(ContextSource::Current),
            Some(default),
            &TomlTable::new(),
            Some(&stored),
        );
        assert_eq!(s.project_id.as_deref(), Some("project_default"));
        assert_eq!(s.source, ScopeSource::Context);

        // 5. the stored scope is last
        let s = resolve_scope(
            None,
            None,
            url,
            None,
            None,
            &TomlTable::new(),
            Some(&stored),
        );
        assert_eq!(s.project_id.as_deref(), Some("project_stored"));
        assert_eq!(s.source, ScopeSource::Credentials);
    }

    #[test]
    fn an_explicit_context_that_is_also_current_beats_the_local_config() {
        let c = contexts();
        let default = c.get("default").unwrap();
        let local_cfg = local(
            r#"
organization = "org_local"
project = "project_local"
"#,
        );
        // `tl --context default` while `default` is current: the flag wins.
        for source in [ContextSource::Flag, ContextSource::Env] {
            let s = resolve_scope(
                None,
                None,
                "https://api.tensorlake.ai",
                Some(source),
                Some(default),
                &local_cfg,
                None,
            );
            assert_eq!(s.project_id.as_deref(), Some("project_default"));
            assert_eq!(s.source, ScopeSource::Context);
        }
        // `context = "default"` in the local config also wins over its sibling keys.
        let s = resolve_scope(
            None,
            None,
            "https://api.tensorlake.ai",
            Some(ContextSource::LocalConfig),
            Some(default),
            &local_cfg,
            None,
        );
        assert_eq!(s.project_id.as_deref(), Some("project_default"));
    }

    #[test]
    fn a_context_for_another_api_url_gives_no_scope() {
        let c = contexts();
        let default = c.get("default").unwrap();
        let stored = StoredCredentials {
            token: "t".into(),
            organization_id: Some("org_other".into()),
            project_id: Some("project_other".into()),
        };
        // `tl --api-url http://localhost:8900` with `default` current: the saved scope of
        // that URL wins, not the scope of `default`.
        for source in [ContextSource::Current, ContextSource::Flag] {
            let s = resolve_scope(
                None,
                None,
                "http://localhost:8900",
                Some(source),
                Some(default),
                &TomlTable::new(),
                Some(&stored),
            );
            assert_eq!(s.project_id.as_deref(), Some("project_other"));
            assert_eq!(s.source, ScopeSource::Credentials);
        }
    }

    #[test]
    fn a_context_with_no_saved_token_uses_the_matching_legacy_token() {
        let c = contexts();
        let default = c.get("default").unwrap();
        // `resolve_token` reads `credentials.toml` from the home directory, so only the
        // scope check is tested here.
        let same_scope = StoredCredentials {
            token: "legacy".into(),
            organization_id: default.organization.clone(),
            project_id: default.project.clone(),
        };
        assert_eq!(
            legacy_token_for(default, &same_scope).as_deref(),
            Some("legacy")
        );
        let other_scope = StoredCredentials {
            project_id: Some("project_other".into()),
            ..same_scope.clone()
        };
        assert_eq!(legacy_token_for(default, &other_scope), None);
        let no_scope = StoredCredentials {
            token: "legacy".into(),
            organization_id: None,
            project_id: None,
        };
        assert_eq!(legacy_token_for(default, &no_scope), None);
    }

    #[test]
    fn a_project_with_no_context_is_a_clear_error() {
        let c = contexts();
        let mut scope = ResolvedScope {
            organization_id: Some("org_1".into()),
            project_id: Some("project_other".into()),
            source: ScopeSource::Flags,
            organization_source: ScopeSource::Context,
        };
        let err = resolve_token(
            "https://api.tensorlake.ai",
            &mut scope,
            Some("default"),
            &c,
            None,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "no token for project project_other. run: tl context create <name> --project project_other"
        );
    }

    #[test]
    fn a_project_flag_picks_the_matching_context() {
        let c = contexts();
        let mut scope = ResolvedScope {
            organization_id: Some("org_1".into()),
            project_id: Some("project_staging".into()),
            source: ScopeSource::Flags,
            organization_source: ScopeSource::Context,
        };
        // No token on disk in unit tests, but the context is named.
        let (_, name) = resolve_token(
            "https://api.tensorlake.ai",
            &mut scope,
            Some("default"),
            &c,
            None,
        )
        .unwrap();
        assert_eq!(name.as_deref(), Some("staging"));
    }

    fn contexts_in_two_organizations() -> ContextsFile {
        let mut c = contexts();
        c.contexts.insert(
            "other".into(),
            entry("https://api.tensorlake.ai", "org_2", "project_other"),
        );
        c
    }

    #[test]
    fn a_project_in_another_organization_takes_that_organization() {
        let c = contexts_in_two_organizations();
        // `tl --project project_other` with `default` (org_1) current: the organization
        // was inherited, so it follows the context that owns the project.
        for inherited in [
            ScopeSource::Context,
            ScopeSource::LocalConfig,
            ScopeSource::Credentials,
        ] {
            let mut scope = ResolvedScope {
                organization_id: Some("org_1".into()),
                project_id: Some("project_other".into()),
                source: ScopeSource::Flags,
                organization_source: inherited,
            };
            let (_, name) = resolve_token(
                "https://api.tensorlake.ai",
                &mut scope,
                Some("default"),
                &c,
                None,
            )
            .unwrap();
            assert_eq!(name.as_deref(), Some("other"));
            assert_eq!(scope.organization_id.as_deref(), Some("org_2"));
            assert_eq!(scope.organization_source, ScopeSource::Context);
            assert_eq!(scope.project_id.as_deref(), Some("project_other"));
            assert_eq!(scope.source, ScopeSource::Flags);
        }

        // No organization at all: the context supplies one.
        let mut scope = ResolvedScope {
            organization_id: None,
            project_id: Some("project_other".into()),
            source: ScopeSource::Flags,
            organization_source: ScopeSource::None,
        };
        resolve_token(
            "https://api.tensorlake.ai",
            &mut scope,
            Some("default"),
            &c,
            None,
        )
        .unwrap();
        assert_eq!(scope.organization_id.as_deref(), Some("org_2"));
    }

    #[test]
    fn an_explicit_organization_that_conflicts_with_the_project_is_an_error() {
        let c = contexts_in_two_organizations();
        let mut scope = ResolvedScope {
            organization_id: Some("org_1".into()),
            project_id: Some("project_other".into()),
            source: ScopeSource::Flags,
            organization_source: ScopeSource::Flags,
        };
        let err = resolve_token(
            "https://api.tensorlake.ai",
            &mut scope,
            Some("default"),
            &c,
            None,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "project project_other belongs to organization org_2 (context 'other'), not org_1. \
             drop --organization, or run: tl context create <name> --organization org_1 \
             --project project_other"
        );
        // The scope is left as given.
        assert_eq!(scope.organization_id.as_deref(), Some("org_1"));

        // The same organization on the flag is fine.
        scope.organization_id = Some("org_2".into());
        let (_, name) = resolve_token(
            "https://api.tensorlake.ai",
            &mut scope,
            Some("default"),
            &c,
            None,
        )
        .unwrap();
        assert_eq!(name.as_deref(), Some("other"));
        assert_eq!(scope.organization_source, ScopeSource::Flags);
    }

    #[test]
    fn no_contexts_falls_back_to_the_stored_token() {
        let stored = StoredCredentials {
            token: "legacy".into(),
            organization_id: None,
            project_id: None,
        };
        let mut scope = ResolvedScope::default();
        let (token, name) = resolve_token(
            "https://api.tensorlake.ai",
            &mut scope,
            None,
            &ContextsFile::default(),
            Some(&stored),
        )
        .unwrap();
        assert_eq!(token.as_deref(), Some("legacy"));
        assert_eq!(name, None);
    }

    #[test]
    fn context_api_url_order() {
        let staging = ContextEntry {
            api_url: "https://api.staging.tensorlake.ai".into(),
            organization: None,
            project: None,
        };
        let local_cfg = local(
            r#"
[tensorlake]
api_url = "http://localhost:8900"
"#,
        );
        // A flag beats everything.
        assert_eq!(
            resolve_api_url(
                Some("http://flag:1"),
                Some(ContextSource::Flag),
                Some(&staging),
                &local_cfg,
                &TomlTable::new()
            ),
            "http://flag:1"
        );
        // A context chosen by flag beats the local config URL.
        assert_eq!(
            resolve_api_url(
                None,
                Some(ContextSource::Flag),
                Some(&staging),
                &local_cfg,
                &TomlTable::new()
            ),
            "https://api.staging.tensorlake.ai"
        );
        // The current context does not.
        assert_eq!(
            resolve_api_url(
                None,
                Some(ContextSource::Current),
                Some(&staging),
                &local_cfg,
                &TomlTable::new()
            ),
            "http://localhost:8900"
        );
        // But it beats the default.
        assert_eq!(
            resolve_api_url(
                None,
                Some(ContextSource::Current),
                Some(&staging),
                &TomlTable::new(),
                &TomlTable::new()
            ),
            "https://api.staging.tensorlake.ai"
        );
    }

    #[test]
    fn id_format_is_checked() {
        assert!(validate_organization_id("org_AbC123").is_ok());
        assert!(validate_project_id("project_AbC-123").is_ok());
        let err = validate_organization_id("organizations/org_AbC123").unwrap_err();
        assert!(err.to_string().contains("starts with 'org_'"), "{err}");
        assert!(validate_project_id("org_AbC123").is_err());
        assert!(validate_project_id("project_").is_err());
    }
}
