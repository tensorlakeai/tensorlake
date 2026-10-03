use crate::config::contexts::{ContextEntry, ContextsFile, context_for_url, load_contexts};
use crate::config::files::{
    DEFAULT_API_URL, StoredCredentials, TomlTable, get_nested_value, load_global_config,
    load_local_config, load_stored_credentials, normalize_api_url,
};
use crate::config::token_store::load_context_token;
use crate::error::{CliError, Result};

/// Where the selected context came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextSource {
    /// `--context` on the command line.
    Flag,
    /// `TENSORLAKE_CONTEXT`.
    Env,
    /// `current` in `contexts.toml`.
    Current,
    /// The saved context for the API URL of this run, when the current context is for
    /// another URL.
    ApiUrl,
}

impl ContextSource {
    pub fn describe(self) -> &'static str {
        match self {
            ContextSource::Flag => "--context flag",
            ContextSource::Env => "TENSORLAKE_CONTEXT",
            ContextSource::Current => "current context",
            ContextSource::ApiUrl => "API URL of this run",
        }
    }
}

/// What to do when `--context` or `TENSORLAKE_CONTEXT` names a context that is not saved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnknownContext {
    /// Fail with a clear error. For commands that run in the selected context.
    Error,
    /// Run as if no context were named. For `tl login`, `tl logout --all`, and `tl context`:
    /// these commands put the saved contexts right, so they must run when the named context
    /// is missing. Otherwise a stale `TENSORLAKE_CONTEXT` would block its own recovery.
    Ignore,
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
}

/// Resolve all configuration.
///
/// A context is one API URL, one organization, one project, and one token. The selected
/// context supplies all four. It is named by `--context`, then `TENSORLAKE_CONTEXT`, else it
/// is the `current` one in `contexts.toml`.
///
/// Lookup order, from high to low: CLI args and env vars > the selected context > local
/// `.tensorlake/config.toml` > global config > defaults. CLI args and env vars are already
/// merged by clap (via `env` attribute), except for `--context`, where the caller passes the
/// flag and `TENSORLAKE_CONTEXT` is read here.
///
/// A context applies only to its own API URL. When the `current` context is for another URL
/// than `--api-url`, the saved context for that URL is used instead (see
/// [`context_for_url`]): the upgrade turned each old per-URL login into such a context, so
/// `--api-url` keeps finding the login it found before. When no context is for the URL, the
/// old per-URL login in `credentials.toml` is used. A context named by `--context` or
/// `TENSORLAKE_CONTEXT` that cannot apply, because of `--api-url` or `--pat`, is an error:
/// the user asked for that context, and running as another login instead would be a
/// surprise.
///
/// The token of a context works for its one project only. So `--organization` or `--project`
/// that names another scope than the context is an error, unless `--pat` or an API key
/// supplies the credential.
///
/// A `--context` or `TENSORLAKE_CONTEXT` name that is not saved is an error, unless
/// `unknown_context` says to ignore it.
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
    unknown_context: UnknownContext,
    debug: bool,
) -> Result<ResolvedConfig> {
    let env_context = std::env::var("TENSORLAKE_CONTEXT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let inputs = Inputs {
        api_url,
        cloud_url,
        api_key,
        pat,
        namespace,
        organization_id,
        project_id,
        context_flag: context,
        context_env: env_context.as_deref(),
        unknown_context,
        debug,
    };
    resolve_with(
        &inputs,
        &load_local_config(),
        &load_global_config(),
        &load_contexts()?,
        load_stored_credentials,
        |name, entry| load_context_token(name, entry.storage).map(|t| t.map(|t| t.token)),
    )
}

/// All command-line and environment inputs to [`resolve`].
struct Inputs<'a> {
    api_url: Option<&'a str>,
    cloud_url: Option<&'a str>,
    api_key: Option<&'a str>,
    pat: Option<&'a str>,
    namespace: Option<&'a str>,
    organization_id: Option<&'a str>,
    project_id: Option<&'a str>,
    context_flag: Option<&'a str>,
    context_env: Option<&'a str>,
    unknown_context: UnknownContext,
    debug: bool,
}

/// [`resolve`] with the files and lookups passed in, so that tests need no home directory.
fn resolve_with(
    inputs: &Inputs,
    local: &TomlTable,
    global: &TomlTable,
    contexts: &ContextsFile,
    stored_credentials: impl Fn(&str) -> Option<StoredCredentials>,
    context_token: impl Fn(&str, &ContextEntry) -> Result<Option<String>>,
) -> Result<ResolvedConfig> {
    let selection = select_context(
        inputs.context_flag,
        inputs.context_env,
        inputs.unknown_context,
        contexts,
    )?;
    let named = selection
        .as_ref()
        .and_then(|(name, source)| Some((name.as_str(), *source, contexts.get(name)?)));
    // A context named by `--context` or `TENSORLAKE_CONTEXT` is what the user asked for. When
    // it cannot apply, stop, instead of going on with another login. The recovery commands
    // (`tl login`, `tl logout --all`, `tl context`) run with `UnknownContext::Ignore` and
    // must still run, so that `TENSORLAKE_CONTEXT=staging tl login --api-url <url>` works.
    let explicit = named.filter(|(_, source, _)| {
        *source != ContextSource::Current && inputs.unknown_context == UnknownContext::Error
    });

    // `--pat` brings its own scope, so neither the context nor the old login applies then.
    if let (Some(pat_context), Some(_)) = (explicit, inputs.pat) {
        return Err(CliError::usage(format!(
            "--pat cannot be used with context '{}': a PAT brings its own scope. \
             drop --pat to use the context, or drop {}",
            pat_context.0,
            flag_name(pat_context.1)
        )));
    }
    let selected = named.filter(|_| inputs.pat.is_none());

    let api_url = resolve_api_url(
        inputs.api_url,
        selected.map(|(_, _, entry)| entry),
        local,
        global,
    );
    let cloud_url = resolve_cloud_url(inputs.cloud_url, &api_url, local, global);
    let namespace = resolve_namespace(inputs.namespace, local, global);
    let api_key = resolve_api_key(inputs.api_key, local, global);

    // The context applies only to its own API URL.
    let context = selected.filter(|(_, _, entry)| normalize_api_url(&entry.api_url) == api_url);
    if let (Some((name, source, entry)), None) = (explicit, context) {
        return Err(CliError::usage(format!(
            "context '{name}' is for {} but the API URL of this run is {api_url}. \
             drop --api-url (or TENSORLAKE_API_URL) to use the context, or drop {}",
            normalize_api_url(&entry.api_url),
            flag_name(source)
        )));
    }
    // The `current` context is for another URL: the saved context for the URL of this run
    // stands in. Not with `--pat`, which brings its own scope.
    let context = context.or_else(|| {
        if inputs.pat.is_some() {
            return None;
        }
        let (name, entry) = context_for_url(contexts, &api_url)?;
        Some((name, ContextSource::ApiUrl, entry))
    });
    let stored = match (context, inputs.pat) {
        (None, None) => stored_credentials(&api_url),
        _ => None,
    };

    let (login_org, login_project) = match (context, stored.as_ref()) {
        (Some((_, _, entry)), _) => (entry.organization.as_deref(), entry.project.as_deref()),
        (None, Some(stored)) => (
            stored.organization_id.as_deref(),
            stored.project_id.as_deref(),
        ),
        (None, None) => (None, None),
    };
    let organization_id = inputs
        .organization_id
        .or(login_org)
        .map(str::to_string)
        .or_else(|| get_nested_value(local, "organization"));
    let project_id = inputs
        .project_id
        .or(login_project)
        .map(str::to_string)
        .or_else(|| get_nested_value(local, "project"));

    // The token of the context is read only when this run uses it: a command that runs in
    // the context, with no API key. The token may be in an OS keychain that is locked or
    // does not answer. That must not stop `tl login`, `tl context use`, or a run that
    // brings its own API key: those are the ways out.
    let uses_context_token = api_key.is_none() && inputs.unknown_context == UnknownContext::Error;
    let personal_access_token = match (inputs.pat, context, stored) {
        (Some(pat), _, _) => Some(pat.to_string()),
        (None, Some((name, _, entry)), _) if uses_context_token => context_token(name, entry)?,
        (None, Some(_), _) => None,
        (None, None, stored) => stored.map(|s| s.token),
    };

    // The recovery commands (`tl login`, `tl logout --all`, `tl context`) do not use the
    // token of the selected context, so a `--project` or `TENSORLAKE_PROJECT_ID` for another
    // project must not stop them. The check names them as the way out of a mismatch.
    if let Some((name, _, entry)) = context
        && api_key.is_none()
        && inputs.unknown_context == UnknownContext::Error
    {
        check_flags_match_context(inputs.organization_id, inputs.project_id, name, entry)?;
    }

    Ok(ResolvedConfig {
        api_url,
        cloud_url,
        namespace,
        api_key,
        personal_access_token,
        organization_id,
        project_id,
        debug: inputs.debug,
        context_name: context.map(|(name, _, _)| name.to_string()),
        context_source: context.map(|(_, source, _)| source),
    })
}

/// Pick the context named by the flag, the env var, or `current`.
///
/// A name that is not in `contexts.toml` is an error, or is skipped when `unknown` is
/// [`UnknownContext::Ignore`]. A stale `current` is always ignored.
pub(crate) fn select_context(
    flag: Option<&str>,
    env: Option<&str>,
    unknown: UnknownContext,
    contexts: &ContextsFile,
) -> Result<Option<(String, ContextSource)>> {
    let named = [(flag, ContextSource::Flag), (env, ContextSource::Env)];
    for (name, source) in named {
        let Some(name) = name else { continue };
        if contexts.get(name).is_some() {
            return Ok(Some((name.to_string(), source)));
        }
        match unknown {
            UnknownContext::Error => return Err(unknown_context(name, source, contexts)),
            UnknownContext::Ignore => continue,
        }
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

/// The token of a context works for its own project only, so a flag that names another
/// organization or project cannot work with it.
/// How the user named a context, for error messages.
fn flag_name(source: ContextSource) -> &'static str {
    match source {
        ContextSource::Flag => "--context",
        ContextSource::Env => "TENSORLAKE_CONTEXT",
        ContextSource::Current => "the current context",
        ContextSource::ApiUrl => "--api-url",
    }
}

fn check_flags_match_context(
    org_flag: Option<&str>,
    project_flag: Option<&str>,
    name: &str,
    entry: &ContextEntry,
) -> Result<()> {
    let checks = [
        ("--project", project_flag, entry.project.as_deref()),
        ("--organization", org_flag, entry.organization.as_deref()),
    ];
    for (flag, wanted, saved) in checks {
        if let (Some(wanted), Some(saved)) = (wanted, saved)
            && wanted != saved
        {
            return Err(CliError::usage(format!(
                "{flag} {wanted} does not match context '{name}' ({saved}). \
                 the token of a context works for its own project only. \
                 run: tl context list, then use --context <name> or tl context use <name>. \
                 to add a context for {wanted}, run: tl login --context <name>"
            )));
        }
    }
    Ok(())
}

fn resolve_api_url(
    cli: Option<&str>,
    context: Option<&ContextEntry>,
    local: &TomlTable,
    global: &TomlTable,
) -> String {
    let api_url = cli
        .map(|s| s.to_string())
        .or_else(|| context.map(|e| e.api_url.clone()))
        .or_else(|| get_nested_value(local, "tensorlake.api_url"))
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

#[cfg(test)]
mod tests {
    use super::*;

    const PROD: &str = "https://api.tensorlake.ai";

    fn entry(api_url: &str, org: &str, project: &str) -> ContextEntry {
        ContextEntry {
            api_url: api_url.to_string(),
            organization: Some(org.to_string()),
            project: Some(project.to_string()),
            storage: None,
        }
    }

    /// `default` (current) and `staging`, both for the production URL.
    fn contexts() -> ContextsFile {
        let mut file = ContextsFile {
            current: Some("default".into()),
            ..Default::default()
        };
        file.contexts
            .insert("default".into(), entry(PROD, "org_1", "project_default"));
        file.contexts
            .insert("staging".into(), entry(PROD, "org_1", "project_staging"));
        file
    }

    fn token(name: &str, _: &ContextEntry) -> Result<Option<String>> {
        Ok(match name {
            "default" => Some("tl_default".into()),
            "staging" => Some("tl_staging".into()),
            _ => None,
        })
    }

    fn stored(api_url: &str) -> Option<StoredCredentials> {
        (api_url == PROD).then(|| StoredCredentials {
            token: "tl_stored".into(),
            organization_id: Some("org_stored".into()),
            project_id: Some("project_stored".into()),
        })
    }

    fn local(text: &str) -> TomlTable {
        toml::from_str(text).expect("valid toml")
    }

    fn inputs<'a>() -> Inputs<'a> {
        Inputs {
            api_url: None,
            cloud_url: None,
            api_key: None,
            pat: None,
            namespace: None,
            organization_id: None,
            project_id: None,
            context_flag: None,
            context_env: None,
            unknown_context: UnknownContext::Error,
            debug: false,
        }
    }

    fn resolve_test(inputs: &Inputs, local: &TomlTable, contexts: &ContextsFile) -> ResolvedConfig {
        resolve_with(inputs, local, &TomlTable::new(), contexts, stored, token).expect("resolves")
    }

    #[test]
    fn select_context_in_lookup_order() {
        let c = contexts();
        let strict = UnknownContext::Error;
        assert_eq!(
            select_context(Some("staging"), Some("default"), strict, &c).unwrap(),
            Some(("staging".into(), ContextSource::Flag))
        );
        assert_eq!(
            select_context(None, Some("staging"), strict, &c).unwrap(),
            Some(("staging".into(), ContextSource::Env))
        );
        assert_eq!(
            select_context(None, None, strict, &c).unwrap(),
            Some(("default".into(), ContextSource::Current))
        );
    }

    #[test]
    fn unknown_context_is_a_clear_error() {
        let c = contexts();
        let strict = UnknownContext::Error;
        let err = select_context(Some("nope"), None, strict, &c).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("unknown context 'nope'"), "{msg}");
        assert!(msg.contains("--context flag"), "{msg}");
        assert!(msg.contains("default, staging"), "{msg}");

        let err = select_context(None, Some("nope"), strict, &c).unwrap_err();
        assert!(err.to_string().contains("TENSORLAKE_CONTEXT"));

        // A stale `current` is ignored, not an error.
        let mut stale = contexts();
        stale.current = Some("gone".into());
        assert_eq!(select_context(None, None, strict, &stale).unwrap(), None);
    }

    /// `tl login`, `tl logout --all`, and `tl context` must run when `TENSORLAKE_CONTEXT`
    /// names a context that is gone: they are the way to put the contexts right.
    #[test]
    fn an_unknown_context_can_be_ignored() {
        let c = contexts();
        let lenient = UnknownContext::Ignore;
        // The unknown name is skipped, and the lookup goes on to `current`.
        assert_eq!(
            select_context(Some("nope"), None, lenient, &c).unwrap(),
            Some(("default".into(), ContextSource::Current))
        );
        assert_eq!(
            select_context(None, Some("nope"), lenient, &c).unwrap(),
            Some(("default".into(), ContextSource::Current))
        );
        // A known name still wins over `current`.
        assert_eq!(
            select_context(Some("nope"), Some("staging"), lenient, &c).unwrap(),
            Some(("staging".into(), ContextSource::Env))
        );
        // With no contexts at all, nothing is selected and nothing fails.
        assert_eq!(
            select_context(None, Some("nope"), lenient, &ContextsFile::default()).unwrap(),
            None
        );

        let mut i = inputs();
        i.context_env = Some("nope");
        i.unknown_context = lenient;
        let r = resolve_test(&i, &TomlTable::new(), &c);
        assert_eq!(r.context_name.as_deref(), Some("default"));
        assert_eq!(r.project_id.as_deref(), Some("project_default"));
    }

    /// A token that cannot be read, say from a locked keychain, must not stop the commands
    /// that put things right: `tl login`, `tl context use`, and a run with an API key.
    #[test]
    fn a_token_that_cannot_be_read_stops_only_the_commands_that_use_it() {
        fn locked(_: &str, _: &ContextEntry) -> Result<Option<String>> {
            Err(CliError::config("the keychain is locked"))
        }
        let run = |i: Inputs| {
            resolve_with(
                &i,
                &TomlTable::new(),
                &TomlTable::new(),
                &contexts(),
                stored,
                locked,
            )
        };

        let err = run(inputs()).unwrap_err().to_string();
        assert!(err.contains("the keychain is locked"), "{err}");

        // A recovery command does not read the token.
        let r = run(Inputs {
            unknown_context: UnknownContext::Ignore,
            ..inputs()
        })
        .unwrap();
        assert_eq!(r.context_name.as_deref(), Some("default"));
        assert_eq!(r.personal_access_token, None);

        // An API key is the credential of the run, so the context token is not read.
        let r = run(Inputs {
            api_key: Some("tl_apiKey"),
            ..inputs()
        })
        .unwrap();
        assert_eq!(r.api_key.as_deref(), Some("tl_apiKey"));
        assert_eq!(r.personal_access_token, None);
        assert_eq!(r.project_id.as_deref(), Some("project_default"));

        // `--pat` never reads the context token.
        let r = run(Inputs {
            pat: Some("tl_flag"),
            ..inputs()
        })
        .unwrap();
        assert_eq!(r.personal_access_token.as_deref(), Some("tl_flag"));
    }

    #[test]
    fn the_current_context_supplies_token_and_scope() {
        let r = resolve_test(&inputs(), &TomlTable::new(), &contexts());
        assert_eq!(r.context_name.as_deref(), Some("default"));
        assert_eq!(r.context_source, Some(ContextSource::Current));
        assert_eq!(r.personal_access_token.as_deref(), Some("tl_default"));
        assert_eq!(r.organization_id.as_deref(), Some("org_1"));
        assert_eq!(r.project_id.as_deref(), Some("project_default"));
        assert_eq!(r.api_url, PROD);
    }

    #[test]
    fn a_named_context_is_used_as_a_whole() {
        let r = resolve_test(
            &Inputs {
                context_env: Some("staging"),
                ..inputs()
            },
            &TomlTable::new(),
            &contexts(),
        );
        assert_eq!(r.context_name.as_deref(), Some("staging"));
        assert_eq!(r.context_source, Some(ContextSource::Env));
        assert_eq!(r.personal_access_token.as_deref(), Some("tl_staging"));
        assert_eq!(r.project_id.as_deref(), Some("project_staging"));
    }

    #[test]
    fn the_context_beats_the_local_config() {
        let local = local(
            r#"
organization = "org_local"
project = "project_local"
[tensorlake]
api_url = "https://api.example.test"
"#,
        );
        let r = resolve_test(&inputs(), &local, &contexts());
        assert_eq!(r.api_url, PROD);
        assert_eq!(r.project_id.as_deref(), Some("project_default"));
        assert_eq!(r.organization_id.as_deref(), Some("org_1"));
    }

    #[test]
    fn a_context_for_another_api_url_does_not_apply() {
        // `--api-url` for a URL the context is not for: the old per-URL login is used.
        let r = resolve_test(
            &Inputs {
                api_url: Some("https://api.example.test"),
                ..inputs()
            },
            &TomlTable::new(),
            &contexts(),
        );
        assert_eq!(r.context_name, None);
        assert_eq!(r.personal_access_token, None);
        assert_eq!(r.project_id, None);
    }

    #[test]
    fn the_saved_context_for_the_api_url_stands_in_for_the_current_one() {
        // After the upgrade, the old login for another server is a context that is not
        // current. `--api-url` for that server must find it, as it found the old login.
        let other = "https://api.example.test";
        let mut contexts = contexts();
        contexts.contexts.insert(
            "api-example-test".into(),
            entry(other, "org_2", "project_other"),
        );
        fn token(name: &str, _: &ContextEntry) -> Result<Option<String>> {
            Ok((name == "api-example-test").then(|| "tl_other".to_string()))
        }
        let r = resolve_with(
            &Inputs {
                api_url: Some(other),
                ..inputs()
            },
            &TomlTable::new(),
            &TomlTable::new(),
            &contexts,
            stored,
            token,
        )
        .expect("resolves");
        assert_eq!(r.context_name.as_deref(), Some("api-example-test"));
        assert_eq!(r.context_source, Some(ContextSource::ApiUrl));
        assert_eq!(r.personal_access_token.as_deref(), Some("tl_other"));
        assert_eq!(r.organization_id.as_deref(), Some("org_2"));
        assert_eq!(r.project_id.as_deref(), Some("project_other"));

        // `--pat` brings its own scope: no context stands in.
        let r = resolve_with(
            &Inputs {
                api_url: Some(other),
                pat: Some("tl_pat"),
                ..inputs()
            },
            &TomlTable::new(),
            &TomlTable::new(),
            &contexts,
            stored,
            token,
        )
        .expect("resolves");
        assert_eq!(r.context_name, None);
        assert_eq!(r.personal_access_token.as_deref(), Some("tl_pat"));
    }

    #[test]
    fn a_named_context_for_another_api_url_is_an_error() {
        let other = "https://api.example.test";
        let err = resolve_with(
            &Inputs {
                api_url: Some(other),
                context_flag: Some("staging"),
                ..inputs()
            },
            &TomlTable::new(),
            &TomlTable::new(),
            &contexts(),
            stored,
            token,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.starts_with(
                "context 'staging' is for https://api.tensorlake.ai but the API URL of this run is https://api.example.test."
            ),
            "{err}"
        );
        assert!(err.contains("drop --context"), "{err}");

        let err = resolve_with(
            &Inputs {
                api_url: Some(other),
                context_env: Some("staging"),
                ..inputs()
            },
            &TomlTable::new(),
            &TomlTable::new(),
            &contexts(),
            stored,
            token,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("drop TENSORLAKE_CONTEXT"), "{err}");

        // The recovery commands still run: `tl login --api-url <other>` saves under the name.
        let r = resolve_test(
            &Inputs {
                api_url: Some(other),
                context_flag: Some("staging"),
                unknown_context: UnknownContext::Ignore,
                ..inputs()
            },
            &TomlTable::new(),
            &contexts(),
        );
        assert_eq!(r.context_name, None);
        assert_eq!(r.api_url, other);
    }

    #[test]
    fn a_pat_with_a_named_context_is_an_error() {
        let err = resolve_with(
            &Inputs {
                pat: Some("tl_flag"),
                context_flag: Some("staging"),
                ..inputs()
            },
            &TomlTable::new(),
            &TomlTable::new(),
            &contexts(),
            stored,
            token,
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.starts_with("--pat cannot be used with context 'staging'"),
            "{err}"
        );
    }

    #[test]
    fn no_context_falls_back_to_the_stored_login() {
        let r = resolve_test(&inputs(), &TomlTable::new(), &ContextsFile::default());
        assert_eq!(r.context_name, None);
        assert_eq!(r.personal_access_token.as_deref(), Some("tl_stored"));
        assert_eq!(r.project_id.as_deref(), Some("project_stored"));
        assert_eq!(r.organization_id.as_deref(), Some("org_stored"));

        // The local config fills in what the login did not set.
        let local = local(r#"project = "project_local""#);
        let r = resolve_with(
            &inputs(),
            &local,
            &TomlTable::new(),
            &ContextsFile::default(),
            |_| None,
            token,
        )
        .unwrap();
        assert_eq!(r.personal_access_token, None);
        assert_eq!(r.project_id.as_deref(), Some("project_local"));
    }

    #[test]
    fn a_pat_flag_brings_its_own_scope() {
        let local = local(r#"project = "project_local""#);
        let r = resolve_test(
            &Inputs {
                pat: Some("tl_flag"),
                ..inputs()
            },
            &local,
            &contexts(),
        );
        assert_eq!(r.context_name, None);
        assert_eq!(r.personal_access_token.as_deref(), Some("tl_flag"));
        assert_eq!(r.project_id.as_deref(), Some("project_local"));
    }

    #[test]
    fn a_project_flag_that_matches_the_context_is_fine() {
        let r = resolve_test(
            &Inputs {
                project_id: Some("project_default"),
                organization_id: Some("org_1"),
                ..inputs()
            },
            &TomlTable::new(),
            &contexts(),
        );
        assert_eq!(r.context_name.as_deref(), Some("default"));
        assert_eq!(r.personal_access_token.as_deref(), Some("tl_default"));
    }

    #[test]
    fn a_project_flag_for_another_project_is_an_error() {
        let err = resolve_with(
            &Inputs {
                project_id: Some("project_staging"),
                ..inputs()
            },
            &TomlTable::new(),
            &TomlTable::new(),
            &contexts(),
            stored,
            token,
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.starts_with(
                "--project project_staging does not match context 'default' (project_default)."
            ),
            "{msg}"
        );
        assert!(msg.contains("tl context use <name>"), "{msg}");
        assert!(msg.contains("tl login --context <name>"), "{msg}");

        let err = resolve_with(
            &Inputs {
                organization_id: Some("org_2"),
                ..inputs()
            },
            &TomlTable::new(),
            &TomlTable::new(),
            &contexts(),
            stored,
            token,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .starts_with("--organization org_2 does not match context 'default' (org_1)."),
            "{err}"
        );
    }

    /// `tl context use <name>` and `tl login --context <name>` are what the mismatch error
    /// tells the user to run. They must not fail with the same error.
    #[test]
    fn a_recovery_command_skips_the_context_check() {
        let r = resolve_test(
            &Inputs {
                project_id: Some("project_staging"),
                unknown_context: UnknownContext::Ignore,
                ..inputs()
            },
            &TomlTable::new(),
            &contexts(),
        );
        assert_eq!(r.context_name.as_deref(), Some("default"));
        assert_eq!(r.project_id.as_deref(), Some("project_staging"));
    }

    #[test]
    fn an_api_key_overrides_the_context_check() {
        // The API key is the credential; its own scope wins on introspection.
        let r = resolve_test(
            &Inputs {
                api_key: Some("tl_apiKey"),
                project_id: Some("project_other"),
                ..inputs()
            },
            &TomlTable::new(),
            &contexts(),
        );
        assert_eq!(r.api_key.as_deref(), Some("tl_apiKey"));
        assert_eq!(r.project_id.as_deref(), Some("project_other"));
    }

    #[test]
    fn a_context_with_no_token_leaves_the_token_empty() {
        let mut c = contexts();
        c.contexts
            .insert("empty".into(), entry(PROD, "org_1", "project_empty"));
        let r = resolve_test(
            &Inputs {
                context_flag: Some("empty"),
                ..inputs()
            },
            &TomlTable::new(),
            &c,
        );
        assert_eq!(r.context_name.as_deref(), Some("empty"));
        assert_eq!(r.personal_access_token, None);
        assert_eq!(r.project_id.as_deref(), Some("project_empty"));
    }

    #[test]
    fn cloud_url_follows_the_api_url() {
        assert_eq!(
            cloud_url_from_api_url("https://api.tensorlake.ai"),
            "https://cloud.tensorlake.ai"
        );
        assert_eq!(
            cloud_url_from_api_url("http://localhost:8900"),
            "https://cloud.tensorlake.ai"
        );
    }
}
