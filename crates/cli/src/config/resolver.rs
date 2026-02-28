use crate::config::files::{TomlTable, get_nested_value, load_credentials, load_global_config, load_local_config};

/// Resolved configuration values with source tracking.
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub api_url: String,
    pub cloud_url: String,
    pub namespace: String,
    pub api_key: Option<String>,
    pub personal_access_token: Option<String>,
    pub organization_id: Option<String>,
    pub organization_id_source: Option<ConfigSource>,
    pub project_id: Option<String>,
    pub project_id_source: Option<ConfigSource>,
    pub debug: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConfigSource {
    Cli,
    Config,
}

impl std::fmt::Display for ConfigSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigSource::Cli => write!(f, "CLI flag or environment variable"),
            ConfigSource::Config => write!(f, "local config (.tensorlake/config.toml)"),
        }
    }
}

/// Resolve all configuration from CLI args > env vars > local config > global config > defaults.
/// CLI args and env vars are already merged by clap (via `env` attribute).
pub fn resolve(
    api_url: Option<&str>,
    cloud_url: Option<&str>,
    api_key: Option<&str>,
    pat: Option<&str>,
    namespace: Option<&str>,
    organization_id: Option<&str>,
    project_id: Option<&str>,
    debug: bool,
) -> ResolvedConfig {
    let local_config = load_local_config();
    let global_config = load_global_config();

    let final_api_url = resolve_api_url(api_url, &local_config, &global_config);
    let final_cloud_url = resolve_cloud_url(cloud_url, &final_api_url, &local_config, &global_config);
    let (final_api_key, final_pat) = resolve_auth(api_key, pat, &local_config, &global_config, &final_api_url);
    let final_namespace = resolve_namespace(namespace, &local_config, &global_config);
    let (org_id, org_source, proj_id, proj_source) = resolve_project_config(organization_id, project_id, &local_config);

    ResolvedConfig {
        api_url: final_api_url,
        cloud_url: final_cloud_url,
        namespace: final_namespace,
        api_key: final_api_key,
        personal_access_token: final_pat,
        organization_id: org_id,
        organization_id_source: org_source,
        project_id: proj_id,
        project_id_source: proj_source,
        debug,
    }
}

fn resolve_api_url(cli: Option<&str>, local: &TomlTable, global: &TomlTable) -> String {
    cli.map(|s| s.to_string())
        .or_else(|| get_nested_value(local, "tensorlake.api_url"))
        .or_else(|| get_nested_value(global, "tensorlake.api_url"))
        .unwrap_or_else(|| "https://api.tensorlake.ai".to_string())
}

fn resolve_cloud_url(cli: Option<&str>, api_url: &str, local: &TomlTable, global: &TomlTable) -> String {
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

fn resolve_auth(
    api_key: Option<&str>,
    pat: Option<&str>,
    local: &TomlTable,
    global: &TomlTable,
    api_url: &str,
) -> (Option<String>, Option<String>) {
    let final_api_key = api_key
        .map(|s| s.to_string())
        .or_else(|| get_nested_value(local, "tensorlake.apikey"))
        .or_else(|| get_nested_value(global, "tensorlake.apikey"));

    let file_pat = load_credentials(api_url);
    let final_pat = pat.map(|s| s.to_string()).or(file_pat);

    (final_api_key, final_pat)
}

fn resolve_namespace(cli: Option<&str>, local: &TomlTable, global: &TomlTable) -> String {
    cli.map(|s| s.to_string())
        .or_else(|| get_nested_value(local, "indexify.namespace"))
        .or_else(|| get_nested_value(global, "indexify.namespace"))
        .unwrap_or_else(|| "default".to_string())
}

fn resolve_project_config(
    org_id: Option<&str>,
    proj_id: Option<&str>,
    local: &TomlTable,
) -> (Option<String>, Option<ConfigSource>, Option<String>, Option<ConfigSource>) {
    let (final_org_id, org_source) = if let Some(id) = org_id {
        (Some(id.to_string()), Some(ConfigSource::Cli))
    } else {
        let from_config = get_nested_value(local, "organization");
        let source = if from_config.is_some() { Some(ConfigSource::Config) } else { None };
        (from_config, source)
    };

    let (final_proj_id, proj_source) = if let Some(id) = proj_id {
        (Some(id.to_string()), Some(ConfigSource::Cli))
    } else {
        let from_config = get_nested_value(local, "project");
        let source = if from_config.is_some() { Some(ConfigSource::Config) } else { None };
        (from_config, source)
    };

    (final_org_id, org_source, final_proj_id, proj_source)
}
