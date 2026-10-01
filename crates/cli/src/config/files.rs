use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{CliError, Result};

/// Type alias for TOML table (matches what toml crate uses internally).
pub type TomlTable = toml::map::Map<String, toml::Value>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoredCredentials {
    pub token: String,
    pub organization_id: Option<String>,
    pub project_id: Option<String>,
}

/// Global config directory: ~/.config/tensorlake/
pub fn config_dir() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".config")
        .join("tensorlake")
}

/// Global config file: ~/.config/tensorlake/.tensorlake_config
pub fn global_config_path() -> PathBuf {
    config_dir().join(".tensorlake_config")
}

/// Credentials file: ~/.config/tensorlake/credentials.toml
pub fn credentials_path() -> PathBuf {
    config_dir().join("credentials.toml")
}

/// Key of the table in `credentials.toml` that holds one token for each context.
pub const CONTEXT_TOKENS_KEY: &str = "contexts";

/// The token saved for one context in `credentials.toml`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextToken {
    pub token: String,
    /// `true` for a login token (a parent). `false` for a token minted from a parent.
    pub parent: bool,
}

/// Cached minted git credentials: ~/.config/tensorlake/git-credentials.toml
///
/// Minted artifact-storage tokens are short-lived and project/repo-scoped, not per-mount, so they
/// cache globally (same convention and permissions as `credentials.toml`) instead of inside a
/// workspace directory. Keyed by `<normalized api url>|<project>|<repo scope>`.
///
/// Shared by the `tl fs` mount stack and the `tl git credential-helper` git integration, which
/// runs on every `git fetch`/`git push` and must not pay a mint round trip each time.
pub fn git_credentials_path() -> PathBuf {
    config_dir().join("git-credentials.toml")
}

fn git_credential_key(api_url: &str, project: &str, scope: &str) -> String {
    format!("{}|{project}|{scope}", normalize_api_url(api_url))
}

/// A cached minted git credential, returned only while comfortably inside its validity window.
pub fn load_git_credential(
    api_url: &str,
    project: &str,
    scope: &str,
) -> Option<(String, String, String)> {
    const EXPIRY_MARGIN_SECS: i64 = 120;
    let content = fs::read_to_string(git_credentials_path()).ok()?;
    let table = parse_toml_table(&content)?;
    let section = table.get(&git_credential_key(api_url, project, scope))?;
    let get = |k: &str| section.get(k).and_then(|v| v.as_str()).map(str::to_string);
    let (username, token, expires_at) = (get("username")?, get("token")?, get("expires_at")?);
    let expires = chrono::DateTime::parse_from_rfc3339(&expires_at).ok()?;
    if expires.timestamp() - chrono::Utc::now().timestamp() < EXPIRY_MARGIN_SECS {
        return None;
    }
    Some((username, token, expires_at))
}

pub fn save_git_credential(
    api_url: &str,
    project: &str,
    scope: &str,
    username: &str,
    token: &str,
    expires_at: &str,
) -> Result<()> {
    let dir = config_dir();
    fs::create_dir_all(&dir)?;
    let path = git_credentials_path();
    let mut table: TomlTable = if path.exists() {
        let content = fs::read_to_string(&path)?;
        parse_toml_table(&content).unwrap_or_default()
    } else {
        TomlTable::new()
    };
    // Drop entries that have already expired while we're here, so the file doesn't accrete.
    let now = chrono::Utc::now().timestamp();
    table.retain(|_, v| {
        v.get("expires_at")
            .and_then(|e| e.as_str())
            .and_then(|e| chrono::DateTime::parse_from_rfc3339(e).ok())
            .is_some_and(|e| e.timestamp() > now)
    });
    let mut section = TomlTable::new();
    section.insert("username".into(), toml::Value::String(username.into()));
    section.insert("token".into(), toml::Value::String(token.into()));
    section.insert("expires_at".into(), toml::Value::String(expires_at.into()));
    table.insert(
        git_credential_key(api_url, project, scope),
        toml::Value::Table(section),
    );
    let content = toml::to_string_pretty(&toml::Value::Table(table))?;
    fs::write(&path, &content)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Purge the minted-git-credential cache (e.g. after an authentication failure, so the next run
/// re-mints instead of retrying a revoked token).
pub fn purge_git_credentials() {
    let _ = fs::remove_file(git_credentials_path());
}

/// Normalize API URL values for credential table keying.
///
/// This avoids mismatches between equivalent URLs like:
/// - https://api.tensorlake.ai
/// - https://api.tensorlake.ai/
/// - https://api.tensorlake.ai:443/
pub fn normalize_api_url(api_url: &str) -> String {
    let trimmed = api_url.trim();
    if let Ok(mut parsed) = url::Url::parse(trimmed) {
        parsed.set_fragment(None);
        parsed.set_query(None);

        if (parsed.scheme() == "https" && parsed.port() == Some(443))
            || (parsed.scheme() == "http" && parsed.port() == Some(80))
        {
            let _ = parsed.set_port(None);
        }

        let mut normalized = parsed.to_string();
        while normalized.ends_with('/') {
            normalized.pop();
        }
        normalized
    } else {
        trimmed.trim_end_matches('/').to_string()
    }
}

fn parse_toml_table(content: &str) -> Option<TomlTable> {
    toml::from_str(content).ok()
}

/// Load the global config file as a TOML table.
pub fn load_global_config() -> TomlTable {
    let path = global_config_path();
    if !path.exists() {
        return TomlTable::new();
    }
    let content = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(_) => return TomlTable::new(),
    };
    parse_toml_table(&content).unwrap_or_default()
}

/// Search upward from cwd for `.tensorlake/config.toml` and load it.
pub fn load_local_config() -> TomlTable {
    if let Some(path) = find_local_config_path() {
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return TomlTable::new(),
        };
        return parse_toml_table(&content).unwrap_or_default();
    }
    TomlTable::new()
}

/// Find the local config path by searching upward from cwd.
pub fn find_local_config_path() -> Option<PathBuf> {
    let current = std::env::current_dir().ok()?;
    let mut dir = current.as_path();
    loop {
        let config = dir.join(".tensorlake").join("config.toml");
        if config.exists() {
            return Some(config);
        }
        match dir.parent() {
            Some(parent) => dir = parent,
            None => return None,
        }
    }
}

/// Save local config to `.tensorlake/config.toml` at project_root.
pub fn save_local_config(config: &TomlTable, project_root: &Path) -> Result<()> {
    let config_dir = project_root.join(".tensorlake");
    if config_dir.exists() && !config_dir.is_dir() {
        return Err(CliError::config(format!(
            "Cannot create configuration directory: '{}' exists as a file",
            config_dir.display()
        )));
    }
    fs::create_dir_all(&config_dir)?;
    let config_path = config_dir.join("config.toml");

    let value = toml::Value::Table(config.clone());
    let content = toml::to_string_pretty(&value)?;
    fs::write(&config_path, &content)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&config_path, fs::Permissions::from_mode(0o600))?;
    }

    // Add .tensorlake/ to .gitignore
    if let Some(gitignore) = find_gitignore_path(project_root) {
        let _ = add_to_gitignore(&gitignore, ".tensorlake/");
    } else {
        let gitignore = project_root.join(".gitignore");
        let _ = add_to_gitignore(&gitignore, ".tensorlake/");
    }

    Ok(())
}

/// Load PAT from credentials file for the given API URL.
pub fn load_credentials(api_url: &str) -> Option<String> {
    load_stored_credentials(api_url).map(|credentials| credentials.token)
}

/// Load PAT and selected scope from credentials file for the given API URL.
pub fn load_stored_credentials(api_url: &str) -> Option<StoredCredentials> {
    let path = credentials_path();
    if !path.exists() {
        return None;
    }
    let content = fs::read_to_string(&path).ok()?;
    let table = parse_toml_table(&content)?;
    extract_scoped_credentials(&table, api_url)
}

/// Read `credentials.toml` as a table. A missing or unreadable file gives an empty table.
pub fn load_credentials_table() -> TomlTable {
    let path = credentials_path();
    if !path.exists() {
        return TomlTable::new();
    }
    fs::read_to_string(&path)
        .ok()
        .and_then(|content| parse_toml_table(&content))
        .unwrap_or_default()
}

/// Write `credentials.toml` with mode 0600.
pub fn write_credentials_table(table: &TomlTable) -> Result<()> {
    let dir = config_dir();
    fs::create_dir_all(&dir)?;
    let path = credentials_path();
    let content = toml::to_string_pretty(&toml::Value::Table(table.clone()))?;
    fs::write(&path, &content)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
}

/// Save PAT to credentials file scoped by API URL.
///
/// This per-URL table is what older CLI versions read. It always holds a copy of the
/// current context's token.
pub fn save_credentials(
    api_url: &str,
    token: &str,
    organization_id: Option<&str>,
    project_id: Option<&str>,
) -> Result<()> {
    let mut table = load_credentials_table();
    set_scoped_credentials(&mut table, api_url, token, organization_id, project_id);
    write_credentials_table(&table)
}

/// Remove the per-URL table for `api_url`, and the legacy unscoped token if the file has one.
pub fn remove_credentials(api_url: &str) -> Result<()> {
    let mut table = load_credentials_table();
    remove_url_tables(&mut table, api_url);
    write_credentials_table(&table)
}

/// Key of the legacy unscoped `token = "..."` entry at the top of `credentials.toml`.
const LEGACY_TOKEN_KEY: &str = "token";

/// Remove every entry that `extract_scoped_credentials` could return for `api_url`.
///
/// That is the per-URL table under each spelling of the URL, and the legacy unscoped token,
/// which the lookup falls back to for any URL. Leaving the legacy token behind would keep the
/// CLI logged in after `tl logout` or `tl context delete`.
fn remove_url_tables(table: &mut TomlTable, api_url: &str) {
    let normalized_url = normalize_api_url(api_url);
    // Collapse equivalent URL keys so we always keep a single canonical entry.
    let keys_to_remove: Vec<String> = table
        .keys()
        .filter(|k| k.as_str() != CONTEXT_TOKENS_KEY && normalize_api_url(k) == normalized_url)
        .cloned()
        .collect();
    for key in keys_to_remove {
        table.remove(&key);
    }
    table.remove(LEGACY_TOKEN_KEY);
}

pub(crate) fn set_scoped_credentials(
    table: &mut TomlTable,
    api_url: &str,
    token: &str,
    organization_id: Option<&str>,
    project_id: Option<&str>,
) {
    remove_url_tables(table, api_url);

    let mut section = TomlTable::new();
    section.insert("token".to_string(), toml::Value::String(token.to_string()));
    if let Some(organization_id) = organization_id {
        section.insert(
            "organization".to_string(),
            toml::Value::String(organization_id.to_string()),
        );
    }
    if let Some(project_id) = project_id {
        section.insert(
            "project".to_string(),
            toml::Value::String(project_id.to_string()),
        );
    }
    table.insert(normalize_api_url(api_url), toml::Value::Table(section));
}

/// Load the token saved for context `name`.
pub fn load_context_token(name: &str) -> Option<ContextToken> {
    context_token_from_table(&load_credentials_table(), name)
}

/// Save the token for context `name`. `parent` is `true` for a login token.
pub fn save_context_token(name: &str, token: &str, parent: bool) -> Result<()> {
    let mut table = load_credentials_table();
    set_context_token(&mut table, name, token, parent);
    write_credentials_table(&table)
}

/// Remove the token saved for context `name`. A missing token is not an error.
pub fn remove_context_token(name: &str) -> Result<()> {
    let mut table = load_credentials_table();
    if remove_context_token_from_table(&mut table, name) {
        write_credentials_table(&table)?;
    }
    Ok(())
}

/// Move the token of context `old` to context `new`.
pub fn rename_context_token(old: &str, new: &str) -> Result<()> {
    let mut table = load_credentials_table();
    if let Some(token) = context_token_from_table(&table, old) {
        remove_context_token_from_table(&mut table, old);
        set_context_token(&mut table, new, &token.token, token.parent);
        write_credentials_table(&table)?;
    }
    Ok(())
}

pub(crate) fn context_token_from_table(table: &TomlTable, name: &str) -> Option<ContextToken> {
    let section = table.get(CONTEXT_TOKENS_KEY)?.get(name)?;
    let token = section.get("token")?.as_str()?.to_string();
    let parent = section
        .get("parent")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    Some(ContextToken { token, parent })
}

pub(crate) fn set_context_token(table: &mut TomlTable, name: &str, token: &str, parent: bool) {
    let contexts = match table.get_mut(CONTEXT_TOKENS_KEY) {
        Some(toml::Value::Table(t)) => t,
        _ => {
            table.insert(
                CONTEXT_TOKENS_KEY.to_string(),
                toml::Value::Table(TomlTable::new()),
            );
            match table.get_mut(CONTEXT_TOKENS_KEY) {
                Some(toml::Value::Table(t)) => t,
                _ => unreachable!("just inserted a table"),
            }
        }
    };
    let mut section = TomlTable::new();
    section.insert("token".to_string(), toml::Value::String(token.to_string()));
    section.insert("parent".to_string(), toml::Value::Boolean(parent));
    contexts.insert(name.to_string(), toml::Value::Table(section));
}

fn remove_context_token_from_table(table: &mut TomlTable, name: &str) -> bool {
    match table.get_mut(CONTEXT_TOKENS_KEY) {
        Some(toml::Value::Table(t)) => t.remove(name).is_some(),
        _ => false,
    }
}

/// All per-URL tables in `credentials.toml` as `(normalized api url, credentials)`.
///
/// The legacy unscoped `token = "..."` format counts as the default API URL.
pub(crate) fn all_scoped_credentials(table: &TomlTable) -> Vec<(String, StoredCredentials)> {
    let mut out = Vec::new();
    for (key, value) in table {
        if key == CONTEXT_TOKENS_KEY {
            continue;
        }
        if let Some(credentials) = credentials_from_value(value) {
            out.push((normalize_api_url(key), credentials));
        }
    }
    if out.is_empty()
        && let Some(token) = table.get(LEGACY_TOKEN_KEY).and_then(|v| v.as_str())
    {
        out.push((
            DEFAULT_API_URL.to_string(),
            StoredCredentials {
                token: token.to_string(),
                organization_id: None,
                project_id: None,
            },
        ));
    }
    out
}

/// The API URL the CLI uses when nothing else sets one.
pub const DEFAULT_API_URL: &str = "https://api.tensorlake.ai";

#[cfg(test)]
fn extract_scoped_token(credentials: &TomlTable, api_url: &str) -> Option<String> {
    extract_scoped_credentials(credentials, api_url).map(|credentials| credentials.token)
}

fn credentials_from_value(value: &toml::Value) -> Option<StoredCredentials> {
    let token = value.get("token").and_then(|v| v.as_str())?.to_string();
    let organization_id = value
        .get("organization")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let project_id = value
        .get("project")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Some(StoredCredentials {
        token,
        organization_id,
        project_id,
    })
}

fn extract_scoped_credentials(credentials: &TomlTable, api_url: &str) -> Option<StoredCredentials> {
    let normalized_api_url = normalize_api_url(api_url);

    // 1) exact key match
    if let Some(credentials) = credentials
        .get(api_url.trim())
        .and_then(credentials_from_value)
    {
        return Some(credentials);
    }

    // 2) canonical key match
    if let Some(credentials) = credentials
        .get(&normalized_api_url)
        .and_then(credentials_from_value)
    {
        return Some(credentials);
    }

    // 3) compatible lookup across previously stored URL variants
    for (key, value) in credentials {
        if key != CONTEXT_TOKENS_KEY
            && normalize_api_url(key) == normalized_api_url
            && let Some(credentials) = credentials_from_value(value)
        {
            return Some(credentials);
        }
    }

    // 4) legacy unscoped format: token = "..."
    credentials
        .get(LEGACY_TOKEN_KEY)
        .and_then(|v| v.as_str())
        .map(|s| StoredCredentials {
            token: s.to_string(),
            organization_id: None,
            project_id: None,
        })
}

/// Get a nested value from a TOML table using dot notation (e.g. "tensorlake.api_url").
pub fn get_nested_value(config: &TomlTable, key: &str) -> Option<String> {
    let keys: Vec<&str> = key.split('.').collect();
    let mut current: &toml::Value = &toml::Value::Table(config.clone());
    for k in &keys {
        current = current.get(k)?;
    }
    current.as_str().map(|s| s.to_string())
}

/// Find the .gitignore at the git repo root.
fn find_gitignore_path(start: &Path) -> Option<PathBuf> {
    let mut dir = start;
    loop {
        if dir.join(".git").is_dir() {
            return Some(dir.join(".gitignore"));
        }
        match dir.parent() {
            Some(parent) => dir = parent,
            None => return None,
        }
    }
}

/// Add entry to .gitignore if not already present.
fn add_to_gitignore(path: &Path, entry: &str) -> Result<()> {
    if path.exists() {
        let content = fs::read_to_string(path)?;
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed == entry || trimmed == format!("/{}", entry) {
                return Ok(());
            }
        }
        let mut new_content = content;
        if !new_content.ends_with('\n') && !new_content.is_empty() {
            new_content.push('\n');
        }
        new_content.push_str(entry);
        new_content.push('\n');
        fs::write(path, new_content)?;
    } else {
        fs::write(path, format!("{}\n", entry))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        all_scoped_credentials, context_token_from_table, extract_scoped_credentials,
        extract_scoped_token, normalize_api_url, remove_url_tables, set_context_token,
        set_scoped_credentials,
    };

    #[test]
    fn normalize_api_url_collapses_common_equivalents() {
        let base = "https://api.tensorlake.ai";
        assert_eq!(normalize_api_url(base), base);
        assert_eq!(normalize_api_url("https://api.tensorlake.ai/"), base);
        assert_eq!(normalize_api_url("https://api.tensorlake.ai:443/"), base);
        assert_eq!(normalize_api_url("https://api.tensorlake.ai///"), base);
    }

    #[test]
    fn extract_scoped_token_handles_url_variants() {
        let content = r#"
["https://api.tensorlake.ai"]
token = "abc123"
"#;
        let table: super::TomlTable = toml::from_str(content).expect("valid toml");

        assert_eq!(
            extract_scoped_token(&table, "https://api.tensorlake.ai").expect("token for exact key"),
            "abc123"
        );
        assert_eq!(
            extract_scoped_token(&table, "https://api.tensorlake.ai/")
                .expect("token for normalized key"),
            "abc123"
        );
        assert_eq!(
            extract_scoped_token(&table, "https://api.tensorlake.ai:443")
                .expect("token for default-port key"),
            "abc123"
        );
    }

    #[test]
    fn extract_scoped_credentials_includes_selected_scope() {
        let content = r#"
["https://api.tensorlake.ai"]
token = "abc123"
organization = "org_123"
project = "project_456"
"#;
        let table: super::TomlTable = toml::from_str(content).expect("valid toml");

        let credentials = extract_scoped_credentials(&table, "https://api.tensorlake.ai/")
            .expect("stored credentials");

        assert_eq!(credentials.token, "abc123");
        assert_eq!(credentials.organization_id.as_deref(), Some("org_123"));
        assert_eq!(credentials.project_id.as_deref(), Some("project_456"));
    }

    #[test]
    fn extract_scoped_token_supports_legacy_unscoped_format() {
        let table: super::TomlTable =
            toml::from_str(r#"token = "legacy-token""#).expect("valid toml");
        assert_eq!(
            extract_scoped_token(&table, "https://api.tensorlake.ai").expect("legacy token"),
            "legacy-token"
        );
    }

    #[test]
    fn context_tokens_live_beside_the_per_url_table() {
        let mut table = super::TomlTable::new();
        set_scoped_credentials(
            &mut table,
            "https://api.tensorlake.ai/",
            "login-token",
            Some("org_1"),
            Some("project_1"),
        );
        set_context_token(&mut table, "default", "login-token", true);
        set_context_token(&mut table, "staging", "minted-token", false);

        // The per-URL table is unchanged, so an older CLI still finds its token.
        let legacy = extract_scoped_credentials(&table, "https://api.tensorlake.ai")
            .expect("per-URL credentials");
        assert_eq!(legacy.token, "login-token");
        assert_eq!(legacy.project_id.as_deref(), Some("project_1"));

        let default = context_token_from_table(&table, "default").expect("default token");
        assert_eq!(default.token, "login-token");
        assert!(default.parent);
        let staging = context_token_from_table(&table, "staging").expect("staging token");
        assert_eq!(staging.token, "minted-token");
        assert!(!staging.parent);
        assert!(context_token_from_table(&table, "missing").is_none());

        // The file round-trips through TOML.
        let text = toml::to_string_pretty(&toml::Value::Table(table)).expect("serialize");
        let parsed: super::TomlTable = toml::from_str(&text).expect("parse");
        assert_eq!(
            context_token_from_table(&parsed, "staging")
                .expect("staging")
                .token,
            "minted-token"
        );
        // The context table is never mistaken for a URL table.
        let urls: Vec<String> = all_scoped_credentials(&parsed)
            .into_iter()
            .map(|(url, _)| url)
            .collect();
        assert_eq!(urls, vec!["https://api.tensorlake.ai".to_string()]);
    }

    #[test]
    fn remove_url_tables_drops_the_legacy_unscoped_token() {
        let content = r#"
token = "legacy-token"

[contexts.default]
token = "legacy-token"
parent = true
"#;
        let mut table: super::TomlTable = toml::from_str(content).expect("valid toml");
        assert!(extract_scoped_credentials(&table, "https://api.tensorlake.ai").is_some());

        remove_url_tables(&mut table, "https://api.tensorlake.ai");

        assert!(
            extract_scoped_credentials(&table, "https://api.tensorlake.ai").is_none(),
            "no token must survive removal: {table:?}"
        );
        assert!(all_scoped_credentials(&table).is_empty());
        // Context tokens are removed separately, so they stay.
        assert_eq!(
            context_token_from_table(&table, "default")
                .expect("context token")
                .token,
            "legacy-token"
        );
    }

    #[test]
    fn all_scoped_credentials_reads_the_legacy_unscoped_format() {
        let table: super::TomlTable =
            toml::from_str(r#"token = "legacy-token""#).expect("valid toml");
        let all = all_scoped_credentials(&table);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, super::DEFAULT_API_URL);
        assert_eq!(all[0].1.token, "legacy-token");
    }
}
