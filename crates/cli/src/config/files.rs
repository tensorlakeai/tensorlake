use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{CliError, Result};

/// Type alias for TOML table (matches what toml crate uses internally).
pub type TomlTable = toml::map::Map<String, toml::Value>;

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
    let path = credentials_path();
    if !path.exists() {
        return None;
    }
    let content = fs::read_to_string(&path).ok()?;
    let table = parse_toml_table(&content)?;
    extract_scoped_token(&table, api_url)
}

/// Save PAT to credentials file scoped by API URL.
pub fn save_credentials(api_url: &str, token: &str) -> Result<()> {
    let dir = config_dir();
    fs::create_dir_all(&dir)?;

    let path = credentials_path();
    let mut table: TomlTable = if path.exists() {
        let content = fs::read_to_string(&path)?;
        parse_toml_table(&content).unwrap_or_default()
    } else {
        TomlTable::new()
    };

    let normalized_url = normalize_api_url(api_url);

    // Collapse equivalent URL keys so we always keep a single canonical entry.
    let keys_to_remove: Vec<String> = table
        .keys()
        .filter(|k| normalize_api_url(k) == normalized_url)
        .cloned()
        .collect();
    for key in keys_to_remove {
        table.remove(&key);
    }

    let mut section = TomlTable::new();
    section.insert("token".to_string(), toml::Value::String(token.to_string()));
    table.insert(normalized_url, toml::Value::Table(section));

    let content = toml::to_string_pretty(&toml::Value::Table(table))?;
    fs::write(&path, &content)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
}

fn extract_scoped_token(credentials: &TomlTable, api_url: &str) -> Option<String> {
    let normalized_api_url = normalize_api_url(api_url);

    // 1) exact key match
    if let Some(token) = credentials
        .get(api_url.trim())
        .and_then(|scoped| scoped.get("token"))
        .and_then(|v| v.as_str())
    {
        return Some(token.to_string());
    }

    // 2) canonical key match
    if let Some(token) = credentials
        .get(&normalized_api_url)
        .and_then(|scoped| scoped.get("token"))
        .and_then(|v| v.as_str())
    {
        return Some(token.to_string());
    }

    // 3) compatible lookup across previously stored URL variants
    for (key, value) in credentials {
        if normalize_api_url(key) == normalized_api_url
            && let Some(token) = value.get("token").and_then(|v| v.as_str())
        {
            return Some(token.to_string());
        }
    }

    // 4) legacy unscoped format: token = "..."
    credentials
        .get("token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
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
    use super::{extract_scoped_token, normalize_api_url};

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
    fn extract_scoped_token_supports_legacy_unscoped_format() {
        let table: super::TomlTable =
            toml::from_str(r#"token = "legacy-token""#).expect("valid toml");
        assert_eq!(
            extract_scoped_token(&table, "https://api.tensorlake.ai").expect("legacy token"),
            "legacy-token"
        );
    }
}
