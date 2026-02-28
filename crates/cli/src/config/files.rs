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
    content
        .parse::<toml::Value>()
        .ok()
        .and_then(|v| v.as_table().cloned())
        .unwrap_or_default()
}

/// Search upward from cwd for `.tensorlake/config.toml` and load it.
pub fn load_local_config() -> TomlTable {
    if let Some(path) = find_local_config_path() {
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return TomlTable::new(),
        };
        return content
            .parse::<toml::Value>()
            .ok()
            .and_then(|v| v.as_table().cloned())
            .unwrap_or_default();
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
    let table: toml::Value = content.parse().ok()?;
    let scoped = table.get(api_url)?;
    scoped.get("token")?.as_str().map(|s| s.to_string())
}

/// Save PAT to credentials file scoped by API URL.
pub fn save_credentials(api_url: &str, token: &str) -> Result<()> {
    let dir = config_dir();
    fs::create_dir_all(&dir)?;

    let path = credentials_path();
    let mut table: TomlTable = if path.exists() {
        let content = fs::read_to_string(&path)?;
        content
            .parse::<toml::Value>()
            .ok()
            .and_then(|v| v.as_table().cloned())
            .unwrap_or_default()
    } else {
        TomlTable::new()
    };

    let mut section = TomlTable::new();
    section.insert("token".to_string(), toml::Value::String(token.to_string()));
    table.insert(api_url.to_string(), toml::Value::Table(section));

    let content = toml::to_string_pretty(&toml::Value::Table(table))?;
    fs::write(&path, &content)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
    }

    Ok(())
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
