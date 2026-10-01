//! `~/.config/tensorlake/contexts.toml`: the named contexts and which one is current.
//!
//! This file holds no secrets, so users can read, edit, and share it. The token for each
//! context lives in `credentials.toml` (see `files::save_context_token`).

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::files::{
    DEFAULT_API_URL, StoredCredentials, TomlTable, all_scoped_credentials, config_dir,
    load_credentials_table, normalize_api_url, set_context_token, write_credentials_table,
};
use crate::error::{CliError, Result};

/// The name `tl login` uses when the user gives none.
pub const DEFAULT_CONTEXT_NAME: &str = "default";

/// One named context: an API URL and the organization and project it works in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextEntry {
    pub api_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organization: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

/// The whole `contexts.toml` file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextsFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<String>,
    #[serde(default)]
    pub contexts: BTreeMap<String, ContextEntry>,
}

impl ContextsFile {
    pub fn get(&self, name: &str) -> Option<&ContextEntry> {
        self.contexts.get(name)
    }

    /// The current context, if `current` names one that exists.
    pub fn current_entry(&self) -> Option<(&str, &ContextEntry)> {
        let name = self.current.as_deref()?;
        self.contexts.get(name).map(|entry| (name, entry))
    }
}

/// Check that `name` is safe to use as a context name and a TOML key.
pub fn validate_context_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.');
    if ok {
        Ok(())
    } else {
        Err(CliError::usage(format!(
            "invalid context name '{name}': use 1-64 letters, digits, '-', '_' or '.'"
        )))
    }
}

/// Load `contexts.toml` to read it. On the first run, build it from the tokens in
/// `credentials.toml`.
///
/// A file that cannot be read or parsed is reported once on stderr and read as empty, so a
/// command that needs no context (an API key, a PAT) still runs. Commands that change the
/// file use [`load_contexts_for_update`], which refuses such a file.
pub fn load_contexts() -> ContextsFile {
    load_contexts_in(&config_dir()).unwrap_or_else(|e| {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| eprintln!("warning: {e}"));
        ContextsFile::default()
    })
}

/// Load `contexts.toml` to change and save it.
///
/// A file that cannot be read or parsed is an error. Saving over it would write back only
/// the contexts of this run and lose all others.
pub fn load_contexts_for_update() -> Result<ContextsFile> {
    load_contexts_in(&config_dir())
}

fn load_contexts_in(dir: &Path) -> Result<ContextsFile> {
    let path = dir.join("contexts.toml");
    if path.exists() {
        return read_contexts_file(&path);
    }

    // First run: migrate the saved login(s) into a context each.
    let mut credentials = load_credentials_table();
    let (contexts, tokens) = migrate_from_credentials(&credentials);
    if contexts.contexts.is_empty() {
        return Ok(contexts);
    }
    for (name, token) in &tokens {
        set_context_token(&mut credentials, name, token);
    }
    // Best effort: a read-only home directory must not stop the command.
    let _ = write_credentials_table(&credentials);
    let _ = save_contexts_in(&contexts, dir);
    Ok(contexts)
}

fn read_contexts_file(path: &Path) -> Result<ContextsFile> {
    let content = fs::read_to_string(path)
        .map_err(|e| CliError::config(format!("cannot read {}: {e}", path.display())))?;
    toml::from_str(&content).map_err(|e: toml::de::Error| {
        CliError::config(format!(
            "{} does not parse: {}. fix the file or move it away, then run the command again",
            path.display(),
            e.message()
        ))
    })
}

/// Write `contexts.toml`.
pub fn save_contexts(contexts: &ContextsFile) -> Result<()> {
    save_contexts_in(contexts, &config_dir())
}

fn save_contexts_in(contexts: &ContextsFile, dir: &Path) -> Result<()> {
    fs::create_dir_all(dir)?;
    let content = toml::to_string_pretty(contexts)?;
    fs::write(dir.join("contexts.toml"), content)?;
    Ok(())
}

/// Build contexts from the per-URL tables of an old `credentials.toml`.
///
/// Returns the contexts and the `(name, token)` pairs to save beside them. The table for the
/// default API URL becomes `default`. A lone table for another URL also becomes `default`.
/// Other tables are named after their host, for example `api-staging-tensorlake-ai`.
pub(crate) fn migrate_from_credentials(
    credentials: &TomlTable,
) -> (ContextsFile, Vec<(String, String)>) {
    let mut file = ContextsFile::default();
    let mut tokens = Vec::new();
    let saved: Vec<(String, StoredCredentials)> = all_scoped_credentials(credentials);
    let has_default_url = saved
        .iter()
        .any(|(url, _)| url == &normalize_api_url(DEFAULT_API_URL));

    for (url, stored) in saved {
        let name = if url == normalize_api_url(DEFAULT_API_URL)
            || (!has_default_url && file.contexts.is_empty())
        {
            DEFAULT_CONTEXT_NAME.to_string()
        } else {
            context_name_from_url(&url)
        };
        let mut name = name;
        let base = name.clone();
        let mut n = 2;
        while file.contexts.contains_key(&name) {
            name = format!("{base}-{n}");
            n += 1;
        }
        file.contexts.insert(
            name.clone(),
            ContextEntry {
                api_url: url,
                organization: stored.organization_id,
                project: stored.project_id,
            },
        );
        tokens.push((name, stored.token));
    }

    if file.contexts.contains_key(DEFAULT_CONTEXT_NAME) {
        file.current = Some(DEFAULT_CONTEXT_NAME.to_string());
    } else {
        file.current = file.contexts.keys().next().cloned();
    }
    (file, tokens)
}

fn context_name_from_url(url: &str) -> String {
    let host = url::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_else(|| url.to_string());
    let name: String = host
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    let name = name.trim_matches('-').to_string();
    if name.is_empty() {
        "context".to_string()
    } else {
        name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_broken_file_is_an_error_and_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("contexts.toml");
        let broken = "current = \"default\"\n[contexts.default\napi_url = \"x\"\n";
        fs::write(&path, broken).unwrap();

        let err = load_contexts_in(dir.path()).unwrap_err().to_string();
        assert!(err.contains("contexts.toml does not parse"), "{err}");
        assert!(err.contains("fix the file or move it away"), "{err}");
        assert_eq!(fs::read_to_string(&path).unwrap(), broken);
    }

    fn entry(api_url: &str, org: Option<&str>, project: Option<&str>) -> ContextEntry {
        ContextEntry {
            api_url: api_url.to_string(),
            organization: org.map(str::to_string),
            project: project.map(str::to_string),
        }
    }

    #[test]
    fn migration_names_the_default_url_default() {
        let table: TomlTable = toml::from_str(
            r#"
["https://api.tensorlake.ai"]
token = "prod-token"
organization = "org_1"
project = "project_1"

["https://api.staging.tensorlake.ai/"]
token = "staging-token"
"#,
        )
        .expect("valid toml");
        let (file, tokens) = migrate_from_credentials(&table);
        assert_eq!(file.current.as_deref(), Some("default"));
        let default = file.get("default").expect("default context");
        assert_eq!(default.api_url, "https://api.tensorlake.ai");
        assert_eq!(default.project.as_deref(), Some("project_1"));
        let staging = file
            .get("api-staging-tensorlake-ai")
            .expect("staging context");
        assert_eq!(staging.api_url, "https://api.staging.tensorlake.ai");
        assert_eq!(staging.project, None);
        let mut tokens = tokens;
        tokens.sort();
        assert_eq!(
            tokens,
            vec![
                (
                    "api-staging-tensorlake-ai".to_string(),
                    "staging-token".to_string()
                ),
                ("default".to_string(), "prod-token".to_string()),
            ]
        );
    }

    #[test]
    fn migration_of_a_lone_other_url_is_default() {
        let table: TomlTable = toml::from_str(
            r#"
["http://localhost:8900"]
token = "dev-token"
"#,
        )
        .expect("valid toml");
        let (file, _) = migrate_from_credentials(&table);
        assert_eq!(file.current.as_deref(), Some("default"));
        assert_eq!(
            file.get("default").expect("default").api_url,
            "http://localhost:8900"
        );
    }

    #[test]
    fn migration_of_an_empty_file_makes_no_context() {
        let (file, tokens) = migrate_from_credentials(&TomlTable::new());
        assert!(file.contexts.is_empty());
        assert!(tokens.is_empty());
        assert_eq!(file.current, None);
    }

    #[test]
    fn contexts_file_round_trips() {
        let mut file = ContextsFile {
            current: Some("default".into()),
            ..Default::default()
        };
        file.contexts.insert(
            "default".into(),
            entry(
                "https://api.tensorlake.ai",
                Some("org_1"),
                Some("project_1"),
            ),
        );
        let text = toml::to_string_pretty(&file).expect("serialize");
        assert!(!text.contains("token"), "no secrets: {text}");
        let parsed: ContextsFile = toml::from_str(&text).expect("parse");
        assert_eq!(parsed, file);
    }

    #[test]
    fn context_names_are_checked() {
        assert!(validate_context_name("default").is_ok());
        assert!(validate_context_name("my-ctx_1.b").is_ok());
        assert!(validate_context_name("").is_err());
        assert!(validate_context_name("has space").is_err());
        assert!(validate_context_name("a/b").is_err());
    }
}
