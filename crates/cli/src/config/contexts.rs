//! `~/.config/tensorlake/contexts.toml`: the named contexts and which one is current.
//!
//! This file holds no secrets, so users can read, edit, and share it. The token for each
//! context lives in the OS keychain, or in `credentials.toml` where there is no keychain;
//! `storage` on each context says which (see `config::token_store`).

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::config::files::{
    DEFAULT_API_URL, StoredCredentials, TomlTable, all_scoped_credentials, config_dir,
    load_credentials_table_in, normalize_api_url, remove_url_credentials_in, write_file_atomically,
};
use crate::config::token_store::{Backend, TokenStorage};
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
    /// Where the token is. `None` on a context written by an older version of this CLI,
    /// which kept every token in `credentials.toml`; the next run moves it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<TokenStorage>,
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

/// Check that no saved context has `name` in another case. `staging` and `STAGING` are one
/// item in the Windows keychain, so two such contexts would share one token. A saved
/// context with exactly this name is fine: a login refreshes it.
pub fn check_no_case_clash(contexts: &ContextsFile, name: &str) -> Result<()> {
    let clash = contexts
        .contexts
        .keys()
        .find(|saved| saved.as_str() != name && saved.eq_ignore_ascii_case(name));
    match clash {
        Some(saved) => Err(CliError::usage(format!(
            "context name '{name}' differs from the saved context '{saved}' only by case. \
             the Windows keychain does not tell such names apart, so both would share one \
             token. use '{saved}', or choose a name that differs in more than case"
        ))),
        None => Ok(()),
    }
}

/// Load `contexts.toml` to read it. On the first run, build it from the tokens in
/// `credentials.toml`.
///
/// A file that cannot be read or parsed is reported once on stderr and read as empty, so a
/// command that needs no context (an API key, a PAT) still runs. Commands that change the
/// file use [`load_contexts_for_update`], which refuses such a file.
///
/// An invalid `TENSORLAKE_TOKEN_STORAGE` is an error for every command. Read as "no
/// contexts", it would turn a typo into "not logged in" while the token is right there.
pub fn load_contexts() -> Result<ContextsFile> {
    let backend = Backend::from_env()?;
    Ok(
        load_contexts_in(&config_dir(), &backend).unwrap_or_else(|e| {
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| eprintln!("warning: {e}"));
            ContextsFile::default()
        }),
    )
}

/// Load `contexts.toml` to change and save it.
///
/// A file that cannot be read or parsed is an error. Saving over it would write back only
/// the contexts of this run and lose all others.
pub fn load_contexts_for_update() -> Result<ContextsFile> {
    load_contexts_in(&config_dir(), &Backend::from_env()?)
}

fn load_contexts_in(dir: &Path, backend: &Backend) -> Result<ContextsFile> {
    let path = dir.join("contexts.toml");
    if path.exists() {
        let mut contexts = read_contexts_file(&path)?;
        // Tokens that an older version left in `credentials.toml` move to the keychain. A
        // keychain that will not take them is reported once; the tokens stay in the file
        // and keep working from there.
        match migrate_tokens(&mut contexts, dir, backend) {
            Ok(true) => save_contexts_in(&contexts, dir)?,
            Ok(false) => {}
            Err(e) => {
                static WARNED: std::sync::Once = std::sync::Once::new();
                WARNED.call_once(|| eprintln!("warning: {e}"));
            }
        }
        return Ok(contexts);
    }

    // First run: migrate the saved login(s) into a context each.
    let credentials = load_credentials_table_in(dir);
    let (mut contexts, tokens) = migrate_from_credentials(&credentials);
    if contexts.contexts.is_empty() {
        return Ok(contexts);
    }
    // If a token cannot be saved, the new context would have none, and a logged-in user
    // would be asked to log in again. Report it and let the old per-URL login stay in use.
    for (name, token) in &tokens {
        let storage = backend.save(dir, name, token, None).map_err(|e| {
            CliError::config(format!("cannot save the token of context '{name}': {e}"))
        })?;
        if let Some(entry) = contexts.contexts.get_mut(name) {
            entry.storage = Some(storage);
        }
    }
    save_contexts_in(&contexts, dir)?;
    // The per-URL tables are now copies of the context tokens. Best effort: a copy that
    // stays is swept by `tl logout --all`.
    let _ = remove_url_credentials_in(dir, &context_urls(&contexts));
    Ok(contexts)
}

/// The API URL of each context, for the per-URL tables that are copies of their tokens.
fn context_urls(contexts: &ContextsFile) -> Vec<String> {
    contexts
        .contexts
        .values()
        .map(|entry| entry.api_url.clone())
        .collect()
}

/// Move the tokens of contexts that do not say where their token is. Returns whether any
/// context changed. Stops at the first token that cannot move; the ones before it stay
/// moved and recorded.
fn migrate_tokens(contexts: &mut ContextsFile, dir: &Path, backend: &Backend) -> Result<bool> {
    let mut changed = false;
    let mut error = None;
    for (name, entry) in contexts.contexts.iter_mut() {
        if entry.storage.is_some() {
            continue;
        }
        match backend.migrate_from_file(dir, name) {
            Ok(Some(storage)) => {
                entry.storage = Some(storage);
                changed = true;
            }
            Ok(None) => {}
            Err(e) => {
                error = Some(CliError::config(format!(
                    "the token of context '{name}' stays in credentials.toml: {e}"
                )));
                break;
            }
        }
    }
    if changed {
        save_contexts_in(contexts, dir)?;
        // Older versions kept a copy of the current context's token in a per-URL table.
        // This version writes none, and a copy that stays would keep the token in plain
        // text after it moved to the keychain.
        let _ = remove_url_credentials_in(dir, &context_urls(contexts));
    }
    match error {
        Some(e) => Err(e),
        None => Ok(changed),
    }
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

/// The write is atomic, so a `tl` command that runs at the same time reads the old file or
/// the new one, never a half-written one.
fn save_contexts_in(contexts: &ContextsFile, dir: &Path) -> Result<()> {
    fs::create_dir_all(dir)?;
    let content = toml::to_string_pretty(contexts)?;
    write_file_atomically(&dir.join("contexts.toml"), content.as_bytes(), None)
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
                storage: None,
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

/// The name a login for `api_url` is saved as when the user names no context.
///
/// `default` when no context has that name, or when the saved `default` is for the same API
/// URL. Otherwise a name from the host, such as `api-staging-tensorlake-ai`, with `-2`, `-3`
/// added while a context of that name is for another URL. A login for a dev or staging
/// server must not replace the `default` login for production.
pub fn login_name_for_url(contexts: &ContextsFile, api_url: &str) -> String {
    let wanted = normalize_api_url(api_url);
    let fits = |name: &str| {
        contexts
            .get(name)
            .is_none_or(|entry| normalize_api_url(&entry.api_url) == wanted)
    };
    if fits(DEFAULT_CONTEXT_NAME) {
        return DEFAULT_CONTEXT_NAME.to_string();
    }
    let base = context_name_from_url(&wanted);
    let mut name = base.clone();
    let mut n = 2;
    while !fits(&name) {
        name = format!("{base}-{n}");
        n += 1;
    }
    name
}

/// The saved context for `api_url`, when the current context is for another URL.
///
/// Prefers the name a login for `api_url` would be saved as (see [`login_name_for_url`]):
/// `default`, else the name from the host that the upgrade gave the old per-URL login. Any
/// other context for the URL comes next, by name. None when no context is for the URL.
pub fn context_for_url<'a>(
    contexts: &'a ContextsFile,
    api_url: &str,
) -> Option<(&'a str, &'a ContextEntry)> {
    let wanted = normalize_api_url(api_url);
    let is_for_url = |entry: &ContextEntry| normalize_api_url(&entry.api_url) == wanted;
    let preferred = login_name_for_url(contexts, api_url);
    contexts
        .contexts
        .get_key_value(&preferred)
        .filter(|(_, entry)| is_for_url(entry))
        .or_else(|| {
            contexts
                .contexts
                .iter()
                .find(|(_, entry)| is_for_url(entry))
        })
        .map(|(name, entry)| (name.as_str(), entry))
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

        let err = load_contexts_in(dir.path(), &Backend::file_only())
            .unwrap_err()
            .to_string();
        assert!(err.contains("contexts.toml does not parse"), "{err}");
        assert!(err.contains("fix the file or move it away"), "{err}");
        assert_eq!(fs::read_to_string(&path).unwrap(), broken);
    }

    #[test]
    fn a_login_name_fits_the_api_url() {
        let dev = "https://api.tensorlake.dev";
        let mut file = ContextsFile::default();
        assert_eq!(login_name_for_url(&file, DEFAULT_API_URL), "default");
        assert_eq!(login_name_for_url(&file, dev), "default");

        // `default` is for production: a dev login gets its own name.
        file.contexts
            .insert("default".into(), entry(DEFAULT_API_URL, None, None));
        assert_eq!(login_name_for_url(&file, DEFAULT_API_URL), "default");
        assert_eq!(login_name_for_url(&file, dev), "api-tensorlake-dev");
        // The same spelled differently is the same URL.
        assert_eq!(
            login_name_for_url(&file, "https://api.tensorlake.ai/"),
            "default"
        );

        // The host name is taken by a context for another URL: add a number.
        file.contexts.insert(
            "api-tensorlake-dev".into(),
            entry("http://127.0.0.1:9", None, None),
        );
        assert_eq!(login_name_for_url(&file, dev), "api-tensorlake-dev-2");
        // A context for the same URL is reused, whatever its name.
        file.contexts
            .insert("api-tensorlake-dev-2".into(), entry(dev, None, None));
        assert_eq!(login_name_for_url(&file, dev), "api-tensorlake-dev-2");
    }

    fn entry(api_url: &str, org: Option<&str>, project: Option<&str>) -> ContextEntry {
        ContextEntry {
            api_url: api_url.to_string(),
            organization: org.map(str::to_string),
            project: project.map(str::to_string),
            storage: None,
        }
    }

    fn file_token(dir: &Path, name: &str) -> Option<String> {
        crate::config::files::load_context_token_from_file_in(dir, name).map(|t| t.token)
    }

    #[test]
    fn the_first_run_turns_per_url_logins_into_contexts_with_a_token_each() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("credentials.toml"),
            r#"["https://api.tensorlake.ai"]
token = "prod-token"
organization = "org_1"
project = "project_1"
"#,
        )
        .unwrap();

        let contexts = load_contexts_in(dir.path(), &Backend::file_only()).unwrap();
        let default = contexts.get("default").unwrap();
        assert_eq!(default.storage, Some(TokenStorage::File));
        assert_eq!(default.project.as_deref(), Some("project_1"));
        assert_eq!(
            file_token(dir.path(), "default").as_deref(),
            Some("prod-token")
        );
        // The per-URL copy is gone, and the file is saved.
        let credentials = fs::read_to_string(dir.path().join("credentials.toml")).unwrap();
        assert!(!credentials.contains("api.tensorlake.ai"), "{credentials}");
        let saved = read_contexts_file(&dir.path().join("contexts.toml")).unwrap();
        assert_eq!(saved, contexts);
    }

    #[test]
    fn a_context_without_storage_gets_its_token_moved_and_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = ContextsFile {
            current: Some("default".into()),
            ..Default::default()
        };
        file.contexts
            .insert("default".into(), entry(DEFAULT_API_URL, None, None));
        file.contexts
            .insert("logged-out".into(), entry(DEFAULT_API_URL, None, None));
        save_contexts_in(&file, dir.path()).unwrap();
        fs::write(
            dir.path().join("credentials.toml"),
            format!(
                "[\"{DEFAULT_API_URL}\"]\ntoken = \"tok\"\n\n[contexts.default]\ntoken = \"tok\"\n"
            ),
        )
        .unwrap();

        let contexts = load_contexts_in(dir.path(), &Backend::file_only()).unwrap();
        let credentials = fs::read_to_string(dir.path().join("credentials.toml")).unwrap();
        assert!(
            !credentials.contains("api.tensorlake.ai"),
            "the per-URL copy goes: {credentials}"
        );
        assert_eq!(file_token(dir.path(), "default").as_deref(), Some("tok"));
        assert_eq!(
            contexts.get("default").unwrap().storage,
            Some(TokenStorage::File)
        );
        // No token, so nothing to record: the next login decides.
        assert_eq!(contexts.get("logged-out").unwrap().storage, None);
        let saved = read_contexts_file(&dir.path().join("contexts.toml")).unwrap();
        assert_eq!(saved, contexts);

        // The second run changes nothing.
        let again = load_contexts_in(dir.path(), &Backend::file_only()).unwrap();
        assert_eq!(again, contexts);
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
    fn the_context_for_a_url_prefers_the_login_name() {
        let other = "https://api.example.test";
        let entry = |api_url: &str| ContextEntry {
            api_url: api_url.to_string(),
            organization: None,
            project: None,
            storage: None,
        };
        let mut contexts = ContextsFile::default();
        assert!(context_for_url(&contexts, other).is_none());

        // The name the upgrade gave the old per-URL login wins over an earlier name.
        contexts.contexts.insert("a-dev".into(), entry(other));
        contexts
            .contexts
            .insert("api-example-test".into(), entry(other));
        contexts
            .contexts
            .insert("default".into(), entry(DEFAULT_API_URL));
        let (name, _) = context_for_url(&contexts, other).unwrap();
        assert_eq!(name, "api-example-test");
        let (name, _) = context_for_url(&contexts, DEFAULT_API_URL).unwrap();
        assert_eq!(name, "default");

        // Without that name, the first context for the URL by name.
        contexts.contexts.remove("api-example-test");
        let (name, _) = context_for_url(&contexts, other).unwrap();
        assert_eq!(name, "a-dev");

        // `default` for another URL does not stand in.
        contexts.contexts.remove("a-dev");
        assert!(context_for_url(&contexts, other).is_none());
    }

    #[test]
    fn context_names_are_checked() {
        assert!(validate_context_name("default").is_ok());
        assert!(validate_context_name("my-ctx_1.b").is_ok());
        assert!(validate_context_name("").is_err());
        assert!(validate_context_name("has space").is_err());
        assert!(validate_context_name("a/b").is_err());
    }

    #[test]
    fn a_name_that_differs_from_a_saved_one_only_by_case_is_an_error() {
        let mut file = ContextsFile::default();
        file.contexts.insert(
            "staging".into(),
            entry("https://api.tensorlake.ai", None, None),
        );
        assert!(
            check_no_case_clash(&file, "staging").is_ok(),
            "the same name"
        );
        assert!(check_no_case_clash(&file, "stage").is_ok());
        let err = check_no_case_clash(&file, "STAGING")
            .unwrap_err()
            .to_string();
        assert!(
            err.starts_with(
                "context name 'STAGING' differs from the saved context 'staging' only by case."
            ),
            "{err}"
        );
        assert!(err.contains("use 'staging'"), "{err}");
    }
}
