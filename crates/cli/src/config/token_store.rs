//! Where the token of each context lives: the OS keychain, or `credentials.toml`.
//!
//! The keychain is the default on macOS and Windows, and on Linux when a Secret Service
//! answers on the session bus. A file that only the user can read is the fallback when there
//! is no keychain: CI, a container, or SSH to a Linux box with no desktop session.
//!
//! `TENSORLAKE_TOKEN_STORAGE=file` keeps every token in the file, for macOS over SSH and
//! for unsigned dev builds. `=keychain` refuses the file. Each context records where its
//! token is in `contexts.toml`, so a read goes straight there and pays no keychain probe.
//!
//! A keychain that exists but fails (locked, access denied, timeout) is an error, never
//! "not logged in": the token is there, `tl` just cannot read it. Only a missing keychain
//! sends a token to the file, and that is said once, when the token is saved.

use std::fmt;
use std::path::Path;
use std::sync::{Arc, OnceLock, mpsc};
use std::time::Duration;

use keyring_core::{CredentialStore, Error as KeyringError};
use serde::{Deserialize, Serialize};

use crate::config::files::{self, ContextToken, config_dir};
use crate::error::{CliError, Result};

/// The environment variable that picks the storage: `file`, `keychain`, or unset.
pub const STORAGE_ENV: &str = "TENSORLAKE_TOKEN_STORAGE";

/// The keychain service name. The account is the context name.
const SERVICE: &str = "tensorlake";

/// How long one keychain call may take. A locked keyring over SSH can open an unlock prompt
/// that nobody sees; without a limit `tl` would wait forever.
const TIMEOUT: Duration = Duration::from_secs(5);

/// Where the token of one context is. Saved per context in `contexts.toml`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenStorage {
    Keychain,
    File,
}

impl fmt::Display for TokenStorage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TokenStorage::Keychain => "keychain",
            TokenStorage::File => "file",
        })
    }
}

/// What the user asked for through [`STORAGE_ENV`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    /// The keychain when there is one, else the file.
    Auto,
    /// The keychain only.
    Keychain,
    /// The file only. New tokens go to the file. The keychain is read only for a context
    /// whose `storage` says its token is there.
    File,
}

impl Policy {
    pub fn from_env() -> Result<Policy> {
        let Ok(value) = std::env::var(STORAGE_ENV) else {
            return Ok(Policy::Auto);
        };
        match value.trim().to_ascii_lowercase().as_str() {
            "" => Ok(Policy::Auto),
            "file" => Ok(Policy::File),
            "keychain" => Ok(Policy::Keychain),
            other => Err(CliError::usage(format!(
                "{STORAGE_ENV}={other} is not valid. use 'file' or 'keychain', or unset it"
            ))),
        }
    }
}

/// Why a keychain call did not work.
#[derive(Debug)]
enum KeychainError {
    /// This machine has no keychain: no store for this OS, or no Secret Service on the bus.
    Unavailable(String),
    /// The keychain is there but did not work: locked, access denied, timed out, or failed.
    Failed(String),
}

impl KeychainError {
    fn into_cli(self, action: &str, name: &str) -> CliError {
        match self {
            KeychainError::Unavailable(reason) => CliError::config(format!(
                "cannot {action} the token of context '{name}': no OS keychain ({reason}). \
                 set {STORAGE_ENV}=file to keep tokens in a file"
            )),
            KeychainError::Failed(reason) => CliError::config(format!(
                "cannot {action} the token of context '{name}' in the OS keychain: {reason}. \
                 set {STORAGE_ENV}=file to keep tokens in a file"
            )),
        }
    }
}

/// The OS keychain, behind a per-call timeout.
///
/// Every call runs on its own thread. That gives the timeout, and it keeps the Linux store
/// off the tokio runtime of `main`: the store blocks on its own runtime, which panics when
/// called from inside another one.
#[derive(Clone, Default)]
struct Keychain {
    /// `None` is the store for this OS. Tests put a mock here.
    store: Option<Arc<CredentialStore>>,
}

impl Keychain {
    fn call<T: Send + 'static>(
        &self,
        op: impl FnOnce(&CredentialStore) -> keyring_core::Result<T> + Send + 'static,
    ) -> std::result::Result<T, KeychainError> {
        let (tx, rx) = mpsc::channel();
        let store = self.store.clone();
        let spawned = std::thread::Builder::new()
            .name("tl-keychain".to_string())
            .spawn(move || {
                let result = match store {
                    Some(store) => Ok(store),
                    None => platform_store(),
                }
                .and_then(|store| op(&*store).map_err(classify));
                let _ = tx.send(result);
            });
        if spawned.is_err() {
            return Err(KeychainError::Failed(
                "cannot start a thread for the keychain call".to_string(),
            ));
        }
        match rx.recv_timeout(TIMEOUT) {
            Ok(result) => result,
            Err(_) => Err(KeychainError::Failed(format!(
                "the OS keychain did not answer in {} seconds",
                TIMEOUT.as_secs()
            ))),
        }
    }

    fn get(&self, name: &str) -> std::result::Result<Option<String>, KeychainError> {
        let name = name.to_string();
        self.call(
            move |store| match store.build(SERVICE, &name, None)?.get_password() {
                Ok(token) => Ok(Some(token)),
                Err(KeyringError::NoEntry) => Ok(None),
                Err(e) => Err(e),
            },
        )
    }

    /// Save, then read back: a keychain that says "saved" but returns nothing would log the
    /// user out at the next command.
    fn set(&self, name: &str, token: &str) -> std::result::Result<(), KeychainError> {
        let name = name.to_string();
        let token = token.to_string();
        self.call(move |store| {
            let entry = store.build(SERVICE, &name, None)?;
            entry.set_password(&token)?;
            let read_back = entry.get_password()?;
            if read_back != token {
                return Err(KeyringError::BadStoreFormat(
                    "the token read back from the keychain differs from the one saved".to_string(),
                ));
            }
            Ok(())
        })
    }

    fn delete(&self, name: &str) -> std::result::Result<(), KeychainError> {
        let name = name.to_string();
        self.call(
            move |store| match store.build(SERVICE, &name, None)?.delete_credential() {
                Ok(()) | Err(KeyringError::NoEntry) => Ok(()),
                Err(e) => Err(e),
            },
        )
    }
}

/// The keychain store for this OS, built once.
fn platform_store() -> std::result::Result<Arc<CredentialStore>, KeychainError> {
    static STORE: OnceLock<std::result::Result<Arc<CredentialStore>, KeychainError>> =
        OnceLock::new();
    STORE
        .get_or_init(build_platform_store)
        .as_ref()
        .map(Arc::clone)
        .map_err(|e| match e {
            KeychainError::Unavailable(r) => KeychainError::Unavailable(r.clone()),
            KeychainError::Failed(r) => KeychainError::Failed(r.clone()),
        })
}

#[cfg(target_os = "macos")]
fn build_platform_store() -> std::result::Result<Arc<CredentialStore>, KeychainError> {
    apple_native_keyring_store::keychain::Store::new()
        .map(|store| store as Arc<CredentialStore>)
        .map_err(|e| KeychainError::Failed(e.to_string()))
}

#[cfg(target_os = "windows")]
fn build_platform_store() -> std::result::Result<Arc<CredentialStore>, KeychainError> {
    windows_native_keyring_store::Store::new()
        .map(|store| store as Arc<CredentialStore>)
        .map_err(|e| KeychainError::Failed(e.to_string()))
}

#[cfg(target_os = "linux")]
fn build_platform_store() -> std::result::Result<Arc<CredentialStore>, KeychainError> {
    // The store connects to the session bus here. No bus, or no Secret Service on it, is
    // "no keychain": a container, CI, or SSH with no desktop session.
    zbus_secret_service_keyring_store::Store::new()
        .map(|store| store as Arc<CredentialStore>)
        .map_err(|e| KeychainError::Unavailable(e.to_string()))
}

#[cfg(not(any(target_os = "macos", target_os = "windows", target_os = "linux")))]
fn build_platform_store() -> std::result::Result<Arc<CredentialStore>, KeychainError> {
    Err(KeychainError::Unavailable(
        "no keychain store for this operating system".to_string(),
    ))
}

/// Sort a keyring error into "no keychain" and "the keychain failed".
///
/// On Linux a platform failure is most often a Secret Service that does not answer, so it
/// counts as no keychain. A locked collection or a dismissed prompt is `NoStorageAccess`,
/// which is a failure: the token is there. macOS and Windows always have a keychain, so
/// every error there is a failure.
fn classify(err: KeyringError) -> KeychainError {
    match err {
        KeyringError::NoDefaultStore => KeychainError::Unavailable(err.to_string()),
        KeyringError::PlatformFailure(_) if cfg!(target_os = "linux") => {
            KeychainError::Unavailable(err.to_string())
        }
        other => KeychainError::Failed(other.to_string()),
    }
}

/// The token store of this run: the policy from the environment, and the keychain.
#[derive(Clone)]
pub struct Backend {
    policy: Policy,
    keychain: Keychain,
}

impl Backend {
    pub fn from_env() -> Result<Backend> {
        Ok(Backend {
            policy: Policy::from_env()?,
            keychain: Keychain::default(),
        })
    }

    /// A backend that never calls a keychain.
    #[cfg(test)]
    pub(crate) fn file_only() -> Backend {
        Backend {
            policy: Policy::File,
            keychain: Keychain::default(),
        }
    }

    #[cfg(test)]
    fn with_store(policy: Policy, store: Arc<CredentialStore>) -> Backend {
        Backend {
            policy,
            keychain: Keychain { store: Some(store) },
        }
    }

    /// Load the token of context `name` from where `storage` says it is.
    ///
    /// `None` is a context from before the `storage` field: the file is read first, then the
    /// keychain when the policy allows one and one is there.
    pub fn load(
        &self,
        dir: &Path,
        name: &str,
        storage: Option<TokenStorage>,
    ) -> Result<Option<ContextToken>> {
        match storage {
            Some(TokenStorage::File) => Ok(files::load_context_token_from_file_in(dir, name)),
            Some(TokenStorage::Keychain) => self
                .keychain
                .get(name)
                .map(|token| token.map(|token| ContextToken { token }))
                .map_err(|e| e.into_cli("read", name)),
            None => {
                if let Some(token) = files::load_context_token_from_file_in(dir, name) {
                    return Ok(Some(token));
                }
                if self.policy == Policy::File {
                    return Ok(None);
                }
                // The keychain is a guess here: a token that another `tl` moved while this
                // one ran. A keychain that fails does not hide a token the file never had.
                Ok(self
                    .keychain
                    .get(name)
                    .ok()
                    .flatten()
                    .map(|token| ContextToken { token }))
            }
        }
    }

    /// Save the token of context `name` and return where it went.
    ///
    /// The keychain when the policy allows and one is there, else the file, said once. The
    /// copy in the other place goes, so an old token cannot come back after a logout.
    /// `previous` is where the context kept its token before, when known.
    pub fn save(
        &self,
        dir: &Path,
        name: &str,
        token: &str,
        previous: Option<TokenStorage>,
    ) -> Result<TokenStorage> {
        let storage = match self.policy {
            Policy::File => {
                files::save_context_token_to_file_in(dir, name, token)?;
                TokenStorage::File
            }
            Policy::Keychain => {
                self.keychain
                    .set(name, token)
                    .map_err(|e| e.into_cli("save", name))?;
                TokenStorage::Keychain
            }
            Policy::Auto => match self.keychain.set(name, token) {
                Ok(()) => TokenStorage::Keychain,
                Err(KeychainError::Unavailable(reason)) => {
                    warn_no_keychain(dir, &reason);
                    files::save_context_token_to_file_in(dir, name, token)?;
                    TokenStorage::File
                }
                Err(e) => return Err(e.into_cli("save", name)),
            },
        };
        match storage {
            TokenStorage::Keychain => files::remove_context_token_from_file_in(dir, name)?,
            TokenStorage::File => {
                if previous == Some(TokenStorage::Keychain) {
                    // Best effort: the new token is saved. A stale keychain item is reported,
                    // not fatal.
                    if let Err(e) = self.keychain.delete(name) {
                        eprintln!("warning: {}", e.into_cli("remove the old copy of", name));
                    }
                }
            }
        }
        Ok(storage)
    }

    /// Remove the token of context `name`. A missing token is not an error.
    pub fn remove(&self, dir: &Path, name: &str, storage: Option<TokenStorage>) -> Result<()> {
        match storage {
            Some(TokenStorage::File) => files::remove_context_token_from_file_in(dir, name),
            Some(TokenStorage::Keychain) => self
                .keychain
                .delete(name)
                .map_err(|e| e.into_cli("remove", name)),
            None => {
                files::remove_context_token_from_file_in(dir, name)?;
                if self.policy != Policy::File {
                    // Best effort, as in `load`: the keychain is a guess for this context.
                    let _ = self.keychain.delete(name);
                }
                Ok(())
            }
        }
    }

    /// [`Backend::load`] for code that goes on to move or remove the token: a file that does
    /// not parse is an error here, not an empty file.
    fn load_for_change(
        &self,
        dir: &Path,
        name: &str,
        storage: Option<TokenStorage>,
    ) -> Result<Option<ContextToken>> {
        match storage {
            Some(TokenStorage::Keychain) => self.load(dir, name, storage),
            Some(TokenStorage::File) => files::load_context_token_from_file_strict_in(dir, name),
            None => match files::load_context_token_from_file_strict_in(dir, name)? {
                Some(token) => Ok(Some(token)),
                None => self.load(dir, name, None),
            },
        }
    }

    /// Move the token of context `old` to context `new`. Returns where the token is now.
    pub fn rename(
        &self,
        dir: &Path,
        old: &str,
        new: &str,
        storage: Option<TokenStorage>,
    ) -> Result<Option<TokenStorage>> {
        let Some(token) = self.load_for_change(dir, old, storage)? else {
            return Ok(None);
        };
        let new_storage = self.save(dir, new, &token.token, None)?;
        self.remove(dir, old, storage)?;
        Ok(Some(new_storage))
    }

    /// Move a token that is in the file to where this backend keeps tokens. Returns where it
    /// is now, or `None` when the file has no token for `name`.
    ///
    /// A keychain that fails leaves the token in the file, recorded as such, so the next
    /// command does not try again and warn again. The next `tl login` tries the keychain.
    pub fn migrate_from_file(&self, dir: &Path, name: &str) -> Result<Option<TokenStorage>> {
        let Some(token) = files::load_context_token_from_file_strict_in(dir, name)? else {
            return Ok(None);
        };
        match self.save(dir, name, &token.token, Some(TokenStorage::File)) {
            Ok(storage) => Ok(Some(storage)),
            Err(e) if self.policy == Policy::Auto => {
                eprintln!("warning: {e}. the token stays in credentials.toml");
                Ok(Some(TokenStorage::File))
            }
            Err(e) => Err(e),
        }
    }
}

/// Said once per run, when a token goes to the file because there is no keychain.
fn warn_no_keychain(dir: &Path, reason: &str) {
    static WARNED: std::sync::Once = std::sync::Once::new();
    WARNED.call_once(|| {
        eprintln!(
            "warning: no OS keychain ({reason}). the token is saved in {}, which only you can \
             read. set {STORAGE_ENV}=file to keep tokens in a file without this warning",
            dir.join("credentials.toml").display()
        );
    });
}

/// Load the token of context `name`. See [`Backend::load`].
pub fn load_context_token(
    name: &str,
    storage: Option<TokenStorage>,
) -> Result<Option<ContextToken>> {
    Backend::from_env()?.load(&config_dir(), name, storage)
}

/// Save the token of context `name` and return where it went. See [`Backend::save`].
pub fn save_context_token(
    name: &str,
    token: &str,
    previous: Option<TokenStorage>,
) -> Result<TokenStorage> {
    Backend::from_env()?.save(&config_dir(), name, token, previous)
}

/// Remove the token of context `name`. See [`Backend::remove`].
pub fn remove_context_token(name: &str, storage: Option<TokenStorage>) -> Result<()> {
    Backend::from_env()?.remove(&config_dir(), name, storage)
}

/// Move the token of context `old` to context `new`. See [`Backend::rename`].
pub fn rename_context_token(
    old: &str,
    new: &str,
    storage: Option<TokenStorage>,
) -> Result<Option<TokenStorage>> {
    Backend::from_env()?.rename(&config_dir(), old, new, storage)
}

#[cfg(test)]
mod tests {
    use super::*;
    use keyring_core::api::CredentialStoreApi;
    use keyring_core::mock;

    fn mock_backend(policy: Policy) -> Backend {
        Backend::with_store(policy, mock::Store::new().unwrap())
    }

    fn file_token(dir: &Path, name: &str) -> Option<String> {
        files::load_context_token_from_file_in(dir, name).map(|t| t.token)
    }

    #[test]
    fn auto_saves_to_the_keychain_and_clears_the_file_copy() {
        let dir = tempfile::tempdir().unwrap();
        let backend = mock_backend(Policy::Auto);
        files::save_context_token_to_file_in(dir.path(), "default", "old").unwrap();

        let storage = backend.save(dir.path(), "default", "new", None).unwrap();
        assert_eq!(storage, TokenStorage::Keychain);
        assert_eq!(file_token(dir.path(), "default"), None, "file copy removed");
        let token = backend
            .load(dir.path(), "default", Some(TokenStorage::Keychain))
            .unwrap()
            .unwrap();
        assert_eq!(token.token, "new");

        backend
            .remove(dir.path(), "default", Some(TokenStorage::Keychain))
            .unwrap();
        assert!(
            backend
                .load(dir.path(), "default", Some(TokenStorage::Keychain))
                .unwrap()
                .is_none()
        );
        // Removing twice is fine.
        backend
            .remove(dir.path(), "default", Some(TokenStorage::Keychain))
            .unwrap();
    }

    #[test]
    fn file_policy_never_touches_the_keychain() {
        let dir = tempfile::tempdir().unwrap();
        let store = mock::Store::new().unwrap();
        let backend = Backend::with_store(Policy::File, store.clone());

        let storage = backend.save(dir.path(), "default", "tok", None).unwrap();
        assert_eq!(storage, TokenStorage::File);
        assert_eq!(file_token(dir.path(), "default").as_deref(), Some("tok"));
        let entry = store.build(SERVICE, "default", None).unwrap();
        assert!(matches!(entry.get_password(), Err(KeyringError::NoEntry)));

        let token = backend
            .load(dir.path(), "default", Some(TokenStorage::File))
            .unwrap()
            .unwrap();
        assert_eq!(token.token, "tok");
        backend
            .remove(dir.path(), "default", Some(TokenStorage::File))
            .unwrap();
        assert_eq!(file_token(dir.path(), "default"), None);
    }

    #[test]
    fn a_context_with_no_recorded_storage_is_read_from_the_file_then_the_keychain() {
        let dir = tempfile::tempdir().unwrap();
        let backend = mock_backend(Policy::Auto);
        assert!(backend.load(dir.path(), "default", None).unwrap().is_none());

        backend.keychain.set("default", "in-keychain").unwrap();
        let token = backend.load(dir.path(), "default", None).unwrap().unwrap();
        assert_eq!(token.token, "in-keychain");

        files::save_context_token_to_file_in(dir.path(), "default", "in-file").unwrap();
        let token = backend.load(dir.path(), "default", None).unwrap().unwrap();
        assert_eq!(token.token, "in-file", "the file wins while both exist");

        // A remove with no recorded storage clears both.
        backend.remove(dir.path(), "default", None).unwrap();
        assert!(backend.load(dir.path(), "default", None).unwrap().is_none());
    }

    #[test]
    fn migration_moves_a_file_token_to_the_keychain() {
        let dir = tempfile::tempdir().unwrap();
        let backend = mock_backend(Policy::Auto);
        assert_eq!(
            backend.migrate_from_file(dir.path(), "default").unwrap(),
            None
        );

        files::save_context_token_to_file_in(dir.path(), "default", "tok").unwrap();
        assert_eq!(
            backend.migrate_from_file(dir.path(), "default").unwrap(),
            Some(TokenStorage::Keychain)
        );
        assert_eq!(file_token(dir.path(), "default"), None);
        assert_eq!(
            backend.keychain.get("default").unwrap().as_deref(),
            Some("tok")
        );
    }

    #[test]
    fn migration_with_file_policy_keeps_the_token_in_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let backend = mock_backend(Policy::File);
        files::save_context_token_to_file_in(dir.path(), "default", "tok").unwrap();
        assert_eq!(
            backend.migrate_from_file(dir.path(), "default").unwrap(),
            Some(TokenStorage::File)
        );
        assert_eq!(file_token(dir.path(), "default").as_deref(), Some("tok"));
    }

    #[test]
    fn rename_moves_the_token_and_reports_where_it_is() {
        let dir = tempfile::tempdir().unwrap();
        let backend = mock_backend(Policy::Auto);
        assert_eq!(backend.rename(dir.path(), "a", "b", None).unwrap(), None);

        backend.save(dir.path(), "a", "tok", None).unwrap();
        assert_eq!(
            backend
                .rename(dir.path(), "a", "b", Some(TokenStorage::Keychain))
                .unwrap(),
            Some(TokenStorage::Keychain)
        );
        assert_eq!(backend.keychain.get("a").unwrap(), None);
        assert_eq!(backend.keychain.get("b").unwrap().as_deref(), Some("tok"));
    }

    #[test]
    fn saving_to_the_file_removes_an_old_keychain_copy() {
        let dir = tempfile::tempdir().unwrap();
        let store = mock::Store::new().unwrap();
        Backend::with_store(Policy::Auto, store.clone())
            .save(dir.path(), "default", "old", None)
            .unwrap();
        let storage = Backend::with_store(Policy::File, store.clone())
            .save(dir.path(), "default", "new", Some(TokenStorage::Keychain))
            .unwrap();
        assert_eq!(storage, TokenStorage::File);
        let entry = store.build(SERVICE, "default", None).unwrap();
        assert!(matches!(entry.get_password(), Err(KeyringError::NoEntry)));
    }

    #[test]
    fn a_keychain_that_fails_is_an_error_not_a_missing_token() {
        let dir = tempfile::tempdir().unwrap();
        let store = mock::Store::new().unwrap();
        let backend = Backend::with_store(Policy::Auto, store.clone());
        backend.save(dir.path(), "default", "tok", None).unwrap();

        // The mock fails once per entry, so build the entry that the next read will use.
        let entry = store.build(SERVICE, "default", None).unwrap();
        let cred: &mock::Cred = entry.as_any().downcast_ref().unwrap();
        cred.set_error(KeyringError::NoStorageAccess("locked".into()));
        let err = backend
            .load(dir.path(), "default", Some(TokenStorage::Keychain))
            .unwrap_err()
            .to_string();
        assert!(err.contains("locked"), "{err}");
        assert!(err.contains(STORAGE_ENV), "{err}");
    }

    #[test]
    fn a_failing_keychain_does_not_block_a_context_with_no_recorded_storage() {
        let dir = tempfile::tempdir().unwrap();
        let store = mock::Store::new().unwrap();
        let backend = Backend::with_store(Policy::Auto, store.clone());
        let fail = || {
            let entry = store.build(SERVICE, "default", None).unwrap();
            let cred: &mock::Cred = entry.as_any().downcast_ref().unwrap();
            cred.set_error(KeyringError::NoStorageAccess("locked".into()));
        };

        fail();
        assert!(backend.load(dir.path(), "default", None).unwrap().is_none());

        files::save_context_token_to_file_in(dir.path(), "default", "tok").unwrap();
        fail();
        // The move fails, the token stays in the file, and that is recorded.
        assert_eq!(
            backend.migrate_from_file(dir.path(), "default").unwrap(),
            Some(TokenStorage::File)
        );
        assert_eq!(file_token(dir.path(), "default").as_deref(), Some("tok"));

        fail();
        backend.remove(dir.path(), "default", None).unwrap();
        assert_eq!(file_token(dir.path(), "default"), None);

        // Forced to the keychain, the same failure is an error.
        files::save_context_token_to_file_in(dir.path(), "default", "tok").unwrap();
        fail();
        let err = Backend::with_store(Policy::Keychain, store.clone())
            .migrate_from_file(dir.path(), "default")
            .unwrap_err()
            .to_string();
        assert!(err.contains("locked"), "{err}");
    }

    #[test]
    fn no_keychain_in_auto_mode_falls_back_to_the_file() {
        let dir = tempfile::tempdir().unwrap();
        // A store that is never there: `NoDefaultStore` on every call.
        struct Absent;
        impl keyring_core::api::CredentialStoreApi for Absent {
            fn vendor(&self) -> String {
                "absent".into()
            }
            fn id(&self) -> String {
                "absent".into()
            }
            fn build(
                &self,
                _: &str,
                _: &str,
                _: Option<&std::collections::HashMap<&str, &str>>,
            ) -> keyring_core::Result<keyring_core::Entry> {
                Err(KeyringError::NoDefaultStore)
            }
            fn persistence(&self) -> keyring_core::CredentialPersistence {
                keyring_core::CredentialPersistence::UntilDelete
            }
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn debug_fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("absent")
            }
        }
        let backend = Backend::with_store(Policy::Auto, Arc::new(Absent));
        let storage = backend.save(dir.path(), "default", "tok", None).unwrap();
        assert_eq!(storage, TokenStorage::File);
        assert_eq!(file_token(dir.path(), "default").as_deref(), Some("tok"));
        // A read with no recorded storage does not fail on the missing keychain.
        files::remove_context_token_from_file_in(dir.path(), "default").unwrap();
        assert!(backend.load(dir.path(), "default", None).unwrap().is_none());

        // With the keychain forced, a missing one is an error.
        let err = Backend::with_store(Policy::Keychain, Arc::new(Absent))
            .save(dir.path(), "default", "tok", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no OS keychain"), "{err}");
    }

    #[test]
    fn policy_names() {
        assert_eq!(TokenStorage::Keychain.to_string(), "keychain");
        assert_eq!(TokenStorage::File.to_string(), "file");
        let text = toml::to_string(&std::collections::BTreeMap::from([(
            "storage",
            TokenStorage::Keychain,
        )]))
        .unwrap();
        assert_eq!(text.trim(), "storage = \"keychain\"");
    }
}
