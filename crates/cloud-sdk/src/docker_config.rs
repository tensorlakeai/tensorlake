//! Capture Docker configuration and resolve registry credentials for one operation.
//!
//! Configuration contents are sensitive. Never include them (or credential helper
//! output) in diagnostics. Helpers run locally; only resolved `auths` are forwarded.

use std::{collections::BTreeSet, path::PathBuf};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use docker_credential::{CredentialRetrievalError, DockerCredential, get_credential_from_reader};
use docker_credentials_config::DockerConfig;
use serde_json::{Map, Value, json};

use crate::sandbox_images::SandboxImageBuildError;

type Result<T> = std::result::Result<T, SandboxImageBuildError>;

/// Read and validate Docker config contents now, without resolving helpers.
///
/// Looks for `$DOCKER_CONFIG/config.json`, then `~/.docker/config.json`.
/// Missing configuration returns `"{}"`, so passing the result to an image
/// build/import never triggers a second discovery. Unreadable or invalid files
/// are errors. Call this at CLI startup, before authentication or application code.
pub fn load_docker_config_json() -> Result<String> {
    let mut paths = Vec::new();
    if let Some(directory) = std::env::var_os("DOCKER_CONFIG") {
        paths.push(PathBuf::from(directory).join("config.json"));
    }
    if let Some(home) = dirs::home_dir() {
        paths.push(home.join(".docker/config.json"));
    }
    load_from_paths(paths)
}

fn load_from_paths(paths: impl IntoIterator<Item = PathBuf>) -> Result<String> {
    for path in paths {
        match std::fs::read_to_string(&path) {
            Ok(contents) => {
                parse_config(&contents)?;
                return Ok(contents);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(SandboxImageBuildError::other(format!(
                    "Failed to read Docker config '{}': {error}",
                    path.display()
                )));
            }
        }
    }
    Ok("{}".to_string())
}

fn parse_config(contents: &str) -> Result<DockerConfig> {
    let invalid = || {
        SandboxImageBuildError::usage(
            "Invalid Docker config JSON: expected a Docker config object with valid auths, credHelpers and credsStore fields",
        )
    };
    // Do not surface serde errors: type errors can include the offending secret.
    let value: Value = serde_json::from_str(contents).map_err(|_| invalid())?;
    if !value.is_object() {
        return Err(invalid());
    }
    serde_json::from_value(value).map_err(|_| invalid())
}

pub(crate) fn resolve_docker_config_json(contents: Option<String>) -> Result<Option<String>> {
    let contents = match contents {
        Some(contents) => contents,
        None => load_docker_config_json()?,
    };
    let config = parse_config(&contents)?;
    let registries: BTreeSet<_> = config
        .auths
        .keys()
        .chain(config.cred_helpers.keys())
        .collect();
    let mut auths = Map::new();
    for registry in registries {
        // DockerConfig::all_credentials reopens the environment-selected config.
        // Resolve from the captured bytes instead, including helper selection.
        let entry = match get_credential_from_reader(contents.as_bytes(), registry) {
            Ok(DockerCredential::UsernamePassword(username, password)) => {
                json!({"auth": STANDARD.encode(format!("{username}:{password}"))})
            }
            Ok(DockerCredential::IdentityToken(token)) => json!({"identitytoken": token}),
            // Preserve discovery's behavior for absent credentials and broken helpers,
            // but never silently turn invalid inline credentials into an anonymous build.
            Err(
                CredentialRetrievalError::NoCredentialConfigured
                | CredentialRetrievalError::HelperCommunicationError
                | CredentialRetrievalError::MalformedHelperResponse
                | CredentialRetrievalError::HelperFailure { .. },
            ) => continue,
            // Dependency releases can add retrieval errors. Fail closed without
            // exposing their payloads, while retaining the explicit exceptions above.
            Err(_) => {
                return Err(SandboxImageBuildError::usage(
                    "Invalid Docker config JSON: failed to resolve registry credentials",
                ));
            }
        };
        auths.insert(registry.clone(), entry);
    }
    if auths.is_empty() {
        return Ok(None);
    }
    Ok(Some(json!({"auths": auths}).to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(password: &str) -> String {
        json!({"auths": {"registry.example.com": {
            "auth": STANDARD.encode(format!("user:{password}"))
        }}})
        .to_string()
    }

    #[test]
    fn concurrent_calls_keep_their_own_credentials() {
        let threads: Vec<_> = (0..16)
            .map(|i| {
                std::thread::spawn(move || {
                    let contents = config(&format!("password-{i}"));
                    let resolved = resolve_docker_config_json(Some(contents.clone()))
                        .unwrap()
                        .unwrap();
                    assert_eq!(
                        serde_json::from_str::<Value>(&resolved).unwrap(),
                        serde_json::from_str::<Value>(&contents).unwrap()
                    );
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
    }

    #[test]
    fn snapshot_survives_file_replacement_and_removal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        let original = config("original");
        std::fs::write(&path, &original).unwrap();
        let captured = load_from_paths([path.clone()]).unwrap();
        std::fs::write(&path, config("replacement")).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(
            resolve_docker_config_json(Some(captured)).unwrap(),
            Some(original)
        );
    }

    #[test]
    fn discovery_falls_back_and_missing_config_is_explicitly_empty() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.json");
        let fallback = dir.path().join("fallback.json");
        std::fs::write(&fallback, config("fallback")).unwrap();
        assert_eq!(
            load_from_paths([missing.clone(), fallback]).unwrap(),
            config("fallback")
        );
        assert_eq!(load_from_paths([missing]).unwrap(), "{}");
        assert_eq!(resolve_docker_config_json(Some("{}".into())).unwrap(), None);
    }

    #[test]
    fn invalid_config_fails_without_exposing_contents() {
        for contents in [
            "",
            "secret",
            "[]",
            "null",
            r#"{"auths":"secret"}"#,
            r#"{"auths":{"registry":{"auth":"secret"}}}"#,
            r#"{"credHelpers":{"registry":123}}"#,
        ] {
            let error = resolve_docker_config_json(Some(contents.into()))
                .unwrap_err()
                .to_string();
            assert!(error.contains("Invalid Docker config JSON"));
            assert!(!error.contains("secret"));
        }
    }

    #[test]
    fn malformed_auth_fails_without_exposing_credentials() {
        for credential in ["secret-without-a-separator", ""] {
            let encoded = STANDARD.encode(credential);
            let contents = json!({"auths": {
                "valid.example.com": {"auth": STANDARD.encode("user:password")},
                "invalid.example.com": {"auth": encoded}
            }});
            let error = resolve_docker_config_json(Some(contents.to_string()))
                .unwrap_err()
                .to_string();
            assert!(error.contains("Invalid Docker config JSON"));
            assert!(!error.contains("secret-without-a-separator"));
            assert!(!error.contains("password"));
            if !encoded.is_empty() {
                assert!(!error.contains(&encoded));
            }
        }
    }

    #[test]
    fn empty_registry_entries_are_still_ignored() {
        assert_eq!(
            resolve_docker_config_json(Some(r#"{"auths":{"registry":{}}}"#.into())).unwrap(),
            None
        );
    }

    #[test]
    fn identity_tokens_are_preserved_and_unrelated_fields_are_not_forwarded() {
        let resolved = resolve_docker_config_json(Some(
            r#"{"auths":{"registry":{"identitytoken":"token"}},"HttpHeaders":{"secret":"value"}}"#
                .into(),
        ))
        .unwrap()
        .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&resolved).unwrap(),
            json!({"auths":{"registry":{"identitytoken":"token"}}})
        );
    }

    #[cfg(unix)]
    #[test]
    fn helper_resolution_and_discovery_in_an_isolated_process() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let helper = dir.path().join("docker-credential-tensorlake-test");
        std::fs::write(
            &helper,
            r#"#!/bin/sh
[ "$1" = get ] || exit 1
printf '%s' '{"Username":"helper-user","Secret":"helper-password"}'
"#,
        )
        .unwrap();
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();
        for (name, script) in [
            ("failed", "#!/bin/sh\nprintf secret-helper-output\nexit 1\n"),
            ("malformed", "#!/bin/sh\nprintf secret-helper-output\n"),
        ] {
            let helper = dir.path().join(format!("docker-credential-{name}"));
            std::fs::write(&helper, script).unwrap();
            std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        // A malformed ambient file makes accidental discovery fail, even when the
        // caller supplies an empty document or uses a credential helper.
        std::fs::write(dir.path().join("config.json"), "invalid ambient config").unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "docker_config::tests::isolated_environment",
                "--nocapture",
            ])
            .env("TENSORLAKE_TEST_DOCKER_CONFIG", "1")
            .env("DOCKER_CONFIG", dir.path())
            .env("HOME", dir.path())
            .env("PATH", dir.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    #[cfg(unix)]
    #[test]
    fn isolated_environment() {
        if std::env::var_os("TENSORLAKE_TEST_DOCKER_CONFIG").is_none() {
            return;
        }
        assert!(resolve_docker_config_json(None).is_err());
        for empty in ["{}", r#"{"auths":{}}"#] {
            assert_eq!(
                resolve_docker_config_json(Some(empty.into())).unwrap(),
                None
            );
        }
        let explicit = config("explicit");
        assert_eq!(
            resolve_docker_config_json(Some(explicit.clone())).unwrap(),
            Some(explicit)
        );
        for helper_config in [
            json!({"auths":{"registry":{}}, "credsStore":"tensorlake-test"}),
            json!({"auths":{"registry":{"auth":"dXNlcjpwYXNz"}},
                   "credHelpers":{"registry":"tensorlake-test"}}),
            json!({"credHelpers":{"registry":"tensorlake-test"}}),
        ] {
            let resolved = resolve_docker_config_json(Some(helper_config.to_string()))
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&resolved).unwrap(),
                json!({"auths":{"registry":{"auth": STANDARD.encode("helper-user:helper-password")}}})
            );
        }
        for helper in ["missing", "failed", "malformed"] {
            for helper_config in [
                json!({"auths":{"registry":{}}, "credsStore":helper}),
                json!({"credHelpers":{"registry":helper}}),
            ] {
                assert_eq!(
                    resolve_docker_config_json(Some(helper_config.to_string())).unwrap(),
                    None
                );
            }
        }
        let path = PathBuf::from(std::env::var_os("DOCKER_CONFIG").unwrap()).join("config.json");
        let default_config = config("discovered");
        std::fs::write(path, &default_config).unwrap();
        assert_eq!(
            resolve_docker_config_json(None).unwrap(),
            Some(default_config)
        );
    }
}
