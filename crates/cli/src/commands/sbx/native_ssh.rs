use crate::error::{CliError, Result};

const SSH_HOSTNAME: &str = "sandbox.tensorlake.ai";
const DEFAULT_IDENTITY_FILE: &str = "~/.ssh/id_ed25519_tensorlake";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSandbox {
    sandbox_id: String,
    name: Option<String>,
}

impl ResolvedSandbox {
    pub fn new(sandbox_id: impl Into<String>, name: Option<&str>) -> Self {
        Self {
            sandbox_id: sandbox_id.into(),
            name: name
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
        }
    }
}

pub fn format_ssh_config(
    sandbox: &ResolvedSandbox,
    host_alias: Option<&str>,
    identity_file: Option<&str>,
) -> Result<String> {
    let host_alias = match host_alias {
        Some(alias) => {
            validate_single_token("host", alias)?;
            alias.trim().to_string()
        }
        None => default_host_alias(sandbox),
    };

    let mut lines = vec![
        format!("Host {host_alias}"),
        format!("    HostName {SSH_HOSTNAME}"),
        format!("    User {}", sandbox.sandbox_id),
    ];

    match identity_file {
        Some(path) => {
            validate_single_token("identity file", path)?;
            lines.push(format!("    IdentityFile {}", path.trim()));
        }
        None => {
            lines.push(
                "    # Set this to the private key whose public key you registered with `tl sbx ssh keys add`"
                    .to_string(),
            );
            lines.push(format!("    IdentityFile {DEFAULT_IDENTITY_FILE}"));
        }
    }

    lines.extend([
        "    IdentitiesOnly yes".to_string(),
        "    ServerAliveInterval 30".to_string(),
        "    ServerAliveCountMax 3".to_string(),
    ]);

    Ok(format!("{}\n", lines.join("\n")))
}

fn default_host_alias(sandbox: &ResolvedSandbox) -> String {
    match sandbox.name.as_deref() {
        Some(name) => format!("tl-{name}"),
        None => format!("tl-{}", sandbox.sandbox_id),
    }
}

fn validate_single_token(label: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(CliError::usage(format!("{label} must not be empty")));
    }
    if trimmed.chars().any(char::is_whitespace) {
        return Err(CliError::usage(format!(
            "{label} must not contain whitespace"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ResolvedSandbox, format_ssh_config};

    #[test]
    fn format_ssh_config_uses_named_alias_and_placeholder_identity_file() {
        let config = format_ssh_config(
            &ResolvedSandbox::new("sbx-123", Some("my-sandbox")),
            None,
            None,
        )
        .expect("config");

        assert_eq!(
            config,
            concat!(
                "Host tl-my-sandbox\n",
                "    HostName sandbox.tensorlake.ai\n",
                "    User sbx-123\n",
                "    # Set this to the private key whose public key you registered with `tl sbx ssh keys add`\n",
                "    IdentityFile ~/.ssh/id_ed25519_tensorlake\n",
                "    IdentitiesOnly yes\n",
                "    ServerAliveInterval 30\n",
                "    ServerAliveCountMax 3\n"
            )
        );
    }
}
