use crate::auth::context::CliContext;
use crate::commands::sbx::{build_network_config, sandbox_endpoint};
use crate::error::{CliError, Result};
use tensorlake::sandboxes::models::{NetworkPolicyUpdate, UpdateSandboxRequest};

#[derive(Clone, Copy)]
pub struct UpdateNetworkArgs<'a> {
    pub clear_network: bool,
    pub no_internet: bool,
    pub network_allow: &'a [String],
    pub network_deny: &'a [String],
}

pub async fn run(ctx: &CliContext, sandbox_id: &str, args: UpdateNetworkArgs<'_>) -> Result<()> {
    let client = ctx.client()?;
    let url = sandbox_endpoint(ctx, &format!("sandboxes/{sandbox_id}"));
    let request = build_update_request(args)?;

    let resp = client
        .patch(&url)
        .json(&request)
        .send()
        .await
        .map_err(CliError::Http)?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(CliError::Other(anyhow::anyhow!(
            "failed to update sandbox network configuration (HTTP {}): {}",
            status,
            body
        )));
    }

    if args.clear_network {
        println!("Cleared network configuration for sandbox {sandbox_id}");
    } else {
        println!("Updated network configuration for sandbox {sandbox_id}");
    }
    Ok(())
}

fn build_update_request(args: UpdateNetworkArgs<'_>) -> Result<UpdateSandboxRequest> {
    if args.clear_network
        && (args.no_internet || !args.network_allow.is_empty() || !args.network_deny.is_empty())
    {
        return Err(CliError::usage(
            "--clear-network cannot be combined with replacement network settings",
        ));
    }

    let network = if args.clear_network {
        NetworkPolicyUpdate::Clear
    } else {
        let config = build_network_config(args.no_internet, args.network_allow, args.network_deny)?
            .ok_or_else(|| CliError::usage("provide a network setting or use --clear-network"))?;
        NetworkPolicyUpdate::Set(config)
    };

    Ok(UpdateSandboxRequest {
        name: None,
        allow_unauthenticated_access: None,
        exposed_ports: None,
        network,
    })
}

#[cfg(test)]
mod tests {
    use super::{UpdateNetworkArgs, build_update_request};

    fn serialize(args: UpdateNetworkArgs<'_>) -> serde_json::Value {
        serde_json::to_value(build_update_request(args).unwrap()).unwrap()
    }

    #[test]
    fn update_request_replaces_the_complete_network_policy() {
        let allow = vec!["api.example.com".to_string(), "10.0.0.0/8".to_string()];
        let deny = vec!["10.10.0.0/16".to_string()];

        let request = serialize(UpdateNetworkArgs {
            clear_network: false,
            no_internet: false,
            network_allow: &allow,
            network_deny: &deny,
        });

        assert_eq!(request["network"]["allow_internet_access"], true);
        assert_eq!(request["network"]["allow_out"], serde_json::json!(allow));
        assert_eq!(request["network"]["deny_out"], serde_json::json!(deny));
        assert!(request.get("name").is_none());
    }

    #[test]
    fn no_internet_blocks_everything() {
        let request = serialize(UpdateNetworkArgs {
            clear_network: false,
            no_internet: true,
            network_allow: &[],
            network_deny: &[],
        });

        assert_eq!(request["network"]["allow_internet_access"], false);
        assert_eq!(request["network"]["allow_out"], serde_json::json!([]));
        assert_eq!(request["network"]["deny_out"], serde_json::json!([]));
    }

    #[test]
    fn update_request_allows_internet_by_default_when_using_deny_rules() {
        let deny = vec!["ads.example.com".to_string()];

        let request = serialize(UpdateNetworkArgs {
            clear_network: false,
            no_internet: false,
            network_allow: &[],
            network_deny: &deny,
        });

        assert_eq!(request["network"]["allow_internet_access"], true);
        assert_eq!(request["network"]["allow_out"], serde_json::json!([]));
        assert_eq!(request["network"]["deny_out"], serde_json::json!(deny));
    }

    #[test]
    fn clear_network_sends_an_explicit_null() {
        let request = serialize(UpdateNetworkArgs {
            clear_network: true,
            no_internet: false,
            network_allow: &[],
            network_deny: &[],
        });

        assert_eq!(request["network"], serde_json::Value::Null);
    }

    #[test]
    fn update_request_rejects_an_empty_update() {
        let result = build_update_request(UpdateNetworkArgs {
            clear_network: false,
            no_internet: false,
            network_allow: &[],
            network_deny: &[],
        });

        assert!(result.is_err());
    }

    #[test]
    fn update_request_rejects_clear_with_replacement_settings() {
        let deny = vec!["example.com".to_string()];
        let result = build_update_request(UpdateNetworkArgs {
            clear_network: true,
            no_internet: false,
            network_allow: &[],
            network_deny: &deny,
        });

        assert!(result.is_err());
    }

    #[test]
    fn update_request_rejects_no_internet_with_rules() {
        let allow = vec!["example.com".to_string()];
        let result = build_update_request(UpdateNetworkArgs {
            clear_network: false,
            no_internet: true,
            network_allow: &allow,
            network_deny: &[],
        });

        assert!(result.is_err());
    }
}
