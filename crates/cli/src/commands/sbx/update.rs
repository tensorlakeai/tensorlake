use crate::auth::context::CliContext;
use crate::commands::sbx::{build_network_config, sandbox_endpoint};
use crate::error::{CliError, Result};
use std::time::Duration;
use tensorlake::sandboxes::models::{NetworkPolicyUpdate, UpdateSandboxRequest};
use tensorlake::sandboxes::{
    models::{ResizeSandboxResources, SandboxInfo},
    resize::ResizeOptions,
};

#[derive(Clone, Copy)]
pub struct UpdateNetworkArgs<'a> {
    pub clear_network: bool,
    pub no_internet: bool,
    pub network_allow: &'a [String],
    pub network_deny: &'a [String],
}

#[derive(Clone, Copy, Default)]
pub struct UpdateResourceArgs {
    pub cpus: Option<f64>,
    pub memory: Option<i64>,
    pub disk_mb: Option<u64>,
    pub no_wait: bool,
    pub timeout: u64,
}

pub async fn run(
    ctx: &CliContext,
    sandbox_id: &str,
    args: UpdateNetworkArgs<'_>,
    resources: UpdateResourceArgs,
) -> Result<()> {
    let target = ResizeSandboxResources {
        cpus: resources.cpus,
        memory_mb: resources.memory,
        disk_mb: resources.disk_mb,
    };
    if !target.is_empty() {
        if args.clear_network
            || args.no_internet
            || !args.network_allow.is_empty()
            || !args.network_deny.is_empty()
        {
            return Err(CliError::usage(
                "resource changes cannot be combined with network settings",
            ));
        }
        target
            .validate()
            .map_err(|e| CliError::usage(e.to_string()))?;
        let client = tensorlake::sandboxes::SandboxesClient::new(
            ctx.scoped_cloud_client()?
                .with_base_url(&super::resolve_sandbox_lifecycle_url(&ctx.api_url)),
            &ctx.namespace,
            super::is_localhost(&ctx.api_url),
        );
        let current = client.get(sandbox_id).await?;
        if target.against(&current)?.is_empty() {
            println!("No resource changes for sandbox {sandbox_id}");
            return Ok(());
        }
        let request = UpdateSandboxRequest {
            resources: Some(target),
            name: None,
            allow_unauthenticated_access: None,
            exposed_ports: None,
            network: NetworkPolicyUpdate::Keep,
        };
        let admitted = client
            .update_with_options(
                sandbox_id,
                &request,
                ResizeOptions {
                    wait: false,
                    ..Default::default()
                },
            )
            .await?;
        if admitted.resource_resize == current.resource_resize
            && target_satisfied(&request, &admitted)
        {
            println!("No resource changes for sandbox {sandbox_id}");
            return Ok(());
        }
        let Some(resize) = &admitted.resource_resize else {
            return Err(CliError::usage(
                "server omitted resize generation; inspect the sandbox before retrying",
            ));
        };
        print_resize(&admitted);
        if !resources.no_wait && resize.status == "pending" {
            let completed = client
                .wait_for_resource_resize(
                    &admitted.sandbox_id,
                    resize.generation,
                    Duration::from_secs(resources.timeout),
                    Duration::from_secs(1),
                )
                .await
                .map_err(|error| match error {
                    tensorlake::error::SdkError::SandboxResize(mut error) => {
                        if error.info.is_none() {
                            error.info = Some(Box::new(admitted.clone().into_inner()));
                        }
                        tensorlake::error::SdkError::SandboxResize(error)
                    }
                    error => error,
                })?;
            print_resize(&completed);
        }
        return Ok(());
    }
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

fn target_satisfied(request: &UpdateSandboxRequest, info: &SandboxInfo) -> bool {
    request.resources.as_ref().is_some_and(|r| {
        r.cpus.is_none_or(|v| v == info.resources.cpus)
            && r.memory_mb.is_none_or(|v| v == info.resources.memory_mb)
            && r.disk_mb
                .is_none_or(|v| info.resources.disk_mb >= 0 && v == info.resources.disk_mb as u64)
    })
}

fn print_resize(info: &SandboxInfo) {
    if let Some(resize) = &info.resource_resize {
        println!(
            "Sandbox {} resize generation {}: {}",
            info.sandbox_id, resize.generation, resize.status
        );
        println!(
            "Confirmed allocation: {} CPUs, {} MiB memory, {} MiB disk",
            info.resources.cpus, info.resources.memory_mb, info.resources.disk_mb
        );
        if resize.status == "pending" {
            println!("Inspect with: tl sbx describe {}", info.sandbox_id);
        }
    }
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
        resources: None,
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
