use std::io::IsTerminal;
use std::path::Path;

use crate::auth::context::CliContext;
use crate::config::contexts::load_contexts;
use crate::config::files::{load_credentials, load_local_config, save_local_config};
use crate::error::{CliError, Result};
use crate::http;
use crate::project::detection::{find_project_root, get_detection_reason};

/// Run the init flow to select organization and project.
/// Returns (organization_id, project_id).
pub async fn run_init_flow(
    ctx: &CliContext,
    interactive: bool,
    create_local_config: bool,
    skip_if_provided: bool,
    force: bool,
    project_root: &Path,
) -> Result<(String, String)> {
    // Check if we should skip
    if skip_if_provided && ctx.has_org_and_project() {
        if interactive {
            eprintln!("organization and project already configured.");
        }
        return Ok((
            ctx.effective_organization_id().unwrap(),
            ctx.effective_project_id().unwrap(),
        ));
    }

    // Check if local config already exists
    let local_config = load_local_config();
    let existing_scope = match (
        local_config.get("organization").and_then(|v| v.as_str()),
        local_config.get("project").and_then(|v| v.as_str()),
        local_config.get("context").and_then(|v| v.as_str()),
    ) {
        (Some(org), Some(proj), _) => Some((org.to_string(), proj.to_string())),
        (_, _, Some(name)) => load_contexts()
            .get(name)
            .and_then(|entry| Some((entry.organization.clone()?, entry.project.clone()?))),
        _ => None,
    };
    if let Some((org, proj)) = existing_scope
        && !force
    {
        if interactive {
            eprintln!("local configuration already exists in .tensorlake/config.toml");
        }
        let replace = interactive
            && std::io::stdin().is_terminal()
            && dialoguer::Confirm::new()
                .with_prompt("Replace it?")
                .default(false)
                .interact()
                .unwrap_or(false);
        if !replace {
            if interactive {
                eprintln!("keeping it. run 'tl init --force' to replace it without a prompt.");
            }
            return Ok((org, proj));
        }
    }

    // Offer the current context, so the directory follows it.
    if interactive && create_local_config && std::io::stdin().is_terminal() {
        let contexts = load_contexts();
        if let Some(name) = ctx.context_name.as_deref()
            && let Some(entry) = contexts.get(name)
            && let (Some(org), Some(proj)) = (entry.organization.clone(), entry.project.clone())
        {
            let use_context = dialoguer::Confirm::new()
                .with_prompt(format!(
                    "Use context '{name}' ({org} / {proj}) for this directory?"
                ))
                .default(true)
                .interact()
                .map_err(|_| CliError::Cancelled)?;
            if use_context {
                let mut config = crate::config::files::TomlTable::new();
                config.insert("context".to_string(), toml::Value::String(name.to_string()));
                save_local_config(&config, project_root)?;
                let config_path = project_root.join(".tensorlake").join("config.toml");
                eprintln!("configuration saved to {}", config_path.display());
                eprintln!("this directory now follows context '{name}'.");
                return Ok((org, proj));
            }
        }
    }

    let pat = ctx
        .personal_access_token
        .clone()
        .or_else(|| load_credentials(&ctx.api_url))
        .ok_or_else(|| {
            if interactive {
                eprintln!("no valid credentials found. please run 'tl login' first.");
            }
            CliError::Cancelled
        })?;

    if interactive {
        eprintln!("initializing TensorLake configuration...\n");
    }

    let http = http::client_builder().build().map_err(CliError::Http)?;

    // Step 1: Fetch organizations
    let orgs_resp = http
        .get(format!("{}/platform/v1/organizations", ctx.api_url))
        .header("Authorization", format!("Bearer {}", pat))
        .send()
        .await
        .map_err(|e| CliError::auth(e.to_string()))?;

    if orgs_resp.status().as_u16() != 200 {
        if interactive {
            eprintln!(
                "could not fetch organizations (HTTP {}). run 'tl login' to re-authenticate.",
                orgs_resp.status()
            );
        }
        return Err(CliError::Cancelled);
    }

    let orgs_body: serde_json::Value = orgs_resp
        .json()
        .await
        .map_err(|e| CliError::auth(e.to_string()))?;
    let organizations = orgs_body
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CliError::auth("unexpected response from server"))?;

    if organizations.is_empty() {
        if interactive {
            eprintln!("no organizations found. create one at your TensorLake dashboard first.");
        }
        return Err(CliError::Cancelled);
    }

    let organization_id;
    if organizations.len() == 1 {
        let org = &organizations[0];
        organization_id = org
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if interactive {
            let name = org
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            eprintln!("found organization: {} ({})", name, organization_id);
        }
    } else {
        if interactive {
            eprintln!("multiple organizations found:");
            for (i, org) in organizations.iter().enumerate() {
                let name = org
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let id = org.get("id").and_then(|v| v.as_str()).unwrap_or("unknown");
                eprintln!("  {}. {} (ID: {})", i + 1, name, id);
            }
        }

        let selection = dialoguer::Select::new()
            .with_prompt("Select an organization")
            .items(
                organizations
                    .iter()
                    .map(|o| {
                        let name = o.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
                        let id = o.get("id").and_then(|v| v.as_str()).unwrap_or("");
                        format!("{} ({})", name, id)
                    })
                    .collect::<Vec<_>>(),
            )
            .default(0)
            .interact()
            .map_err(|_| CliError::Cancelled)?;

        let org = &organizations[selection];
        organization_id = org
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if interactive {
            let name = org
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            eprintln!("selected: {}", name);
        }
    }

    // Step 2: Fetch projects
    if interactive {
        eprintln!();
    }
    let projects_resp = http
        .get(format!(
            "{}/platform/v1/organizations/{}/projects",
            ctx.api_url, organization_id
        ))
        .header("Authorization", format!("Bearer {}", pat))
        .send()
        .await
        .map_err(|e| CliError::auth(e.to_string()))?;

    if projects_resp.status().as_u16() != 200 {
        if interactive {
            eprintln!(
                "could not fetch projects (HTTP {}). run 'tl login' to re-authenticate.",
                projects_resp.status()
            );
        }
        return Err(CliError::Cancelled);
    }

    let projects_body: serde_json::Value = projects_resp
        .json()
        .await
        .map_err(|e| CliError::auth(e.to_string()))?;
    let projects = projects_body
        .get("items")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CliError::auth("unexpected response from server"))?;

    if projects.is_empty() {
        if interactive {
            eprintln!(
                "no projects found in this organization. create one at your TensorLake dashboard first."
            );
        }
        return Err(CliError::Cancelled);
    }

    let project_id;
    if projects.len() == 1 {
        let proj = &projects[0];
        project_id = proj
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if interactive {
            let name = proj
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            eprintln!("found project: {} ({})", name, project_id);
        }
    } else {
        if interactive {
            eprintln!("multiple projects found:");
            for (i, proj) in projects.iter().enumerate() {
                let name = proj
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let id = proj.get("id").and_then(|v| v.as_str()).unwrap_or("unknown");
                eprintln!("  {}. {} (ID: {})", i + 1, name, id);
            }
        }

        let selection = dialoguer::Select::new()
            .with_prompt("Select a project")
            .items(
                projects
                    .iter()
                    .map(|p| {
                        let name = p.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
                        let id = p.get("id").and_then(|v| v.as_str()).unwrap_or("");
                        format!("{} ({})", name, id)
                    })
                    .collect::<Vec<_>>(),
            )
            .default(0)
            .interact()
            .map_err(|_| CliError::Cancelled)?;

        let proj = &projects[selection];
        project_id = proj
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if interactive {
            let name = proj
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            eprintln!("selected: {}", name);
        }
    }

    // Step 3: Save to local config
    if create_local_config {
        if interactive {
            eprintln!();
        }

        let mut config = crate::config::files::TomlTable::new();
        config.insert(
            "organization".to_string(),
            toml::Value::String(organization_id.clone()),
        );
        config.insert(
            "project".to_string(),
            toml::Value::String(project_id.clone()),
        );
        save_local_config(&config, project_root)?;

        if interactive {
            let config_path = project_root.join(".tensorlake").join("config.toml");
            eprintln!("configuration saved to {}", config_path.display());
            eprintln!(
                "\nYou can now use TensorLake commands in this project without specifying --organization and --project flags."
            );
        }
    }

    Ok((organization_id, project_id))
}

/// CLI entry point for `tensorlake init`.
pub async fn run(
    ctx: &CliContext,
    directory: Option<&str>,
    no_confirm: bool,
    force: bool,
) -> Result<()> {
    let project_root = if let Some(dir) = directory {
        let path = std::path::Path::new(dir).canonicalize()?;
        eprintln!("using specified directory: {}", path.display());
        path
    } else if no_confirm {
        let root = find_project_root(None);
        let reason = get_detection_reason(&root);
        eprintln!("using project root: {} ({})", root.display(), reason);
        root
    } else {
        let root = find_project_root(None);
        let reason = get_detection_reason(&root);
        eprintln!("Detected project root: {}", root.display());
        eprintln!("Reason: {}", reason);

        let confirm = dialoguer::Confirm::new()
            .with_prompt("Is this correct?")
            .default(true)
            .interact()
            .map_err(|_| CliError::Cancelled)?;

        if !confirm {
            let input: String = dialoguer::Input::new()
                .with_prompt("Enter project root directory")
                .default(root.display().to_string())
                .interact_text()
                .map_err(|_| CliError::Cancelled)?;
            std::path::Path::new(&input).canonicalize()?
        } else {
            root
        }
    };

    run_init_flow(ctx, true, true, false, force, &project_root).await?;
    Ok(())
}
