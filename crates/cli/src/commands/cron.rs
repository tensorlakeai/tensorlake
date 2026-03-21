use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use comfy_table::Cell;
use tensorlake_cloud_sdk::ClientBuilder;
use tensorlake_cloud_sdk::cron::CronClient;
use tensorlake_cloud_sdk::error::SdkError;

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

// ── helpers ──────────────────────────────────────────────────────────────────

fn cron_client(ctx: &CliContext) -> Result<CronClient> {
    let token = ctx.bearer_token()?;
    let mut builder = ClientBuilder::new(&ctx.api_url).bearer_token(&token);
    let use_scope_headers = ctx.personal_access_token.is_some() && ctx.api_key.is_none();
    if use_scope_headers {
        if let (Some(org), Some(proj)) =
            (ctx.effective_organization_id(), ctx.effective_project_id())
        {
            builder = builder.scope(&org, &proj);
        }
    }
    Ok(CronClient::new(builder.build().map_err(CliError::from)?))
}

fn map_sdk_error(error: SdkError) -> CliError {
    match error {
        SdkError::Authentication(_) => {
            CliError::auth("authentication failed. set TENSORLAKE_API_KEY or run 'tl login'.")
        }
        SdkError::Authorization(_) => {
            CliError::auth("permission denied. check your API key permissions or run 'tl init'.")
        }
        SdkError::ServerError { status, message } => {
            let code = status.as_u16();
            if (400..500).contains(&code) {
                // Try to extract the `error` field from the JSON body.
                let detail = serde_json::from_str::<serde_json::Value>(&message)
                    .ok()
                    .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(str::to_string))
                    .unwrap_or(message);
                CliError::usage(detail)
            } else {
                CliError::Other(anyhow::anyhow!("server error ({})", code))
            }
        }
        other => CliError::from(other),
    }
}

fn ms_to_display(ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ms)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| ms.to_string())
}

// ── create ────────────────────────────────────────────────────────────────────

pub async fn create(
    ctx: &CliContext,
    application: &str,
    cron_expression: &str,
    input_json: Option<&str>,
    input_file: Option<&str>,
) -> Result<()> {
    // Build optional base64 payload.
    let input_base64: Option<String> = match (input_json, input_file) {
        (Some(_), Some(_)) => {
            return Err(CliError::usage(
                "--input-json and --input-file are mutually exclusive",
            ));
        }
        (Some(json), None) => Some(BASE64.encode(json.as_bytes())),
        (None, Some(path)) => {
            let bytes = std::fs::read(path).map_err(|e| {
                CliError::usage(format!("could not read input file '{}': {}", path, e))
            })?;
            Some(BASE64.encode(&bytes))
        }
        (None, None) => None,
    };

    let client = cron_client(ctx)?;
    let resp = client
        .create(&ctx.namespace, application, cron_expression, input_base64)
        .await
        .map_err(map_sdk_error)?;

    println!(
        "Cron schedule created for '{}': {}",
        application, resp.schedule_id
    );
    Ok(())
}

// ── list ──────────────────────────────────────────────────────────────────────

pub async fn list(ctx: &CliContext, application: &str) -> Result<()> {
    let client = cron_client(ctx)?;
    let resp = client
        .list(&ctx.namespace, application)
        .await
        .map_err(map_sdk_error)?;

    if resp.schedules.is_empty() {
        println!("no cron schedules found for '{}'", application);
        return Ok(());
    }

    let mut table = new_table(&[
        "ID",
        "Expression",
        "Next Run (UTC)",
        "Last Run (UTC)",
        "Status",
    ]);
    for s in &resp.schedules {
        let next = ms_to_display(s.next_fire_time_ms);
        let last = s
            .last_fired_at_ms
            .map(ms_to_display)
            .unwrap_or_else(|| "never".to_string());
        let status = if s.enabled { "active" } else { "disabled" };
        table.add_row(vec![
            Cell::new(&s.id),
            Cell::new(&s.cron_expression),
            Cell::new(next),
            Cell::new(last),
            Cell::new(status),
        ]);
    }
    println!("{table}");

    let count = resp.schedules.len();
    if count == 1 {
        println!("1 schedule");
    } else {
        println!("{} schedules", count);
    }
    Ok(())
}

// ── delete ────────────────────────────────────────────────────────────────────

pub async fn delete(ctx: &CliContext, application: &str, schedule_id: &str) -> Result<()> {
    let client = cron_client(ctx)?;
    client
        .delete(&ctx.namespace, application, schedule_id)
        .await
        .map_err(map_sdk_error)?;

    println!(
        "Cron schedule '{}' deleted from '{}'",
        schedule_id, application
    );
    Ok(())
}
