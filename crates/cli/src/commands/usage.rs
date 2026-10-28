use comfy_table::Cell;
use serde::Deserialize;
use tensorlake::ClientBuilder;
use tensorlake::error::SdkError;
use tensorlake::usage::models::{
    AttributedUsageCharge, SandboxUsage, UsageChargeBasis, UsageSummary,
};
use tensorlake::usage::{SandboxUsageRequest, UsageClient, UsageWindow};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result};
use crate::output::table::new_table;

/// How far back `--last` reads, instead of the current billing period.
#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum Lookback {
    #[value(name = "1d")]
    Day,
    #[value(name = "7d")]
    Week,
    #[value(name = "30d")]
    Month,
}

fn window(last: Option<Lookback>) -> UsageWindow {
    match last {
        None => UsageWindow::CurrentPeriod,
        Some(Lookback::Day) => UsageWindow::LastDay,
        Some(Lookback::Week) => UsageWindow::LastWeek,
        Some(Lookback::Month) => UsageWindow::Last30Days,
    }
}

const SANDBOX_PAGE_SIZE: u32 = 100;

pub async fn summary(ctx: &CliContext, last: Option<Lookback>, json: bool) -> Result<()> {
    let summary = usage_client(ctx)?
        .summary(window(last))
        .await
        .map_err(map_sdk_error)?
        .into_inner();

    if json {
        println!("{}", serde_json::to_string_pretty(&summary)?);
        return Ok(());
    }
    print_summary(&summary);
    Ok(())
}

pub async fn sandboxes(ctx: &CliContext, last: Option<Lookback>, json: bool) -> Result<()> {
    let client = usage_client(ctx)?;
    let mut request = SandboxUsageRequest {
        window: window(last),
        limit: Some(SANDBOX_PAGE_SIZE),
        cursor: None,
    };
    let mut first = None;
    let mut sandboxes = Vec::new();
    loop {
        let page = client
            .sandboxes(&request)
            .await
            .map_err(map_sdk_error)?
            .into_inner();
        request.cursor = page.next_page_cursor();
        sandboxes.extend(page.sandboxes.iter().cloned());
        first.get_or_insert(page);
        if request.cursor.is_none() {
            break;
        }
    }
    let Some(first) = first else {
        return Ok(());
    };

    if json {
        let body = serde_json::json!({
            "windowStart": first.window_start,
            "windowEnd": first.window_end,
            "attributedUsageCharge": first.attributed_usage_charge,
            "sandboxes": sandboxes,
        });
        println!("{}", serde_json::to_string_pretty(&body)?);
        return Ok(());
    }

    println!("Window: {} to {}", first.window_start, first.window_end);
    if sandboxes.is_empty() {
        println!("no sandbox usage found");
        return Ok(());
    }
    print_sandboxes(&sandboxes, first.attributed_usage_charge.as_ref());
    Ok(())
}

fn print_summary(summary: &UsageSummary) {
    println!(
        "Window:            {} to {}",
        summary.window_start, summary.window_end
    );
    println!("Compute units:     {}", summary.compute_units);
    match &summary.usage_charge {
        Some(charge) => println!(
            "Usage charge:      {}",
            format_minor(&charge.amount_minor, &charge.currency)
        ),
        None => println!("Usage charge:      -"),
    }
    if let Some(included) = &summary.included_usage {
        println!(
            "Included usage:    {} of {} used this period",
            format_minor(&included.period_used_amount_minor, &included.currency),
            format_minor(&included.included_amount_minor, &included.currency)
        );
    }
    println!("Running sandboxes: {}", summary.running_sandboxes);

    if summary.projects.is_empty() {
        return;
    }
    let mut table = new_table(&["Project", "Compute units"]);
    for project in &summary.projects {
        table.add_row(vec![
            Cell::new(&project.project_name),
            Cell::new(&project.compute_units),
        ]);
    }
    println!("{table}");
}

fn print_sandboxes(sandboxes: &[SandboxUsage], charge: Option<&AttributedUsageCharge>) {
    let currency = charge.map(|c| c.currency.as_str()).unwrap_or("usd");
    let mut table = new_table(&[
        "Sandbox",
        "Project",
        "vCPU-hours",
        "Memory GiB-hours",
        "Disk GiB-hours",
        "Snapshot GiB-hours",
        "Egress GiB",
        "Cost",
    ]);
    for sandbox in sandboxes {
        let resources = &sandbox.resources;
        table.add_row(vec![
            Cell::new(&sandbox.sandbox_id),
            Cell::new(
                sandbox
                    .project
                    .as_ref()
                    .map(|p| p.project_name.as_str())
                    .unwrap_or("-"),
            ),
            Cell::new(&resources.cpu.quantity),
            Cell::new(&resources.memory.quantity),
            Cell::new(&resources.sandbox_storage.quantity),
            Cell::new(&resources.snapshot_storage.quantity),
            Cell::new(&resources.network_egress.quantity),
            Cell::new(
                sandbox
                    .attributed_amount_minor
                    .as_deref()
                    .map(|amount| format_minor(amount, currency))
                    .unwrap_or_else(|| "-".to_string()),
            ),
        ]);
    }
    println!("{table}");

    let count = sandboxes.len();
    let noun = if count == 1 { "sandbox" } else { "sandboxes" };
    match charge {
        Some(charge) => {
            let basis = match charge.basis {
                UsageChargeBasis::Invoice => "invoiced",
                _ => "estimated",
            };
            println!(
                "{count} {noun}, {} total ({basis})",
                format_minor(&charge.amount_minor, &charge.currency)
            );
        }
        None => println!("{count} {noun}"),
    }
}

/// Format an amount in minor units (cents) as a decimal with its currency.
fn format_minor(amount_minor: &str, currency: &str) -> String {
    let currency = currency.to_uppercase();
    let Ok(cents) = amount_minor.parse::<i128>() else {
        return format!("{amount_minor} {currency} minor units");
    };
    let sign = if cents < 0 { "-" } else { "" };
    let cents = cents.unsigned_abs();
    format!("{sign}{}.{:02} {currency}", cents / 100, cents % 100)
}

fn usage_client(ctx: &CliContext) -> Result<UsageClient> {
    let organization_id = ctx
        .effective_organization_id()
        .ok_or_else(|| CliError::auth("missing organization ID; run `tl init`"))?;
    let token = ctx.bearer_token()?;
    let client = ClientBuilder::new(&ctx.api_url)
        .bearer_token(&token)
        .build()?;
    Ok(UsageClient::new(client, organization_id))
}

fn map_sdk_error(error: SdkError) -> CliError {
    match error {
        SdkError::Authentication(_) => {
            CliError::auth("authentication failed. set TENSORLAKE_API_KEY or run 'tl login'.")
        }
        SdkError::Authorization(_) => CliError::auth(
            "permission denied. you must be a member of the organization to read its usage.",
        ),
        SdkError::ServerError { status, message } if status.is_client_error() => CliError::usage(
            format!("could not read usage: {}", remote_message(&message)),
        ),
        other => CliError::from(other),
    }
}

#[derive(Deserialize)]
struct ErrorBody {
    message: String,
}

fn remote_message(raw: &str) -> String {
    serde_json::from_str::<ErrorBody>(raw)
        .map(|body| body.message)
        .unwrap_or_else(|_| raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_minor_units() {
        assert_eq!(format_minor("1234", "usd"), "12.34 USD");
        assert_eq!(format_minor("5", "usd"), "0.05 USD");
        assert_eq!(format_minor("-250", "usd"), "-2.50 USD");
        assert_eq!(format_minor("abc", "usd"), "abc USD minor units");
    }
}
