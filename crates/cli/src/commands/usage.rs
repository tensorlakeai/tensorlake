use comfy_table::Cell;
use tensorlake::ClientBuilder;
use tensorlake::error::SdkError;
use tensorlake::usage::models::{
    SandboxUsage, SandboxUsageExportWindow, UsageChargeBasis, UsageChargeResources,
    UsageProjectRef, UsageSummary,
};
use tensorlake::usage::{UsageClient, UsageWindow};

use crate::auth::context::CliContext;
use crate::error::{CliError, Result, remote_message};
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
    let export = usage_client(ctx)?
        .sandboxes(window(last))
        .await
        .map_err(map_sdk_error)?
        .into_inner();

    if json {
        println!("{}", serde_json::to_string_pretty(&export)?);
        return Ok(());
    }

    let window = &export.window;
    println!("Window: {} to {}", window.window_start, window.window_end);
    if export.sandboxes.is_empty() {
        println!("no sandbox usage found");
        return Ok(());
    }
    print_sandboxes(&export.sandboxes, window);
    Ok(())
}

const USAGE_VALUE_NOTE: &str =
    "Usage value is usage at the plan's overage rate, before included usage.";

fn print_summary(summary: &UsageSummary) {
    println!(
        "Window:            {} to {}",
        summary.window_start, summary.window_end
    );
    let charge = summary.usage_charge.as_ref();
    let value = summary.usage_value.as_ref();
    match (charge, value) {
        (Some(charge), _) => println!(
            "Charge:            {}",
            format_minor(&charge.amount_minor, &charge.currency)
        ),
        // Allowance plans can only price a window inside the current billing
        // period, but every window has a usage value.
        (None, Some(_)) => {
            println!("Charge:            - (window crosses the billing period start)")
        }
        (None, None) => println!("Charge:            -"),
    }
    if let Some(value) = value {
        println!(
            "Usage value:       {}",
            format_minor(&value.amount_minor, &value.currency)
        );
    }
    if let Some(included) = &summary.included_usage {
        println!(
            "Included usage:    {} of {} used this period",
            format_minor(&included.period_used_amount_minor, &included.currency),
            format_minor(&included.included_amount_minor, &included.currency)
        );
    }
    println!("Running sandboxes: {}", summary.running_sandboxes);

    if charge.is_some() || value.is_some() {
        let mut headers = vec!["Resource"];
        if charge.is_some() {
            headers.push("Charge");
        }
        if value.is_some() {
            headers.push("Usage value");
        }
        let mut table = new_table(&headers);
        let splits: Vec<_> = [charge, value]
            .into_iter()
            .flatten()
            .map(|split| (resource_amounts(&split.resources), &split.currency))
            .collect();
        for (index, name) in ["CPU", "Memory", "Disk", "Snapshot storage"]
            .into_iter()
            .enumerate()
        {
            let mut row = vec![Cell::new(name)];
            for (amounts, currency) in &splits {
                row.push(Cell::new(format_minor(amounts[index], currency)));
            }
            table.add_row(row);
        }
        println!("{table}");
    }
    if value.is_some() {
        println!("{USAGE_VALUE_NOTE}");
    }
    if charge.is_none() && value.is_none() {
        // Without a usage price there is no money to show.
        print_compute_units(summary);
        return;
    }
    println!("Run `tl usage sandboxes` for cost per project and sandbox.");
}

fn print_compute_units(summary: &UsageSummary) {
    println!("Compute units:     {}", summary.compute_units);
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

fn resource_amounts(resources: &UsageChargeResources) -> [&str; 4] {
    [
        &resources.cpu_amount_minor,
        &resources.memory_amount_minor,
        &resources.sandbox_storage_amount_minor,
        &resources.snapshot_storage_amount_minor,
    ]
}

fn print_sandboxes(sandboxes: &[SandboxUsage], window: &SandboxUsageExportWindow) {
    let charge = window.attributed_usage_charge.as_ref();
    let value = window.attributed_usage_value.as_ref();
    let money_headers = || {
        let mut headers = Vec::new();
        if charge.is_some() {
            headers.push("Charge");
        }
        if value.is_some() {
            headers.push("Usage value");
        }
        headers
    };
    let money_cells = |attributed: Option<i128>, valued: Option<i128>| {
        let mut cells = Vec::new();
        if let Some(charge) = charge {
            cells.push(Cell::new(format_amount(attributed, &charge.currency)));
        }
        if let Some(value) = value {
            cells.push(Cell::new(format_amount(valued, &value.currency)));
        }
        cells
    };

    let mut headers = vec![
        "Sandbox",
        "Project",
        "vCPU-hours",
        "Memory GiB-hours",
        "Disk GiB-hours",
        "Snapshot GiB-hours",
        "Egress GiB",
    ];
    headers.extend(money_headers());
    let mut table = new_table(&headers);
    for sandbox in sandboxes {
        let resources = &sandbox.resources;
        let mut row = vec![
            Cell::new(&sandbox.sandbox_id),
            Cell::new(project_name(sandbox.project.as_ref())),
            Cell::new(&resources.cpu.quantity),
            Cell::new(&resources.memory.quantity),
            Cell::new(&resources.sandbox_storage.quantity),
            Cell::new(&resources.snapshot_storage.quantity),
            Cell::new(&resources.network_egress.quantity),
        ];
        row.extend(money_cells(
            parse_minor(sandbox.attributed_amount_minor.as_deref()),
            parse_minor(sandbox.usage_value_amount_minor.as_deref()),
        ));
        table.add_row(row);
    }
    println!("{table}");

    if charge.is_some() || value.is_some() {
        let mut headers = vec!["Project", "Sandboxes"];
        headers.extend(money_headers());
        let mut table = new_table(&headers);
        for project in project_totals(sandboxes) {
            let mut row = vec![
                Cell::new(project_name(project.project)),
                Cell::new(project.sandbox_count),
            ];
            row.extend(money_cells(project.attributed, project.valued));
            table.add_row(row);
        }
        println!("{table}");
    }

    let count = sandboxes.len();
    let noun = if count == 1 { "sandbox" } else { "sandboxes" };
    let mut totals = Vec::new();
    if let Some(charge) = charge {
        let basis = match charge.basis {
            UsageChargeBasis::Invoice => "invoiced",
            UsageChargeBasis::CurrentPriceEstimate => "estimated",
            UsageChargeBasis::Unknown => "unknown basis",
        };
        totals.push(format!(
            "{} charge ({basis})",
            format_minor(&charge.amount_minor, &charge.currency)
        ));
    }
    if let Some(value) = value {
        totals.push(format!(
            "{} usage value",
            format_minor(&value.amount_minor, &value.currency)
        ));
    }
    if totals.is_empty() {
        println!("{count} {noun}");
    } else {
        println!("{count} {noun}, {}", totals.join(", "));
    }
    if charge.is_none() && value.is_some() {
        println!(
            "The window crosses the billing period start, so it has no charge; showing usage value."
        );
    }
    if value.is_some() {
        println!("{USAGE_VALUE_NOTE}");
    }
}

/// One project's share of the export. The server attributes amounts across
/// all sandboxes, so the per-project sums are exact.
struct ProjectTotal<'a> {
    project: Option<&'a UsageProjectRef>,
    sandbox_count: usize,
    attributed: Option<i128>,
    valued: Option<i128>,
}

/// Per-project totals in the export's order, which groups sandboxes by
/// project.
fn project_totals(sandboxes: &[SandboxUsage]) -> Vec<ProjectTotal<'_>> {
    let mut totals: Vec<ProjectTotal<'_>> = Vec::new();
    for sandbox in sandboxes {
        let project = sandbox.project.as_ref();
        let id = project.map(|p| p.project_id.as_str());
        let index = match totals
            .iter()
            .position(|t| t.project.map(|p| p.project_id.as_str()) == id)
        {
            Some(index) => index,
            None => {
                totals.push(ProjectTotal {
                    project,
                    sandbox_count: 0,
                    attributed: Some(0),
                    valued: Some(0),
                });
                totals.len() - 1
            }
        };
        let total = &mut totals[index];
        total.sandbox_count += 1;
        total.attributed = add_minor(
            total.attributed,
            parse_minor(sandbox.attributed_amount_minor.as_deref()),
        );
        total.valued = add_minor(
            total.valued,
            parse_minor(sandbox.usage_value_amount_minor.as_deref()),
        );
    }
    totals
}

fn add_minor(total: Option<i128>, amount: Option<i128>) -> Option<i128> {
    Some(total? + amount?)
}

fn parse_minor(amount_minor: Option<&str>) -> Option<i128> {
    amount_minor?.parse().ok()
}

fn project_name(project: Option<&UsageProjectRef>) -> &str {
    project.map(|p| p.project_name.as_str()).unwrap_or("-")
}

fn format_amount(amount_minor: Option<i128>, currency: &str) -> String {
    amount_minor
        .map(|cents| format_cents(cents, currency))
        .unwrap_or_else(|| "-".to_string())
}

/// Format an amount in minor units (cents) as a decimal with its currency.
fn format_minor(amount_minor: &str, currency: &str) -> String {
    match amount_minor.parse::<i128>() {
        Ok(cents) => format_cents(cents, currency),
        Err(_) => format!("{amount_minor} {} minor units", currency.to_uppercase()),
    }
}

fn format_cents(cents: i128, currency: &str) -> String {
    let currency = currency.to_uppercase();
    let sign = if cents < 0 { "-" } else { "" };
    let cents = cents.unsigned_abs();
    format!("{sign}{}.{:02} {currency}", cents / 100, cents % 100)
}

fn usage_client(ctx: &CliContext) -> Result<UsageClient> {
    let organization_id = ctx.effective_organization_id().ok_or_else(|| {
        CliError::auth("missing organization ID; pass --organization or run `tl init`")
    })?;
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

    fn sandbox(id: &str, project: Option<&str>, charge: Option<&str>, value: &str) -> SandboxUsage {
        let resource = serde_json::json!({
            "quantity": "0", "unit": "GiB-hour",
            "attributedAmountMinor": null, "usageValueAmountMinor": null,
        });
        serde_json::from_value(serde_json::json!({
            "sandboxId": id,
            "project": project.map(|p| serde_json::json!({"projectId": p, "projectName": p})),
            "attributedAmountMinor": charge,
            "usageValueAmountMinor": value,
            "resources": {
                "cpu": resource, "memory": resource, "sandboxStorage": resource,
                "snapshotStorage": resource, "networkEgress": resource,
            },
        }))
        .unwrap()
    }

    #[test]
    fn totals_amounts_per_project() {
        let sandboxes = [
            sandbox("a", Some("p1"), Some("10"), "100"),
            sandbox("b", Some("p1"), Some("5"), "50"),
            sandbox("c", None, None, "7"),
        ];
        let totals = project_totals(&sandboxes);
        assert_eq!(totals.len(), 2);
        assert_eq!(project_name(totals[0].project), "p1");
        assert_eq!(totals[0].sandbox_count, 2);
        assert_eq!(totals[0].attributed, Some(15));
        assert_eq!(totals[0].valued, Some(150));
        assert_eq!(project_name(totals[1].project), "-");
        assert_eq!(totals[1].attributed, None);
        assert_eq!(totals[1].valued, Some(7));
    }
}
