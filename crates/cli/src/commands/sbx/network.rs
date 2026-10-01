use clap::{Args, Subcommand};
use comfy_table::Cell;
use tensorlake::sandboxes::{SandboxesClient, network::NetworkQuery};

use crate::{
    auth::context::CliContext,
    error::{CliError, Result},
    output::table::new_table,
};

#[derive(Args)]
pub struct QueryArgs {
    /// Sandbox ID (works for stopped sandboxes; never opens a guest connection)
    sandbox_id: String,
    /// Start of the query window, Unix milliseconds (default: last hour)
    #[arg(long)]
    from_ms: Option<i64>,
    /// End of the query window, Unix milliseconds
    #[arg(long)]
    to_ms: Option<i64>,
    /// At most 500 records per page
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..=500))]
    limit: Option<u16>,
    /// Opaque next_cursor from a previous event page
    #[arg(long)]
    cursor: Option<String>,
    #[arg(long)]
    json: bool,
}
impl QueryArgs {
    fn query(&self) -> NetworkQuery {
        NetworkQuery {
            from_ms: self.from_ms,
            to_ms: self.to_ms,
            limit: self.limit,
            cursor: self.cursor.clone(),
        }
    }
}

#[derive(Subcommand)]
pub enum NetworkCommands {
    /// Show observed connections, DNS, policy decisions and capture gaps
    Events(QueryArgs),
    /// Summarize observed connections; byte counters may be unavailable
    Destinations(QueryArgs),
    /// Show the last collector heartbeat and its declared coverage
    Status { sandbox_id: String },
}

pub async fn run(ctx: &CliContext, command: NetworkCommands) -> Result<()> {
    let client = SandboxesClient::new(ctx.scoped_cloud_client()?, &ctx.namespace, true);
    match command {
        NetworkCommands::Events(args) => {
            let response = client
                .network_events(&args.sandbox_id, &args.query())
                .await?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&*response)?);
                return Ok(());
            }
            let mut table =
                new_table(&["Time (ms)", "Observation", "Destination", "DNS", "Detail"]);
            for event in &response.events {
                table.add_row([
                    Cell::new(event.observed_at_ms),
                    Cell::new(&event.event_kind),
                    Cell::new(if event.destination_ip.is_empty() {
                        "-".into()
                    } else {
                        format!("{}:{}", event.destination_ip, event.destination_port)
                    }),
                    Cell::new(event.dns_name.as_deref().unwrap_or("-")),
                    Cell::new(&event.detail),
                ]);
            }
            println!("{table}");
            if response.events.is_empty() {
                println!(
                    "No observations recorded. Check capture status before interpreting this as no traffic."
                );
            }
            if let Some(cursor) = &response.next_cursor {
                println!("Next cursor: {cursor}");
            }
        }
        NetworkCommands::Destinations(args) => {
            if args.cursor.is_some() {
                return Err(CliError::usage("--cursor applies only to network events"));
            }
            let response = client
                .network_destinations(&args.sandbox_id, &args.query())
                .await?;
            if args.json {
                println!("{}", serde_json::to_string_pretty(&*response)?);
                return Ok(());
            }
            let mut table = new_table(&[
                "Destination",
                "Transport",
                "Connections",
                "Sent bytes",
                "Received bytes",
                "Unknown bytes",
            ]);
            for row in &response.destinations {
                table.add_row([
                    Cell::new(format!("{}:{}", row.destination_ip, row.destination_port)),
                    Cell::new(&row.transport),
                    Cell::new(row.observed_connections),
                    Cell::new(
                        row.original_bytes
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "unknown".into()),
                    ),
                    Cell::new(
                        row.reply_bytes
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "unknown".into()),
                    ),
                    Cell::new(row.unknown_byte_connections),
                ]);
            }
            println!(
                "{table}\nWindow: {}–{} ms. Connection counts are not request counts.",
                response.from_ms, response.to_ms
            );
            if response.truncated {
                println!("Destination limit reached; narrow the time window for more detail.");
            }
        }
        NetworkCommands::Status { sandbox_id } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&*client.network_status(&sandbox_id).await?)?
            );
        }
    }
    Ok(())
}
