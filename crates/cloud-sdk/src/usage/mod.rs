//! Sandbox usage and attributed cost for an organization.
//!
//! These are the Platform API reads behind the Cloud dashboard's usage page.
//! A window is either the current billing period or the last 1, 7 or 30 days.

use std::str::FromStr;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use reqwest::header::ACCEPT;
use serde::Deserialize;

use crate::{
    client::{Client, Traced},
    error::SdkError,
};

pub mod models;

use models::{SandboxUsage, SandboxUsageExport, SandboxUsageExportWindow, UsageSummary};

/// The time window a usage read covers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UsageWindow {
    /// The organization's current billing period, through now.
    #[default]
    CurrentPeriod,
    /// The last 24 hours.
    LastDay,
    /// The last 7 days.
    LastWeek,
    /// The last 30 days, the oldest usage the API serves.
    Last30Days,
}

impl UsageWindow {
    fn lookback_days(self) -> Option<i64> {
        match self {
            Self::CurrentPeriod => None,
            Self::LastDay => Some(1),
            Self::LastWeek => Some(7),
            Self::Last30Days => Some(30),
        }
    }

    /// The range query for this window. A lookback sends an exact range
    /// ending at `now`; the current period sends none, so the server resolves
    /// the billing period itself.
    fn range_query(self, now: DateTime<Utc>) -> Vec<(&'static str, String)> {
        let Some(days) = self.lookback_days() else {
            return Vec::new();
        };
        let format = |at: DateTime<Utc>| at.to_rfc3339_opts(SecondsFormat::Millis, true);
        vec![
            ("fromDate", format(now - Duration::days(days))),
            ("toDate", format(now)),
        ]
    }
}

impl FromStr for UsageWindow {
    type Err = SdkError;

    /// Parse a lookback such as `1d`, `7d` or `30d`.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "1d" => Ok(Self::LastDay),
            "7d" => Ok(Self::LastWeek),
            "30d" => Ok(Self::Last30Days),
            other => Err(SdkError::ClientError(format!(
                "unsupported usage window '{other}'; expected 1d, 7d or 30d"
            ))),
        }
    }
}

/// One line of the export's newline-delimited JSON.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum ExportRecord {
    Window(Box<SandboxUsageExportWindow>),
    Sandbox(Box<SandboxUsage>),
    End { count: u64 },
}

/// Collect an export body, rejecting one that lacks its window header or its
/// end record, or whose count disagrees: a stream cut short after the
/// response started looks otherwise like a smaller, valid export.
fn parse_export(body: &str) -> Result<SandboxUsageExport, SdkError> {
    let incomplete =
        |detail: &str| SdkError::ClientError(format!("incomplete sandbox usage export: {detail}"));
    let mut lines = body.lines().filter(|line| !line.trim().is_empty());
    let window = match lines.next().map(serde_json::from_str::<ExportRecord>) {
        Some(Ok(ExportRecord::Window(window))) => *window,
        Some(Err(err)) => return Err(err.into()),
        _ => return Err(incomplete("missing window header")),
    };
    let mut sandboxes = Vec::new();
    for line in lines {
        match serde_json::from_str::<ExportRecord>(line)? {
            ExportRecord::Sandbox(sandbox) => sandboxes.push(*sandbox),
            ExportRecord::End { count } if count == sandboxes.len() as u64 => {
                return Ok(SandboxUsageExport { window, sandboxes });
            }
            ExportRecord::End { count } => {
                return Err(incomplete(&format!(
                    "expected {count} sandboxes, received {}",
                    sandboxes.len()
                )));
            }
            ExportRecord::Window(_) => return Err(incomplete("repeated window header")),
        }
    }
    Err(incomplete("missing end record"))
}

#[derive(Clone)]
pub struct UsageClient {
    client: Client,
    organization_id: String,
}

impl UsageClient {
    pub fn new(client: Client, organization_id: impl Into<String>) -> Self {
        Self {
            client,
            organization_id: organization_id.into(),
        }
    }

    fn endpoint(&self, suffix: &str) -> Result<String, SdkError> {
        if self.organization_id.trim().is_empty() {
            return Err(SdkError::ClientError(
                "an organization ID is required to read usage".to_string(),
            ));
        }
        Ok(format!(
            "/platform/v1/organizations/{}/sandbox-usage{suffix}",
            urlencoding::encode(&self.organization_id)
        ))
    }

    /// Organization usage totals for a window.
    pub async fn summary(&self, window: UsageWindow) -> Result<Traced<UsageSummary>, SdkError> {
        let path = self.endpoint("/breakdown")?;
        let mut query = vec![("granularity", "day".to_string())];
        query.extend(window.range_query(Utc::now()));
        let query: Vec<(&str, &str)> = query.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let req = self.client.build_get_json_request(&path, Some(&query))?;
        self.client.execute_json(req).await
    }

    /// Usage and attributed cost of every sandbox in the window, in one
    /// request. The server computes the attribution once and streams it.
    pub async fn sandboxes(
        &self,
        window: UsageWindow,
    ) -> Result<Traced<SandboxUsageExport>, SdkError> {
        let path = self.endpoint("/sandboxes/export")?;
        let query = window.range_query(Utc::now());
        let query: Vec<(&str, &str)> = query.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let req = self
            .client
            .request(reqwest::Method::GET, &path)
            .query(&query)
            .header(ACCEPT, "application/x-ndjson")
            .build()?;
        let traced = self.client.execute_traced(req).await?;
        let trace_id = traced.trace_id.clone();
        let body = traced.into_inner().text().await?;
        Ok(Traced::new(trace_id, parse_export(&body)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_lookbacks() {
        assert_eq!("1d".parse::<UsageWindow>().unwrap(), UsageWindow::LastDay);
        assert_eq!("7d".parse::<UsageWindow>().unwrap(), UsageWindow::LastWeek);
        assert_eq!(
            "30d".parse::<UsageWindow>().unwrap(),
            UsageWindow::Last30Days
        );
        assert!("2d".parse::<UsageWindow>().is_err());
    }

    #[test]
    fn lookbacks_send_an_exact_range_and_current_period_sends_none() {
        let now = DateTime::parse_from_rfc3339("2026-10-28T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert!(UsageWindow::CurrentPeriod.range_query(now).is_empty());
        assert_eq!(
            UsageWindow::LastWeek.range_query(now),
            vec![
                ("fromDate", "2026-10-21T12:00:00.000Z".to_string()),
                ("toDate", "2026-10-28T12:00:00.000Z".to_string()),
            ]
        );
    }

    const WINDOW: &str = r#"{"type":"window","windowStart":"2026-10-01T00:00:00.000Z","windowEnd":"2026-10-28T12:00:00.000Z","attributedUsageCharge":null,"includedUsage":null,"attributedUsageValue":null}"#;

    #[test]
    fn export_requires_the_end_record_and_matching_count() {
        let complete = format!("{WINDOW}\n{{\"type\":\"end\",\"count\":0}}\n");
        assert!(parse_export(&complete).unwrap().sandboxes.is_empty());

        let truncated = format!("{WINDOW}\n");
        let err = parse_export(&truncated).unwrap_err().to_string();
        assert!(err.contains("missing end record"), "{err}");

        let short = format!("{WINDOW}\n{{\"type\":\"end\",\"count\":2}}\n");
        let err = parse_export(&short).unwrap_err().to_string();
        assert!(err.contains("expected 2 sandboxes"), "{err}");

        let err = parse_export("").unwrap_err().to_string();
        assert!(err.contains("missing window header"), "{err}");
    }
}
