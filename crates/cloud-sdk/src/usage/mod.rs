//! Sandbox usage and attributed cost for an organization.
//!
//! These are the Platform API reads behind the Cloud dashboard's usage page.
//! A window is either the current billing period or the last 1, 7 or 30 days.

use std::str::FromStr;

use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    client::{Client, Traced},
    error::SdkError,
};

pub mod models;

use models::{SandboxUsagePage, UsageSummary};

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

/// Where the next page of per-sandbox usage starts.
///
/// It carries the window the first page resolved, so every page reads the
/// same window even when the window ends at "now". Callers treat it as an
/// opaque value and pass it back unchanged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxUsageCursor {
    window_start: String,
    window_end: String,
    cursor: u64,
}

impl SandboxUsagePage {
    /// The cursor for the following page, or `None` on the last page.
    pub fn next_page_cursor(&self) -> Option<SandboxUsageCursor> {
        self.next_cursor.map(|cursor| SandboxUsageCursor {
            window_start: self.window_start.clone(),
            window_end: self.window_end.clone(),
            cursor,
        })
    }
}

/// A request for one page of per-sandbox usage.
#[derive(Clone, Debug, Default)]
pub struct SandboxUsageRequest {
    /// Ignored when `cursor` is set; the cursor keeps the first page's window.
    pub window: UsageWindow,
    /// Page size, 1 to 100. The server defaults to 50.
    pub limit: Option<u32>,
    pub cursor: Option<SandboxUsageCursor>,
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

    /// One page of per-sandbox usage, ordered as the dashboard shows it.
    ///
    /// Pass [`SandboxUsagePage::next_page_cursor`] back in
    /// [`SandboxUsageRequest::cursor`] to read the next page.
    pub async fn sandboxes(
        &self,
        request: &SandboxUsageRequest,
    ) -> Result<Traced<SandboxUsagePage>, SdkError> {
        let path = self.endpoint("/sandboxes")?;
        let mut query: Vec<(&str, String)> = Vec::new();
        match &request.cursor {
            Some(cursor) => {
                query.push(("fromDate", cursor.window_start.clone()));
                query.push(("toDate", cursor.window_end.clone()));
                query.push(("cursor", cursor.cursor.to_string()));
            }
            None => query.extend(request.window.range_query(Utc::now())),
        }
        if let Some(limit) = request.limit {
            query.push(("limit", limit.to_string()));
        }
        let query: Vec<(&str, &str)> = query.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let req = self.client.build_get_json_request(&path, Some(&query))?;
        self.client.execute_json(req).await
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
}
