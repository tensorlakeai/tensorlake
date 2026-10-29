//! Sandbox usage responses.
//!
//! Money amounts are integer minor currency units (for example cents), and
//! quantities and compute units are decimal strings. Both stay strings so
//! callers never lose precision to floating point.

use serde::{Deserialize, Serialize};

/// An amount of money in minor currency units.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageAmount {
    pub currency: String,
    pub amount_minor: String,
}

/// The metered usage charge split across resources.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageChargeResources {
    pub cpu_amount_minor: String,
    pub memory_amount_minor: String,
    pub sandbox_storage_amount_minor: String,
    pub snapshot_storage_amount_minor: String,
}

/// Metered usage for the window, excluding subscription fees and tax.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageCharge {
    pub currency: String,
    pub amount_minor: String,
    pub resources: UsageChargeResources,
}

/// Plan allowance consumption, valued at the overage rate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncludedUsage {
    pub currency: String,
    pub included_amount_minor: String,
    pub period_used_amount_minor: String,
    pub range_usage_amount_minor: String,
    pub period_start: String,
    pub period_end: String,
}

/// Compute units used by one project.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectComputeUnits {
    pub project_id: String,
    pub project_name: String,
    pub compute_units: String,
}

/// Organization usage totals for a window.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageSummary {
    pub window_start: String,
    pub window_end: String,
    pub compute_units: String,
    /// Null when the organization has no metered price, and on plans with
    /// included usage (Pro, Enterprise) when the window is not inside the
    /// current billing period. Use `usage_value` then.
    pub usage_charge: Option<UsageCharge>,
    #[serde(default)]
    pub included_usage: Option<IncludedUsage>,
    /// Usage valued at the plan's overage rate, before included usage. Set
    /// for every window on plans with included usage, null otherwise.
    #[serde(default)]
    pub usage_value: Option<UsageCharge>,
    pub running_sandboxes: u64,
    pub projects: Vec<ProjectComputeUnits>,
}

/// Whether an attributed charge comes from a finalized invoice or from the
/// current price.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageChargeBasis {
    Invoice,
    CurrentPriceEstimate,
    #[serde(other)]
    Unknown,
}

/// A share of the organization's pooled usage charge.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttributedUsageCharge {
    pub currency: String,
    pub amount_minor: String,
    pub basis: UsageChargeBasis,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageProjectRef {
    pub project_id: String,
    pub project_name: String,
}

/// Usage of one resource by a sandbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AttributedResourceUsage {
    pub quantity: String,
    /// `vCPU-hour`, `GiB-hour` or `GiB`.
    pub unit: String,
    pub attributed_amount_minor: Option<String>,
    #[serde(default)]
    pub usage_value_amount_minor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxResourceUsage {
    pub cpu: AttributedResourceUsage,
    pub memory: AttributedResourceUsage,
    pub sandbox_storage: AttributedResourceUsage,
    pub snapshot_storage: AttributedResourceUsage,
    pub network_egress: AttributedResourceUsage,
}

/// Usage and attributed charge of one sandbox.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxUsage {
    pub sandbox_id: String,
    pub project: Option<UsageProjectRef>,
    pub attributed_amount_minor: Option<String>,
    #[serde(default)]
    pub usage_value_amount_minor: Option<String>,
    pub resources: SandboxResourceUsage,
}

/// The window and pooled amounts an export's sandboxes share.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxUsageExportWindow {
    pub window_start: String,
    pub window_end: String,
    /// The organization's usage charge; the sandboxes' attributed amounts add
    /// up to it exactly. Null when the organization has no metered price, and
    /// on plans with included usage when the window is not inside the current
    /// billing period.
    pub attributed_usage_charge: Option<AttributedUsageCharge>,
    pub included_usage: Option<IncludedUsage>,
    /// The organization's usage at the plan's overage rate; the sandboxes'
    /// usage values add up to it exactly. Set for every window on plans with
    /// included usage.
    pub attributed_usage_value: Option<UsageAmount>,
}

/// Every sandbox of a window with its usage and attributed charge.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxUsageExport {
    #[serde(flatten)]
    pub window: SandboxUsageExportWindow,
    pub sandboxes: Vec<SandboxUsage>,
}
