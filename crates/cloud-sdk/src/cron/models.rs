use serde::{Deserialize, Serialize};

/// Request body for creating a cron schedule.
#[derive(Clone, Debug, Serialize)]
pub struct CreateCronScheduleRequest {
    pub cron_expression: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_base64: Option<String>,
}

/// Response from creating a cron schedule.
#[derive(Clone, Debug, Deserialize)]
pub struct CreateCronScheduleResponse {
    pub schedule_id: String,
}

/// A single cron schedule as returned by the list endpoint.
#[derive(Clone, Debug, Deserialize)]
pub struct CronSchedule {
    pub id: String,
    pub application_name: String,
    pub cron_expression: String,
    pub next_fire_time_ms: i64,
    pub last_fired_at_ms: Option<i64>,
    pub enabled: bool,
}

/// Response from listing cron schedules.
#[derive(Clone, Debug, Deserialize)]
pub struct ListCronSchedulesResponse {
    pub schedules: Vec<CronSchedule>,
}
