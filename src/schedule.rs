//! Persistent cron schedules: durable rows in `workflow_schedules` that a
//! reconciler installs into per-schedule firing loops. A schedule names a
//! registered workflow and a cron spec; the engine fires it on each tick under a
//! deterministic id so a tick runs exactly once across executors.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The input a scheduled workflow receives on each cron tick: the tick's
/// wall-clock instant plus any user value attached to the schedule via
/// [`ScheduleOptions::context`].
///
/// Serialized as `{"scheduled_time": <RFC3339>, "context": <value>}` — the shape
/// the other DBOS SDKs deliver too, so a scheduled run's persisted input is
/// portable across them. A scheduled workflow declares this as its input type:
///
/// ```
/// use durare::{DurableContext, Result, ScheduledInput};
///
/// #[durare::workflow(schedule = "0 0 * * * *")]
/// async fn hourly(_ctx: DurableContext, tick: ScheduledInput) -> Result<()> {
///     println!("fired for {}", tick.scheduled_time);
///     Ok(())
/// }
/// # fn main() {}
/// ```
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScheduledInput {
    /// The cron tick instant this run fires for.
    pub scheduled_time: DateTime<Utc>,
    /// The user value attached to the schedule (`None` if none was set).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,
}

impl ScheduledInput {
    /// Deserialize [`context`](Self::context) into a concrete type, returning
    /// `Ok(None)` when no context was attached.
    pub fn context_as<T: serde::de::DeserializeOwned>(&self) -> crate::Result<Option<T>> {
        match &self.context {
            Some(v) => Ok(Some(serde_json::from_value(v.clone())?)),
            None => Ok(None),
        }
    }
}

/// Lifecycle state of a persisted schedule. Stored as the cross-SDK strings
/// `ACTIVE` / `PAUSED`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScheduleStatus {
    /// The reconciler installs it and it fires on its cron ticks.
    Active,
    /// The reconciler leaves it uninstalled; it does not fire.
    Paused,
}

impl ScheduleStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Paused => "PAUSED",
        }
    }

    pub(crate) fn parse(s: &str) -> Self {
        match s {
            "PAUSED" => Self::Paused,
            _ => Self::Active,
        }
    }
}

/// A persisted cron schedule for a registered workflow.
#[derive(Clone, Debug)]
pub struct WorkflowSchedule {
    /// Stable identifier; a new one is minted whenever the schedule is recreated
    /// so the reconciler can detect a replacement.
    pub schedule_id: String,
    /// Unique human name used to address the schedule (pause/resume/delete/get).
    pub schedule_name: String,
    /// The registered workflow this schedule fires.
    pub workflow_name: String,
    /// 6-field cron spec (second precision).
    pub schedule: String,
    /// Whether the schedule is currently active or paused.
    pub status: ScheduleStatus,
    /// Optional user value attached to the schedule (surfaced via get/list).
    pub context: Option<Value>,
    /// When the schedule last fired a tick.
    pub last_fired_at: Option<DateTime<Utc>>,
    /// Backfill missed ticks when the schedule is (re)installed after downtime.
    pub automatic_backfill: bool,
    /// IANA timezone the cron spec is interpreted in (`None` = UTC).
    pub cron_timezone: Option<String>,
    /// Queue to route each tick to (`None` runs the tick directly).
    pub queue_name: Option<String>,
}

impl WorkflowSchedule {
    /// The input delivered to the workflow for the tick at `instant`: the tick
    /// time plus this schedule's attached context. The single place tick input
    /// is shaped, so every firing path (live loop, backfill, trigger) agrees.
    pub(crate) fn tick_input(&self, instant: DateTime<Utc>) -> ScheduledInput {
        ScheduledInput {
            scheduled_time: instant,
            context: self.context.clone(),
        }
    }
}

/// Optional settings for [`DurableEngine::create_schedule`](crate::DurableEngine::create_schedule).
#[derive(Clone, Default)]
pub struct ScheduleOptions {
    pub(crate) context: Option<Value>,
    pub(crate) automatic_backfill: bool,
    pub(crate) cron_timezone: Option<String>,
    pub(crate) queue_name: Option<String>,
}

impl ScheduleOptions {
    /// Default schedule options (no queue, no timezone, no attached context).
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a user value passed along with the schedule.
    pub fn context<T: serde::Serialize>(mut self, ctx: &T) -> Self {
        self.context = serde_json::to_value(ctx).ok();
        self
    }

    /// Backfill missed ticks when the schedule is (re)installed after downtime.
    pub fn automatic_backfill(mut self, on: bool) -> Self {
        self.automatic_backfill = on;
        self
    }

    /// Interpret the cron spec in this IANA timezone instead of UTC.
    pub fn cron_timezone(mut self, tz: impl Into<String>) -> Self {
        self.cron_timezone = Some(tz.into());
        self
    }

    /// Route each tick to the named queue instead of running it directly.
    pub fn queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = Some(name.into());
        self
    }
}

/// One entry for [`DurableEngine::apply_schedules`](crate::DurableEngine::apply_schedules),
/// which creates each schedule or replaces an existing one of the same name.
#[derive(Clone)]
pub struct ApplySchedule {
    pub(crate) schedule_name: String,
    pub(crate) workflow_name: String,
    pub(crate) schedule: String,
    pub(crate) options: ScheduleOptions,
}

impl ApplySchedule {
    /// A schedule named `schedule_name` firing `workflow_name` on the `cron`
    /// spec (6-field, second precision).
    pub fn new(
        schedule_name: impl Into<String>,
        workflow_name: impl Into<String>,
        cron: impl Into<String>,
    ) -> Self {
        Self {
            schedule_name: schedule_name.into(),
            workflow_name: workflow_name.into(),
            schedule: cron.into(),
            options: ScheduleOptions::new(),
        }
    }

    /// Attach the same options a [`create_schedule`](crate::DurableEngine::create_schedule)
    /// call would take (context, backfill, timezone, queue).
    pub fn options(mut self, options: ScheduleOptions) -> Self {
        self.options = options;
        self
    }
}

/// Filters for [`DurableEngine::list_schedules`](crate::DurableEngine::list_schedules).
/// An empty filter returns every schedule.
#[derive(Clone, Default)]
pub struct ScheduleFilter {
    /// Keep only schedules in one of these statuses.
    pub statuses: Vec<ScheduleStatus>,
    /// Keep only schedules whose workflow is one of these.
    pub workflow_names: Vec<String>,
    /// Keep only schedules whose name starts with one of these prefixes.
    pub name_prefixes: Vec<String>,
}
