//! Result envelope types for execution outcomes.
//!
//! Two-layer determinism contract:
//! - spec_hash: execution spec identity
//! - envelope_hash: input data identity

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::compiler::cost_model::StatsQuality;
use crate::compiler::scheduling::TaskSchedule;
use crate::compiler::plan_bundle::PlanBundleArtifact;
use crate::executor::maintenance::MaintenanceReport;
use crate::executor::metrics_collector::{CollectedMetrics, TraceMetricsSummary};
use crate::executor::warnings::RunWarning;

/// Compact result envelope containing both determinism layers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub outputs: Vec<MaterializationResult>,
    pub spec_hash: [u8; 32],
    pub envelope_hash: [u8; 32],
    pub rulepack_fingerprint: [u8; 32],
    pub compliance_capture_json: Option<String>,
    pub tuner_hints: Vec<TuningHint>,
    pub plan_bundles: Vec<PlanBundleArtifact>,
    /// Deterministic execution schedule metadata.
    pub task_schedule: Option<TaskSchedule>,
    /// Statistics quality grade used by cost/scheduling decisions.
    pub stats_quality: Option<StatsQuality>,
    /// WS-P7: Real physical metrics collected from the executed plan tree.
    pub collected_metrics: Option<CollectedMetrics>,
    /// Stable summary metrics contract for observability consumers.
    pub trace_metrics_summary: Option<TraceMetricsSummary>,
    /// WS-P11: Post-execution Delta maintenance reports.
    pub maintenance_reports: Vec<MaintenanceReport>,
    /// WS-P10: Non-fatal warnings collected during pipeline execution.
    ///
    /// Captures errors from best-effort operations (plan bundle capture,
    /// maintenance, compliance serialization) that were previously swallowed
    /// silently. Each entry is a human-readable message with error context.
    pub warnings: Vec<RunWarning>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}

/// Per-table materialization outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializationResult {
    pub table_name: String,
    pub delta_location: Option<String>,
    pub rows_written: u64,
    pub partition_count: u32,
    pub delta_version: Option<i64>,
    pub files_added: Option<u64>,
    pub bytes_written: Option<u64>,
}

/// Two-layer determinism contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeterminismContract {
    pub spec_hash: [u8; 32],
    pub envelope_hash: [u8; 32],
}

/// Runtime tuning hint emitted after execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TuningHint {
    pub target_partitions: u32,
    pub batch_size: u32,
    pub repartition_joins: bool,
    pub repartition_aggregations: bool,
}

impl DeterminismContract {
    /// A replay is valid iff both spec_hash AND envelope_hash match.
    pub fn is_replay_valid(&self, original: &DeterminismContract) -> bool {
        self.spec_hash == original.spec_hash && self.envelope_hash == original.envelope_hash
    }
}

impl RunResult {
    /// Create a new RunResult builder.
    pub fn builder() -> RunResultBuilder {
        RunResultBuilder::default()
    }

    /// Get the determinism contract for this run.
    pub fn determinism_contract(&self) -> DeterminismContract {
        DeterminismContract {
            spec_hash: self.spec_hash,
            envelope_hash: self.envelope_hash,
        }
    }

    /// Serialize to JSON for Python consumption.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[derive(Debug, Default)]
pub struct RunResultBuilder {
    outputs: Vec<MaterializationResult>,
    spec_hash: [u8; 32],
    envelope_hash: [u8; 32],
    rulepack_fingerprint: [u8; 32],
    compliance_capture_json: Option<String>,
    tuner_hints: Vec<TuningHint>,
    plan_bundles: Vec<PlanBundleArtifact>,
    task_schedule: Option<TaskSchedule>,
    stats_quality: Option<StatsQuality>,
    collected_metrics: Option<CollectedMetrics>,
    trace_metrics_summary: Option<TraceMetricsSummary>,
    maintenance_reports: Vec<MaintenanceReport>,
    warnings: Vec<RunWarning>,
    started_at: Option<DateTime<Utc>>,
}

impl RunResultBuilder {
    pub fn with_spec_hash(mut self, hash: [u8; 32]) -> Self {
        self.spec_hash = hash;
        self
    }

    pub fn with_envelope_hash(mut self, hash: [u8; 32]) -> Self {
        self.envelope_hash = hash;
        self
    }

    pub fn with_rulepack_fingerprint(mut self, fp: [u8; 32]) -> Self {
        self.rulepack_fingerprint = fp;
        self
    }

    pub fn with_compliance_capture(mut self, capture_json: Option<String>) -> Self {
        self.compliance_capture_json = capture_json;
        self
    }

    pub fn add_tuning_hint(mut self, hint: TuningHint) -> Self {
        self.tuner_hints.push(hint);
        self
    }

    pub fn with_plan_bundles(mut self, bundles: Vec<PlanBundleArtifact>) -> Self {
        self.plan_bundles = bundles;
        self
    }

    pub fn with_task_schedule(mut self, schedule: Option<TaskSchedule>) -> Self {
        self.task_schedule = schedule;
        self
    }

    pub fn with_stats_quality(mut self, stats_quality: Option<StatsQuality>) -> Self {
        self.stats_quality = stats_quality;
        self
    }

    pub fn with_collected_metrics(mut self, metrics: Option<CollectedMetrics>) -> Self {
        self.collected_metrics = metrics;
        self
    }

    pub fn with_trace_metrics_summary(
        mut self,
        summary: Option<TraceMetricsSummary>,
    ) -> Self {
        self.trace_metrics_summary = summary;
        self
    }

    pub fn with_maintenance_reports(mut self, reports: Vec<MaintenanceReport>) -> Self {
        self.maintenance_reports = reports;
        self
    }

    /// Append a single warning message to the result.
    ///
    /// Use this for best-effort operations that failed non-fatally.
    pub fn add_warning(mut self, warning: RunWarning) -> Self {
        self.warnings.push(warning);
        self
    }

    /// Replace the entire warnings list.
    pub fn with_warnings(mut self, warnings: Vec<RunWarning>) -> Self {
        self.warnings = warnings;
        self
    }

    pub fn started_now(mut self) -> Self {
        self.started_at = Some(Utc::now());
        self
    }

    pub fn add_output(mut self, output: MaterializationResult) -> Self {
        self.outputs.push(output);
        self
    }

    pub fn build(self) -> RunResult {
        RunResult {
            outputs: self.outputs,
            spec_hash: self.spec_hash,
            envelope_hash: self.envelope_hash,
            rulepack_fingerprint: self.rulepack_fingerprint,
            compliance_capture_json: self.compliance_capture_json,
            tuner_hints: self.tuner_hints,
            plan_bundles: self.plan_bundles,
            task_schedule: self.task_schedule,
            stats_quality: self.stats_quality,
            collected_metrics: self.collected_metrics,
            trace_metrics_summary: self.trace_metrics_summary,
            maintenance_reports: self.maintenance_reports,
            warnings: self.warnings,
            started_at: self.started_at.unwrap_or_else(Utc::now),
            completed_at: Utc::now(),
        }
    }
}
