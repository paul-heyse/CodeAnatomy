//! Structured warning taxonomy for non-fatal execution degradations.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Stable warning code for machine-readable downstream handling.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WarningCode {
    PlanBundleRuntimeCaptureFailed,
    PlanBundleArtifactBuildFailed,
    OptionalSubstraitCaptureFailed,
    OptionalSqlCaptureFailed,
    OptionalDeltaCodecCaptureFailed,
    ComplianceExplainCaptureFailed,
    CompliancePushdownProbeFailed,
    CompliancePushdownProbeSkipped,
    ComplianceOptimizerLabFailed,
    ComplianceSerializationFailed,
    PushdownContractViolation,
    CostModelStatsFallback,
    SemanticValidationWarning,
    DeltaCompatibilityDrift,
    MaintenanceFailed,
    ReservedProfileKnobIgnored,
    LegacyParameterTemplatesRejected,
}

impl WarningCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PlanBundleRuntimeCaptureFailed => "plan_bundle_runtime_capture_failed",
            Self::PlanBundleArtifactBuildFailed => "plan_bundle_artifact_build_failed",
            Self::OptionalSubstraitCaptureFailed => "optional_substrait_capture_failed",
            Self::OptionalSqlCaptureFailed => "optional_sql_capture_failed",
            Self::OptionalDeltaCodecCaptureFailed => "optional_delta_codec_capture_failed",
            Self::ComplianceExplainCaptureFailed => "compliance_explain_capture_failed",
            Self::CompliancePushdownProbeFailed => "compliance_pushdown_probe_failed",
            Self::CompliancePushdownProbeSkipped => "compliance_pushdown_probe_skipped",
            Self::ComplianceOptimizerLabFailed => "compliance_optimizer_lab_failed",
            Self::ComplianceSerializationFailed => "compliance_serialization_failed",
            Self::PushdownContractViolation => "pushdown_contract_violation",
            Self::CostModelStatsFallback => "cost_model_stats_fallback",
            Self::SemanticValidationWarning => "semantic_validation_warning",
            Self::DeltaCompatibilityDrift => "delta_compatibility_drift",
            Self::MaintenanceFailed => "maintenance_failed",
            Self::ReservedProfileKnobIgnored => "reserved_profile_knob_ignored",
            Self::LegacyParameterTemplatesRejected => "legacy_parameter_templates_rejected",
        }
    }
}

/// Execution stage for warning provenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum WarningStage {
    Preflight,
    Compilation,
    PlanBundle,
    Compliance,
    Materialization,
    Maintenance,
    RuntimeProfile,
}

/// Structured warning payload surfaced in `RunResult`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunWarning {
    pub code: WarningCode,
    pub stage: WarningStage,
    pub message: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub context: BTreeMap<String, String>,
}

impl RunWarning {
    pub fn new(code: WarningCode, stage: WarningStage, message: impl Into<String>) -> Self {
        Self {
            code,
            stage,
            message: message.into(),
            context: BTreeMap::new(),
        }
    }

    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }
}

/// Aggregate warning counts keyed by warning code string.
pub fn warning_counts_by_code(warnings: &[RunWarning]) -> BTreeMap<String, u64> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for warning in warnings {
        *counts.entry(warning.code.as_str().to_string()).or_default() += 1;
    }
    counts
}
