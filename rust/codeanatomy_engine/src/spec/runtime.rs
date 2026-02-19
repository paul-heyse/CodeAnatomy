//! Runtime toggles for optional compliance and tuner behavior.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::contracts::pushdown_mode::PushdownEnforcementMode;
use crate::session::capture::GovernancePolicy;

/// Runtime tuner operating mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum RuntimeTunerMode {
    #[default]
    Off,
    Observe,
    Apply,
}

/// Rule tracing verbosity mode for planner/rule instrumentation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum RuleTraceMode {
    #[default]
    Disabled,
    PhaseOnly,
    Full,
}

/// Preview redaction policy for `datafusion.preview`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum PreviewRedactionMode {
    #[default]
    None,
    DenyList,
    AllowList,
}

/// Supported OTLP transport protocols.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum OtlpProtocol {
    #[default]
    #[serde(rename = "http/protobuf")]
    HttpProtobuf,
    #[serde(rename = "http/json")]
    HttpJson,
    #[serde(rename = "grpc")]
    Grpc,
}

/// Deterministic tracing presets for common operating profiles.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TracingPreset {
    Maximal,
    MaximalNoData,
    ProductionLean,
}

/// OpenTelemetry export throughput and sampling policy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TraceExportPolicy {
    #[serde(default = "default_sampler")]
    pub traces_sampler: String,
    #[serde(default)]
    pub traces_sampler_arg: Option<String>,
    #[serde(default = "default_bsp_max_queue_size")]
    pub bsp_max_queue_size: u32,
    #[serde(default = "default_bsp_max_export_batch_size")]
    pub bsp_max_export_batch_size: u32,
    #[serde(default = "default_bsp_schedule_delay_ms")]
    pub bsp_schedule_delay_ms: u64,
    #[serde(default = "default_bsp_export_timeout_ms")]
    pub bsp_export_timeout_ms: u64,
}

impl Default for TraceExportPolicy {
    fn default() -> Self {
        Self {
            traces_sampler: default_sampler(),
            traces_sampler_arg: None,
            bsp_max_queue_size: default_bsp_max_queue_size(),
            bsp_max_export_batch_size: default_bsp_max_export_batch_size(),
            bsp_schedule_delay_ms: default_bsp_schedule_delay_ms(),
            bsp_export_timeout_ms: default_bsp_export_timeout_ms(),
        }
    }
}

/// Tracing control plane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TracingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub record_metrics: bool,
    #[serde(default)]
    pub rule_mode: RuleTraceMode,
    #[serde(default)]
    pub plan_diff: bool,
    #[serde(default)]
    pub preview_limit: usize,
    #[serde(default)]
    pub preview_redaction_mode: PreviewRedactionMode,
    #[serde(default)]
    pub preview_redacted_columns: Vec<String>,
    #[serde(default = "default_preview_redaction_token")]
    pub preview_redaction_token: String,
    #[serde(default = "default_preview_max_width")]
    pub preview_max_width: usize,
    #[serde(default = "default_preview_max_row_height")]
    pub preview_max_row_height: usize,
    #[serde(default = "default_preview_min_compacted_col_width")]
    pub preview_min_compacted_col_width: usize,
    #[serde(default)]
    pub instrument_object_store: bool,
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    #[serde(default)]
    pub otlp_protocol: Option<OtlpProtocol>,
    #[serde(default)]
    pub otel_service_name: Option<String>,
    #[serde(default)]
    pub otel_resource_attributes: BTreeMap<String, String>,
    #[serde(default)]
    pub custom_span_fields: BTreeMap<String, String>,
    #[serde(default)]
    pub export_policy: TraceExportPolicy,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            record_metrics: true,
            rule_mode: RuleTraceMode::Disabled,
            plan_diff: false,
            preview_limit: 0,
            preview_redaction_mode: PreviewRedactionMode::None,
            preview_redacted_columns: Vec::new(),
            preview_redaction_token: default_preview_redaction_token(),
            preview_max_width: default_preview_max_width(),
            preview_max_row_height: default_preview_max_row_height(),
            preview_min_compacted_col_width: default_preview_min_compacted_col_width(),
            instrument_object_store: false,
            otlp_endpoint: None,
            otlp_protocol: None,
            otel_service_name: None,
            otel_resource_attributes: BTreeMap::new(),
            custom_span_fields: BTreeMap::new(),
            export_policy: TraceExportPolicy::default(),
        }
    }
}

impl TracingPreset {
    fn to_config(self) -> TracingConfig {
        let mut config = TracingConfig {
            enabled: true,
            record_metrics: true,
            instrument_object_store: true,
            ..TracingConfig::default()
        };
        match self {
            Self::Maximal => {
                config.rule_mode = RuleTraceMode::Full;
                config.plan_diff = true;
                config.preview_limit = 5;
            }
            Self::MaximalNoData => {
                config.rule_mode = RuleTraceMode::Full;
                config.plan_diff = true;
                config.preview_limit = 0;
            }
            Self::ProductionLean => {
                config.rule_mode = RuleTraceMode::PhaseOnly;
                config.plan_diff = false;
                config.preview_limit = 0;
            }
        }
        config
    }
}

/// Runtime controls that do not affect logical correctness.
///
/// All fields use `#[serde(default)]` for backward-compatible deserialization.
/// New fields added by Wave 3 integration: `capture_substrait`,
/// `capture_optimizer_lab`, `capture_delta_codec`, `enable_tracing`,
/// `enable_rule_tracing`, `enable_plan_preview`, `enable_function_factory`,
/// `enable_domain_planner`, `enable_relation_planner`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Capture compliance diagnostics (EXPLAIN/rule impact) as side-channel output.
    #[serde(default)]
    pub compliance_capture: bool,
    /// Adaptive tuner mode.
    #[serde(default)]
    pub tuner_mode: RuntimeTunerMode,

    // -- WS-P2: Substrait portability artifact capture --
    /// When true, encode optimized plans to Substrait bytes for portability.
    #[serde(default)]
    pub capture_substrait: bool,
    /// When true, run offline optimizer lab capture during compliance mode.
    #[serde(default)]
    pub capture_optimizer_lab: bool,
    /// When true, capture Delta extension-codec plan bytes in plan artifacts.
    #[serde(default)]
    pub capture_delta_codec: bool,
    /// Pushdown contract enforcement policy.
    #[serde(default)]
    pub pushdown_enforcement_mode: PushdownEnforcementMode,
    /// Planning extension governance policy.
    #[serde(default)]
    pub extension_governance_mode: GovernancePolicy,

    // -- WS-P13: OpenTelemetry tracing --
    /// Enable OpenTelemetry-native execution tracing (requires `tracing` feature).
    #[serde(default)]
    pub enable_tracing: bool,
    /// Instrument individual optimizer rules with info spans and plan diffs.
    /// Expensive; intended for compliance mode only.
    #[serde(default)]
    pub enable_rule_tracing: bool,
    /// Record first N rows in tracing spans for plan preview / debugging.
    #[serde(default)]
    pub enable_plan_preview: bool,

    // -- WS-P14: Function factory and domain planner --
    /// Enable the function factory for custom UDF registration.
    #[serde(default)]
    pub enable_function_factory: bool,
    /// Enable the domain-specific query planner.
    #[serde(default)]
    pub enable_domain_planner: bool,
    /// Enable relation-planner installation for domain SQL table factors.
    #[serde(default)]
    pub enable_relation_planner: bool,

    /// Additional deterministic lineage metadata to include in Delta commits.
    #[serde(default)]
    pub lineage_tags: BTreeMap<String, String>,

    /// Optional preset applied before explicit tracing config.
    #[serde(default)]
    pub tracing_preset: Option<TracingPreset>,

    // -- Maximal tracing control plane --
    #[serde(default)]
    pub tracing: TracingConfig,
}

impl RuntimeConfig {
    /// Effective tracing config with backward-compatible promotion from legacy
    /// booleans (`enable_tracing`, `enable_rule_tracing`, `enable_plan_preview`).
    pub fn effective_tracing(&self) -> TracingConfig {
        let mut merged = self
            .tracing_preset
            .map(TracingPreset::to_config)
            .unwrap_or_default();

        // Explicit tracing config replaces the preset when non-default.
        if self.tracing != TracingConfig::default() {
            merged = self.tracing.clone();
        }

        if self.enable_tracing {
            merged.enabled = true;
        }
        if self.enable_rule_tracing && merged.rule_mode == RuleTraceMode::Disabled {
            merged.rule_mode = RuleTraceMode::Full;
        }
        if self.enable_plan_preview && merged.preview_limit == 0 {
            merged.preview_limit = 5;
        }
        merged
    }
}

fn default_sampler() -> String {
    "parentbased_always_on".to_string()
}

fn default_bsp_max_queue_size() -> u32 {
    2048
}

fn default_bsp_max_export_batch_size() -> u32 {
    512
}

fn default_bsp_schedule_delay_ms() -> u64 {
    5_000
}

fn default_bsp_export_timeout_ms() -> u64 {
    30_000
}

fn default_preview_max_width() -> usize {
    96
}

fn default_preview_max_row_height() -> usize {
    4
}

fn default_preview_min_compacted_col_width() -> usize {
    10
}

fn default_preview_redaction_token() -> String {
    "[REDACTED]".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_effective_tracing_promotes_legacy_booleans() {
        let runtime = RuntimeConfig {
            enable_tracing: true,
            enable_rule_tracing: true,
            enable_plan_preview: true,
            ..RuntimeConfig::default()
        };

        let effective = runtime.effective_tracing();
        assert!(effective.enabled);
        assert_eq!(effective.rule_mode, RuleTraceMode::Full);
        assert_eq!(effective.preview_limit, 5);
    }

    #[test]
    fn test_effective_tracing_preserves_explicit_config() {
        let runtime = RuntimeConfig {
            enable_tracing: true,
            enable_rule_tracing: true,
            tracing: TracingConfig {
                enabled: false,
                rule_mode: RuleTraceMode::PhaseOnly,
                preview_limit: 9,
                ..TracingConfig::default()
            },
            ..RuntimeConfig::default()
        };

        let effective = runtime.effective_tracing();
        assert!(effective.enabled);
        assert_eq!(effective.rule_mode, RuleTraceMode::PhaseOnly);
        assert_eq!(effective.preview_limit, 9);
    }

    #[test]
    fn test_effective_tracing_uses_preset_when_tracing_config_default() {
        let runtime = RuntimeConfig {
            tracing_preset: Some(TracingPreset::MaximalNoData),
            ..RuntimeConfig::default()
        };

        let effective = runtime.effective_tracing();
        assert!(effective.enabled);
        assert_eq!(effective.rule_mode, RuleTraceMode::Full);
        assert!(effective.plan_diff);
        assert_eq!(effective.preview_limit, 0);
    }

    #[test]
    fn test_effective_tracing_explicit_config_replaces_preset() {
        let runtime = RuntimeConfig {
            tracing_preset: Some(TracingPreset::Maximal),
            tracing: TracingConfig {
                enabled: true,
                rule_mode: RuleTraceMode::PhaseOnly,
                plan_diff: false,
                preview_limit: 1,
                ..TracingConfig::default()
            },
            ..RuntimeConfig::default()
        };

        let effective = runtime.effective_tracing();
        assert_eq!(effective.rule_mode, RuleTraceMode::PhaseOnly);
        assert!(!effective.plan_diff);
        assert_eq!(effective.preview_limit, 1);
    }

    #[test]
    fn test_runtime_config_defaults_new_capture_flags_to_false() {
        let runtime = RuntimeConfig::default();
        assert!(!runtime.capture_substrait);
        assert!(!runtime.capture_optimizer_lab);
        assert!(!runtime.capture_delta_codec);
        assert_eq!(
            runtime.pushdown_enforcement_mode,
            PushdownEnforcementMode::Warn
        );
        assert_eq!(
            runtime.extension_governance_mode,
            GovernancePolicy::Permissive
        );
    }

    #[test]
    fn test_runtime_config_deserializes_new_capture_flags() {
        let payload = serde_json::json!({
            "capture_substrait": true,
            "capture_optimizer_lab": true,
            "capture_delta_codec": true,
            "pushdown_enforcement_mode": "strict",
            "extension_governance_mode": "strict_allowlist"
        });
        let runtime: RuntimeConfig = serde_json::from_value(payload).expect("valid runtime config");
        assert!(runtime.capture_substrait);
        assert!(runtime.capture_optimizer_lab);
        assert!(runtime.capture_delta_codec);
        assert_eq!(
            runtime.pushdown_enforcement_mode,
            PushdownEnforcementMode::Strict
        );
        assert_eq!(
            runtime.extension_governance_mode,
            GovernancePolicy::StrictAllowlist
        );
    }

    #[test]
    fn test_runtime_config_lineage_tags_default_empty() {
        let runtime = RuntimeConfig::default();
        assert!(runtime.lineage_tags.is_empty());
    }

    #[test]
    fn test_runtime_config_deserializes_lineage_tags() {
        let payload = serde_json::json!({
            "lineage_tags": {
                "team": "graph",
                "env": "dev"
            }
        });
        let runtime: RuntimeConfig = serde_json::from_value(payload).expect("valid runtime config");
        assert_eq!(runtime.lineage_tags.get("team"), Some(&"graph".to_string()));
        assert_eq!(runtime.lineage_tags.get("env"), Some(&"dev".to_string()));
    }
}
