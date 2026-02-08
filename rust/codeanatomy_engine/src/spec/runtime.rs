//! Runtime toggles for optional compliance and tuner behavior.

use serde::{Deserialize, Serialize};

/// Runtime tuner operating mode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum RuntimeTunerMode {
    #[default]
    Off,
    Observe,
    Apply,
}

/// Runtime controls that do not affect logical correctness.
///
/// All fields use `#[serde(default)]` for backward-compatible deserialization.
/// New fields added by Wave 3 integration: `capture_substrait`, `enable_tracing`,
/// `enable_rule_tracing`, `enable_plan_preview`, `enable_function_factory`,
/// `enable_domain_planner`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
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
}
