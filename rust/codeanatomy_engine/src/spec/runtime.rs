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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Capture compliance diagnostics (EXPLAIN/rule impact) as side-channel output.
    #[serde(default)]
    pub compliance_capture: bool,
    /// Adaptive tuner mode.
    #[serde(default)]
    pub tuner_mode: RuntimeTunerMode,
}
