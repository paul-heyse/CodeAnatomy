//! Rule intent specifications for execution policy.

use serde::{Deserialize, Serialize};

/// Rule intent specification with classification and parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleIntent {
    pub name: String,
    pub class: RuleClass,
    pub params: serde_json::Value,
}

/// Rule classification for semantic execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuleClass {
    SemanticIntegrity,
    DeltaScanAware,
    CostShape,
    Safety,
}

/// Rulepack profile for execution strategy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RulepackProfile {
    Default,
    LowLatency,
    Replay,
    Strict,
}

/// Parameter template for dynamic query parameterization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterTemplate {
    pub name: String,
    pub base_table: String,
    pub filter_column: String,
    pub parameter_type: String,
}
