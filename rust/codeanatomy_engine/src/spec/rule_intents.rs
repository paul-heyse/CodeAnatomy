//! Rule intent specifications for execution policy.

use serde::{Deserialize, Serialize};

/// Rule intent specification with classification and parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
///
/// **Deprecated**: Use `TypedParameter` (in `spec::parameters`) instead.
/// Parameter templates rely on env-var string substitution and SQL literal
/// interpolation, which is less type-safe and less deterministic than the
/// typed parameter path. This type is retained only for transitional
/// compatibility. When both `typed_parameters` and `parameter_templates`
/// are specified in a `SemanticExecutionSpec`, compilation fails with a
/// parameter mode exclusivity error.
///
/// Migration guide:
/// - `ParameterTemplate { name, base_table, filter_column, parameter_type: "string" }`
///   becomes `TypedParameter { target: FilterEq { base_table, filter_column }, value: Utf8(...) }`
/// - Env-var lookup (`CODEANATOMY_PARAM_<NAME>`) is replaced by direct value binding.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParameterTemplate {
    /// Template name (also determines env-var suffix: `CODEANATOMY_PARAM_<NAME>`).
    pub name: String,
    /// Source table to apply the filter to.
    pub base_table: String,
    /// Column to filter on via `= <value>` predicate.
    pub filter_column: String,
    /// Type hint for SQL literal rendering ("string", "int", "float", "bool").
    pub parameter_type: String,
}
