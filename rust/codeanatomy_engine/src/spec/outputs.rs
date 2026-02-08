//! Output target specifications for materialization.

use serde::{Deserialize, Serialize};

/// Materialization mode for output targets.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MaterializationMode {
    Append,
    Overwrite,
}

impl Default for MaterializationMode {
    fn default() -> Self {
        Self::Overwrite
    }
}

/// Output target specification for materialized views.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputTarget {
    pub table_name: String,
    #[serde(default)]
    pub delta_location: Option<String>,
    pub source_view: String,
    pub columns: Vec<String>,
    #[serde(default)]
    pub materialization_mode: MaterializationMode,
}
