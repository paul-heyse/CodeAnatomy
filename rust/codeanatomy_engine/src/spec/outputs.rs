//! Output target specifications for materialization.

use serde::{Deserialize, Serialize};

/// Materialization mode for output targets.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MaterializationMode {
    Append,
    Overwrite,
}

/// Output target specification for materialized views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputTarget {
    pub table_name: String,
    pub source_view: String,
    pub columns: Vec<String>,
    pub materialization_mode: MaterializationMode,
}
