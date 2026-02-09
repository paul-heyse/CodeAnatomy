//! Output target specifications for materialization.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Materialization mode for output targets.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum MaterializationMode {
    Append,
    #[default]
    Overwrite,
}

/// Output target specification for materialized views.
///
/// Defines a single materialization destination: a Delta table that
/// receives the output of a compiled semantic view.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputTarget {
    /// Logical table name used for session catalog registration and
    /// `write_table()` targeting.
    pub table_name: String,
    /// Optional Delta table URI. When provided, overrides the default
    /// location derived from `table_name`. Used for external or
    /// cross-storage-account output paths.
    #[serde(default)]
    pub delta_location: Option<String>,
    /// Name of the compiled semantic view whose DataFrame is written
    /// to this target.
    pub source_view: String,
    /// Column projection list for the output schema.
    pub columns: Vec<String>,
    #[serde(default)]
    pub materialization_mode: MaterializationMode,
    /// Partition columns for the Delta write. When non-empty, the
    /// DataFrameWriteOptions will include `with_partition_by()` to
    /// produce Hive-style partitioned output. The columns must exist
    /// in the output schema.
    #[serde(default)]
    pub partition_by: Vec<String>,
    /// User-supplied key-value metadata pairs merged into the Delta
    /// commit properties alongside codeanatomy provenance hashes
    /// (`codeanatomy.spec_hash`, `codeanatomy.envelope_hash`).
    #[serde(default)]
    pub write_metadata: BTreeMap<String, String>,
    /// Maximum number of Delta commit retries on conflict. When set,
    /// the Delta writer will retry optimistic commits up to this many
    /// times before failing. `None` uses the Delta-rs default.
    #[serde(default)]
    pub max_commit_retries: Option<u32>,
}
