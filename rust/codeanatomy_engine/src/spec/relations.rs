//! Input relations and view definitions with transformation semantics.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Input relation specification — logical name mapped to Delta table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputRelation {
    pub logical_name: String,
    pub delta_location: String,
    #[serde(default)]
    pub requires_lineage: bool,
    pub version_pin: Option<i64>,
}

/// View definition with transformation semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewDefinition {
    pub name: String,
    pub view_kind: String,
    /// View-to-view dependencies ONLY (not input relations).
    pub view_dependencies: Vec<String>,
    pub transform: ViewTransform,
    pub output_schema: SchemaContract,
}

/// View transformation variants with tagged enum serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum ViewTransform {
    Normalize {
        source: String,
        id_columns: Vec<String>,
        span_columns: Option<(String, String)>,
        text_columns: Vec<String>,
    },
    Relate {
        left: String,
        right: String,
        join_type: JoinType,
        join_keys: Vec<JoinKeyPair>,
    },
    Union {
        sources: Vec<String>,
        discriminator_column: Option<String>,
        distinct: bool,
    },
    Project {
        source: String,
        columns: Vec<String>,
    },
    Filter {
        source: String,
        /// SQL expression string.
        predicate: String,
    },
    Aggregate {
        source: String,
        group_by: Vec<String>,
        aggregations: Vec<AggregationExpr>,
    },

    /// Incremental change detection via Delta CDF.
    ///
    /// Expresses a change-data-feed scan as a planning primitive,
    /// allowing the optimizer to push down predicates into the CDF scan
    /// and compose the result with downstream joins/unions.
    IncrementalCdf {
        /// Delta table location.
        source: String,
        /// Starting version for CDF range (inclusive).
        starting_version: Option<i64>,
        /// Ending version for CDF range (inclusive).
        ending_version: Option<i64>,
        /// Starting timestamp (ISO 8601).
        starting_timestamp: Option<String>,
        /// Ending timestamp (ISO 8601).
        ending_timestamp: Option<String>,
    },

    /// Table metadata inspection via `delta_snapshot` UDTF.
    ///
    /// Produces a single-row relation with table metadata:
    /// version, schema_json, protocol, properties, partition_columns.
    /// Useful for schema-aware conditional compilation.
    Metadata {
        /// Delta table location.
        source: String,
    },

    /// File manifest via `delta_add_actions` UDTF.
    ///
    /// Produces one row per file in the Delta table, with path, size,
    /// stats, and partition values. Useful for file-level cost estimation
    /// and selective file scanning.
    FileManifest {
        /// Delta table location.
        source: String,
    },

    /// Canonical CPG output transform routed through Rust execution authority.
    ///
    /// This variant replaces Python-side CPG finalize builders by encoding
    /// the output family and upstream source views directly in the execution spec.
    CpgEmit {
        output_kind: CpgOutputKind,
        #[serde(default)]
        sources: Vec<String>,
    },
}

/// Canonical CPG output families.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CpgOutputKind {
    Nodes,
    Edges,
    Props,
    PropsMap,
    EdgesBySrc,
    EdgesByDst,
}

/// Join key pair for relate transformations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinKeyPair {
    pub left_key: String,
    pub right_key: String,
}

/// Join type semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Semi,
    Anti,
}

/// Aggregation expression for aggregate transformations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregationExpr {
    pub column: String,
    pub function: String,
    pub alias: String,
}

/// Schema contract with column name -> type mapping.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaContract {
    pub columns: BTreeMap<String, String>,
}
