//! Input relations and view definitions with transformation semantics.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Input relation specification — logical name mapped to Delta table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputRelation {
    pub logical_name: String,
    pub delta_location: String,
    pub requires_lineage: bool,
    pub version_pin: Option<i64>,
}

/// View definition with transformation semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[serde(tag = "kind")]
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
}

/// Join key pair for relate transformations.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct AggregationExpr {
    pub column: String,
    pub function: String,
    pub alias: String,
}

/// Schema contract with column name -> type mapping.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaContract {
    pub columns: BTreeMap<String, String>,
}
