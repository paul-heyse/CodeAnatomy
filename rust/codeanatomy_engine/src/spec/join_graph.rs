//! Join graph specification for semantic execution planning.

use serde::{Deserialize, Serialize};
use crate::spec::relations::JoinType;

/// Join graph encoding cross-relation dependencies.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct JoinGraph {
    pub edges: Vec<JoinEdge>,
    pub constraints: Vec<JoinConstraint>,
}

/// Join edge between two relations.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinEdge {
    pub left_relation: String,
    pub right_relation: String,
    pub join_type: JoinType,
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
}

/// Join constraint encoding semantic invariants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinConstraint {
    pub name: String,
    pub constraint_type: String,
    pub relations: Vec<String>,
    pub columns: Vec<String>,
}
