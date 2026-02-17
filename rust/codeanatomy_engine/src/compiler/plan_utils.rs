//! Shared compiler helper utilities.
//!
//! These helpers centralize normalized plan rendering, digest hashing, and
//! common view-graph indexing/fanout computations used across compile surfaces.

use std::collections::HashMap;

use blake3::Hasher;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{displayable, ExecutionPlan};

use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::relations::ViewDefinition;

pub fn normalize_logical(plan: &LogicalPlan) -> String {
    format!("{}", plan.display_indent())
}

pub fn normalize_physical(plan: &dyn ExecutionPlan) -> String {
    format!("{}", displayable(plan).indent(true))
}

pub fn blake3_hash_bytes(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

pub fn view_index(spec: &SemanticExecutionSpec) -> HashMap<&str, &ViewDefinition> {
    spec.view_definitions
        .iter()
        .map(|view| (view.name.as_str(), view))
        .collect()
}

pub fn compute_view_fanout(spec: &SemanticExecutionSpec) -> HashMap<String, usize> {
    let mut fanout: HashMap<String, usize> = spec
        .view_definitions
        .iter()
        .map(|view| (view.name.clone(), 0usize))
        .collect();

    for view in &spec.view_definitions {
        for dep in &view.view_dependencies {
            *fanout.entry(dep.clone()).or_insert(0) += 1;
        }
    }

    for target in &spec.output_targets {
        *fanout.entry(target.source_view.clone()).or_insert(0) += 1;
    }

    fanout
}
