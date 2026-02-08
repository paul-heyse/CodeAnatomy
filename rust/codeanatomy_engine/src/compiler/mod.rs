//! WS4 + WS4.5: Global Plan Combiner + Graph Validation.
//!
//! Walks the view dependency graph topologically and builds a single
//! LogicalPlan DAG inside one SessionContext.

pub mod cache_boundaries;
pub mod graph_validator;
pub mod join_builder;
pub mod plan_compiler;
pub mod union_builder;
pub mod view_builder;
