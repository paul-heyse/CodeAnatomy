//! WS4 + WS4.5: Global Plan Combiner + Graph Validation.
//!
//! Walks the view dependency graph topologically and builds a single
//! LogicalPlan DAG inside one SessionContext.

pub mod cache_boundaries;
pub mod cache_policy;
pub mod graph_validator;
pub mod inline_policy;
pub mod join_builder;
pub mod param_compiler;
pub mod plan_bundle;
pub mod plan_codec;
pub mod plan_compiler;
pub mod pushdown_probe_extract;
pub mod semantic_validator;
pub mod optimizer_pipeline;
pub mod scheduling;
pub mod cost_model;
pub mod substrait;
pub mod udtf_builder;
pub mod union_builder;
pub mod view_builder;
