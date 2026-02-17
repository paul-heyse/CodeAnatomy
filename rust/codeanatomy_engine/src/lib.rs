//! CodeAnatomy Semantic Execution Engine.
//!
//! Rust-first engine that compiles a `SemanticExecutionSpec` into a single
//! DataFusion `LogicalPlan` DAG, executes it, and materializes CPG outputs
//! to Delta tables.
//!
//! Design principles:
//! 1. One SessionContext per run
//! 2. One LogicalPlan DAG (compose at logical level only)
//! 3. Rules-as-policy (AnalyzerRules, OptimizerRules, PhysicalOptimizerRules)
//! 4. Delta-native providers only (no Arrow Dataset fallback)
//! 5. Builder-first session construction via SessionStateBuilder
//! 6. Two-layer determinism (spec_hash + envelope_hash)

pub mod compat;
pub mod compiler;
pub mod compliance;
pub mod contracts;
pub mod executor;
pub mod providers;
pub mod rules;
pub mod schema;
pub mod session;
pub mod spec;
pub mod stability;
pub mod tuner;

pub use compiler::compile_contract::{
    compile_request, compile_response_to_json, CompilePlanArtifact, CompileRequest, CompileResponse,
};
