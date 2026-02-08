//! WS1: SemanticExecutionSpec — the immutable contract between Python and Rust.
//!
//! Python builds it; Rust consumes it. Nothing else crosses the boundary.

pub mod execution_spec;
pub mod hashing;
pub mod join_graph;
pub mod outputs;
pub mod parameters;
pub mod relations;
pub mod runtime;
pub mod rule_intents;
