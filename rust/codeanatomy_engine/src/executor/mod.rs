//! WS6: Execution Engine.
//!
//! Stream-first execution + CPG materialization via Delta write path.

pub mod delta_writer;
pub mod maintenance;
pub mod metrics_collector;
pub mod result;
pub mod runner;
pub mod tracing;
