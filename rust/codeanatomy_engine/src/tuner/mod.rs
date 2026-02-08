//! WS7: Adaptive Tuner.
//!
//! Bounded auto-tuning with narrow scope. Never mutates correctness-affecting
//! options; tunes only bounded execution knobs.

pub mod adaptive;

// Re-export key types for convenience
pub use adaptive::{AdaptiveTuner, ExecutionMetrics, TunerConfig, TunerMode};
