//! Stability infrastructure: harness + offline optimizer lab.
//!
//! - `harness`: Targeted performance and correctness tests for complex plan
//!   combination patterns (WS6.5).
//! - `optimizer_lab`: Offline optimizer lab for deterministic rulepack
//!   experiments (Scope Item 5).

pub mod harness;
pub mod optimizer_lab;

// Re-export key types for test infrastructure
pub use harness::{StabilityFixture, StabilityReport, StabilityResult, StabilityResultBuilder};

// Re-export optimizer lab types for external use
pub use optimizer_lab::{diff_lab_results, run_optimizer_lab, LabResult, RuleStep};
