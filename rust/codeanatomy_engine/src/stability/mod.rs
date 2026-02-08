//! WS6.5: Join/Union Stability Harness.
//!
//! Targeted performance and correctness tests for complex plan combination patterns.

pub mod harness;

// Re-export key types for test infrastructure
pub use harness::{StabilityFixture, StabilityReport, StabilityResult, StabilityResultBuilder};
