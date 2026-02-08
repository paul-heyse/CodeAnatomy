//! WS10: Optional Compliance Profile Modules.
//!
//! Observability/compliance as optional profile modules, not core runtime obligations.
//! Core profile: max throughput. Strict/replay: include explain/rule traces.

pub mod capture;

// Re-export key types for compliance workflows
pub use capture::{
    capture_explain_verbose, ComplianceCapture, RetentionPolicy, RuleImpact, RulepackSnapshot,
};
