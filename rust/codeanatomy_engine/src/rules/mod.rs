//! WS2.5 + WS5: Rulepack Lifecycle + Rule Intent Compiler.
//!
//! Defines immutable `RulepackProfile → RuleSet` mapping.
//! Profile switch = new SessionState, not in-place mutation.

pub mod analyzer;
pub mod intent_compiler;
pub mod optimizer;
pub mod overlay;
pub mod physical;
pub mod registry;
pub mod rulepack;
