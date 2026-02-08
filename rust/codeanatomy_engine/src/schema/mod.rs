//! WS8: Schema Runtime Utilities.
//!
//! Information schema helpers, schema diff, cast injection.

pub mod introspection;

// Re-export key functions for convenience
pub use introspection::{
    cast_alignment_exprs, hash_arrow_schema, is_additive_evolution, schema_diff,
};
