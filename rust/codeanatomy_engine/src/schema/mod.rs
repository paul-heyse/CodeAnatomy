//! WS8: Schema Runtime Utilities.
//!
//! Information schema helpers, schema diff, cast injection.

pub mod introspection;
pub mod policy_ops;

// Re-export key functions for convenience
pub use introspection::{
    cast_alignment_exprs, hash_arrow_schema, is_additive_evolution, schema_diff,
};
pub use policy_ops::{
    apply_delta_scan_policy, apply_scan_policy, dataset_contract, dataset_name, dataset_policy,
    dataset_schema,
};
