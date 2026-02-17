//! CodeAnatomy DataFusion Python extension surface.
//!
//! This module is staged for decomposition; runtime behavior remains
//! delegated to `legacy` while focused submodules are introduced.

mod legacy;

pub(crate) mod cache_tables;
pub(crate) mod delta_maintenance;
pub(crate) mod delta_mutations;
pub(crate) mod delta_provider;
pub(crate) mod helpers;
pub(crate) mod plugin_bridge;
pub(crate) mod session_utils;
pub(crate) mod udf_registration;

pub use legacy::*;
