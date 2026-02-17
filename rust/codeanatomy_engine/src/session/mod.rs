//! WS2: Session Factory — deterministic SessionContext construction.
//!
//! Uses `SessionStateBuilder` for builder-first, deterministic session creation.
//! No ad-hoc post-build mutation.

pub mod envelope;
pub mod extraction;
pub mod factory;
pub mod format_policy;
pub mod capture;
pub mod planning_manifest;
pub mod planning_surface;
pub mod profile_coverage;
pub mod profiles;
pub mod runtime_profiles;
