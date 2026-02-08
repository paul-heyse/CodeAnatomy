//! WS3: Delta Provider Manager.
//!
//! Always register native Delta providers. No Arrow Dataset fallback.
//! Reuses existing `datafusion_ext::delta_control_plane`.

pub mod registration;
pub mod scan_config;
pub mod snapshot;

// Re-export key types for convenience
pub use registration::{register_extraction_inputs, TableRegistration};
pub use scan_config::{
    has_lineage_tracking, lineage_column_name, standard_scan_config, validate_scan_config,
};
pub use snapshot::{snapshot_metadata, table_version, validate_version_pin, SnapshotMode};
