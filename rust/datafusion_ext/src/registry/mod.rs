//! Registry snapshot decomposition surface.

pub mod capabilities;
pub(crate) mod legacy;
pub mod metadata;
pub mod snapshot;
pub mod snapshot_types;

pub use legacy::{registry_snapshot, snapshot_hook_capabilities, FunctionHookCapabilities, RegistrySnapshot};
