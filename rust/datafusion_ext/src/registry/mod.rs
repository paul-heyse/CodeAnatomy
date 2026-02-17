//! Registry snapshot decomposition surface.

pub mod capabilities;
pub(crate) mod legacy;
pub mod metadata;
pub mod snapshot;
pub mod snapshot_types;

pub use capabilities::snapshot_hook_capabilities;
pub use snapshot::registry_snapshot;
pub use snapshot_types::{FunctionHookCapabilities, RegistrySnapshot};
