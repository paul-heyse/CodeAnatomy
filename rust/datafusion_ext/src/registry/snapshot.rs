//! Snapshot assembly wrappers.

use datafusion::execution::session_state::SessionState;

use crate::registry::legacy::RegistrySnapshot;

pub fn registry_snapshot(state: &SessionState) -> RegistrySnapshot {
    crate::registry::legacy::registry_snapshot(state)
}
