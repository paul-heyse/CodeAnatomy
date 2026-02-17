//! Capability projection wrappers.

use datafusion::execution::session_state::SessionState;

use crate::registry::legacy::FunctionHookCapabilities;

pub fn snapshot_hook_capabilities(state: &SessionState) -> Vec<FunctionHookCapabilities> {
    crate::registry::legacy::snapshot_hook_capabilities(state)
}
