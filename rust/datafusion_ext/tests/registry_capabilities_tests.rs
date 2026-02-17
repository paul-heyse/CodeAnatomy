use datafusion::prelude::SessionContext;
use datafusion_ext::registry::capabilities::snapshot_hook_capabilities;

#[test]
fn capability_projection_runs_against_fresh_session_state() {
    let ctx = SessionContext::new();
    let state = ctx.state();
    let capabilities = snapshot_hook_capabilities(&state);
    assert!(!capabilities.is_empty());
}
