use datafusion::prelude::SessionContext;
use datafusion_ext::install_type_planner_native;

#[test]
fn install_type_planner_registers_type_planner() {
    let ctx = SessionContext::new();
    install_type_planner_native(&ctx).expect("install type planner");
    let state_debug = format!("{:?}", ctx.state());
    assert!(state_debug.contains("CodeAnatomyTypePlanner"));
}
