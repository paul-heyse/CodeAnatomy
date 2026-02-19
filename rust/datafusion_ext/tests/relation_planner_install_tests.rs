use datafusion::prelude::SessionContext;
use datafusion_ext::install_relation_planner_native;

#[test]
fn install_relation_planner_registers_relation_planner() {
    let ctx = SessionContext::new();
    let baseline = ctx.state().relation_planners().len();
    install_relation_planner_native(&ctx).expect("install relation planner");
    let installed = ctx.state().relation_planners().len();
    assert!(installed >= baseline + 1);
}
