use codeanatomy_engine::compiler::standalone_optimizer_harness::run_standalone_optimizer_harness;
use datafusion::optimizer::optimizer::Optimizer;
use datafusion::prelude::SessionContext;

#[test]
fn standalone_optimizer_harness_runs_with_default_rules() {
    let ctx = SessionContext::new();
    let plan = ctx
        .read_empty()
        .expect("read_empty")
        .into_unoptimized_plan();
    let optimizer = Optimizer::new();
    let result = run_standalone_optimizer_harness(plan, optimizer.rules.clone(), 4, true, true)
        .expect("harness run");
    assert!(
        !result.pass_traces.is_empty(),
        "expected traces for default optimizer rules"
    );
}

#[test]
fn standalone_optimizer_harness_supports_empty_rule_set() {
    let ctx = SessionContext::new();
    let plan = ctx
        .read_empty()
        .expect("read_empty")
        .into_unoptimized_plan();
    let result =
        run_standalone_optimizer_harness(plan, Vec::new(), 2, false, false).expect("harness run");
    assert!(result.pass_traces.is_empty());
}
