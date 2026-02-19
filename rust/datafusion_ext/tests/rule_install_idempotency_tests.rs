use datafusion::prelude::SessionContext;
use datafusion_ext::physical_rules::install_physical_rules;
use datafusion_ext::planner_rules::install_policy_rules;

fn analyzer_rule_count(ctx: &SessionContext, name: &str) -> usize {
    ctx.state()
        .analyzer()
        .rules
        .iter()
        .filter(|rule| rule.name() == name)
        .count()
}

fn physical_rule_count(ctx: &SessionContext, name: &str) -> usize {
    ctx.state()
        .physical_optimizers()
        .iter()
        .filter(|rule| rule.name() == name)
        .count()
}

#[test]
fn install_policy_rules_is_idempotent() {
    let ctx = SessionContext::new();
    let baseline = analyzer_rule_count(&ctx, "codeanatomy_policy_rule");

    install_policy_rules(&ctx).expect("first install");
    let after_first = analyzer_rule_count(&ctx, "codeanatomy_policy_rule");
    assert_eq!(after_first, baseline + 1);

    install_policy_rules(&ctx).expect("second install");
    let after_second = analyzer_rule_count(&ctx, "codeanatomy_policy_rule");
    assert_eq!(after_second, after_first);
}

#[test]
fn install_physical_rules_is_idempotent() {
    let ctx = SessionContext::new();
    let baseline = physical_rule_count(&ctx, "codeanatomy_physical_rule");

    install_physical_rules(&ctx).expect("first install");
    let after_first = physical_rule_count(&ctx, "codeanatomy_physical_rule");
    assert_eq!(after_first, baseline + 1);

    install_physical_rules(&ctx).expect("second install");
    let after_second = physical_rule_count(&ctx, "codeanatomy_physical_rule");
    assert_eq!(after_second, after_first);
}
