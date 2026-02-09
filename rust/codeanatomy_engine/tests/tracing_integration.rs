#![cfg(feature = "tracing")]

use codeanatomy_engine::rules::registry::CpgRuleSet;
use codeanatomy_engine::session::factory::SessionFactory;
use codeanatomy_engine::session::profiles::{EnvironmentClass, EnvironmentProfile};
use codeanatomy_engine::spec::runtime::{RuleTraceMode, TracingConfig};

#[tokio::test]
async fn test_execution_instrumentation_rule_is_last() {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Small));
    let ruleset = CpgRuleSet::new(vec![], vec![], vec![]);
    let tracing_config = TracingConfig {
        enabled: true,
        record_metrics: true,
        rule_mode: RuleTraceMode::PhaseOnly,
        preview_limit: 3,
        ..TracingConfig::default()
    };

    let (ctx, _envelope) = factory
        .build_session(&ruleset, [9u8; 32], Some(&tracing_config))
        .await
        .expect("session with tracing should build");
    let physical_rules = ctx.state().physical_optimizers().to_vec();
    assert_eq!(
        physical_rules.first().expect("phase sentinel start").name(),
        "__trace_physical_optimizer_phase",
    );
    assert_eq!(
        physical_rules.last().expect("phase sentinel end").name(),
        "__trace_physical_optimizer_phase",
    );
    let last_non_sentinel = physical_rules
        .iter()
        .rev()
        .find(|rule| rule.name() != "__trace_physical_optimizer_phase")
        .expect("instrumentation rule should be present");
    assert_eq!(last_non_sentinel.name(), "Instrument");
}

#[tokio::test]
async fn test_disabled_tracing_does_not_append_instrument_rule() {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Small));
    let ruleset = CpgRuleSet::new(vec![], vec![], vec![]);
    let tracing_config = TracingConfig {
        enabled: false,
        ..TracingConfig::default()
    };

    let (ctx, _envelope) = factory
        .build_session(&ruleset, [3u8; 32], Some(&tracing_config))
        .await
        .expect("session without tracing should build");
    assert!(
        ctx.state().physical_optimizers().is_empty(),
        "no additional physical rules should be installed when tracing is disabled",
    );
}
