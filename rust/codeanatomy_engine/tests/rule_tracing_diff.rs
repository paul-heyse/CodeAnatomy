#![cfg(feature = "tracing")]

use std::sync::Arc;

use codeanatomy_engine::rules::registry::CpgRuleSet;
use codeanatomy_engine::session::factory::SessionFactory;
use codeanatomy_engine::session::profiles::{EnvironmentClass, EnvironmentProfile};
use codeanatomy_engine::spec::runtime::{RuleTraceMode, TracingConfig};
use codeanatomy_engine::stability::optimizer_lab::RuleStep;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

#[derive(Debug)]
struct NoopAnalyzerRule;

impl AnalyzerRule for NoopAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "noop_analyzer"
    }
}

#[derive(Debug)]
struct NoopOptimizerRule;

impl OptimizerRule for NoopOptimizerRule {
    fn name(&self) -> &str {
        "noop_optimizer"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    #[allow(deprecated)]
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        Ok(Transformed::no(plan))
    }
}

#[derive(Debug)]
struct NoopPhysicalRule;

impl PhysicalOptimizerRule for NoopPhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "noop_physical"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn custom_ruleset() -> CpgRuleSet {
    CpgRuleSet::new(
        vec![Arc::new(NoopAnalyzerRule)],
        vec![Arc::new(NoopOptimizerRule)],
        vec![Arc::new(NoopPhysicalRule)],
    )
}

#[tokio::test]
async fn test_phase_only_rule_mode_uses_only_phase_sentinels() {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Small));
    let tracing_config = TracingConfig {
        enabled: true,
        rule_mode: RuleTraceMode::PhaseOnly,
        plan_diff: true,
        ..TracingConfig::default()
    };
    let ruleset = custom_ruleset();

    let state = factory
        .build_session_state(&ruleset, [0xAA; 32], Some(&tracing_config))
        .await
        .expect("phase-only tracing session should build");
    let ctx = state.ctx;

    let state = ctx.state();
    let analyzer_rules = &state.analyzer().rules;
    assert_eq!(
        analyzer_rules.first().expect("sentinel start").name(),
        "__trace_analyzer_phase"
    );
    assert_eq!(
        analyzer_rules.last().expect("sentinel end").name(),
        "__trace_analyzer_phase"
    );
    assert!(
        !analyzer_rules
            .iter()
            .any(|rule| format!("{rule:?}").contains("InstrumentedAnalyzerRule")),
        "phase-only must not wrap analyzer rules with per-rule instrumentation",
    );

    let optimizer_rules = state.optimizers();
    assert_eq!(
        optimizer_rules.first().expect("sentinel start").name(),
        "__trace_optimizer_phase"
    );
    assert_eq!(
        optimizer_rules.last().expect("sentinel end").name(),
        "__trace_optimizer_phase"
    );
    assert!(
        !optimizer_rules
            .iter()
            .any(|rule| format!("{rule:?}").contains("InstrumentedOptimizerRule")),
        "phase-only must not wrap optimizer rules with per-rule instrumentation",
    );
}

#[tokio::test]
async fn test_full_rule_mode_wraps_rules_with_plan_diff_enabled() {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Small));
    let tracing_config = TracingConfig {
        enabled: true,
        rule_mode: RuleTraceMode::Full,
        plan_diff: true,
        ..TracingConfig::default()
    };
    let ruleset = custom_ruleset();

    let state = factory
        .build_session_state(&ruleset, [0xBB; 32], Some(&tracing_config))
        .await
        .expect("full tracing session should build");
    let ctx = state.ctx;

    let state = ctx.state();
    let analyzer_rules = &state.analyzer().rules;
    assert!(
        analyzer_rules
            .iter()
            .any(|rule| format!("{rule:?}").contains("InstrumentedAnalyzerRule")),
        "full mode must wrap analyzer rules",
    );
    assert!(
        analyzer_rules
            .iter()
            .any(|rule| format!("{rule:?}").contains("plan_diff: true")),
        "full mode should carry plan_diff=true into wrappers",
    );

    let optimizer_rules = state.optimizers();
    assert!(
        optimizer_rules
            .iter()
            .any(|rule| format!("{rule:?}").contains("InstrumentedOptimizerRule")),
        "full mode must wrap optimizer rules",
    );
    assert!(
        optimizer_rules
            .iter()
            .any(|rule| format!("{rule:?}").contains("plan_diff: true")),
        "full mode should carry plan_diff=true into wrappers",
    );
}

// ---------------------------------------------------------------------------
// Scope 5: Lab result types are serializable
// ---------------------------------------------------------------------------

/// Scope 5: RuleStep derives Serialize + Deserialize and round-trips through JSON.
#[test]
fn test_lab_rule_step_is_serializable() {
    let step = RuleStep {
        ordinal: 5,
        rule_name: "simplify_expressions".to_string(),
        plan_digest: [0xBBu8; 32],
    };

    let json = serde_json::to_string(&step).expect("RuleStep must serialize");
    assert!(json.contains("simplify_expressions"));
    assert!(json.contains("ordinal"));

    let deserialized: RuleStep =
        serde_json::from_str(&json).expect("RuleStep must deserialize");
    assert_eq!(deserialized.ordinal, 5);
    assert_eq!(deserialized.rule_name, "simplify_expressions");
    assert_eq!(deserialized.plan_digest, [0xBBu8; 32]);
}
