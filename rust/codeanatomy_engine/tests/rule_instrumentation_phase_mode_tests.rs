#![cfg(feature = "tracing")]

use std::sync::Arc;

use codeanatomy_engine::rules::registry::CpgRuleSet;
use codeanatomy_engine::session::factory::SessionFactory;
use codeanatomy_engine::session::profiles::{EnvironmentClass, EnvironmentProfile};
use codeanatomy_engine::spec::runtime::{RuleTraceMode, TracingConfig};
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
async fn phase_only_mode_instruments_phases_without_rule_wrappers() {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Small));
    let tracing_config = TracingConfig {
        enabled: true,
        rule_mode: RuleTraceMode::PhaseOnly,
        ..TracingConfig::default()
    };

    let state = factory
        .build_session_state(&custom_ruleset(), [0x33; 32], Some(&tracing_config))
        .await
        .expect("phase-only tracing session should build");

    let state = state.ctx.state();
    let analyzers = &state.analyzer().rules;
    assert_eq!(
        analyzers.first().expect("sentinel start").name(),
        "__trace_analyzer_phase"
    );
    assert_eq!(
        analyzers.last().expect("sentinel end").name(),
        "__trace_analyzer_phase"
    );
    assert!(
        !analyzers
            .iter()
            .any(|rule| format!("{rule:?}").contains("InstrumentedAnalyzerRule")),
        "phase-only mode should not wrap analyzer rules",
    );
}
