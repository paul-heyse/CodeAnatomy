use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_optimizer::optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::config::ConfigOptions;

use datafusion_ext::physical_rules::CodeAnatomyPhysicalRule;

fn test_plan() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    Arc::new(EmptyExec::new(schema))
}

#[test]
fn coalesce_applies_when_dynamic_and_sort_pushdown_disabled() {
    let rule = CodeAnatomyPhysicalRule;
    let mut config = ConfigOptions::default();
    config.optimizer.enable_dynamic_filter_pushdown = false;
    config.optimizer.enable_sort_pushdown = false;

    let optimized = rule.optimize(test_plan(), &config).expect("optimize succeeds");

    assert!(optimized.as_any().is::<CoalescePartitionsExec>());
}

#[test]
fn coalesce_is_skipped_when_dynamic_filter_pushdown_enabled() {
    let rule = CodeAnatomyPhysicalRule;
    let mut config = ConfigOptions::default();
    config.optimizer.enable_dynamic_filter_pushdown = true;
    config.optimizer.enable_sort_pushdown = false;

    let optimized = rule.optimize(test_plan(), &config).expect("optimize succeeds");

    assert!(!optimized.as_any().is::<CoalescePartitionsExec>());
}

#[test]
fn coalesce_is_skipped_when_sort_pushdown_enabled() {
    let rule = CodeAnatomyPhysicalRule;
    let mut config = ConfigOptions::default();
    config.optimizer.enable_dynamic_filter_pushdown = false;
    config.optimizer.enable_sort_pushdown = true;

    let optimized = rule.optimize(test_plan(), &config).expect("optimize succeeds");

    assert!(!optimized.as_any().is::<CoalescePartitionsExec>());
}
