//! Standalone logical optimizer harness for deterministic compile-only runs.

use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::{Optimizer, OptimizerContext};
use datafusion::optimizer::OptimizerRule;
use datafusion_common::Result;

use crate::compiler::optimizer_pipeline::{OptimizerPassTrace, OptimizerPhase};
use crate::compiler::plan_utils::blake3_hash_bytes;

#[derive(Debug, Clone)]
pub struct StandaloneOptimizerHarnessResult {
    pub optimized_plan: LogicalPlan,
    pub pass_traces: Vec<OptimizerPassTrace>,
}

/// Run logical optimization with deterministic per-rule observer traces.
pub fn run_standalone_optimizer_harness(
    unoptimized_plan: LogicalPlan,
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    max_passes: usize,
    skip_failed_rules: bool,
    capture_plan_diffs: bool,
) -> Result<StandaloneOptimizerHarnessResult> {
    let optimizer = Optimizer::with_rules(rules);
    let clamped_passes = max_passes.min(u8::MAX as usize) as u8;
    let context = OptimizerContext::new()
        .with_max_passes(clamped_passes)
        .with_skip_failing_rules(skip_failed_rules);

    let mut traces = Vec::new();
    let initial_text = format!("{}", unoptimized_plan.display_indent());
    let mut prev_digest: Option<[u8; 32]> = Some(blake3_hash_bytes(initial_text.as_bytes()));
    let optimized_plan = optimizer.optimize(unoptimized_plan, &context, |plan, rule| {
        let after_text = format!("{}", plan.display_indent());
        let after_digest = blake3_hash_bytes(after_text.as_bytes());
        let before_digest = prev_digest.unwrap_or(after_digest);
        prev_digest = Some(after_digest);
        traces.push(OptimizerPassTrace {
            pass_id: format!("logical:step{}", traces.len()),
            phase: OptimizerPhase::LogicalOptimizer,
            rule_name: rule.name().to_string(),
            before_digest,
            after_digest,
            plan_changed: before_digest != after_digest,
            plan_diff: if capture_plan_diffs && before_digest != after_digest {
                Some(format!("after_len={}", after_text.len()))
            } else {
                None
            },
            duration_us: 0,
        });
    })?;

    Ok(StandaloneOptimizerHarnessResult {
        optimized_plan,
        pass_traces: traces,
    })
}
