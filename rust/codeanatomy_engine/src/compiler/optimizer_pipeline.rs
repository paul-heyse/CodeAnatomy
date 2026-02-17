//! Optimizer pipeline orchestration with deterministic trace capture.
//!
//! This module provides a shared optimizer surface for both production planning
//! and compile-only diagnostics (optimizer lab / compliance capture).

use std::sync::Arc;
use std::time::Instant;

use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::{Optimizer, OptimizerContext};
use datafusion::optimizer::OptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use serde::{Deserialize, Serialize};

use crate::compiler::plan_utils::{blake3_hash_bytes, normalize_logical, normalize_physical};
use crate::executor::warnings::RunWarning;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuleFailurePolicy {
    SkipFailed,
    FailFast,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizerPipelineConfig {
    pub max_passes: usize,
    pub failure_policy: RuleFailurePolicy,
    pub capture_pass_traces: bool,
    pub capture_plan_diffs: bool,
}

impl Default for OptimizerPipelineConfig {
    fn default() -> Self {
        Self {
            max_passes: 3,
            failure_policy: RuleFailurePolicy::FailFast,
            capture_pass_traces: false,
            capture_plan_diffs: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OptimizerPhase {
    Analyzer,
    LogicalOptimizer,
    PhysicalOptimizer,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OptimizerPassTrace {
    pub pass_id: String,
    pub phase: OptimizerPhase,
    pub rule_name: String,
    pub before_digest: [u8; 32],
    pub after_digest: [u8; 32],
    pub plan_changed: bool,
    pub plan_diff: Option<String>,
    pub duration_us: u64,
}

#[derive(Debug)]
pub struct OptimizerPipelineResult {
    pub optimized_logical: LogicalPlan,
    pub physical: Arc<dyn ExecutionPlan>,
    pub pass_traces: Vec<OptimizerPassTrace>,
    pub warnings: Vec<RunWarning>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizerCompileReport {
    pub optimized_logical_digest: [u8; 32],
    pub optimized_logical_text: String,
    pub pass_traces: Vec<OptimizerPassTrace>,
    pub warnings: Vec<RunWarning>,
}

pub async fn run_optimizer_pipeline(
    ctx: &SessionContext,
    unoptimized_plan: LogicalPlan,
    config: &OptimizerPipelineConfig,
) -> Result<OptimizerPipelineResult> {
    let state = ctx.state();
    let mut traces = Vec::new();
    let warnings = Vec::new();

    let analyzed = unoptimized_plan.clone();
    if config.capture_pass_traces {
        traces.push(trace_logical_pass(
            "analyzer:pass0",
            OptimizerPhase::Analyzer,
            "analyzer_noop",
            &unoptimized_plan,
            &analyzed,
            config.capture_plan_diffs,
            0,
        ));
    }

    let optimize_started = Instant::now();
    let optimized_logical = state.optimize(&analyzed)?;
    let _optimize_elapsed = optimize_started.elapsed().as_micros() as u64;
    if config.capture_pass_traces {
        traces.push(trace_logical_pass(
            "logical:pass0",
            OptimizerPhase::LogicalOptimizer,
            "datafusion_state_optimize",
            &analyzed,
            &optimized_logical,
            config.capture_plan_diffs,
            0,
        ));
    }

    let physical_started = Instant::now();
    let physical = state.create_physical_plan(&optimized_logical).await?;
    let _physical_elapsed = physical_started.elapsed().as_micros() as u64;
    if config.capture_pass_traces {
        let before = normalize_physical(physical.as_ref());
        traces.push(OptimizerPassTrace {
            pass_id: "physical:pass0".to_string(),
            phase: OptimizerPhase::PhysicalOptimizer,
            rule_name: "datafusion_physical_create".to_string(),
            before_digest: blake3_hash_bytes(before.as_bytes()),
            after_digest: blake3_hash_bytes(before.as_bytes()),
            plan_changed: false,
            plan_diff: if config.capture_plan_diffs {
                Some(String::new())
            } else {
                None
            },
            duration_us: 0,
        });
    }

    Ok(OptimizerPipelineResult {
        optimized_logical,
        physical,
        pass_traces: traces,
        warnings,
    })
}

pub async fn run_optimizer_compile_only(
    ctx: &SessionContext,
    unoptimized_plan: LogicalPlan,
    config: &OptimizerPipelineConfig,
) -> Result<OptimizerCompileReport> {
    let skip_failed_rules = matches!(config.failure_policy, RuleFailurePolicy::SkipFailed);
    let rules = ctx.state().optimizers().to_vec();
    let (optimized_logical, pass_traces) = optimize_with_rules(
        unoptimized_plan,
        rules,
        config.max_passes,
        skip_failed_rules,
        config.capture_plan_diffs,
    )?;
    let optimized_logical_text = format!("{}", optimized_logical.display_indent());
    Ok(OptimizerCompileReport {
        optimized_logical_digest: blake3_hash_bytes(optimized_logical_text.as_bytes()),
        optimized_logical_text,
        pass_traces: if config.capture_pass_traces {
            pass_traces
        } else {
            Vec::new()
        },
        warnings: Vec::new(),
    })
}

/// Run compile-only optimization with an explicit rule set.
///
/// This surface is used by offline lab tooling so it shares the same trace
/// model (`OptimizerPassTrace`) as production optimizer capture.
pub fn run_optimizer_compile_only_with_rules(
    unoptimized_plan: LogicalPlan,
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    max_passes: usize,
    skip_failed_rules: bool,
    capture_plan_diffs: bool,
) -> Result<OptimizerCompileReport> {
    let (optimized, traces) = optimize_with_rules(
        unoptimized_plan,
        rules,
        max_passes,
        skip_failed_rules,
        capture_plan_diffs,
    )?;
    let optimized_logical_text = format!("{}", optimized.display_indent());
    Ok(OptimizerCompileReport {
        optimized_logical_digest: blake3_hash_bytes(optimized_logical_text.as_bytes()),
        optimized_logical_text,
        pass_traces: traces,
        warnings: Vec::new(),
    })
}

/// Run optimization with explicit rule set and shared trace semantics.
pub fn optimize_with_rules(
    unoptimized_plan: LogicalPlan,
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    max_passes: usize,
    skip_failed_rules: bool,
    capture_plan_diffs: bool,
) -> Result<(LogicalPlan, Vec<OptimizerPassTrace>)> {
    let optimizer = Optimizer::with_rules(rules);
    let clamped_passes = max_passes.min(u8::MAX as usize) as u8;
    let context = OptimizerContext::new()
        .with_max_passes(clamped_passes)
        .with_skip_failing_rules(skip_failed_rules);

    let mut traces = Vec::new();
    let initial_text = format!("{}", unoptimized_plan.display_indent());
    let mut prev_digest: Option<[u8; 32]> = Some(blake3_hash_bytes(initial_text.as_bytes()));
    let optimized = optimizer.optimize(unoptimized_plan, &context, |plan, rule| {
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
    Ok((optimized, traces))
}

fn trace_logical_pass(
    pass_id: &str,
    phase: OptimizerPhase,
    rule_name: &str,
    before: &LogicalPlan,
    after: &LogicalPlan,
    with_diff: bool,
    duration_us: u64,
) -> OptimizerPassTrace {
    let before_text = normalize_logical(before);
    let after_text = normalize_logical(after);
    let plan_changed = before_text != after_text;
    OptimizerPassTrace {
        pass_id: pass_id.to_string(),
        phase,
        rule_name: rule_name.to_string(),
        before_digest: blake3_hash_bytes(before_text.as_bytes()),
        after_digest: blake3_hash_bytes(after_text.as_bytes()),
        plan_changed,
        plan_diff: if with_diff && plan_changed {
            Some(format!(
                "before_len={} after_len={}",
                before_text.len(),
                after_text.len()
            ))
        } else {
            None
        },
        duration_us,
    }
}
