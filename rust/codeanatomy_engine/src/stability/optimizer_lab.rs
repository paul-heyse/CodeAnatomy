//! Offline optimizer lab for deterministic rulepack experiments.
//!
//! Provides a standalone harness to run DataFusion optimizer rules against a
//! `LogicalPlan` outside the normal execution path. Each rule application is
//! observed and recorded as a `RuleStep` with a BLAKE3 plan digest, enabling
//! deterministic per-rule diffs and reproducible CI experiments.
//!
//! # Design
//!
//! The lab is intentionally decoupled from session construction so that:
//! - Rule subsets can be tested in isolation.
//! - Max-pass overrides enable what-if scenarios.
//! - Step traces are comparable across runs for regression detection.
//! - Results can optionally feed into compliance capture.

use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::{Optimizer, OptimizerContext};
use datafusion::optimizer::OptimizerRule;
use datafusion_common::Result;
use serde::{Deserialize, Serialize};

/// A single optimizer rule application step captured during a lab run.
///
/// Each step records the rule that was applied and a BLAKE3 digest of the
/// resulting plan text, enabling deterministic diff comparisons across runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleStep {
    /// Zero-based ordinal of this step within the lab run.
    pub ordinal: usize,
    /// Name of the optimizer rule that was applied.
    pub rule_name: String,
    /// BLAKE3 digest of the plan's `display_indent()` output after this rule.
    pub plan_digest: [u8; 32],
}

/// Summary of a completed optimizer lab run.
///
/// Contains the optimized plan and the full trace of rule application steps.
/// The trace is ordered by application sequence and includes a digest at each
/// step for deterministic comparison.
#[derive(Debug, Clone)]
pub struct LabResult {
    /// The optimized logical plan after all passes.
    pub optimized_plan: LogicalPlan,
    /// Ordered trace of every rule application observed during optimization.
    pub steps: Vec<RuleStep>,
    /// Number of distinct rules that produced plan changes.
    pub rules_with_changes: usize,
}

/// Run an offline optimizer lab experiment.
///
/// Constructs an `Optimizer` from the given rule subset, configures it with
/// the specified max passes and failure policy, then optimizes the input plan
/// while recording each rule application step.
///
/// # Arguments
///
/// * `input` - The logical plan to optimize.
/// * `rules` - Subset of optimizer rules to apply (order matters).
/// * `max_passes` - Maximum optimization passes (clamped to u8 range per DataFusion API).
/// * `skip_failed_rules` - If true, rules that error are skipped; if false, errors propagate.
///
/// # Returns
///
/// A `LabResult` containing the optimized plan, step trace, and change count.
///
/// # Errors
///
/// Returns a `DataFusionError` if optimization fails and `skip_failed_rules` is false.
pub fn run_optimizer_lab(
    input: LogicalPlan,
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    max_passes: usize,
    skip_failed_rules: bool,
) -> Result<LabResult> {
    let optimizer = Optimizer::with_rules(rules);

    // DataFusion 51 OptimizerContext::with_max_passes takes u8.
    // Clamp to u8::MAX to avoid truncation surprises.
    let clamped_passes: u8 = max_passes.min(u8::MAX as usize) as u8;

    let config = OptimizerContext::new()
        .with_max_passes(clamped_passes)
        .with_skip_failing_rules(skip_failed_rules);

    let mut steps: Vec<RuleStep> = Vec::new();
    let mut prev_digest: Option<[u8; 32]> = None;
    let mut change_count: usize = 0;

    let optimized = optimizer.optimize(input, &config, |plan, rule| {
        let plan_text = plan.display_indent().to_string();
        let digest = *blake3::hash(plan_text.as_bytes()).as_bytes();

        // Track whether this rule actually changed the plan.
        let changed = prev_digest.map_or(true, |prev| prev != digest);
        if changed {
            change_count += 1;
        }
        prev_digest = Some(digest);

        steps.push(RuleStep {
            ordinal: steps.len(),
            rule_name: rule.name().to_string(),
            plan_digest: digest,
        });
    })?;

    Ok(LabResult {
        optimized_plan: optimized,
        steps,
        rules_with_changes: change_count,
    })
}

/// Run a lab experiment using rules extracted from an existing `CpgRuleSet`.
///
/// Convenience wrapper that extracts the optimizer rules from a rule set and
/// delegates to `run_optimizer_lab`.
pub fn run_lab_from_ruleset(
    input: LogicalPlan,
    ruleset: &crate::rules::registry::CpgRuleSet,
    max_passes: usize,
    skip_failed_rules: bool,
) -> Result<LabResult> {
    run_optimizer_lab(
        input,
        ruleset.optimizer_rules.clone(),
        max_passes,
        skip_failed_rules,
    )
}

/// Compare two lab results and report step-level differences.
///
/// Returns a list of (ordinal, old_digest, new_digest) tuples for steps where
/// the plan digest diverged. Useful for identifying which rule application
/// caused a plan change between two experiment runs.
pub fn diff_lab_results(
    baseline: &LabResult,
    candidate: &LabResult,
) -> Vec<(usize, Option<[u8; 32]>, Option<[u8; 32]>)> {
    let max_len = baseline.steps.len().max(candidate.steps.len());
    let mut diffs = Vec::new();

    for i in 0..max_len {
        let base_digest = baseline.steps.get(i).map(|s| s.plan_digest);
        let cand_digest = candidate.steps.get(i).map(|s| s.plan_digest);

        if base_digest != cand_digest {
            diffs.push((i, base_digest, cand_digest));
        }
    }

    diffs
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[test]
    fn test_run_optimizer_lab_empty_rules() {
        let ctx = SessionContext::new();
        let plan = ctx.read_empty().unwrap().into_unoptimized_plan();

        let result = run_optimizer_lab(plan, vec![], 3, false);
        assert!(result.is_ok());

        let lab = result.unwrap();
        assert!(lab.steps.is_empty());
        assert_eq!(lab.rules_with_changes, 0);
    }

    #[test]
    fn test_run_optimizer_lab_with_builtin_rules() {
        let ctx = SessionContext::new();
        let plan = ctx.read_empty().unwrap().into_unoptimized_plan();

        // Use DataFusion's default optimizer rules as a test bed.
        let default_optimizer = Optimizer::new();
        let rules = default_optimizer.rules.clone();

        let result = run_optimizer_lab(plan, rules, 3, true);
        assert!(result.is_ok());

        let lab = result.unwrap();
        // Default optimizer has multiple rules; steps should be non-empty.
        assert!(!lab.steps.is_empty());

        // Each step should have a valid ordinal sequence.
        for (i, step) in lab.steps.iter().enumerate() {
            assert_eq!(step.ordinal, i);
            assert!(!step.rule_name.is_empty());
        }
    }

    #[test]
    fn test_rule_step_determinism() {
        let ctx = SessionContext::new();
        let plan = ctx.read_empty().unwrap().into_unoptimized_plan();

        let default_optimizer = Optimizer::new();
        let rules = default_optimizer.rules.clone();

        let result1 = run_optimizer_lab(plan.clone(), rules.clone(), 3, true).unwrap();
        let result2 = run_optimizer_lab(plan, rules, 3, true).unwrap();

        // Same input + same rules = same step digests.
        assert_eq!(result1.steps.len(), result2.steps.len());
        for (s1, s2) in result1.steps.iter().zip(result2.steps.iter()) {
            assert_eq!(s1.rule_name, s2.rule_name);
            assert_eq!(s1.plan_digest, s2.plan_digest);
        }
    }

    #[test]
    fn test_max_passes_clamping() {
        let ctx = SessionContext::new();
        let plan = ctx.read_empty().unwrap().into_unoptimized_plan();

        // usize::MAX should be clamped to u8::MAX without panic.
        let result = run_optimizer_lab(plan, vec![], usize::MAX, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_diff_lab_results_identical() {
        let ctx = SessionContext::new();
        let plan = ctx.read_empty().unwrap().into_unoptimized_plan();

        let default_optimizer = Optimizer::new();
        let rules = default_optimizer.rules.clone();

        let result1 = run_optimizer_lab(plan.clone(), rules.clone(), 3, true).unwrap();
        let result2 = run_optimizer_lab(plan, rules, 3, true).unwrap();

        let diffs = diff_lab_results(&result1, &result2);
        assert!(diffs.is_empty(), "identical runs should produce no diffs");
    }

    #[test]
    fn test_diff_lab_results_different_rules() {
        let ctx = SessionContext::new();
        let plan = ctx.read_empty().unwrap().into_unoptimized_plan();

        let result_empty = run_optimizer_lab(plan.clone(), vec![], 3, true).unwrap();

        let default_optimizer = Optimizer::new();
        let result_default =
            run_optimizer_lab(plan, default_optimizer.rules.clone(), 3, true).unwrap();

        // Different rule sets should produce different step counts, hence diffs.
        let diffs = diff_lab_results(&result_empty, &result_default);
        // One is empty, other is not, so diffs should exist (at least all of result_default).
        assert_eq!(diffs.len(), result_default.steps.len());
    }

    #[test]
    fn test_rule_step_serialization() {
        let step = RuleStep {
            ordinal: 0,
            rule_name: "test_rule".to_string(),
            plan_digest: [42u8; 32],
        };

        let json = serde_json::to_string(&step).unwrap();
        let deserialized: RuleStep = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.ordinal, 0);
        assert_eq!(deserialized.rule_name, "test_rule");
        assert_eq!(deserialized.plan_digest, [42u8; 32]);
    }

    #[test]
    fn test_lab_result_from_ruleset() {
        use crate::rules::registry::CpgRuleSet;

        let ctx = SessionContext::new();
        let plan = ctx.read_empty().unwrap().into_unoptimized_plan();

        let ruleset = CpgRuleSet::new(vec![], vec![], vec![]);
        let result = run_lab_from_ruleset(plan, &ruleset, 3, false);
        assert!(result.is_ok());
        assert!(result.unwrap().steps.is_empty());
    }
}
