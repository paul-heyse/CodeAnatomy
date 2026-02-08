//! Logical OptimizerRules using TreeNode API.
//!
//! Rewrites logical plans for CPG-specific optimizations.
//! Uses DataFusion's TreeNode transform API for declarative rewrites.

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::OptimizerRule;
use datafusion::optimizer::optimizer::OptimizerConfig;
use datafusion_common::Result;
use datafusion_expr::Expr;

/// SpanContainmentRewriteRule rewrites redundant span containment predicates.
///
/// Detects the pattern:
///   `a.bstart <= b.bstart AND b.bend <= a.bend`
///
/// And rewrites to:
///   `byte_span_contains(a.bstart, a.bend, b.bstart, b.bend)`
///
/// This enables better predicate pushdown and join optimization for
/// CPG queries that frequently test for byte span containment.
#[derive(Debug, Default)]
pub struct SpanContainmentRewriteRule;

impl OptimizerRule for SpanContainmentRewriteRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Transform plan tree top-down, rewriting span containment patterns
        plan.transform_down(|node| match node {
            LogicalPlan::Filter(filter) => {
                // Try to rewrite the filter predicate
                if let Some(rewritten) = try_rewrite_span_containment(&filter.predicate) {
                    // Build new filter with rewritten predicate
                    let new_filter = datafusion_expr::logical_plan::Filter::try_new(
                        rewritten,
                        filter.input.clone(),
                    )?;
                    Ok(Transformed::yes(LogicalPlan::Filter(new_filter)))
                } else {
                    Ok(Transformed::no(LogicalPlan::Filter(filter)))
                }
            }
            other => Ok(Transformed::no(other)),
        })
    }

    fn name(&self) -> &str {
        "span_containment_rewrite"
    }
}

/// DeltaScanAwareRule preserves pushdown opportunities for Delta scans.
///
/// Ensures that filter predicates that can be pushed down to Delta table
/// scans (file-level pruning) are not rewritten in ways that break pushdown.
///
/// This rule runs late in the optimization pipeline to verify that earlier
/// rewrites haven't broken Delta-specific optimizations.
#[derive(Debug, Default)]
pub struct DeltaScanAwareRule;

impl OptimizerRule for DeltaScanAwareRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Placeholder: ensure filter predicates preserve Delta pushdown
        // In production, this would:
        // 1. Identify TableScan nodes over Delta tables
        // 2. Check filters above those scans
        // 3. Ensure predicates remain in Delta-pushable form
        // 4. Potentially split filters into pushable/non-pushable portions

        // For now, this is a no-op that preserves the plan
        Ok(Transformed::no(plan))
    }

    fn name(&self) -> &str {
        "delta_scan_aware"
    }
}

/// Attempts to rewrite span containment predicate patterns.
///
/// Detects conjunctions of the form:
///   `a.bstart <= b.bstart AND b.bend <= a.bend`
///
/// And rewrites to a UDF call:
///   `byte_span_contains(a.bstart, a.bend, b.bstart, b.bend)`
///
/// # Arguments
///
/// * `predicate` - Filter predicate expression to analyze
///
/// # Returns
///
/// Some(rewritten_expr) if the pattern matches, None otherwise
fn try_rewrite_span_containment(_predicate: &Expr) -> Option<Expr> {
    // TODO: Implement pattern matching for span containment
    //
    // Pattern to match:
    // 1. Top-level AND of two comparisons
    // 2. First: a.bstart <= b.bstart
    // 3. Second: b.bend <= a.bend
    // 4. Extract column references for a.bstart, a.bend, b.bstart, b.bend
    // 5. Build UDF call: byte_span_contains(a.bstart, a.bend, b.bstart, b.bend)
    //
    // This requires:
    // - Walking the expression tree to find BinaryExpr(AND)
    // - Checking both sides are comparisons (<=)
    // - Extracting column references
    // - Building ScalarUDF call expression
    //
    // Placeholder: return None (no rewrite)
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[test]
    fn test_span_containment_rule_name() {
        let rule = SpanContainmentRewriteRule;
        assert_eq!(rule.name(), "span_containment_rewrite");
    }

    #[test]
    fn test_delta_scan_aware_rule_name() {
        let rule = DeltaScanAwareRule;
        assert_eq!(rule.name(), "delta_scan_aware");
    }

    #[tokio::test]
    async fn test_span_containment_rule_preserves_valid_plan() {
        let ctx = SessionContext::new();

        // Create a simple plan
        let df = ctx.read_empty().unwrap();
        let plan = df.logical_plan().clone();

        let rule = SpanContainmentRewriteRule;
        let state = ctx.state();

        let result = rule.rewrite(plan.clone(), &state);

        assert!(result.is_ok());
        let transformed = result.unwrap();
        // Should return Transformed::no since we don't have span patterns
        assert!(!transformed.transformed);
    }

    #[tokio::test]
    async fn test_delta_scan_aware_rule_preserves_plan() {
        let ctx = SessionContext::new();

        // Create a simple plan
        let df = ctx.read_empty().unwrap();
        let plan = df.logical_plan().clone();

        let rule = DeltaScanAwareRule;
        let state = ctx.state();

        let result = rule.rewrite(plan.clone(), &state);

        assert!(result.is_ok());
        let transformed = result.unwrap();
        // Should return Transformed::no (placeholder always preserves)
        assert!(!transformed.transformed);
    }

    #[test]
    fn test_try_rewrite_span_containment_returns_none_for_now() {
        // Placeholder test: should return None until pattern matching is implemented
        let expr = col("x").eq(lit(1));
        let result = try_rewrite_span_containment(&expr);
        assert!(result.is_none());
    }
}
