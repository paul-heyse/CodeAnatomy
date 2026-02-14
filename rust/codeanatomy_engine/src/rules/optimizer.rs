//! Logical OptimizerRules using TreeNode API.
//!
//! Rewrites logical plans for CPG-specific optimizations.
//! Uses DataFusion's TreeNode transform API for declarative rewrites.

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::OptimizerConfig;
use datafusion::optimizer::OptimizerRule;
use datafusion_common::Result;
use datafusion_expr::{Expr, Operator};

/// SpanContainmentRewriteRule rewrites redundant span containment predicates.
///
/// Detects the pattern:
///   `a.bstart <= b.bstart AND b.bend <= a.bend`
///
/// And rewrites to a canonical order:
///   `(a.bstart <= b.bstart) AND (b.bend <= a.bend)`
///
/// This canonicalization makes optimizer behavior deterministic and provides
/// a stable target for downstream rule matching.
#[derive(Debug, Default)]
pub struct SpanContainmentRewriteRule;

impl OptimizerRule for SpanContainmentRewriteRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_down(|node| match node {
            LogicalPlan::Filter(filter) => {
                if let Some(rewritten) = try_rewrite_span_containment(&filter.predicate) {
                    let new_filter =
                        datafusion_expr::logical_plan::Filter::try_new(rewritten, filter.input)?;
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
/// Canonicalizes conjunction ordering for deterministic pushdown behavior:
/// `A AND B AND C` terms are sorted lexicographically by expression string.
#[derive(Debug, Default)]
pub struct DeltaScanAwareRule;

impl OptimizerRule for DeltaScanAwareRule {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_down(|node| match node {
            LogicalPlan::Filter(filter) => {
                if let Some(rewritten) = canonicalize_conjunction(&filter.predicate) {
                    let new_filter =
                        datafusion_expr::logical_plan::Filter::try_new(rewritten, filter.input)?;
                    Ok(Transformed::yes(LogicalPlan::Filter(new_filter)))
                } else {
                    Ok(Transformed::no(LogicalPlan::Filter(filter)))
                }
            }
            other => Ok(Transformed::no(other)),
        })
    }

    fn name(&self) -> &str {
        "delta_scan_aware"
    }
}

fn try_rewrite_span_containment(predicate: &Expr) -> Option<Expr> {
    let Expr::BinaryExpr(and_expr) = predicate else {
        return None;
    };
    if and_expr.op != Operator::And {
        return None;
    }

    let left_pair = extract_leq_columns(and_expr.left.as_ref())?;
    let right_pair = extract_leq_columns(and_expr.right.as_ref())?;

    let left_is_start =
        is_start_column(left_pair.0.name.as_str()) && is_start_column(left_pair.1.name.as_str());
    let right_is_start =
        is_start_column(right_pair.0.name.as_str()) && is_start_column(right_pair.1.name.as_str());
    let left_is_end =
        is_end_column(left_pair.0.name.as_str()) && is_end_column(left_pair.1.name.as_str());
    let right_is_end =
        is_end_column(right_pair.0.name.as_str()) && is_end_column(right_pair.1.name.as_str());

    let (start_expr, end_expr, start_pair, end_pair) = if left_is_start && right_is_end {
        (
            and_expr.left.as_ref().clone(),
            and_expr.right.as_ref().clone(),
            left_pair,
            right_pair,
        )
    } else if left_is_end && right_is_start {
        (
            and_expr.right.as_ref().clone(),
            and_expr.left.as_ref().clone(),
            right_pair,
            left_pair,
        )
    } else {
        return None;
    };

    if start_pair.0.relation != end_pair.1.relation || start_pair.1.relation != end_pair.0.relation
    {
        return None;
    }

    let rewritten = start_expr.and(end_expr);
    if rewritten == *predicate {
        return None;
    }
    Some(rewritten)
}

fn extract_leq_columns(
    expr: &Expr,
) -> Option<(datafusion_common::Column, datafusion_common::Column)> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op != Operator::LtEq {
        return None;
    }
    let Expr::Column(left) = binary.left.as_ref() else {
        return None;
    };
    let Expr::Column(right) = binary.right.as_ref() else {
        return None;
    };
    Some((left.clone(), right.clone()))
}

fn is_start_column(name: &str) -> bool {
    name == "bstart" || name.ends_with("_bstart")
}

fn is_end_column(name: &str) -> bool {
    name == "bend" || name.ends_with("_bend")
}

fn flatten_conjunction(expr: &Expr, out: &mut Vec<Expr>) {
    if let Expr::BinaryExpr(binary) = expr {
        if binary.op == Operator::And {
            flatten_conjunction(binary.left.as_ref(), out);
            flatten_conjunction(binary.right.as_ref(), out);
            return;
        }
    }
    out.push(expr.clone());
}

fn canonicalize_conjunction(predicate: &Expr) -> Option<Expr> {
    let mut terms = Vec::new();
    flatten_conjunction(predicate, &mut terms);
    if terms.len() < 2 {
        return None;
    }
    let mut sorted = terms.clone();
    sorted.sort_by_key(Expr::to_string);
    if sorted == terms {
        return None;
    }
    let mut iter = sorted.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, Expr::and))
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
        let df = ctx.read_empty().unwrap();
        let plan = df.logical_plan().clone();

        let rule = SpanContainmentRewriteRule;
        let state = ctx.state();
        let result = rule.rewrite(plan.clone(), &state);

        assert!(result.is_ok());
        let transformed = result.unwrap();
        assert!(!transformed.transformed);
    }

    #[tokio::test]
    async fn test_delta_scan_aware_rule_preserves_plan() {
        let ctx = SessionContext::new();
        let df = ctx.read_empty().unwrap();
        let plan = df.logical_plan().clone();

        let rule = DeltaScanAwareRule;
        let state = ctx.state();
        let result = rule.rewrite(plan.clone(), &state);

        assert!(result.is_ok());
    }

    #[test]
    fn test_try_rewrite_span_containment_returns_none_for_non_span() {
        let expr = col("x").eq(lit(1));
        let result = try_rewrite_span_containment(&expr);
        assert!(result.is_none());
    }

    #[test]
    fn test_try_rewrite_span_containment_reorders_predicates() {
        let expr = col("inner.bend")
            .lt_eq(col("outer.bend"))
            .and(col("outer.bstart").lt_eq(col("inner.bstart")));
        let result = try_rewrite_span_containment(&expr);
        assert!(result.is_some());
    }

    #[test]
    fn test_canonicalize_conjunction_sorts_terms() {
        let expr = col("b").eq(lit(1)).and(col("a").eq(lit(1)));
        let rewritten = canonicalize_conjunction(&expr);
        assert!(rewritten.is_some());
    }
}
