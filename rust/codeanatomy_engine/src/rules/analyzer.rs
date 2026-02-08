//! Semantic integrity and safety AnalyzerRules.
//!
//! Validates logical plans for semantic correctness and safety constraints.
//! These rules run during the analysis phase before optimization.

use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{Expr, LogicalPlan};

/// SemanticIntegrityRule validates that required columns exist in the plan.
///
/// Ensures that all column references in the logical plan correspond to
/// actual columns available in the referenced tables or subqueries.
#[derive(Debug, Default)]
pub struct SemanticIntegrityRule;

impl AnalyzerRule for SemanticIntegrityRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // Validation-only rule: walk plan and verify column integrity
        validate_semantic_integrity(&plan)?;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "semantic_integrity"
    }
}

/// SafetyRule rejects non-deterministic constructs.
///
/// Validates that plans do not contain non-deterministic functions
/// (e.g., RANDOM(), NOW() without binding) unless explicitly whitelisted.
/// This ensures reproducible query execution for CPG builds.
#[derive(Debug, Default)]
pub struct SafetyRule;

impl AnalyzerRule for SafetyRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // Walk plan expressions and reject non-deterministic functions
        validate_safety(&plan, false)?;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "safety_rule"
    }
}

/// StrictSafetyRule applies stricter safety validation.
///
/// Extended variant of SafetyRule with additional constraints for
/// the Strict rulepack profile. Rejects more classes of non-deterministic
/// or potentially unsafe operations.
#[derive(Debug, Default)]
pub struct StrictSafetyRule;

impl AnalyzerRule for StrictSafetyRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        // Apply stricter validation rules
        validate_safety(&plan, true)?;
        Ok(plan)
    }

    fn name(&self) -> &str {
        "strict_safety_rule"
    }
}

/// Validates semantic integrity of the logical plan.
///
/// Walks the plan tree and verifies that:
/// - All column references exist in their input schemas
/// - Join conditions reference valid columns from both sides
/// - Aggregate functions reference valid input columns
///
/// # Arguments
///
/// * `plan` - Logical plan to validate
///
/// # Returns
///
/// Ok(()) if validation passes, Err otherwise
fn validate_semantic_integrity(plan: &LogicalPlan) -> Result<()> {
    // Recursive validation through plan tree
    for child in plan.inputs() {
        validate_semantic_integrity(child)?;
    }

    // Validate expressions in the current plan node
    match plan {
        LogicalPlan::Filter(filter) => {
            validate_expression_columns(&filter.predicate, plan.schema())?;
        }
        LogicalPlan::Projection(projection) => {
            for expr in &projection.expr {
                validate_expression_columns(expr, plan.schema())?;
            }
        }
        LogicalPlan::Aggregate(aggregate) => {
            for expr in &aggregate.group_expr {
                validate_expression_columns(expr, plan.schema())?;
            }
            for expr in &aggregate.aggr_expr {
                validate_expression_columns(expr, plan.schema())?;
            }
        }
        LogicalPlan::Join(join) => {
            for (left, right) in &join.on {
                validate_expression_columns(left, join.left.schema())?;
                validate_expression_columns(right, join.right.schema())?;
            }
            if let Some(filter) = &join.filter {
                validate_expression_columns(filter, plan.schema())?;
            }
        }
        // Other plan types: add validation as needed
        _ => {}
    }

    Ok(())
}

/// Validates column references in an expression.
///
/// Placeholder for column validation logic. In production, this would
/// recursively walk expression trees and verify that all Column references
/// exist in the provided schema.
///
/// # Arguments
///
/// * `expr` - Expression to validate
/// * `schema` - Schema containing available columns
///
/// # Returns
///
/// Ok(()) if all columns exist, Err otherwise
fn validate_expression_columns(
    _expr: &Expr,
    _schema: &datafusion_common::DFSchemaRef,
) -> Result<()> {
    // TODO: Implement full expression tree walking and column validation
    // For now, this is a placeholder that always succeeds
    Ok(())
}

/// Validates safety constraints on the logical plan.
///
/// Checks for non-deterministic functions and other safety violations.
///
/// # Arguments
///
/// * `plan` - Logical plan to validate
/// * `strict` - If true, apply stricter validation rules
///
/// # Returns
///
/// Ok(()) if validation passes, Err otherwise
fn validate_safety(plan: &LogicalPlan, strict: bool) -> Result<()> {
    // Recursive validation through plan tree
    for child in plan.inputs() {
        validate_safety(child, strict)?;
    }

    // Validate expressions for non-deterministic functions
    match plan {
        LogicalPlan::Filter(filter) => {
            validate_expression_safety(&filter.predicate, strict)?;
        }
        LogicalPlan::Projection(projection) => {
            for expr in &projection.expr {
                validate_expression_safety(expr, strict)?;
            }
        }
        LogicalPlan::Aggregate(aggregate) => {
            for expr in &aggregate.group_expr {
                validate_expression_safety(expr, strict)?;
            }
            for expr in &aggregate.aggr_expr {
                validate_expression_safety(expr, strict)?;
            }
        }
        // Other plan types: add validation as needed
        _ => {}
    }

    Ok(())
}

/// Validates safety constraints on an expression.
///
/// Placeholder for safety validation logic. In production, this would
/// recursively walk expression trees and check for non-deterministic
/// functions like RANDOM(), NOW(), etc.
///
/// # Arguments
///
/// * `expr` - Expression to validate
/// * `strict` - If true, apply stricter validation rules
///
/// # Returns
///
/// Ok(()) if expression is safe, Err otherwise
fn validate_expression_safety(expr: &Expr, strict: bool) -> Result<()> {
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};

    // Check for non-deterministic scalar functions
    if let Expr::ScalarFunction(func) = expr {
        let func_name = func.name().to_lowercase();

        // Reject always-non-deterministic functions
        if func_name == "random" || func_name == "uuid" {
            return Err(DataFusionError::Plan(format!(
                "Non-deterministic function '{}' is not allowed in CPG builds",
                func_name
            )));
        }

        // In strict mode, reject additional functions
        if strict && (func_name == "now" || func_name == "current_timestamp") {
            return Err(DataFusionError::Plan(format!(
                "Time-dependent function '{}' is not allowed in strict mode",
                func_name
            )));
        }
    }

    // Recursively validate child expressions using TreeNode API
    expr.apply(|child_expr| {
        validate_expression_safety(child_expr, strict)?;
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;

    #[test]
    fn test_semantic_integrity_rule_name() {
        let rule = SemanticIntegrityRule;
        assert_eq!(rule.name(), "semantic_integrity");
    }

    #[test]
    fn test_safety_rule_name() {
        let rule = SafetyRule;
        assert_eq!(rule.name(), "safety_rule");
    }

    #[test]
    fn test_strict_safety_rule_name() {
        let rule = StrictSafetyRule;
        assert_eq!(rule.name(), "strict_safety_rule");
    }

    #[tokio::test]
    async fn test_semantic_integrity_accepts_valid_plan() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let config = state.config_options();

        // Create a simple valid plan
        let df = ctx.read_empty().unwrap();
        let plan = df.logical_plan().clone();

        let rule = SemanticIntegrityRule;
        let result = rule.analyze(plan.clone(), config);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_safety_rule_accepts_valid_plan() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let config = state.config_options();

        // Create a simple valid plan
        let df = ctx.read_empty().unwrap();
        let plan = df.logical_plan().clone();

        let rule = SafetyRule;
        let result = rule.analyze(plan.clone(), config);

        assert!(result.is_ok());
    }
}
