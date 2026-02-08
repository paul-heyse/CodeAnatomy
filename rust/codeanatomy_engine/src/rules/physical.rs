//! PhysicalOptimizerRules for CPG-specific physical optimization.
//!
//! Optimizes physical execution plans based on CPG workload characteristics
//! and environment profile tuning parameters.

use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;

/// CpgPhysicalRule extends base CodeAnatomyPhysicalRule with CPG-specific optimizations.
///
/// Applies physical-level optimizations tailored to CPG query patterns:
/// - Coalescing small batches after selective filters
/// - Hash join memory hints for large graph structures
/// - Schema validation after rewrites
#[derive(Debug)]
pub struct CpgPhysicalRule {
    /// Whether to coalesce partitions after filter operations
    pub coalesce_after_filter: bool,
    /// Optional memory hint for hash join operations (in bytes)
    pub hash_join_memory_hint: Option<usize>,
}

impl Default for CpgPhysicalRule {
    fn default() -> Self {
        Self {
            coalesce_after_filter: true,
            hash_join_memory_hint: None,
        }
    }
}

impl PhysicalOptimizerRule for CpgPhysicalRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Delegate to base CodeAnatomyPhysicalRule for standard optimizations
        let base_rule = datafusion_ext::physical_rules::CodeAnatomyPhysicalRule::default();
        let mut optimized = base_rule.optimize(plan, config)?;

        // Apply CPG-specific optimizations
        if self.coalesce_after_filter {
            optimized = apply_post_filter_coalescing(optimized)?;
        }

        if let Some(_memory_hint) = self.hash_join_memory_hint {
            optimized = apply_hash_join_hints(optimized)?;
        }

        Ok(optimized)
    }

    fn name(&self) -> &str {
        "cpg_physical_rule"
    }

    fn schema_check(&self) -> bool {
        // Enable built-in schema validation after rewrite
        true
    }
}

/// CostShapeRule applies repartitioning strategy based on environment profile.
///
/// Adjusts parallelism and partition counts based on estimated costs and
/// the target environment's capabilities (small/medium/large).
#[derive(Debug)]
pub struct CostShapeRule {
    /// Target number of partitions for repartitioning
    pub target_partitions: u32,
}

impl Default for CostShapeRule {
    fn default() -> Self {
        Self {
            target_partitions: 8,
        }
    }
}

impl PhysicalOptimizerRule for CostShapeRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Apply repartitioning strategy based on cost estimates
        // In production, this would:
        // 1. Estimate operator costs from plan statistics
        // 2. Identify expensive operators (large joins, aggregations)
        // 3. Insert repartitioning operations where beneficial
        // 4. Use target_partitions as the parallelism goal
        //
        // For now, this is a placeholder that preserves the plan
        Ok(plan)
    }

    fn name(&self) -> &str {
        "cost_shape_rule"
    }

    fn schema_check(&self) -> bool {
        // Enable schema validation
        true
    }
}

/// Applies coalescing after filter operations to reduce small batch overhead.
///
/// Identifies filter operations that may produce many small batches and
/// inserts CoalesceBatches operators to improve downstream performance.
///
/// # Arguments
///
/// * `plan` - Physical execution plan to optimize
///
/// # Returns
///
/// Optimized plan with coalescing operators inserted
fn apply_post_filter_coalescing(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    // TODO: Implement filter-aware coalescing
    // For now, return plan unchanged
    Ok(plan)
}

/// Applies hash join memory hints for large join operations.
///
/// Annotates hash join operators with memory allocation hints to improve
/// performance on large CPG graph structures.
///
/// # Arguments
///
/// * `plan` - Physical execution plan to optimize
///
/// # Returns
///
/// Plan with hash join hints applied
fn apply_hash_join_hints(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    // TODO: Implement hash join hint injection
    // For now, return plan unchanged
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::empty::EmptyExec;

    #[test]
    fn test_cpg_physical_rule_name() {
        let rule = CpgPhysicalRule::default();
        assert_eq!(rule.name(), "cpg_physical_rule");
    }

    #[test]
    fn test_cpg_physical_rule_schema_check_enabled() {
        let rule = CpgPhysicalRule::default();
        assert!(rule.schema_check());
    }

    #[test]
    fn test_cost_shape_rule_name() {
        let rule = CostShapeRule::default();
        assert_eq!(rule.name(), "cost_shape_rule");
    }

    #[test]
    fn test_cost_shape_rule_schema_check_enabled() {
        let rule = CostShapeRule::default();
        assert!(rule.schema_check());
    }

    #[tokio::test]
    async fn test_cpg_physical_rule_preserves_valid_plan() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let config = state.config_options();

        // Create a simple physical plan
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));

        let rule = CpgPhysicalRule::default();
        let result = rule.optimize(plan.clone(), config);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cost_shape_rule_with_custom_partitions() {
        let ctx = SessionContext::new();
        let state = ctx.state();
        let config = state.config_options();

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));

        let rule = CostShapeRule {
            target_partitions: 16,
        };
        let result = rule.optimize(plan.clone(), config);

        assert!(result.is_ok());
    }

    #[test]
    fn test_cpg_physical_rule_with_custom_config() {
        let rule = CpgPhysicalRule {
            coalesce_after_filter: false,
            hash_join_memory_hint: Some(1048576),
        };

        assert!(!rule.coalesce_after_filter);
        assert_eq!(rule.hash_join_memory_hint, Some(1048576));
    }
}
