//! Lineage extraction from DataFusion LogicalPlan.
//!
//! When lineage transitions to Rust, this module provides
//! Rust-native lineage extraction from LogicalPlan via the
//! `TreeNode` trait for recursive plan walking.
//!
//! **Status:** Future stub. Python `lineage/datafusion.py` is
//! the current authority. This file documents the target API.

use datafusion::logical_expr::LogicalPlan;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::Result;
use std::collections::HashSet;

/// Extract referenced table names from a LogicalPlan.
///
/// DataFusion provides plan introspection via:
/// - `LogicalPlan::inputs()` — child plan references
/// - `LogicalPlan.apply()` / `TreeNode` trait — recursive plan walking
/// - `LogicalPlan::TableScan` variant — extract table references
/// - `Expr::Column` — extract column references
pub fn referenced_tables(plan: &LogicalPlan) -> Result<Vec<String>> {
    let mut tables = HashSet::new();

    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            tables.insert(scan.table_name.to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(tables.into_iter().collect())
}

/// Extract referenced columns from a LogicalPlan.
pub fn referenced_columns(plan: &LogicalPlan) -> Result<Vec<(String, String)>> {
    let mut columns = HashSet::new();

    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            if let Some(projection) = &scan.projection {
                let schema = scan.source.schema();
                for idx in projection {
                    let field = schema.field(*idx);
                    columns.insert((
                        scan.table_name.to_string(),
                        field.name().to_string(),
                    ));
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(columns.into_iter().collect())
}
