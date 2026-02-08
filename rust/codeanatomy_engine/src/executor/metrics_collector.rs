//! Real physical metrics collection from executed DataFusion plan trees.
//!
//! Walks the `ExecutionPlan` tree after execution to extract real metrics
//! (output rows, spill counts, elapsed compute, scan selectivity) that
//! replace the synthetic placeholder values in the tuner.
//!
//! ## Usage
//!
//! After executing a physical plan, call `collect_plan_metrics(plan.as_ref())`
//! to aggregate metrics across all operators.

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use serde::{Deserialize, Serialize};

/// Real execution metrics collected from the executed plan tree.
///
/// Replaces the synthetic metrics at materializer.rs:208-214 once the
/// integration agent wires plan capture into the execution path.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CollectedMetrics {
    /// Total rows produced across all output partitions.
    pub output_rows: u64,
    /// Total spill events across all operators.
    pub spill_count: u64,
    /// Total bytes spilled to disk.
    pub spilled_bytes: u64,
    /// Total CPU time across all operators (nanoseconds).
    pub elapsed_compute_nanos: u64,
    /// Peak memory usage estimate (bytes).
    pub peak_memory_bytes: u64,
    /// Scan selectivity: output_rows / input_rows for leaf scans.
    /// 0.0..1.0, where 1.0 means all rows passed filters.
    pub scan_selectivity: f64,
    /// Number of partitions in the physical plan.
    pub partition_count: usize,
    /// Per-operator metric summaries.
    pub operator_metrics: Vec<OperatorMetricSummary>,
}

/// Per-operator metric summary extracted from the plan tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorMetricSummary {
    /// Operator name (e.g., "HashJoinExec", "ParquetExec").
    pub operator_name: String,
    /// Rows produced by this operator.
    pub output_rows: u64,
    /// CPU time for this operator (nanoseconds).
    pub elapsed_compute_nanos: u64,
    /// Spill events for this operator.
    pub spill_count: u64,
    /// Bytes spilled by this operator.
    pub spilled_bytes: u64,
    /// Current memory usage for this operator.
    pub memory_usage: u64,
}

/// Walk the executed plan tree and collect real metrics.
///
/// DataFusion populates `MetricsSet` on each `ExecutionPlan` node during
/// execution. After execution completes, we walk the tree and aggregate.
///
/// Key `MetricValue` variants used:
/// - `OutputRows(Count)` -- rows produced
/// - `ElapsedCompute(Time)` -- CPU time
/// - `SpillCount(Count)` -- spill events
/// - `SpilledBytes(Count)` -- bytes spilled
pub fn collect_plan_metrics(plan: &dyn ExecutionPlan) -> CollectedMetrics {
    let mut collected = CollectedMetrics::default();
    let mut scan_input_rows: u64 = 0;
    let mut scan_output_rows: u64 = 0;

    collect_recursive(plan, &mut collected, &mut scan_input_rows, &mut scan_output_rows);

    // Compute scan selectivity from leaf scan nodes.
    // If no scan nodes were found (e.g., in-memory only plans), default to 1.0.
    collected.scan_selectivity = if scan_input_rows > 0 {
        scan_output_rows as f64 / scan_input_rows as f64
    } else {
        1.0
    };

    collected.partition_count = plan.output_partitioning().partition_count();

    collected
}

/// Recursively walk the plan tree, collecting metrics from each node.
fn collect_recursive(
    plan: &dyn ExecutionPlan,
    collected: &mut CollectedMetrics,
    scan_input_rows: &mut u64,
    scan_output_rows: &mut u64,
) {
    // Collect metrics from this node
    if let Some(metrics) = plan.metrics() {
        let aggregated = metrics.aggregate_by_name();

        let output_rows = aggregated.output_rows().unwrap_or(0);
        let elapsed_nanos = aggregated.elapsed_compute().unwrap_or(0);
        let spills = aggregated.spill_count().unwrap_or(0);
        let spill_bytes = aggregated.spilled_bytes().unwrap_or(0);

        collected.output_rows += output_rows as u64;
        collected.elapsed_compute_nanos += elapsed_nanos as u64;
        collected.spill_count += spills as u64;
        collected.spilled_bytes += spill_bytes as u64;

        // Track scan selectivity from leaf nodes.
        // Leaf scan operators have names containing "Scan", "Parquet", or "Delta".
        let name = plan.name();
        if name.contains("Scan") || name.contains("Parquet") || name.contains("Delta") {
            *scan_output_rows += output_rows as u64;
            // For scans, input rows approximated from partition statistics when
            // available. This gives the pre-pushdown row count for selectivity.
            // partition_statistics(None) returns aggregate stats across all partitions.
            if let Ok(stats) = plan.partition_statistics(None) {
                if let Some(num_rows) = stats.num_rows.get_value() {
                    *scan_input_rows += *num_rows as u64;
                } else {
                    *scan_input_rows += output_rows as u64;
                }
            } else {
                *scan_input_rows += output_rows as u64;
            }
        }

        collected.operator_metrics.push(OperatorMetricSummary {
            operator_name: name.to_string(),
            output_rows: output_rows as u64,
            elapsed_compute_nanos: elapsed_nanos as u64,
            spill_count: spills as u64,
            spilled_bytes: spill_bytes as u64,
            memory_usage: 0, // Populated from gauge if available in future
        });
    }

    // Recurse into children
    for child in plan.children() {
        collect_recursive(child.as_ref(), collected, scan_input_rows, scan_output_rows);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collected_metrics_default() {
        let metrics = CollectedMetrics::default();
        assert_eq!(metrics.output_rows, 0);
        assert_eq!(metrics.spill_count, 0);
        assert_eq!(metrics.spilled_bytes, 0);
        assert_eq!(metrics.elapsed_compute_nanos, 0);
        assert_eq!(metrics.peak_memory_bytes, 0);
        assert!((metrics.scan_selectivity - 0.0).abs() < f64::EPSILON);
        assert_eq!(metrics.partition_count, 0);
        assert!(metrics.operator_metrics.is_empty());
    }

    #[test]
    fn test_operator_metric_summary_clone() {
        let summary = OperatorMetricSummary {
            operator_name: "TestExec".to_string(),
            output_rows: 100,
            elapsed_compute_nanos: 5000,
            spill_count: 1,
            spilled_bytes: 1024,
            memory_usage: 2048,
        };
        let cloned = summary.clone();
        assert_eq!(cloned.operator_name, "TestExec");
        assert_eq!(cloned.output_rows, 100);
        assert_eq!(cloned.spill_count, 1);
    }
}
