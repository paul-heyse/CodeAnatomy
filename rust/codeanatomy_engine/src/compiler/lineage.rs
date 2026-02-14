//! Lineage extraction from DataFusion LogicalPlan and Substrait payloads.

use std::collections::{BTreeMap, BTreeSet};

use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};

/// Scan lineage extracted from a `LogicalPlan::TableScan`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ScanLineage {
    pub dataset_name: String,
    pub projected_columns: Vec<String>,
    pub pushed_filters: Vec<String>,
}

/// Rust-native lineage report consumed by the Python bridge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct LineageReport {
    pub scans: Vec<ScanLineage>,
    pub required_columns_by_dataset: BTreeMap<String, Vec<String>>,
    pub filters: Vec<String>,
    pub referenced_tables: Vec<String>,
    pub required_udfs: Vec<String>,
    pub required_rewrite_tags: Vec<String>,
    pub referenced_udfs: Vec<String>,
}

/// Extract lineage from a logical plan.
pub fn extract_lineage(plan: &LogicalPlan) -> Result<LineageReport> {
    let mut projected_by_dataset: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut pushed_filters_by_dataset: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut global_filters: BTreeSet<String> = BTreeSet::new();

    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            let dataset = scan.table_name.to_string();

            let projected = projected_by_dataset.entry(dataset.clone()).or_default();
            if let Some(indices) = &scan.projection {
                let schema = scan.source.schema();
                for index in indices {
                    let field = schema.field(*index);
                    projected.insert(field.name().to_string());
                }
            }

            let pushed_filters = pushed_filters_by_dataset.entry(dataset).or_default();
            for filter in &scan.filters {
                let text = filter.to_string();
                pushed_filters.insert(text.clone());
                global_filters.insert(text);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    let mut scans = Vec::with_capacity(
        projected_by_dataset
            .len()
            .max(pushed_filters_by_dataset.len()),
    );
    let all_datasets: BTreeSet<String> = projected_by_dataset
        .keys()
        .chain(pushed_filters_by_dataset.keys())
        .cloned()
        .collect();
    let mut required_columns_by_dataset: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for dataset in &all_datasets {
        let projected_columns = projected_by_dataset
            .get(dataset)
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_else(Vec::new);
        let pushed_filters = pushed_filters_by_dataset
            .get(dataset)
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_else(Vec::new);

        if !projected_columns.is_empty() {
            required_columns_by_dataset.insert(dataset.clone(), projected_columns.clone());
        }

        scans.push(ScanLineage {
            dataset_name: dataset.clone(),
            projected_columns,
            pushed_filters,
        });
    }

    Ok(LineageReport {
        scans,
        required_columns_by_dataset,
        filters: global_filters.into_iter().collect(),
        referenced_tables: all_datasets.into_iter().collect(),
        required_udfs: Vec::new(),
        required_rewrite_tags: Vec::new(),
        referenced_udfs: Vec::new(),
    })
}

/// Decode Substrait bytes and extract lineage.
#[cfg(feature = "substrait")]
pub fn lineage_from_substrait(ctx: &SessionContext, payload_bytes: &[u8]) -> Result<LineageReport> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| {
            DataFusionError::Execution(format!(
                "Failed to create Tokio runtime for Substrait lineage decode: {err}"
            ))
        })?;
    let logical_plan = runtime.block_on(crate::compiler::substrait::decode_substrait(
        ctx,
        payload_bytes,
    ))?;
    extract_lineage(&logical_plan)
}

/// Decode Substrait bytes and extract lineage.
#[cfg(not(feature = "substrait"))]
pub fn lineage_from_substrait(
    _ctx: &SessionContext,
    _payload_bytes: &[u8],
) -> Result<LineageReport> {
    Err(DataFusionError::NotImplemented(
        "substrait feature not enabled".to_string(),
    ))
}

/// Extract referenced table names from a LogicalPlan.
pub fn referenced_tables(plan: &LogicalPlan) -> Result<Vec<String>> {
    Ok(extract_lineage(plan)?.referenced_tables)
}

/// Extract referenced columns from a LogicalPlan.
pub fn referenced_columns(plan: &LogicalPlan) -> Result<Vec<(String, String)>> {
    let lineage = extract_lineage(plan)?;
    let mut output: Vec<(String, String)> = Vec::new();
    for (dataset, columns) in lineage.required_columns_by_dataset {
        for column in columns {
            output.push((dataset.clone(), column));
        }
    }
    output.sort();
    output.dedup();
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    #[tokio::test]
    async fn test_extract_lineage_for_filtered_scan() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .expect("record batch");
        ctx.register_batch("scan_table", batch)
            .expect("register batch");

        let df = ctx
            .sql("SELECT a FROM scan_table WHERE b > 10")
            .await
            .expect("query");
        let report = extract_lineage(df.logical_plan()).expect("lineage");

        assert_eq!(report.referenced_tables, vec!["scan_table"]);
        assert_eq!(report.scans.len(), 1);
        assert_eq!(report.scans[0].dataset_name, "scan_table");
    }
}
