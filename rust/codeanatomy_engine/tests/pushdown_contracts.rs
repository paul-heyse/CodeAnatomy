use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::{col, lit, SessionContext};

use codeanatomy_engine::compiler::pushdown_probe_extract::verify_pushdown_contracts;
use codeanatomy_engine::contracts::pushdown_mode::PushdownEnforcementMode;
use codeanatomy_engine::providers::pushdown_contract::{
    FilterPushdownStatus, PushdownProbe,
};

async fn context_with_table() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();
    ctx
}

#[tokio::test]
async fn test_verify_pushdown_contract_inexact_with_residual_satisfied() {
    let ctx = context_with_table().await;
    let expr = col("id").gt(lit(1));
    let df = ctx.table("t").await.unwrap().filter(expr.clone()).unwrap();
    let optimized = ctx.state().optimize(df.logical_plan()).unwrap();
    let residual_predicate = collect_filter_predicates(&optimized)
        .into_iter()
        .next()
        .unwrap_or_else(|| expr.to_string());

    let mut probes = BTreeMap::new();
    probes.insert(
        "t".to_string(),
        PushdownProbe {
            provider: "t".to_string(),
            filter_sql: vec![residual_predicate],
            statuses: vec![FilterPushdownStatus::Inexact],
        },
    );

    let report = verify_pushdown_contracts(&optimized, &probes, PushdownEnforcementMode::Warn);
    assert!(report.violations.is_empty());
}

#[tokio::test]
async fn test_verify_pushdown_contract_detects_missing_residual() {
    let ctx = context_with_table().await;
    let df = ctx.table("t").await.unwrap();
    let optimized = ctx.state().optimize(df.logical_plan()).unwrap();

    let mut probes = BTreeMap::new();
    probes.insert(
        "t".to_string(),
        PushdownProbe {
            provider: "t".to_string(),
            filter_sql: vec!["id > Int32(1)".to_string()],
            statuses: vec![FilterPushdownStatus::Inexact],
        },
    );

    let report = verify_pushdown_contracts(&optimized, &probes, PushdownEnforcementMode::Warn);
    assert!(!report.violations.is_empty());
}

fn collect_filter_predicates(plan: &datafusion::logical_expr::LogicalPlan) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![plan.clone()];
    while let Some(node) = stack.pop() {
        if let datafusion::logical_expr::LogicalPlan::Filter(filter) = &node {
            out.push(filter.predicate.to_string());
        }
        for input in node.inputs() {
            stack.push((*input).clone());
        }
    }
    out
}
