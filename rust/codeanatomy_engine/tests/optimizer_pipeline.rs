use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use codeanatomy_engine::compiler::optimizer_pipeline::{
    run_optimizer_compile_only, run_optimizer_compile_only_with_rules, run_optimizer_pipeline,
    OptimizerPipelineConfig,
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
async fn test_optimizer_pipeline_capture_traces() {
    let ctx = context_with_table().await;
    let df = ctx
        .table("t")
        .await
        .unwrap()
        .filter(datafusion::prelude::col("id").gt_eq(datafusion::prelude::lit(2)))
        .unwrap();
    let config = OptimizerPipelineConfig {
        capture_pass_traces: true,
        capture_plan_diffs: true,
        ..OptimizerPipelineConfig::default()
    };
    let result = run_optimizer_pipeline(&ctx, df.logical_plan().clone(), &config)
        .await
        .unwrap();
    assert!(!result.pass_traces.is_empty());
}

#[tokio::test]
async fn test_optimizer_compile_report_is_deterministic() {
    let ctx = context_with_table().await;
    let df = ctx.table("t").await.unwrap();
    let config = OptimizerPipelineConfig {
        capture_pass_traces: true,
        ..OptimizerPipelineConfig::default()
    };
    let a = run_optimizer_compile_only(&ctx, df.logical_plan().clone(), &config)
        .await
        .unwrap();
    let b = run_optimizer_compile_only(&ctx, df.logical_plan().clone(), &config)
        .await
        .unwrap();
    assert_eq!(a.optimized_logical_digest, b.optimized_logical_digest);
    assert_eq!(a.pass_traces, b.pass_traces);
}

#[tokio::test]
async fn test_optimizer_compile_only_with_rules_uses_shared_trace_shape() {
    let ctx = context_with_table().await;
    let df = ctx.table("t").await.unwrap();
    let report =
        run_optimizer_compile_only_with_rules(df.logical_plan().clone(), vec![], 3, true, true)
            .unwrap();
    assert!(report.optimized_logical_digest != [0u8; 32]);
    assert!(report.pass_traces.is_empty());
}
