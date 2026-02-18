use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use codeanatomy_engine::providers::{
    build_interval_align_provider, execute_interval_align, IntervalAlignProviderConfig,
};
use datafusion::logical_expr::{col, TableProviderFilterPushDown};
use datafusion::prelude::{lit, SessionContext};

fn interval_inputs() -> (
    Arc<Schema>,
    Vec<RecordBatch>,
    Arc<Schema>,
    Vec<RecordBatch>,
) {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("bstart", DataType::Int64, false),
        Field::new("bend", DataType::Int64, false),
        Field::new("left_value", DataType::Int64, false),
    ]));
    let left_batch = RecordBatch::try_new(
        left_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a.py", "b.py"])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Int64Array::from(vec![20, 30])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .expect("left batch");
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("bstart", DataType::Int64, false),
        Field::new("bend", DataType::Int64, false),
        Field::new("score", DataType::Int64, false),
    ]));
    let right_batch = RecordBatch::try_new(
        right_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a.py", "a.py"])),
            Arc::new(Int64Array::from(vec![0, 8])),
            Arc::new(Int64Array::from(vec![25, 50])),
            Arc::new(Int64Array::from(vec![7, 3])),
        ],
    )
    .expect("right batch");
    (left_schema, vec![left_batch], right_schema, vec![right_batch])
}

#[tokio::test]
async fn interval_align_provider_reports_inexact_pushdown() {
    let (left_schema, left_batches, right_schema, right_batches) = interval_inputs();
    let provider = build_interval_align_provider(
        left_schema,
        left_batches,
        right_schema,
        right_batches,
        IntervalAlignProviderConfig::default(),
    )
    .await
    .expect("provider");
    let filter = col("path").eq(lit("a.py"));
    let statuses = provider
        .supports_filters_pushdown(&[&filter])
        .expect("statuses");
    assert_eq!(statuses, vec![TableProviderFilterPushDown::Inexact]);
}

#[tokio::test]
async fn interval_align_provider_supports_projection_filter_and_limit() {
    let (left_schema, left_batches, right_schema, right_batches) = interval_inputs();
    let mut config = IntervalAlignProviderConfig::default();
    config.how = "left".to_string();
    let provider = build_interval_align_provider(
        left_schema,
        left_batches,
        right_schema,
        right_batches,
        config,
    )
    .await
    .expect("provider");
    let ctx = SessionContext::new();
    ctx.register_table("intervals", provider).expect("register table");
    let df = ctx
        .sql("SELECT path, match_kind FROM intervals WHERE path = 'a.py' LIMIT 1")
        .await
        .expect("query");
    let batches = df.collect().await.expect("collect");
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

#[tokio::test]
async fn interval_align_execute_returns_left_join_rows() {
    let (left_schema, left_batches, right_schema, right_batches) = interval_inputs();
    let mut config = IntervalAlignProviderConfig::default();
    config.how = "left".to_string();
    let (schema, batches) = execute_interval_align(
        left_schema,
        left_batches,
        right_schema,
        right_batches,
        config,
    )
    .await
    .expect("execute interval align");
    assert!(schema.field_with_name("match_kind").is_ok());
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 2);
}
