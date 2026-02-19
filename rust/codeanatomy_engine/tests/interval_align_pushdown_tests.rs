use std::sync::Arc;

use codeanatomy_engine::providers::interval_align_provider::{
    IntervalAlignProvider, IntervalAlignProviderConfig,
};
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion_expr::{col, lit};

fn sample_provider() -> IntervalAlignProvider {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("bstart", DataType::Int64, false),
        Field::new("bend", DataType::Int64, false),
        Field::new("id", DataType::Int64, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("bstart", DataType::Int64, false),
        Field::new("bend", DataType::Int64, false),
        Field::new("rid", DataType::Int64, false),
    ]));
    let left_batch = RecordBatch::try_new(
        Arc::clone(&left_schema),
        vec![
            Arc::new(StringArray::from(vec!["f.py"])),
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .expect("left batch");
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![
            Arc::new(StringArray::from(vec!["f.py"])),
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![12])),
            Arc::new(Int64Array::from(vec![2])),
        ],
    )
    .expect("right batch");

    IntervalAlignProvider::try_new(
        left_schema,
        vec![left_batch],
        right_schema,
        vec![right_batch],
        IntervalAlignProviderConfig::default(),
    )
    .expect("provider")
}

#[test]
fn supports_filters_pushdown_classifies_interval_filters() {
    let provider = sample_provider();
    let left_only_filter = col("id").gt(lit(0_i64));
    let match_score_filter = col("match_score").gt(lit(0.0_f64));

    let statuses = provider
        .supports_filters_pushdown(&[&left_only_filter, &match_score_filter])
        .expect("pushdown statuses");

    assert_eq!(statuses[0], TableProviderFilterPushDown::Exact);
    assert_eq!(statuses[1], TableProviderFilterPushDown::Inexact);
}

#[test]
fn supports_filters_pushdown_marks_volatile_expressions_unsupported() {
    let provider = sample_provider();
    let volatile_filter = datafusion::functions::expr_fn::random().gt(lit(0.5_f64));
    let statuses = provider
        .supports_filters_pushdown(&[&volatile_filter])
        .expect("pushdown statuses");
    assert_eq!(statuses[0], TableProviderFilterPushDown::Unsupported);
}
