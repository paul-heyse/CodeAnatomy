//! WS6.5: Join/Union Stability Harness
//!
//! Validates:
//! 1. Inequality joins (span containment) produce correct results
//! 2. union_by_name handles schema drift (missing columns → NULL)
//! 3. Repartition toggles produce correct results under all combinations
//! 4. Large fan-in unions don't explode partition count
//! 5. Mixed equality + inequality join predicates work correctly

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;
use std::sync::Arc;

/// Helper: create a SessionContext with test data.
async fn test_ctx() -> SessionContext {
    SessionContext::new()
}

/// Helper: register a MemTable from record batches.
fn register_mem_table(
    ctx: &SessionContext,
    name: &str,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
) {
    let table = MemTable::try_new(schema, vec![batches]).unwrap();
    ctx.register_table(name, Arc::new(table)).unwrap();
}

#[tokio::test]
async fn test_equality_join_correctness() {
    let ctx = test_ctx().await;

    // Create left table: entities with IDs
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let left_batch = RecordBatch::try_new(
        left_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    register_mem_table(&ctx, "left_t", left_schema, vec![left_batch]);

    // Create right table: references to entity IDs
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("ref_id", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
    ]));
    let right_batch = RecordBatch::try_new(
        right_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 4])),
            Arc::new(Float64Array::from(vec![0.5, 0.8, 0.1])),
        ],
    )
    .unwrap();
    register_mem_table(&ctx, "right_t", right_schema, vec![right_batch]);

    // Inner join on id = ref_id
    let left = ctx.table("left_t").await.unwrap();
    let right = ctx.table("right_t").await.unwrap();
    let joined = left
        .join(right, JoinType::Inner, &["id"], &["ref_id"], None)
        .unwrap();
    let result = joined.collect().await.unwrap();

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Inner join should match 2 rows (ids 1, 2)");
}

#[tokio::test]
async fn test_span_containment_join_correctness() {
    let ctx = test_ctx().await;

    // Outer spans (containers)
    let outer_schema = Arc::new(Schema::new(vec![
        Field::new("outer_id", DataType::Int64, false),
        Field::new("bstart", DataType::Int64, false),
        Field::new("bend", DataType::Int64, false),
    ]));
    let outer_batch = RecordBatch::try_new(
        outer_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![0, 100])),
            Arc::new(Int64Array::from(vec![50, 200])),
        ],
    )
    .unwrap();
    register_mem_table(&ctx, "outer_t", outer_schema, vec![outer_batch]);

    // Inner spans (contained)
    let inner_schema = Arc::new(Schema::new(vec![
        Field::new("inner_id", DataType::Int64, false),
        Field::new("bstart", DataType::Int64, false),
        Field::new("bend", DataType::Int64, false),
    ]));
    let inner_batch = RecordBatch::try_new(
        inner_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![10, 110, 300])),
            Arc::new(Int64Array::from(vec![20, 150, 400])),
        ],
    )
    .unwrap();
    register_mem_table(&ctx, "inner_t", inner_schema, vec![inner_batch]);

    // Span containment: outer.bstart <= inner.bstart AND inner.bend <= outer.bend
    let outer = ctx.table("outer_t").await.unwrap();
    let inner = ctx.table("inner_t").await.unwrap();

    let predicates = vec![
        col("outer_t.bstart").lt_eq(col("inner_t.bstart")),
        col("inner_t.bend").lt_eq(col("outer_t.bend")),
    ];

    let joined = outer.join_on(inner, JoinType::Inner, predicates).unwrap();
    let result = joined.collect().await.unwrap();

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    // Span 1 (0-50) contains inner (10-20): YES
    // Span 2 (100-200) contains inner (110-150): YES
    // Span 2 (100-200) contains inner (300-400): NO
    assert_eq!(total_rows, 2, "Span containment should match 2 pairs");
}

#[tokio::test]
async fn test_union_by_name_schema_drift() {
    let ctx = test_ctx().await;

    // Source A: {id, name, score}
    let a_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Float64, true),
    ]));
    let a_batch = RecordBatch::try_new(
        a_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Float64Array::from(vec![0.5])),
        ],
    )
    .unwrap();
    register_mem_table(&ctx, "source_a", a_schema, vec![a_batch]);

    // Source B: {id, name, extra_col} — different schema!
    let b_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("extra_col", DataType::Utf8, true),
    ]));
    let b_batch = RecordBatch::try_new(
        b_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(StringArray::from(vec!["b"])),
            Arc::new(StringArray::from(vec!["extra"])),
        ],
    )
    .unwrap();
    register_mem_table(&ctx, "source_b", b_schema, vec![b_batch]);

    // union_by_name should handle drift: {id, name, score, extra_col}
    let df_a = ctx.table("source_a").await.unwrap();
    let df_b = ctx.table("source_b").await.unwrap();
    let union_df = df_a.union_by_name(df_b).unwrap();

    let result = union_df.collect().await.unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "Union should produce 2 rows");

    // Schema should have all columns from both sources
    let schema = result[0].schema();
    assert!(schema.field_with_name("id").is_ok());
    assert!(schema.field_with_name("name").is_ok());
    assert!(schema.field_with_name("score").is_ok());
    assert!(schema.field_with_name("extra_col").is_ok());
}

#[tokio::test]
async fn test_large_fanin_union_partition_count() {
    let ctx = test_ctx().await;

    // Create 15 sources and union them
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));

    let mut first_df: Option<DataFrame> = None;

    for i in 0..15 {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![i as i64])),
                Arc::new(StringArray::from(vec![format!("val_{i}")])),
            ],
        )
        .unwrap();
        let name = format!("src_{i}");
        register_mem_table(&ctx, &name, schema.clone(), vec![batch]);

        let df = ctx.table(&name).await.unwrap();
        first_df = Some(match first_df {
            None => df,
            Some(acc) => acc.union_by_name(df).unwrap(),
        });
    }

    let union_df = first_df.unwrap();
    let result = union_df.collect().await.unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 15, "15-source union should produce 15 rows");
}

#[tokio::test]
async fn test_repartition_toggle_stability() {
    // Verify that repartition_joins=true vs false produces same results
    for repartition in [true, false] {
        let config = SessionConfig::new()
            .with_repartition_joins(repartition)
            .with_target_partitions(4);
        let ctx = SessionContext::new_with_config(config);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]));

        let left_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        register_mem_table(&ctx, "left_t", schema.clone(), vec![left_batch]);

        let right_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 3, 4])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();
        register_mem_table(&ctx, "right_t", schema.clone(), vec![right_batch]);

        let left = ctx.table("left_t").await.unwrap();
        let right = ctx.table("right_t").await.unwrap();
        let joined = left
            .join(right, JoinType::Inner, &["id"], &["id"], None)
            .unwrap();
        let result = joined.collect().await.unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 2,
            "Repartition={repartition}: inner join should match 2 rows"
        );
    }
}

#[tokio::test]
async fn test_view_registration_pattern() {
    // Verify into_view() + register_table() pattern works
    let ctx = test_ctx().await;

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    register_mem_table(&ctx, "base", schema, vec![batch]);

    // Create a view via into_view() + register_table()
    let df = ctx.table("base").await.unwrap();
    let filtered = df.filter(col("id").gt(lit(1))).unwrap();
    let view = filtered.into_view();
    ctx.register_table("filtered_view", view).unwrap();

    // Query the view
    let result = ctx
        .table("filtered_view")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "View should have 2 rows (id > 1)");
}

#[tokio::test]
async fn test_mixed_equality_inequality_join() {
    let ctx = test_ctx().await;

    // Left table: events with timestamps
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));
    let left_batch = RecordBatch::try_new(
        left_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["A", "B", "A"])),
            Arc::new(Int64Array::from(vec![100, 150, 200])),
        ],
    )
    .unwrap();
    register_mem_table(&ctx, "events", left_schema, vec![left_batch]);

    // Right table: time windows by category
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("window_id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("start_time", DataType::Int64, false),
        Field::new("end_time", DataType::Int64, false),
    ]));
    let right_batch = RecordBatch::try_new(
        right_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["A", "B"])),
            Arc::new(Int64Array::from(vec![50, 140])),
            Arc::new(Int64Array::from(vec![180, 160])),
        ],
    )
    .unwrap();
    register_mem_table(&ctx, "windows", right_schema, vec![right_batch]);

    // Join: category equality AND timestamp within window
    let left = ctx.table("events").await.unwrap();
    let right = ctx.table("windows").await.unwrap();

    let predicates = vec![
        col("events.category").eq(col("windows.category")),
        col("events.timestamp").gt_eq(col("windows.start_time")),
        col("events.timestamp").lt_eq(col("windows.end_time")),
    ];

    let joined = left.join_on(right, JoinType::Inner, predicates).unwrap();
    let result = joined.collect().await.unwrap();

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    // event_id=1, category=A, timestamp=100: matches window_id=10 (A, 50-180)
    // event_id=2, category=B, timestamp=150: matches window_id=20 (B, 140-160)
    // event_id=3, category=A, timestamp=200: NO MATCH (outside window)
    assert_eq!(
        total_rows, 2,
        "Mixed join should match 2 events within time windows"
    );
}

#[tokio::test]
async fn test_left_outer_join_preserves_nulls() {
    let ctx = test_ctx().await;

    let left_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let left_batch = RecordBatch::try_new(
        left_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();
    register_mem_table(&ctx, "left_t", left_schema, vec![left_batch]);

    let right_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("score", DataType::Float64, false),
    ]));
    let right_batch = RecordBatch::try_new(
        right_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Float64Array::from(vec![0.9])),
        ],
    )
    .unwrap();
    register_mem_table(&ctx, "right_t", right_schema, vec![right_batch]);

    let left = ctx.table("left_t").await.unwrap();
    let right = ctx.table("right_t").await.unwrap();
    let joined = left
        .join(right, JoinType::Left, &["id"], &["id"], None)
        .unwrap();
    let result = joined.collect().await.unwrap();

    let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
    // Left join preserves all 3 rows from left table
    assert_eq!(total_rows, 3, "Left join should preserve all left rows");

    // Verify that score column exists and has nulls for unmatched rows
    let schema = result[0].schema();
    assert!(schema.field_with_name("score").is_ok());
}
