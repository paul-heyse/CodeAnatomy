//! Schema-drift-safe union builder using DataFrame::union_by_name.
//!
//! Provides column-name-aligned unions with optional deduplication and discriminator columns.

use datafusion::prelude::*;
use datafusion_common::Result;

/// Build a union from multiple source DataFrames.
///
/// Features:
/// - Uses DataFrame::union_by_name() for schema drift safety
/// - Supports union_by_name_distinct() for deduplication
/// - Adds optional discriminator column to track source provenance
/// - Reorders projected columns deterministically
///
/// Returns error if sources is empty.
pub async fn build_union(
    ctx: &SessionContext,
    sources: &[String],
    discriminator_column: &Option<String>,
    distinct: bool,
) -> Result<DataFrame> {
    if sources.is_empty() {
        return Err(datafusion_common::DataFusionError::Plan(
            "Union requires at least one source".to_string(),
        ));
    }

    // Build union by accumulating sources
    let mut union_df = None;

    for source in sources {
        let mut df = ctx.table(source).await?;

        // Add discriminator column if specified
        if let Some(disc_col) = discriminator_column {
            let source_literal = lit(source.clone());
            df = df.with_column(disc_col, source_literal)?;
        }

        // Accumulate union
        union_df = match union_df {
            None => Some(df),
            Some(acc) => {
                if distinct {
                    Some(acc.union_by_name_distinct(df)?)
                } else {
                    Some(acc.union_by_name(df)?)
                }
            }
        };
    }

    let Some(mut combined) = union_df else {
        return Err(datafusion_common::DataFusionError::Plan(
            "Union requires at least one source".to_string(),
        ));
    };

    let mut ordered_columns: Vec<String> = combined
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    ordered_columns.sort();
    if !ordered_columns.is_empty() {
        combined = combined.select(ordered_columns.iter().map(col).collect::<Vec<_>>())?;
    }

    Ok(combined)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    async fn setup_test_tables() -> SessionContext {
        let ctx = SessionContext::new();

        // Table 1
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap();

        let table1 = MemTable::try_new(schema1, vec![vec![batch1]]).unwrap();
        ctx.register_table("table1", Arc::new(table1)).unwrap();

        // Table 2 (same schema)
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["charlie", "diana"])),
            ],
        )
        .unwrap();

        let table2 = MemTable::try_new(schema2, vec![vec![batch2]]).unwrap();
        ctx.register_table("table2", Arc::new(table2)).unwrap();

        // Table 3 (superset schema - drift case)
        let schema3 = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("extra", DataType::Utf8, true),
        ]));

        let batch3 = RecordBatch::try_new(
            schema3.clone(),
            vec![
                Arc::new(Int64Array::from(vec![5])),
                Arc::new(StringArray::from(vec!["eve"])),
                Arc::new(StringArray::from(vec!["extra_data"])),
            ],
        )
        .unwrap();

        let table3 = MemTable::try_new(schema3, vec![vec![batch3]]).unwrap();
        ctx.register_table("table3", Arc::new(table3)).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_build_union_empty_sources_fails() {
        let ctx = SessionContext::new();

        let result = build_union(&ctx, &[], &None, false).await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("at least one source"));
    }

    #[tokio::test]
    async fn test_build_union_single_source() {
        let ctx = setup_test_tables().await;

        let sources = vec!["table1".to_string()];
        let df = build_union(&ctx, &sources, &None, false).await.unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn test_build_union_multiple_sources() {
        let ctx = setup_test_tables().await;

        let sources = vec!["table1".to_string(), "table2".to_string()];
        let df = build_union(&ctx, &sources, &None, false).await.unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4); // 2 from table1 + 2 from table2
    }

    #[tokio::test]
    async fn test_build_union_with_discriminator() {
        let ctx = setup_test_tables().await;

        let sources = vec!["table1".to_string(), "table2".to_string()];
        let disc_col = Some("source".to_string());
        let df = build_union(&ctx, &sources, &disc_col, false)
            .await
            .unwrap();

        let schema = df.schema();
        // Should have original columns + discriminator
        assert!(schema.field_with_name(None, "source").is_ok());

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[tokio::test]
    async fn test_build_union_distinct() {
        let ctx = setup_test_tables().await;

        // Create a table with duplicate data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch_dup = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])), // Same as table1
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap();

        let table_dup = MemTable::try_new(schema, vec![vec![batch_dup]]).unwrap();
        ctx.register_table("table_dup", Arc::new(table_dup))
            .unwrap();

        let sources = vec!["table1".to_string(), "table_dup".to_string()];
        let df = build_union(&ctx, &sources, &None, true).await.unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // Should be 2 (deduplicated), not 4
        assert_eq!(total_rows, 2);
    }
}
