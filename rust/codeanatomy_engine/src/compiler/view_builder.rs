//! View builder for Normalize, Filter, Project, Aggregate transforms.
//!
//! Constructs DataFrames for single-source transformations.

use datafusion::prelude::*;
use datafusion_common::Result;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_functions_aggregate::expr_fn::{avg, count, max, min, sum};

use crate::spec::relations::AggregationExpr;

/// Build a normalized view with entity_id, span, and text normalization.
///
/// Adds:
/// - entity_id via stable_id UDF
/// - span via span_make UDF (if span_columns provided)
/// - normalized text via utf8_normalize UDF
pub async fn build_normalize(
    ctx: &SessionContext,
    _view_name: &str,
    source: &str,
    id_columns: &[String],
    span_columns: &Option<(String, String)>,
    text_columns: &[String],
) -> Result<DataFrame> {
    let mut df = ctx.table(source).await?;

    // Add entity_id via stable_id UDF
    let id_col_exprs: Vec<Expr> = id_columns.iter().map(|c| col(c)).collect();
    let stable_id_udf = ctx.udf("stable_id")?;
    let entity_id_expr = stable_id_udf.call(id_col_exprs);
    df = df.with_column("entity_id", entity_id_expr)?;

    // Add span if specified
    if let Some((bstart_col, bend_col)) = span_columns {
        let span_make_udf = ctx.udf("span_make")?;
        let span_expr = span_make_udf.call(vec![col(bstart_col), col(bend_col)]);
        df = df.with_column("span", span_expr)?;
    }

    // Normalize text columns
    let utf8_normalize_udf = ctx.udf("utf8_normalize")?;
    for text_col in text_columns {
        let normalized_expr = utf8_normalize_udf.call(vec![col(text_col)]);
        let normalized_name = format!("{}_normalized", text_col);
        df = df.with_column(&normalized_name, normalized_expr)?;
    }

    Ok(df)
}

/// Build a filtered view using SQL expression predicate.
pub async fn build_filter(
    ctx: &SessionContext,
    source: &str,
    predicate: &str,
) -> Result<DataFrame> {
    let df = ctx.table(source).await?;
    let schema = df.schema();

    // Parse SQL expression in context of DataFrame schema
    let filter_expr = ctx.parse_sql_expr(predicate, &schema)?;

    df.filter(filter_expr)
}

/// Build a projected view selecting specific columns.
pub async fn build_project(
    ctx: &SessionContext,
    source: &str,
    columns: &[String],
) -> Result<DataFrame> {
    let df = ctx.table(source).await?;
    let select_exprs: Vec<Expr> = columns.iter().map(|c| col(c)).collect();

    df.select(select_exprs)
}

/// Build an aggregated view with grouping and aggregation expressions.
pub async fn build_aggregate(
    ctx: &SessionContext,
    source: &str,
    group_by: &[String],
    aggregations: &[AggregationExpr],
) -> Result<DataFrame> {
    let df = ctx.table(source).await?;

    // Build group expressions
    let group_exprs: Vec<Expr> = group_by.iter().map(|c| col(c)).collect();

    // Build aggregation expressions
    let mut agg_exprs: Vec<Expr> = Vec::new();
    for agg in aggregations {
        let col_expr = col(&agg.column);
        let agg_expr = match agg.function.as_str() {
            "count" => count(col_expr),
            "sum" => sum(col_expr),
            "avg" => avg(col_expr),
            "min" => min(col_expr),
            "max" => max(col_expr),
            other => {
                return Err(datafusion_common::DataFusionError::Plan(format!(
                    "Unsupported aggregation function: {}",
                    other
                )))
            }
        };

        agg_exprs.push(agg_expr.alias(&agg.alias));
    }

    df.aggregate(group_exprs, agg_exprs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    async fn setup_test_context() -> SessionContext {
        let ctx = SessionContext::new();

        // Create test table with id, bstart, bend, text columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("bstart", DataType::Int64, false),
            Field::new("bend", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![0, 10, 20])),
                Arc::new(Int64Array::from(vec![5, 15, 25])),
                Arc::new(StringArray::from(vec!["hello", "world", "test"])),
            ],
        )
        .unwrap();

        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("test_source", Arc::new(table))
            .unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_build_project() {
        let ctx = setup_test_context().await;

        let df = build_project(&ctx, "test_source", &vec!["id".to_string(), "text".to_string()])
            .await
            .unwrap();

        let schema = df.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "text");
    }

    #[tokio::test]
    async fn test_build_filter() {
        let ctx = setup_test_context().await;

        let df = build_filter(&ctx, "test_source", "id > 1")
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // id=2 and id=3
    }

    #[tokio::test]
    async fn test_build_aggregate() {
        let ctx = setup_test_context().await;

        let aggregations = vec![
            AggregationExpr {
                column: "id".to_string(),
                function: "count".to_string(),
                alias: "count_id".to_string(),
            },
            AggregationExpr {
                column: "bstart".to_string(),
                function: "max".to_string(),
                alias: "max_bstart".to_string(),
            },
        ];

        let df = build_aggregate(&ctx, "test_source", &[], &aggregations)
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 2);
    }
}
