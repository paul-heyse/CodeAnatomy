//! Join builder with equality and inequality join strategies.
//!
//! Provides:
//! - Equality joins via DataFrame::join() → HashJoinExec
//! - Inequality joins via DataFrame::join_on() → NestedLoopJoinExec
//! - Span containment detection for optimized join strategy selection

use datafusion::prelude::*;
use datafusion_common::Result;
use datafusion_expr::JoinType as DfJoinType;

use crate::spec::relations::{JoinKeyPair, JoinType};

/// Check if join keys represent span containment pattern.
///
/// Span containment is detected when join keys match the pattern:
/// - left: bstart, bend (outer span)
/// - right: bstart, bend (inner span)
///
/// Returns true if this is a span containment join.
fn is_span_containment_keys(keys: &[JoinKeyPair]) -> bool {
    if keys.len() != 2 {
        return false;
    }

    let mut has_bstart = false;
    let mut has_bend = false;

    for key in keys {
        if (key.left_key == "bstart" && key.right_key == "bstart")
            || (key.left_key.ends_with("_bstart") && key.right_key.ends_with("_bstart"))
        {
            has_bstart = true;
        }
        if (key.left_key == "bend" && key.right_key == "bend")
            || (key.left_key.ends_with("_bend") && key.right_key.ends_with("_bend"))
        {
            has_bend = true;
        }
    }

    has_bstart && has_bend
}

/// Map spec JoinType to DataFusion JoinType.
fn map_join_type(join_type: &JoinType) -> DfJoinType {
    match join_type {
        JoinType::Inner => DfJoinType::Inner,
        JoinType::Left => DfJoinType::Left,
        JoinType::Right => DfJoinType::Right,
        JoinType::Full => DfJoinType::Full,
        JoinType::Semi => DfJoinType::LeftSemi,
        JoinType::Anti => DfJoinType::LeftAnti,
    }
}

/// Build a join using equality or inequality strategy based on join keys.
///
/// Strategy selection:
/// - If join_keys is empty → error (no cartesian products)
/// - If join_keys look like span containment → build_span_containment_join (inequality)
/// - Otherwise → build_equality_join (hash join)
pub async fn build_join(
    ctx: &SessionContext,
    left_source: &str,
    right_source: &str,
    join_type: &JoinType,
    join_keys: &[JoinKeyPair],
) -> Result<DataFrame> {
    if join_keys.is_empty() {
        return Err(datafusion_common::DataFusionError::Plan(
            "Join requires at least one join key (no cartesian products)".to_string(),
        ));
    }

    // Check for span containment pattern
    if is_span_containment_keys(join_keys) && matches!(join_type, JoinType::Inner | JoinType::Left)
    {
        build_span_containment_join(ctx, left_source, right_source, join_type).await
    } else {
        build_equality_join(ctx, left_source, right_source, join_type, join_keys).await
    }
}

/// Build an equality join using DataFrame::join() → HashJoinExec.
async fn build_equality_join(
    ctx: &SessionContext,
    left_source: &str,
    right_source: &str,
    join_type: &JoinType,
    join_keys: &[JoinKeyPair],
) -> Result<DataFrame> {
    let left = ctx.table(left_source).await?;
    let right = ctx.table(right_source).await?;

    let left_cols: Vec<&str> = join_keys.iter().map(|k| k.left_key.as_str()).collect();
    let right_cols: Vec<&str> = join_keys.iter().map(|k| k.right_key.as_str()).collect();

    let df_join_type = map_join_type(join_type);

    left.join(right, df_join_type, &left_cols, &right_cols, None)
}

/// Build a span containment join using DataFrame::join_on() → NestedLoopJoinExec.
///
/// Span containment predicate:
/// outer.bstart <= inner.bstart AND inner.bend <= outer.bend
async fn build_span_containment_join(
    ctx: &SessionContext,
    left_source: &str,
    right_source: &str,
    join_type: &JoinType,
) -> Result<DataFrame> {
    let left = ctx.table(left_source).await?;
    let right = ctx.table(right_source).await?;

    let df_join_type = map_join_type(join_type);

    // Build span containment predicate:
    // left.bstart <= right.bstart AND right.bend <= left.bend
    let predicates = vec![
        col("left.bstart").lt_eq(col("right.bstart")),
        col("right.bend").lt_eq(col("left.bend")),
    ];

    left.join_on(right, df_join_type, predicates)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    async fn setup_test_tables() -> SessionContext {
        let ctx = SessionContext::new();

        // Left table with id and bstart/bend
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("bstart", DataType::Int64, false),
            Field::new("bend", DataType::Int64, false),
        ]));

        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Int64Array::from(vec![0, 10])),
                Arc::new(Int64Array::from(vec![20, 30])),
            ],
        )
        .unwrap();

        let left_table = MemTable::try_new(left_schema, vec![vec![left_batch]]).unwrap();
        ctx.register_table("left_table", Arc::new(left_table))
            .unwrap();

        // Right table with id and bstart/bend
        let right_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("bstart", DataType::Int64, false),
            Field::new("bend", DataType::Int64, false),
        ]));

        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![5, 15])),
                Arc::new(Int64Array::from(vec![15, 25])),
            ],
        )
        .unwrap();

        let right_table = MemTable::try_new(right_schema, vec![vec![right_batch]]).unwrap();
        ctx.register_table("right_table", Arc::new(right_table))
            .unwrap();

        ctx
    }

    #[test]
    fn test_is_span_containment_keys() {
        // Positive case: exact bstart/bend match
        let keys = vec![
            JoinKeyPair {
                left_key: "bstart".to_string(),
                right_key: "bstart".to_string(),
            },
            JoinKeyPair {
                left_key: "bend".to_string(),
                right_key: "bend".to_string(),
            },
        ];
        assert!(is_span_containment_keys(&keys));

        // Positive case: prefixed bstart/bend
        let keys = vec![
            JoinKeyPair {
                left_key: "outer_bstart".to_string(),
                right_key: "inner_bstart".to_string(),
            },
            JoinKeyPair {
                left_key: "outer_bend".to_string(),
                right_key: "inner_bend".to_string(),
            },
        ];
        assert!(is_span_containment_keys(&keys));

        // Negative case: not span keys
        let keys = vec![JoinKeyPair {
            left_key: "id".to_string(),
            right_key: "id".to_string(),
        }];
        assert!(!is_span_containment_keys(&keys));

        // Negative case: only one span key
        let keys = vec![JoinKeyPair {
            left_key: "bstart".to_string(),
            right_key: "bstart".to_string(),
        }];
        assert!(!is_span_containment_keys(&keys));
    }

    #[test]
    fn test_map_join_type() {
        assert_eq!(map_join_type(&JoinType::Inner), DfJoinType::Inner);
        assert_eq!(map_join_type(&JoinType::Left), DfJoinType::Left);
        assert_eq!(map_join_type(&JoinType::Right), DfJoinType::Right);
        assert_eq!(map_join_type(&JoinType::Full), DfJoinType::Full);
        assert_eq!(map_join_type(&JoinType::Semi), DfJoinType::LeftSemi);
        assert_eq!(map_join_type(&JoinType::Anti), DfJoinType::LeftAnti);
    }

    #[tokio::test]
    async fn test_build_equality_join() {
        let ctx = setup_test_tables().await;

        let join_keys = vec![JoinKeyPair {
            left_key: "id".to_string(),
            right_key: "id".to_string(),
        }];

        let result = build_equality_join(
            &ctx,
            "left_table",
            "right_table",
            &JoinType::Inner,
            &join_keys,
        )
        .await;

        assert!(result.is_ok());
        let df = result.unwrap();
        let schema = df.schema();

        // Should have columns from both sides
        assert!(schema.fields().len() >= 3);
    }

    #[tokio::test]
    async fn test_build_join_with_empty_keys_fails() {
        let ctx = setup_test_tables().await;

        let result = build_join(&ctx, "left_table", "right_table", &JoinType::Inner, &[]).await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("no cartesian products"));
    }

    #[tokio::test]
    async fn test_build_join_selects_equality_strategy() {
        let ctx = setup_test_tables().await;

        let join_keys = vec![JoinKeyPair {
            left_key: "id".to_string(),
            right_key: "id".to_string(),
        }];

        let result = build_join(&ctx, "left_table", "right_table", &JoinType::Inner, &join_keys)
            .await;

        assert!(result.is_ok());
    }
}
