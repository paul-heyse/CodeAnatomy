//! Typed parameter compilation for parametric planning (WS-P3).
//!
//! Compiles `TypedParameter` bindings into DataFusion execution:
//! - Positional placeholders (`$1`, `$2`, ...) via `ParamValues::List`
//! - Direct filter expressions via `col().eq(lit(scalar))`
//!
//! No named-map binding path (`ParamValues::Map` is not used).

use datafusion::common::{ParamValues, ScalarValue};
use datafusion::prelude::*;
use datafusion_common::{plan_err, Result};

use crate::spec::parameters::{ParameterTarget, TypedParameter};

/// Compile placeholder parameters into positional `ParamValues::List`.
///
/// Extracts all `PlaceholderPos` parameters, validates their positions form
/// a contiguous 1-based sequence with no gaps or duplicates, and returns
/// the corresponding `ParamValues::List`.
///
/// # Contract
///
/// - Positions are 1-based
/// - No gaps are allowed (1..N must all be present)
/// - Duplicate positions are rejected
///
/// # Arguments
///
/// * `params` - Slice of typed parameters to compile
///
/// # Returns
///
/// `ParamValues::List` with scalar values ordered by position
pub fn compile_positional_param_values(params: &[TypedParameter]) -> Result<ParamValues> {
    let mut positional: Vec<(u32, ScalarValue)> = Vec::new();
    for p in params {
        if let ParameterTarget::PlaceholderPos { position } = p.target {
            positional.push((position, p.value.to_scalar_value()?));
        }
    }
    if positional.is_empty() {
        return Ok(ParamValues::List(vec![]));
    }

    positional.sort_by_key(|(pos, _)| *pos);

    // Check for duplicates
    for window in positional.windows(2) {
        if window[0].0 == window[1].0 {
            return plan_err!(
                "Duplicate placeholder position ${}: typed_parameters must have unique positions",
                window[0].0
            );
        }
    }

    // Validate contiguous 1..N sequence
    for (idx, (pos, _)) in positional.iter().enumerate() {
        let expected = (idx as u32) + 1;
        if *pos != expected {
            return plan_err!(
                "Typed parameter placeholder positions must be contiguous from 1; \
                 expected ${}, got ${}",
                expected,
                pos
            );
        }
    }

    Ok(ParamValues::List(
        positional
            .into_iter()
            .map(|(_, v)| v.into())
            .collect(),
    ))
}

/// Apply typed parameters to a DataFrame using positional binding + typed filter expressions.
///
/// Two-phase application:
/// 1. Positional placeholders are bound via `df.with_param_values(ParamValues::List(...))`
/// 2. `FilterEq` parameters are applied as typed `col(filter_column).eq(lit(scalar))` filters
///
/// Returns the original DataFrame unchanged if `params` is empty.
///
/// # Arguments
///
/// * `df` - DataFrame to apply parameters to
/// * `params` - Typed parameters to apply
///
/// # Returns
///
/// DataFrame with all parameters applied
pub async fn apply_typed_parameters(
    df: DataFrame,
    params: &[TypedParameter],
) -> Result<DataFrame> {
    if params.is_empty() {
        return Ok(df);
    }

    // Phase 1: Apply positional placeholders
    let placeholder_values = compile_positional_param_values(params)?;
    let mut result = if matches!(placeholder_values, ParamValues::List(ref v) if !v.is_empty()) {
        df.with_param_values(placeholder_values)?
    } else {
        df
    };

    // Phase 2: Apply FilterEq parameters as typed filter expressions
    for param in params {
        if let ParameterTarget::FilterEq { filter_column, .. } = &param.target {
            let scalar = param.value.to_scalar_value()?;
            result = result.filter(col(filter_column).eq(lit(scalar)))?;
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::parameters::{ParameterTarget, ParameterValue, TypedParameter};
    use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::MemTable;
    use std::sync::Arc;

    fn make_placeholder(pos: u32, value: ParameterValue) -> TypedParameter {
        TypedParameter {
            label: None,
            target: ParameterTarget::PlaceholderPos { position: pos },
            value,
        }
    }

    fn make_filter_eq(column: &str, value: ParameterValue) -> TypedParameter {
        TypedParameter {
            label: None,
            target: ParameterTarget::FilterEq {
                base_table: None,
                filter_column: column.to_string(),
            },
            value,
        }
    }

    #[test]
    fn test_compile_empty_params() {
        let result = compile_positional_param_values(&[]).unwrap();
        assert!(matches!(result, ParamValues::List(v) if v.is_empty()));
    }

    #[test]
    fn test_compile_single_placeholder() {
        let params = vec![make_placeholder(1, ParameterValue::Int64(42))];
        let result = compile_positional_param_values(&params).unwrap();
        if let ParamValues::List(values) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].value, ScalarValue::Int64(Some(42)));
        } else {
            panic!("Expected ParamValues::List");
        }
    }

    #[test]
    fn test_compile_contiguous_placeholders() {
        let params = vec![
            make_placeholder(2, ParameterValue::Utf8("world".to_string())),
            make_placeholder(1, ParameterValue::Utf8("hello".to_string())),
            make_placeholder(3, ParameterValue::Int64(99)),
        ];
        let result = compile_positional_param_values(&params).unwrap();
        if let ParamValues::List(values) = result {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0].value, ScalarValue::Utf8(Some("hello".to_string())));
            assert_eq!(values[1].value, ScalarValue::Utf8(Some("world".to_string())));
            assert_eq!(values[2].value, ScalarValue::Int64(Some(99)));
        } else {
            panic!("Expected ParamValues::List");
        }
    }

    #[test]
    fn test_compile_rejects_gap() {
        let params = vec![
            make_placeholder(1, ParameterValue::Int64(1)),
            make_placeholder(3, ParameterValue::Int64(3)),
        ];
        let result = compile_positional_param_values(&params);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("contiguous"));
    }

    #[test]
    fn test_compile_rejects_duplicate() {
        let params = vec![
            make_placeholder(1, ParameterValue::Int64(1)),
            make_placeholder(1, ParameterValue::Int64(2)),
        ];
        let result = compile_positional_param_values(&params);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Duplicate"));
    }

    #[test]
    fn test_compile_rejects_zero_position() {
        let params = vec![make_placeholder(0, ParameterValue::Int64(1))];
        let result = compile_positional_param_values(&params);
        assert!(result.is_err());
    }

    #[test]
    fn test_compile_ignores_filter_eq_params() {
        let params = vec![
            make_placeholder(1, ParameterValue::Int64(42)),
            make_filter_eq("col", ParameterValue::Utf8("test".to_string())),
        ];
        let result = compile_positional_param_values(&params).unwrap();
        if let ParamValues::List(values) = result {
            assert_eq!(values.len(), 1);
        } else {
            panic!("Expected ParamValues::List");
        }
    }

    async fn setup_test_table() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("test_table", Arc::new(table)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn test_apply_empty_params() {
        let ctx = setup_test_table().await;
        let df = ctx.table("test_table").await.unwrap();
        let result = apply_typed_parameters(df, &[]).await.unwrap();
        let batches = result.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_apply_filter_eq_int64() {
        let ctx = setup_test_table().await;
        let df = ctx.table("test_table").await.unwrap();
        let params = vec![make_filter_eq("id", ParameterValue::Int64(2))];
        let result = apply_typed_parameters(df, &params).await.unwrap();
        let batches = result.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn test_apply_filter_eq_utf8() {
        let ctx = setup_test_table().await;
        let df = ctx.table("test_table").await.unwrap();
        let params = vec![make_filter_eq("name", ParameterValue::Utf8("bob".to_string()))];
        let result = apply_typed_parameters(df, &params).await.unwrap();
        let batches = result.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn test_apply_multiple_filter_eq() {
        let ctx = setup_test_table().await;
        let df = ctx.table("test_table").await.unwrap();
        let params = vec![
            make_filter_eq("id", ParameterValue::Int64(2)),
            make_filter_eq("name", ParameterValue::Utf8("bob".to_string())),
        ];
        let result = apply_typed_parameters(df, &params).await.unwrap();
        let batches = result.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn test_apply_filter_eq_no_match() {
        let ctx = setup_test_table().await;
        let df = ctx.table("test_table").await.unwrap();
        let params = vec![make_filter_eq("id", ParameterValue::Int64(999))];
        let result = apply_typed_parameters(df, &params).await.unwrap();
        let batches = result.collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }
}
