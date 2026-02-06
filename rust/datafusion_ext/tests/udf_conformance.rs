use std::collections::HashMap;
use std::sync::Arc;
#[cfg(feature = "async-udf")]
use std::sync::{Mutex, OnceLock};

use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, Int64Array, LargeStringArray, ListArray,
    ListBuilder, MapBuilder, StringArray, StringBuilder, StringViewArray, StructArray, UInt64Array,
    UInt8Array,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use datafusion::datasource::MemTable;
use datafusion::optimizer::OptimizerConfig;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{lit, Expr};
use datafusion_expr_common::sort_properties::{ExprProperties, SortProperties};
use tokio::runtime::Runtime;

use datafusion_ext::{
    install_expr_planners_native, install_sql_macro_factory_native, registry_snapshot, udf_expr,
    udf_registry,
};

fn hash64_value(value: &str) -> i64 {
    let mut hasher = Blake2bVar::new(8).expect("blake2b supports 8-byte output");
    hasher.update(value.as_bytes());
    let mut out = [0u8; 8];
    hasher.finalize_variable(&mut out).expect("hash output");
    let unsigned = u64::from_be_bytes(out);
    let masked = unsigned & ((1_u64 << 63) - 1);
    masked as i64
}

fn hash128_value(value: &str) -> String {
    let mut hasher = Blake2bVar::new(16).expect("blake2b supports 16-byte output");
    hasher.update(value.as_bytes());
    let mut out = [0u8; 16];
    hasher.finalize_variable(&mut out).expect("hash output");
    hex::encode(out)
}

fn run_query(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>> {
    let runtime = Runtime::new().expect("tokio runtime");
    let df = runtime.block_on(ctx.sql(sql))?;
    runtime.block_on(df.collect())
}

#[cfg(feature = "async-udf")]
fn async_policy_test_guard() -> std::sync::MutexGuard<'static, ()> {
    static ASYNC_POLICY_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let lock = ASYNC_POLICY_TEST_LOCK.get_or_init(|| Mutex::new(()));
    lock.lock().expect("async policy test lock poisoned")
}

fn normalize_type_name(value: &str) -> String {
    let text = value.trim().to_ascii_lowercase();
    if text.starts_with("list(") || text.starts_with("list<") {
        let element = if text.contains("utf8") || text.contains("string") {
            "string"
        } else if text.contains("int64") || text.contains("bigint") {
            "int64"
        } else if text.contains("int32") || text.contains("integer") {
            "int32"
        } else if text.contains("int16") || text.contains("smallint") {
            "int16"
        } else if text.contains("int8") || text.contains("tinyint") {
            "int8"
        } else if text.contains("uint64") {
            "uint64"
        } else if text.contains("uint32") {
            "uint32"
        } else if text.contains("uint16") {
            "uint16"
        } else if text.contains("uint8") {
            "uint8"
        } else if text.contains("float64") || text.contains("double") {
            "float64"
        } else if text.contains("float32") || text.contains("float") || text.contains("real") {
            "float32"
        } else if text.contains("bool") {
            "bool"
        } else if text.contains("decimal") {
            return text;
        } else {
            "null"
        };
        return format!("list<{element}>");
    }
    if text.contains("utf8") {
        return "string".to_string();
    }
    if text.contains("int64") || text.contains("bigint") {
        return "int64".to_string();
    }
    if text.contains("int32") || text.contains("integer") {
        return "int32".to_string();
    }
    if text.contains("int16") || text.contains("smallint") {
        return "int16".to_string();
    }
    if text.contains("int8") || text.contains("tinyint") {
        return "int8".to_string();
    }
    if text.contains("uint64") {
        return "uint64".to_string();
    }
    if text.contains("uint32") {
        return "uint32".to_string();
    }
    if text.contains("uint16") {
        return "uint16".to_string();
    }
    if text.contains("uint8") {
        return "uint8".to_string();
    }
    if text.contains("float64") || text.contains("double") {
        return "float64".to_string();
    }
    if text.contains("float32") || text.contains("float") || text.contains("real") {
        return "float32".to_string();
    }
    if text.contains("bool") {
        return "bool".to_string();
    }
    if text.contains("decimal") {
        return text;
    }
    text
}

#[test]
fn stable_hash64_matches_expected() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(&ctx, "SELECT stable_hash64('alpha') AS value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), hash64_value("alpha"));
    Ok(())
}

#[test]
fn stable_hash64_named_args_match_expected() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(&ctx, "SELECT stable_hash64(value => 'alpha') AS value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), hash64_value("alpha"));
    Ok(())
}

#[test]
fn stable_hash64_coerce_types() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let state = ctx.state();
    let udf = state
        .scalar_functions()
        .get("stable_hash64")
        .expect("stable_hash64 udf");
    let coerced = udf.inner().coerce_types(&[DataType::Utf8])?;
    assert_eq!(coerced, vec![DataType::Utf8]);
    assert!(udf.inner().coerce_types(&[]).is_err());
    assert!(udf
        .inner()
        .coerce_types(&[DataType::Utf8, DataType::Utf8])
        .is_err());
    Ok(())
}

#[test]
fn stable_hash128_named_args_match_expected() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(&ctx, "SELECT stable_hash128(value => 'alpha') AS value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    assert_eq!(array.value(0), hash128_value("alpha"));
    Ok(())
}

#[test]
fn scalar_udfs_preserve_row_count() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let array =
        Arc::new(StringArray::from(vec![Some("alpha"), None, Some("beta")])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;
    let batches = run_query(&ctx, "SELECT stable_hash64(value) AS value FROM t")?;
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 3);
    Ok(())
}

#[test]
fn scalar_udf_snapshot_metadata_complete() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let snapshot = registry_snapshot::registry_snapshot(&ctx.state());
    for name in snapshot.scalar.iter() {
        assert!(
            snapshot.signature_inputs.contains_key(name),
            "missing signature_inputs for {name}"
        );
        assert!(
            snapshot.return_types.contains_key(name),
            "missing return_types for {name}"
        );
    }
    Ok(())
}

#[test]
fn information_schema_routines_match_snapshot() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    udf_registry::register_all(&ctx)?;
    let snapshot = registry_snapshot::registry_snapshot(&ctx.state());
    let custom_names: std::collections::HashSet<String> =
        snapshot.custom_udfs.into_iter().collect();

    let batches = run_query(
        &ctx,
        "SELECT routine_name, function_type, data_type, is_deterministic FROM information_schema.routines",
    )?;
    let batch = &batches[0];
    let routine_names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("routine_name");
    let function_types = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("function_type");
    let data_types = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("data_type");
    let deterministic = batch
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("is_deterministic");

    let mut routine_map: HashMap<String, Vec<(String, Option<String>, bool)>> = HashMap::new();
    for row in 0..batch.num_rows() {
        if routine_names.is_null(row) || function_types.is_null(row) {
            continue;
        }
        let name = routine_names.value(row).to_string();
        let function_type = function_types.value(row).to_string();
        let data_type = if data_types.is_null(row) {
            None
        } else {
            Some(normalize_type_name(data_types.value(row)))
        };
        let is_det = deterministic.value(row);
        routine_map
            .entry(name)
            .or_default()
            .push((function_type, data_type, is_det));
    }

    let check_kind = |names: Vec<String>, expected_kind: &str| {
        for name in names {
            if !custom_names.contains(&name) {
                continue;
            }
            let Some(rows) = routine_map.get(&name) else {
                panic!("missing information_schema.routines entry for {name}");
            };
            let expected_returns = snapshot
                .return_types
                .get(&name)
                .map(|types| {
                    types
                        .iter()
                        .map(|value| normalize_type_name(value))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let has_any_signature = snapshot
                .signature_inputs
                .get(&name)
                .map(|rows| {
                    if expected_kind == "WINDOW" && rows.iter().any(|row| row.is_empty()) {
                        return true;
                    }
                    rows.iter()
                        .any(|row| row.iter().any(|entry| entry == "null"))
                })
                .unwrap_or(false);
            let expected_det = snapshot
                .volatility
                .get(&name)
                .map(|value| value == "immutable")
                .unwrap_or(false);
            let matched = rows.iter().any(|(kind, dtype, det)| {
                if kind != expected_kind || *det != expected_det {
                    return false;
                }
                match dtype {
                    Some(value) => expected_returns.contains(value),
                    None => has_any_signature,
                }
            });
            assert!(matched, "information_schema.routines mismatch for {name}");
        }
    };

    check_kind(snapshot.scalar, "SCALAR");
    check_kind(snapshot.aggregate, "AGGREGATE");
    check_kind(snapshot.window, "WINDOW");
    Ok(())
}

#[test]
fn map_get_default_reports_short_circuit() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let state = ctx.state();
    let udf = state
        .scalar_functions()
        .get("map_get_default")
        .expect("map_get_default udf");
    assert!(udf.short_circuits());
    Ok(())
}

#[test]
fn registry_snapshot_reports_short_circuits() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let snapshot = registry_snapshot::registry_snapshot(&ctx.state());
    assert_eq!(snapshot.short_circuits.get("map_get_default"), Some(&true));
    Ok(())
}

#[test]
fn simplify_span_len_constant_folds() -> Result<()> {
    let udf = datafusion_ext::udf::span_len_udf();
    let fields = Fields::from(vec![
        Field::new("bstart", DataType::Int64, true),
        Field::new("bend", DataType::Int64, true),
        Field::new("line_base", DataType::Int32, true),
        Field::new("col_unit", DataType::Utf8, true),
        Field::new("end_exclusive", DataType::Boolean, true),
    ]);
    let span = StructArray::new(
        fields,
        vec![
            Arc::new(Int64Array::from(vec![Some(10)])),
            Arc::new(Int64Array::from(vec![Some(25)])),
            Arc::new(Int32Array::from(vec![Some(0)])),
            Arc::new(StringArray::from(vec![Some("byte")])),
            Arc::new(BooleanArray::from(vec![Some(true)])),
        ],
        None,
    );
    let expr = lit(ScalarValue::Struct(Arc::new(span)));
    let props = ExecutionProps::new();
    let info = SimplifyContext::new(&props);
    let result = udf.inner().simplify(vec![expr], &info)?;
    let ExprSimplifyResult::Simplified(expr) = result else {
        panic!("span_len did not simplify");
    };
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => {
            assert_eq!(value, 15);
        }
        other => panic!("unexpected simplified expr: {other:?}"),
    }
    Ok(())
}

#[test]
fn simplify_list_unique_sorted_constant_folds() -> Result<()> {
    let udf = datafusion_ext::udf::list_unique_sorted_udf();
    let mut builder = ListBuilder::new(StringBuilder::new());
    builder.values().append_value("b");
    builder.values().append_value("a");
    builder.values().append_value("b");
    builder.append(true);
    let list_array = builder.finish();
    let expr = lit(ScalarValue::List(Arc::new(list_array)));
    let props = ExecutionProps::new();
    let info = SimplifyContext::new(&props);
    let result = udf.inner().simplify(vec![expr], &info)?;
    let ExprSimplifyResult::Simplified(expr) = result else {
        panic!("list_unique_sorted did not simplify");
    };
    let Expr::Literal(ScalarValue::List(array), _) = expr else {
        panic!("unexpected simplified expr");
    };
    let list_values = array.value(0);
    let values = list_values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("list values");
    let collected: Vec<_> = values.iter().map(|v| v.unwrap()).collect();
    assert_eq!(collected, vec!["a", "b"]);
    Ok(())
}

#[test]
fn simplify_map_get_default_constant_folds() -> Result<()> {
    let udf = datafusion_ext::udf::map_get_default_udf();
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    builder.keys().append_value("alpha");
    builder.values().append_value("bravo");
    builder.append(true)?;
    let map_array = builder.finish();
    let map_expr = lit(ScalarValue::Map(Arc::new(map_array)));
    let key_expr = lit(ScalarValue::Utf8(Some("alpha".to_string())));
    let default_expr = lit(ScalarValue::Utf8(Some("fallback".to_string())));
    let props = ExecutionProps::new();
    let info = SimplifyContext::new(&props);
    let result = udf
        .inner()
        .simplify(vec![map_expr, key_expr, default_expr], &info)?;
    let ExprSimplifyResult::Simplified(expr) = result else {
        panic!("map_get_default did not simplify");
    };
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _) => {
            assert_eq!(value, "bravo");
        }
        other => panic!("unexpected simplified expr: {other:?}"),
    }
    Ok(())
}

#[test]
fn identity_udfs_preserve_ordering() -> Result<()> {
    #[cfg(feature = "async-udf")]
    let _guard = async_policy_test_guard();
    let ctx = SessionContext::new();
    let enable_async = cfg!(feature = "async-udf");
    udf_registry::register_all_with_policy(&ctx, enable_async, Some(1000), Some(64))?;
    let ordered = SortProperties::Ordered(arrow::compute::SortOptions {
        descending: false,
        nulls_first: true,
    });
    let props = ExprProperties::new_unknown()
        .with_order(ordered)
        .with_preserves_lex_ordering(true);

    let state = ctx.state();
    let cpg = state
        .scalar_functions()
        .get("cpg_score")
        .expect("cpg_score udf");
    assert!(cpg.preserves_lex_ordering(&[props.clone()])?);
    assert_eq!(cpg.output_ordering(&[props.clone()])?, ordered);

    Ok(())
}

#[cfg(feature = "async-udf")]
#[test]
fn async_echo_uses_configured_batch_size() -> Result<()> {
    let _guard = async_policy_test_guard();
    let ctx = SessionContext::new();
    udf_registry::register_all_with_policy(&ctx, true, Some(1000), Some(128))?;
    let state = ctx.state();
    let udf = state
        .scalar_functions()
        .get("async_echo")
        .expect("async_echo udf");
    let async_udf = udf.as_async().expect("async udf");
    assert_eq!(async_udf.ideal_batch_size(), Some(128));
    Ok(())
}

#[cfg(feature = "async-udf")]
#[test]
fn async_echo_uses_session_batch_size_when_policy_unset() -> Result<()> {
    let _guard = async_policy_test_guard();
    let config = SessionConfig::new().with_batch_size(64);
    let ctx = SessionContext::new_with_config(config);
    udf_registry::register_all_with_policy(&ctx, true, Some(1000), None)?;
    let state = ctx.state();
    let udf = state
        .scalar_functions()
        .get("async_echo")
        .expect("async_echo udf");
    let async_udf = udf.as_async().expect("async udf");
    assert_eq!(async_udf.ideal_batch_size(), Some(64));
    Ok(())
}

#[cfg(feature = "async-udf")]
#[test]
fn async_echo_handles_chunked_batches() -> Result<()> {
    let _guard = async_policy_test_guard();
    let ctx = SessionContext::new();
    udf_registry::register_all_with_policy(&ctx, true, Some(250), Some(3))?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    // Use an exact multiple of the configured async chunk size to avoid
    // DataFusion's mixed-length async chunk aggregation edge case.
    let values: Vec<Option<String>> = (0..9).map(|index| Some(format!("v{index}"))).collect();
    let array = Arc::new(StringArray::from(values)) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;
    let batches = run_query(&ctx, "SELECT async_echo(value) AS value FROM t")?;
    let mut collected: Vec<String> = Vec::new();
    for batch in batches {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column");
        for value in array.iter() {
            collected.push(value.expect("non-null").to_string());
        }
    }
    assert_eq!(collected.len(), 9);
    let mut expected: Vec<String> = (0..9).map(|index| format!("v{index}")).collect();
    collected.sort();
    expected.sort();
    assert_eq!(collected, expected);
    Ok(())
}

#[test]
fn information_schema_parameters_match_snapshot() -> Result<()> {
    let config = SessionConfig::new().with_information_schema(true);
    let ctx = SessionContext::new_with_config(config);
    udf_registry::register_all(&ctx)?;
    let snapshot = registry_snapshot::registry_snapshot(&ctx.state());
    let custom_names: std::collections::HashSet<String> =
        snapshot.custom_udfs.into_iter().collect();

    let batches = run_query(
        &ctx,
        "SELECT specific_name, parameter_mode, ordinal_position, parameter_name, data_type, rid FROM information_schema.parameters",
    )?;
    let batch = &batches[0];
    let names = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("specific_name");
    let modes = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("parameter_mode");
    let ordinals = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("ordinal_position");
    let param_names = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("parameter_name");
    let data_types = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("data_type");
    let rids = batch
        .column(5)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("rid");

    let mut param_map: HashMap<String, HashMap<u8, Vec<(u64, Option<String>, String)>>> =
        HashMap::new();
    for row in 0..batch.num_rows() {
        if names.is_null(row) || modes.is_null(row) || data_types.is_null(row) {
            continue;
        }
        if modes.value(row) != "IN" {
            continue;
        }
        let name = names.value(row).to_string();
        let dtype = normalize_type_name(data_types.value(row));
        let ordinal = ordinals.value(row);
        let param_name = if param_names.is_null(row) {
            None
        } else {
            Some(param_names.value(row).to_string())
        };
        let rid = rids.value(row);
        param_map
            .entry(name)
            .or_default()
            .entry(rid)
            .or_default()
            .push((ordinal, param_name, dtype));
    }

    for name in snapshot
        .scalar
        .into_iter()
        .chain(snapshot.aggregate)
        .chain(snapshot.window)
    {
        if !custom_names.contains(&name) {
            continue;
        }
        let expected_inputs = snapshot
            .signature_inputs
            .get(&name)
            .cloned()
            .unwrap_or_default();
        let expected_param_names = snapshot.parameter_names.get(&name).cloned();
        let has_any_signature = expected_inputs
            .iter()
            .any(|row| row.iter().any(|entry| entry == "null"));
        let has_only_nullary =
            !expected_inputs.is_empty() && expected_inputs.iter().all(|row| row.is_empty());
        if expected_inputs.is_empty() || has_any_signature {
            continue;
        }
        if has_only_nullary {
            continue;
        }
        let Some(signature_rows) = param_map.get(&name) else {
            panic!("missing information_schema.parameters entries for {name}");
        };

        let mut matched = false;
        for params in signature_rows.values() {
            let mut params_sorted = params.clone();
            params_sorted.sort_by_key(|(ordinal, _, _)| *ordinal);
            let types = params_sorted
                .iter()
                .map(|(_, _, dtype)| dtype.clone())
                .collect::<Vec<_>>();
            if !expected_inputs.is_empty() && !expected_inputs.contains(&types) {
                continue;
            }
            if let Some(expected_names) = expected_param_names.as_ref() {
                let names = params_sorted
                    .iter()
                    .map(|(_, name, _)| name.clone().unwrap_or_default())
                    .collect::<Vec<_>>();
                if &names != expected_names {
                    continue;
                }
            }
            matched = true;
            break;
        }
        assert!(matched, "information_schema.parameters mismatch for {name}");
    }
    Ok(())
}

#[test]
fn stable_hash128_matches_expected() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(&ctx, "SELECT stable_hash128('beta') AS value")?;
    let column_index = if batches[0].num_columns() > 1 { 1 } else { 0 };
    let array = batches[0]
        .column(column_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    assert_eq!(array.value(0), hash128_value("beta"));
    Ok(())
}

#[test]
fn prefixed_hash64_matches_expected() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(&ctx, "SELECT prefixed_hash64('ns', 'gamma') AS value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    let expected = format!("ns:{}", hash64_value("gamma"));
    assert_eq!(array.value(0), expected);
    Ok(())
}

#[test]
fn stable_id_matches_expected() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(&ctx, "SELECT stable_id('ns', 'delta') AS value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    let expected = format!("ns:{}", hash128_value("delta"));
    assert_eq!(array.value(0), expected);
    Ok(())
}

#[test]
fn col_to_byte_returns_expected() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(&ctx, "SELECT col_to_byte('abcdef', 3, 'BYTE') AS value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), 3);
    Ok(())
}

#[test]
fn arrow_metadata_extracts_key() -> Result<()> {
    let mut metadata = HashMap::new();
    metadata.insert("line_base".to_string(), "10".to_string());
    let field = Field::new("code", DataType::Utf8, true).with_metadata(metadata);
    let schema = Arc::new(Schema::new(vec![field]));
    let array = Arc::new(StringArray::from(vec!["ok"])) as Arc<dyn arrow::array::Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    ctx.register_table("t", Arc::new(table))?;
    let batches = run_query(
        &ctx,
        "SELECT arrow_metadata(code, 'line_base') AS value FROM t",
    )?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    assert_eq!(array.value(0), "10");
    Ok(())
}

#[test]
fn range_generates_series() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(&ctx, "SELECT value FROM range(1, 4) ORDER BY value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    let values: Vec<i64> = array.iter().flatten().collect();
    assert_eq!(values, vec![1, 2, 3]);
    Ok(())
}

#[test]
fn udf_docs_exposes_stable_hash64() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(
        &ctx,
        "SELECT name FROM udf_docs() WHERE name = 'stable_hash64'",
    )?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    assert_eq!(array.value(0), "stable_hash64");
    Ok(())
}

#[test]
fn external_delta_udtfs_are_registered() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;

    let err = run_query(&ctx, "SELECT * FROM read_delta_cdf(CAST(1 AS BIGINT))")
        .expect_err("read_delta_cdf should be registered and validate literals");
    assert!(
        err.to_string()
            .contains("read_delta_cdf expects argument 1 to be a string literal"),
        "unexpected error: {err}"
    );

    let err = run_query(&ctx, "SELECT * FROM delta_add_actions(CAST(1 AS BIGINT))")
        .expect_err("delta_add_actions should be registered and validate literals");
    assert!(
        err.to_string()
            .contains("delta_add_actions expects argument 1 to be a string literal"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[test]
fn external_delta_udtfs_require_literal_string_args() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;

    let err =
        run_query(&ctx, "SELECT * FROM read_delta()").expect_err("argument count should fail");
    assert!(
        err.to_string().contains("read_delta expects 1 arguments"),
        "unexpected error: {err}"
    );

    let err = run_query(&ctx, "SELECT * FROM delta_snapshot(CAST(1 AS BIGINT))")
        .expect_err("non-string literal should fail");
    assert!(
        err.to_string()
            .contains("delta_snapshot expects argument 1 to be a string literal"),
        "unexpected error: {err}"
    );
    Ok(())
}

#[test]
fn stable_hash64_accepts_large_utf8() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::LargeUtf8,
        true,
    )]));
    let array = Arc::new(LargeStringArray::from(vec!["alpha"])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;
    let batches = run_query(&ctx, "SELECT stable_hash64(value) AS value FROM t")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), hash64_value("alpha"));
    Ok(())
}

#[test]
fn stable_hash64_accepts_utf8_view() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8View,
        true,
    )]));
    let array = Arc::new(StringViewArray::from(vec!["alpha"])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;
    let batches = run_query(&ctx, "SELECT stable_hash64(value) AS value FROM t")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), hash64_value("alpha"));
    Ok(())
}

#[test]
fn sql_macro_function_executes() -> Result<()> {
    let ctx = SessionContext::new();
    install_sql_macro_factory_native(&ctx)?;
    run_query(
        &ctx,
        "CREATE FUNCTION add_one(x INT) RETURNS INT RETURN $1 + CAST(1 AS INT)",
    )?;
    let batches = run_query(&ctx, "SELECT add_one(CAST(41 AS INT)) AS value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int32 column");
    assert_eq!(array.value(0), 42);
    Ok(())
}

#[test]
fn sql_macro_function_drop_removes_function() -> Result<()> {
    let ctx = SessionContext::new();
    install_sql_macro_factory_native(&ctx)?;
    run_query(
        &ctx,
        "CREATE FUNCTION add_two(x INT) RETURNS INT RETURN $1 + 2",
    )?;
    run_query(&ctx, "DROP FUNCTION add_two")?;
    let result = run_query(&ctx, "SELECT add_two(1) AS value");
    assert!(result.is_err());
    Ok(())
}

#[test]
fn sql_macro_aggregate_alias_respects_drop() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    install_sql_macro_factory_native(&ctx)?;
    run_query(
        &ctx,
        "CREATE FUNCTION count_distinct_alias(value STRING) RETURNS BIGINT RETURN count_distinct_agg($1)",
    )?;
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let array =
        Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("a")])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;
    let batches = run_query(&ctx, "SELECT count_distinct_alias(value) AS value FROM t")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), 2);
    run_query(&ctx, "DROP FUNCTION count_distinct_alias")?;
    let batches = run_query(&ctx, "SELECT count_distinct_agg(value) AS value FROM t")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), 2);
    Ok(())
}

#[test]
fn sql_macro_window_alias_executes() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    install_sql_macro_factory_native(&ctx)?;
    run_query(
        &ctx,
        "CREATE FUNCTION row_number_alias() LANGUAGE window RETURN 'row_number_window'",
    )?;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let array = Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;
    let batches = run_query(
        &ctx,
        "SELECT row_number_alias() OVER (ORDER BY id) AS value FROM t ORDER BY id",
    )?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("uint64 column");
    let values: Vec<u64> = array.iter().flatten().collect();
    assert_eq!(values, vec![1, 2, 3]);
    Ok(())
}

#[test]
fn arrow_operator_rewrites_to_get_field() -> Result<()> {
    let ctx = SessionContext::new();
    install_expr_planners_native(&ctx, &["codeanatomy_domain"])?;
    let struct_fields = Fields::from(vec![Field::new("name", DataType::Utf8, true)]);
    let struct_array = StructArray::try_new(
        struct_fields.clone(),
        vec![Arc::new(StringArray::from(vec!["alpha"])) as Arc<dyn Array>],
        None,
    )?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Struct(struct_fields),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array)])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;
    let batches = run_query(&ctx, "EXPLAIN SELECT payload->'name' AS name FROM t")?;
    let batch = &batches[0];
    let schema = batch.schema();
    let plan_index =
        schema.index_of("plan").ok().unwrap_or_else(
            || {
                if schema.fields().len() > 1 {
                    1
                } else {
                    0
                }
            },
        );
    let array = batch
        .column(plan_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    let plan_text = array.iter().flatten().collect::<Vec<&str>>().join("\n");
    assert!(
        plan_text.contains("get_field"),
        "expected get_field in plan, got: {plan_text}"
    );
    Ok(())
}

#[test]
fn list_unique_null_treatment_respects_clause() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let array = Arc::new(StringArray::from(vec![Some("a"), None, Some("a")])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;

    let batches = run_query(
        &ctx,
        "SELECT list_unique(value) IGNORE NULLS AS value FROM t",
    )?;
    let list_array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("list column");
    let values = list_array.value(0);
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string list");
    let collected: Vec<Option<&str>> = values.iter().collect();
    assert_eq!(collected, vec![Some("a")]);

    let batches = run_query(
        &ctx,
        "SELECT list_unique(value) RESPECT NULLS AS value FROM t",
    )?;
    let list_array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("list column");
    let values = list_array.value(0);
    let values = values
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string list");
    assert_eq!(values.len(), 2);
    assert_eq!(values.value(0), "a");
    assert!(values.is_null(1));
    Ok(())
}

#[test]
fn list_unique_sliding_window_retracts() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let ts_values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
    let values =
        Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("c")])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![ts_values, values])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;

    let batches = run_query(
        &ctx,
        "SELECT ts, list_unique(value) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS value FROM t ORDER BY ts",
    )?;
    let list_array = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("list column");
    let expected = vec![
        vec![Some("a")],
        vec![Some("a"), Some("b")],
        vec![Some("b"), Some("c")],
    ];
    for (row, expected_values) in expected.iter().enumerate() {
        let values = list_array.value(row);
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string list");
        let collected: Vec<Option<&str>> = values.iter().collect();
        assert_eq!(&collected, expected_values);
    }
    Ok(())
}

#[test]
fn count_distinct_sliding_window_retracts() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let ts_values = Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
    let values =
        Arc::new(StringArray::from(vec![Some("a"), Some("b"), Some("b")])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![ts_values, values])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;

    let batches = run_query(
        &ctx,
        "SELECT ts, count_distinct_agg(value) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS value FROM t ORDER BY ts",
    )?;
    let array = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    let expected = [1_i64, 2, 1];
    for (row, expected_value) in expected.iter().enumerate() {
        assert_eq!(array.value(row), *expected_value);
    }
    Ok(())
}

#[test]
fn list_unique_window_plan_contains_window_agg() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("ts", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let ts_values = Arc::new(Int32Array::from(vec![1, 2])) as Arc<dyn Array>;
    let values = Arc::new(StringArray::from(vec![Some("a"), Some("b")])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![ts_values, values])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;
    let batches = run_query(
        &ctx,
        "EXPLAIN SELECT list_unique(value) OVER (ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS value FROM t",
    )?;
    let plan_column = if batches[0].num_columns() > 1 { 1 } else { 0 };
    let plan = batches[0]
        .column(plan_column)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    let plan_text = plan.iter().flatten().collect::<Vec<&str>>().join("\n");
    assert!(
        plan_text.contains("WindowAgg"),
        "expected WindowAgg in plan, got: {plan_text}"
    );
    Ok(())
}

#[cfg(feature = "async-udf")]
#[test]
fn async_echo_executes_when_enabled() -> Result<()> {
    let _guard = async_policy_test_guard();
    let ctx = SessionContext::new();
    udf_registry::register_all_with_policy(&ctx, true, Some(250), Some(128))?;
    let batches = run_query(&ctx, "SELECT async_echo('hello') AS value")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    assert_eq!(array.value(0), "hello");
    Ok(())
}

#[test]
fn count_distinct_null_treatment_respects_clause() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let array = Arc::new(StringArray::from(vec![Some("a"), None, Some("a")])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![array])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;

    let batches = run_query(
        &ctx,
        "SELECT count_distinct_agg(value) IGNORE NULLS AS value FROM t",
    )?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), 1);

    let batches = run_query(
        &ctx,
        "SELECT count_distinct_agg(value) RESPECT NULLS AS value FROM t",
    )?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), 2);
    Ok(())
}

#[test]
fn count_distinct_default_value_on_empty() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let table = MemTable::try_new(schema, vec![vec![]])?;
    ctx.register_table("t", Arc::new(table))?;
    let batches = run_query(&ctx, "SELECT count_distinct_agg(value) AS value FROM t")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), 0);
    Ok(())
}

#[test]
fn count_distinct_window_uses_sliding_accumulator() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let ids = Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as Arc<dyn Array>;
    let values = Arc::new(StringArray::from(vec![
        Some("a"),
        Some("b"),
        Some("a"),
        None,
    ])) as Arc<dyn Array>;
    let batch = RecordBatch::try_new(schema.clone(), vec![ids, values])?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table("t", Arc::new(table))?;

    let batches = run_query(
        &ctx,
        "SELECT count_distinct_agg(value) IGNORE NULLS OVER (
            ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS value FROM t ORDER BY id",
    )?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    let values: Vec<i64> = array.iter().flatten().collect();
    assert_eq!(values, vec![1, 2, 2, 1]);
    Ok(())
}

#[test]
fn udf_expr_registry_first_resolves_builtin_registry() -> Result<()> {
    let ctx = SessionContext::new();
    let state = ctx.state();
    let registry = state
        .function_registry()
        .expect("function registry available");
    let expr = udf_expr::expr_from_registry_or_specs(registry, "sqrt", vec![lit(4_f64)], None)?;
    assert!(format!("{expr}").to_lowercase().contains("sqrt"));
    assert!(udf_expr::expr_from_name("sqrt", vec![lit(4_f64)], None).is_err());
    Ok(())
}

#[test]
fn interval_align_score_matches_span_len() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(&ctx, "SELECT interval_align_score(0, 5, 10, 20) AS score")?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("float64 column");
    assert_eq!(array.value(0), -10.0);
    Ok(())
}

#[test]
fn asof_select_returns_latest_value() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(
        &ctx,
        "SELECT asof_select(value, order_key) AS value FROM (VALUES \
         ('a', 1), ('b', 3), ('c', 2)) t(value, order_key)",
    )?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string column");
    assert_eq!(array.value(0), "b");
    Ok(())
}

#[test]
fn dedupe_best_by_score_behaves_like_row_number() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(
        &ctx,
        "SELECT key, score, dedupe_best_by_score() OVER (PARTITION BY key ORDER BY score DESC) AS rn \
         FROM (VALUES ('a', 1), ('a', 2), ('b', 3)) t(key, score)",
    )?;
    let batch = &batches[0];
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("key column");
    let scores = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("score column");
    let ranks = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("rank column");
    let mut rows: Vec<(String, i64, u64)> = Vec::new();
    for row in 0..batch.num_rows() {
        rows.push((
            keys.value(row).to_string(),
            scores.value(row),
            ranks.value(row),
        ));
    }
    rows.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    let mut expected = vec![
        ("a".to_string(), 1, 2),
        ("a".to_string(), 2, 1),
        ("b".to_string(), 3, 1),
    ];
    expected.sort_by(|left, right| left.0.cmp(&right.0).then(left.1.cmp(&right.1)));
    assert_eq!(rows, expected);
    Ok(())
}
