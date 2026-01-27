use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Int32Array, Int64Array, LargeStringArray, ListArray, StringArray,
    StringViewArray, StructArray, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;
use tokio::runtime::Runtime;

use datafusion_ext::{
    install_expr_planners_native, install_sql_macro_factory_native, registry_snapshot, udf_registry,
};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

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

fn write_temp_csv(contents: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("timestamp")
        .as_nanos();
    let filename = format!("datafusion_ext_read_csv_{nanos}.csv");
    path.push(filename);
    fs::write(&path, contents).expect("write csv file");
    path
}

fn normalize_type_name(value: &str) -> String {
    let text = value.trim().to_ascii_lowercase();
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
        if expected_inputs.is_empty() || has_any_signature {
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
fn range_table_generates_series() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let batches = run_query(&ctx, "SELECT value FROM range_table(1, 4) ORDER BY value")?;
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
fn sql_macro_table_alias_reads_csv() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    install_sql_macro_factory_native(&ctx)?;
    let path = write_temp_csv("value\n1\n2\n3\n");
    run_query(
        &ctx,
        "CREATE FUNCTION read_csv_alias(path STRING) RETURNS STRING LANGUAGE table RETURN 'read_csv'",
    )?;
    let sql = format!(
        "SELECT COUNT(*) AS value FROM read_csv_alias('{}')",
        path.to_string_lossy()
    );
    let batches = run_query(&ctx, &sql)?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), 3);
    let _ = fs::remove_file(&path);
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
fn read_csv_constant_folding_respects_limit() -> Result<()> {
    let ctx = SessionContext::new();
    udf_registry::register_all(&ctx)?;
    let path = write_temp_csv("value\n1\n2\n3\n4\n");
    let sql = format!(
        "SELECT COUNT(*) AS value FROM read_csv('{}', 1 + 1)",
        path.to_string_lossy()
    );
    let batches = run_query(&ctx, &sql)?;
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("int64 column");
    assert_eq!(array.value(0), 2);
    let _ = fs::remove_file(&path);
    Ok(())
}
