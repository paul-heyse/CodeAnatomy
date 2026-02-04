use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::io::Cursor;
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::{DataType, SchemaRef};
use arrow::ipc::reader::StreamReader;
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::execution::context::SessionContext;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::{DFSchema, DataFusionError, Result, ScalarValue, Statistics};
use datafusion_datasource::file_compression_type::FileCompressionType;
use datafusion_datasource::file_format::FileFormat;
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_csv::CsvFormat;
use datafusion_datasource_parquet::file_format::ParquetFormat;
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr_fn::col;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{Expr, SortExpr, TableProviderFilterPushDown, TableType};
use hex;
use object_store::parse_url_opts;
use serde_json::{Map as JsonMap, Value as JsonValue};
use tokio::runtime::{Handle, Runtime};
use tokio::task::block_in_place;
use url::{Position, Url};

#[derive(Default)]
struct ListingOverrides {
    schema: Option<SchemaRef>,
    file_extension: Option<String>,
    partition_cols: Option<Vec<(String, DataType)>>,
    file_sort_order: Option<Vec<Vec<SortExpr>>>,
    storage_options: Option<HashMap<String, String>>,
}

pub fn register_external_udtfs(ctx: &SessionContext) -> Result<()> {
    let read_parquet: Arc<dyn TableFunctionImpl> =
        Arc::new(ReadParquetTableFunction::new(ctx.clone()));
    ctx.register_udtf("read_parquet", Arc::clone(&read_parquet));
    let read_csv: Arc<dyn TableFunctionImpl> = Arc::new(ReadCsvTableFunction::new(ctx.clone()));
    ctx.register_udtf("read_csv", Arc::clone(&read_csv));
    Ok(())
}

#[derive(Clone)]
struct ReadParquetTableFunction {
    ctx: SessionContext,
}

impl ReadParquetTableFunction {
    fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl fmt::Debug for ReadParquetTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadParquetTableFunction").finish()
    }
}

impl TableFunctionImpl for ReadParquetTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        ensure_arg_count(args, "read_parquet", 3)?;
        let path = extract_required_path(args, "read_parquet")?;
        let mut limit = extract_optional_usize(args, "read_parquet", 1, "limit")?;
        let options = parse_options_json(args, "read_parquet", 2)?;

        let mut listing = ListingOverrides::default();
        let mut format = ParquetFormat::new();
        if let Some(mut options) = options {
            if let Some(option_limit) = take_optional_usize(&mut options, "limit", "read_parquet")?
            {
                if limit.is_some() {
                    return Err(DataFusionError::Plan(
                        "read_parquet limit provided twice".to_string(),
                    ));
                }
                limit = Some(option_limit);
            }
            listing.schema = take_schema(&mut options, "read_parquet")?;
            listing.file_extension = take_optional_string(&mut options, "file_extension", "read_parquet")?;
            listing.partition_cols = take_partition_cols(&mut options, "read_parquet")?;
            listing.file_sort_order = take_sort_order(&mut options, "file_sort_order", "read_parquet")?;
            listing.storage_options = take_storage_options(&mut options, "storage_options", "read_parquet")?;

            if let Some(pruning) = take_optional_bool(&mut options, "parquet_pruning", "read_parquet")? {
                format = format.with_enable_pruning(pruning);
            }
            if let Some(skip) = take_optional_bool(&mut options, "skip_metadata", "read_parquet")? {
                format = format.with_skip_metadata(skip);
            }
            if let Some(hint) = take_optional_usize(&mut options, "metadata_size_hint", "read_parquet")? {
                format = format.with_metadata_size_hint(Some(hint));
            }
            ensure_no_unknown_options(&options, "read_parquet")?;
        }

        let provider = listing_table_provider(
            &self.ctx,
            Arc::new(format),
            &path,
            listing,
        )?;
        Ok(wrap_with_limit(provider, limit))
    }
}

#[derive(Clone)]
struct ReadCsvTableFunction {
    ctx: SessionContext,
}

impl ReadCsvTableFunction {
    fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl fmt::Debug for ReadCsvTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadCsvTableFunction").finish()
    }
}

impl TableFunctionImpl for ReadCsvTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        ensure_arg_count(args, "read_csv", 3)?;
        let path = extract_required_path(args, "read_csv")?;
        let mut limit = extract_optional_usize(args, "read_csv", 1, "limit")?;
        let options = parse_options_json(args, "read_csv", 2)?;

        let mut listing = ListingOverrides::default();
        let mut format = CsvFormat::default();
        if let Some(mut options) = options {
            if let Some(option_limit) = take_optional_usize(&mut options, "limit", "read_csv")? {
                if limit.is_some() {
                    return Err(DataFusionError::Plan(
                        "read_csv limit provided twice".to_string(),
                    ));
                }
                limit = Some(option_limit);
            }
            listing.schema = take_schema(&mut options, "read_csv")?;
            listing.file_extension = take_optional_string(&mut options, "file_extension", "read_csv")?;
            listing.partition_cols = take_partition_cols(&mut options, "read_csv")?;
            listing.file_sort_order = take_sort_order(&mut options, "file_sort_order", "read_csv")?;
            listing.storage_options = take_storage_options(&mut options, "storage_options", "read_csv")?;

            if let Some(has_header) = take_optional_bool(&mut options, "has_header", "read_csv")? {
                format = format.with_has_header(has_header);
            }
            if let Some(delimiter) = take_optional_char(&mut options, "delimiter", "read_csv")? {
                format = format.with_delimiter(delimiter);
            }
            if let Some(quote) = take_optional_char(&mut options, "quote", "read_csv")? {
                format = format.with_quote(quote);
            }
            if let Some(escape) = take_optional_char(&mut options, "escape", "read_csv")? {
                format = format.with_escape(Some(escape));
            }
            if let Some(terminator) = take_optional_char(&mut options, "terminator", "read_csv")? {
                format = format.with_terminator(Some(terminator));
            }
            if let Some(comment) = take_optional_char(&mut options, "comment", "read_csv")? {
                format = format.with_comment(Some(comment));
            }
            if let Some(newlines) = take_optional_bool(&mut options, "newlines_in_values", "read_csv")? {
                format = format.with_newlines_in_values(newlines);
            }
            if let Some(max_records) = take_optional_usize(&mut options, "schema_infer_max_records", "read_csv")? {
                format = format.with_schema_infer_max_rec(max_records);
            }
            if let Some(regex) = take_optional_string(&mut options, "null_regex", "read_csv")? {
                format = format.with_null_regex(Some(regex));
            }
            if let Some(truncated) = take_optional_bool(&mut options, "truncated_rows", "read_csv")? {
                format = format.with_truncated_rows(truncated);
            }
            if let Some(compression) = take_optional_string(&mut options, "compression", "read_csv")? {
                let compression_type = FileCompressionType::from_str(&compression)?;
                format = format.with_file_compression_type(compression_type);
                if listing.file_extension.is_none() {
                    listing.file_extension = Some(format.get_ext_with_compression(&compression_type)?);
                }
            }
            ensure_no_unknown_options(&options, "read_csv")?;
        }

        let provider = listing_table_provider(
            &self.ctx,
            Arc::new(format),
            &path,
            listing,
        )?;
        Ok(wrap_with_limit(provider, limit))
    }
}

fn extract_required_path(args: &[Expr], func_name: &str) -> Result<String> {
    let expr = args.first().ok_or_else(|| {
        DataFusionError::Plan(format!("{func_name} expects at least one argument"))
    })?;
    let simplified = simplify_expr(expr.clone())?;
    let path = extract_literal_string(&simplified).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{func_name} expects a literal string path argument"
        ))
    })?;
    Ok(path)
}

fn ensure_arg_count(args: &[Expr], func_name: &str, max_args: usize) -> Result<()> {
    if args.len() > max_args {
        return Err(DataFusionError::Plan(format!(
            "{func_name} expects at most {max_args} arguments"
        )));
    }
    Ok(())
}

fn extract_optional_usize(
    args: &[Expr],
    func_name: &str,
    index: usize,
    label: &str,
) -> Result<Option<usize>> {
    let expr = extract_optional_expr(args, index)?;
    let Some(expr) = expr else {
        return Ok(None);
    };
    let value = extract_literal_usize(&expr).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{func_name} expects {label} to be an integer literal"
        ))
    })?;
    Ok(Some(value))
}

fn parse_options_json(
    args: &[Expr],
    func_name: &str,
    index: usize,
) -> Result<Option<JsonMap<String, JsonValue>>> {
    let expr = extract_optional_expr(args, index)?;
    let Some(expr) = expr else {
        return Ok(None);
    };
    let value = extract_literal_string(&expr).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{func_name} expects options_json to be a string literal"
        ))
    })?;
    let payload: JsonValue = serde_json::from_str(&value)
        .map_err(|err| DataFusionError::Plan(format!("{func_name} options JSON invalid: {err}")))?;
    match payload {
        JsonValue::Null => Ok(None),
        JsonValue::Object(map) => Ok(Some(map)),
        _ => Err(DataFusionError::Plan(format!(
            "{func_name} options_json must be a JSON object"
        ))),
    }
}

fn take_optional_bool(
    map: &mut JsonMap<String, JsonValue>,
    key: &str,
    func_name: &str,
) -> Result<Option<bool>> {
    match map.remove(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Bool(value)) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Plan(format!(
            "{func_name} option '{key}' must be boolean, got {other:?}"
        ))),
    }
}

fn take_optional_string(
    map: &mut JsonMap<String, JsonValue>,
    key: &str,
    func_name: &str,
) -> Result<Option<String>> {
    match map.remove(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) => Ok(Some(value)),
        Some(other) => Err(DataFusionError::Plan(format!(
            "{func_name} option '{key}' must be a string, got {other:?}"
        ))),
    }
}

fn take_optional_usize(
    map: &mut JsonMap<String, JsonValue>,
    key: &str,
    func_name: &str,
) -> Result<Option<usize>> {
    match map.remove(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(number)) => number
            .as_u64()
            .and_then(|value| usize::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "{func_name} option '{key}' must be a positive integer"
                ))
            }),
        Some(other) => Err(DataFusionError::Plan(format!(
            "{func_name} option '{key}' must be a number, got {other:?}"
        ))),
    }
}

fn take_optional_char(
    map: &mut JsonMap<String, JsonValue>,
    key: &str,
    func_name: &str,
) -> Result<Option<u8>> {
    let Some(value) = take_optional_string(map, key, func_name)? else {
        return Ok(None);
    };
    let mut chars = value.chars();
    let Some(ch) = chars.next() else {
        return Err(DataFusionError::Plan(format!(
            "{func_name} option '{key}' must be a single character"
        )));
    };
    if chars.next().is_some() {
        return Err(DataFusionError::Plan(format!(
            "{func_name} option '{key}' must be a single character"
        )));
    }
    Ok(Some(ch as u8))
}

fn take_schema(map: &mut JsonMap<String, JsonValue>, func_name: &str) -> Result<Option<SchemaRef>> {
    let Some(value) = map.remove("schema_ipc") else {
        return Ok(None);
    };
    match value {
        JsonValue::Null => Ok(None),
        JsonValue::String(text) => {
            let bytes = decode_schema_ipc_string(&text, func_name)?;
            Ok(Some(schema_from_ipc(bytes)?))
        }
        JsonValue::Array(items) => {
            let mut bytes = Vec::with_capacity(items.len());
            for item in items {
                let Some(value) = item.as_u64() else {
                    return Err(DataFusionError::Plan(format!(
                        "{func_name} option 'schema_ipc' must be an array of bytes"
                    )));
                };
                if value > u8::MAX as u64 {
                    return Err(DataFusionError::Plan(format!(
                        "{func_name} option 'schema_ipc' must be bytes"
                    )));
                }
                bytes.push(value as u8);
            }
            Ok(Some(schema_from_ipc(bytes)?))
        }
        other => Err(DataFusionError::Plan(format!(
            "{func_name} option 'schema_ipc' must be a hex/base64 string or byte array, got {other:?}"
        ))),
    }
}

fn decode_schema_ipc_string(text: &str, func_name: &str) -> Result<Vec<u8>> {
    if let Some(encoded) = text.strip_prefix("base64:") {
        return decode_schema_ipc_base64(encoded, func_name);
    }
    if looks_like_hex(text) {
        return hex::decode(text).map_err(|err| {
            DataFusionError::Plan(format!(
                "{func_name} option 'schema_ipc' must be hex-encoded: {err}"
            ))
        });
    }
    decode_schema_ipc_base64(text, func_name)
}

fn decode_schema_ipc_base64(text: &str, func_name: &str) -> Result<Vec<u8>> {
    STANDARD.decode(text).map_err(|err| {
        DataFusionError::Plan(format!(
            "{func_name} option 'schema_ipc' must be base64-encoded: {err}"
        ))
    })
}

fn looks_like_hex(text: &str) -> bool {
    !text.is_empty()
        && text.len() % 2 == 0
        && text.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn take_partition_cols(
    map: &mut JsonMap<String, JsonValue>,
    func_name: &str,
) -> Result<Option<Vec<(String, DataType)>>> {
    let Some(value) = map.remove("table_partition_cols") else {
        return Ok(None);
    };
    let JsonValue::Array(items) = value else {
        return Err(DataFusionError::Plan(format!(
            "{func_name} option 'table_partition_cols' must be an array"
        )));
    };
    let mut cols = Vec::with_capacity(items.len());
    for item in items {
        match item {
            JsonValue::String(name) => {
                cols.push((name, default_partition_type()));
            }
            JsonValue::Array(values) => {
                if values.len() != 2 {
                    return Err(DataFusionError::Plan(format!(
                        "{func_name} option 'table_partition_cols' entries must have 2 items"
                    )));
                }
                let name = values[0]
                    .as_str()
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "{func_name} option 'table_partition_cols' name must be string"
                        ))
                    })?
                    .to_string();
                let dtype = values[1]
                    .as_str()
                    .and_then(parse_data_type)
                    .unwrap_or_else(default_partition_type);
                cols.push((name, dtype));
            }
            JsonValue::Object(mut obj) => {
                let name = obj
                    .remove("name")
                    .and_then(|value| value.as_str().map(str::to_string))
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "{func_name} option 'table_partition_cols' requires name"
                        ))
                    })?;
                let dtype = obj
                    .remove("data_type")
                    .and_then(|value| value.as_str().and_then(parse_data_type))
                    .unwrap_or_else(default_partition_type);
                cols.push((name, dtype));
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "{func_name} option 'table_partition_cols' entries must be string or object, got {other:?}"
                )));
            }
        }
    }
    Ok(Some(cols))
}

fn take_sort_order(
    map: &mut JsonMap<String, JsonValue>,
    key: &str,
    func_name: &str,
) -> Result<Option<Vec<Vec<SortExpr>>>> {
    let Some(value) = map.remove(key) else {
        return Ok(None);
    };
    let JsonValue::Array(items) = value else {
        return Err(DataFusionError::Plan(format!(
            "{func_name} option '{key}' must be an array"
        )));
    };
    if items.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let mut result = Vec::new();
    match &items[0] {
        JsonValue::Array(_) => {
            for item in items {
                let JsonValue::Array(inner) = item else {
                    return Err(DataFusionError::Plan(format!(
                        "{func_name} option '{key}' must be an array of arrays"
                    )));
                };
                result.push(parse_sort_exprs(&inner, func_name, key)?);
            }
        }
        JsonValue::String(_) | JsonValue::Object(_) => {
            result.push(parse_sort_exprs(&items, func_name, key)?);
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "{func_name} option '{key}' entries must be string or object, got {other:?}"
            )));
        }
    }
    Ok(Some(result))
}

fn parse_sort_exprs(
    items: &[JsonValue],
    func_name: &str,
    key: &str,
) -> Result<Vec<SortExpr>> {
    let mut exprs = Vec::with_capacity(items.len());
    for item in items {
        match item {
            JsonValue::String(name) => {
                exprs.push(SortExpr::new(col(name.as_str()), true, true));
            }
            JsonValue::Object(map) => {
                let name = map
                    .get("name")
                    .or_else(|| map.get("expr"))
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "{func_name} option '{key}' sort entries require 'name'"
                        ))
                    })?;
                let asc = map
                    .get("asc")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(true);
                let nulls_first = map
                    .get("nulls_first")
                    .and_then(|value| value.as_bool())
                    .unwrap_or(true);
                exprs.push(SortExpr::new(col(name), asc, nulls_first));
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "{func_name} option '{key}' sort entries must be string or object, got {other:?}"
                )));
            }
        }
    }
    Ok(exprs)
}

fn take_storage_options(
    map: &mut JsonMap<String, JsonValue>,
    key: &str,
    func_name: &str,
) -> Result<Option<HashMap<String, String>>> {
    let Some(value) = map.remove(key) else {
        return Ok(None);
    };
    let JsonValue::Object(entries) = value else {
        return Err(DataFusionError::Plan(format!(
            "{func_name} option '{key}' must be a JSON object"
        )));
    };
    let mut options = HashMap::with_capacity(entries.len());
    for (key, value) in entries {
        let JsonValue::String(text) = value else {
            return Err(DataFusionError::Plan(format!(
                "{func_name} option '{key}' values must be strings"
            )));
        };
        options.insert(key, text);
    }
    Ok(Some(options))
}

fn ensure_no_unknown_options(map: &JsonMap<String, JsonValue>, func_name: &str) -> Result<()> {
    if map.is_empty() {
        return Ok(());
    }
    let keys: Vec<String> = map.keys().cloned().collect();
    Err(DataFusionError::Plan(format!(
        "{func_name} options contain unsupported keys: {}",
        keys.join(", ")
    )))
}

fn parse_data_type(name: &str) -> Option<DataType> {
    match name.to_ascii_lowercase().as_str() {
        "utf8" | "string" => Some(DataType::Utf8),
        "int64" | "bigint" => Some(DataType::Int64),
        "int32" | "int" => Some(DataType::Int32),
        "int16" => Some(DataType::Int16),
        "int8" => Some(DataType::Int8),
        "uint64" => Some(DataType::UInt64),
        "uint32" => Some(DataType::UInt32),
        "uint16" => Some(DataType::UInt16),
        "uint8" => Some(DataType::UInt8),
        "bool" | "boolean" => Some(DataType::Boolean),
        "float64" | "double" => Some(DataType::Float64),
        "float32" | "float" => Some(DataType::Float32),
        _ => None,
    }
}

fn default_partition_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
}

fn extract_optional_expr(args: &[Expr], index: usize) -> Result<Option<Expr>> {
    let expr = args.get(index);
    let Some(expr) = expr else {
        return Ok(None);
    };
    let simplified = simplify_expr(expr.clone())?;
    match simplified {
        Expr::Literal(value, _) if value.is_null() => Ok(None),
        _ => Ok(Some(simplified)),
    }
}

fn simplify_expr(expr: Expr) -> Result<Expr> {
    let props = ExecutionProps::new();
    let schema = Arc::new(DFSchema::empty());
    let context = SimplifyContext::new(&props).with_schema(schema);
    let simplifier = ExprSimplifier::new(context);
    simplifier.simplify(expr)
}

fn extract_literal_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => Some(value.clone()),
        _ => None,
    }
}

fn extract_literal_usize(expr: &Expr) -> Option<usize> {
    let value = match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => (*value).try_into().ok(),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => (*value as i64).try_into().ok(),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => (*value).try_into().ok(),
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Some(*value as usize),
        _ => None,
    };
    value
}

fn listing_table_provider(
    ctx: &SessionContext,
    format: Arc<dyn FileFormat>,
    path: &str,
    overrides: ListingOverrides,
) -> Result<Arc<dyn TableProvider>> {
    let state = ctx.state();
    maybe_register_object_store(ctx, path, overrides.storage_options)?;
    let mut options = ListingOptions::new(format).with_session_config_options(state.config());
    let has_partition_override = overrides.partition_cols.is_some();
    if let Some(partitions) = overrides.partition_cols {
        options = options.with_table_partition_cols(partitions);
    }
    if let Some(sort_order) = overrides.file_sort_order {
        options = options.with_file_sort_order(sort_order);
    }
    let table_path = ListingTableUrl::parse(path)
        .or_else(|_| ListingTableUrl::parse(format!("file://{path}").as_str()))?;
    let extension = if let Some(extension) = overrides.file_extension {
        extension
    } else {
        infer_file_extension(&table_path, &options, path)?
    };
    options = options.with_file_extension(extension);

    if !has_partition_override
        && state
            .config_options()
            .execution
            .listing_table_factory_infer_partitions
    {
        let partitions = block_on(options.infer_partitions(&state, &table_path))?;
        let partition_cols = partitions
            .into_iter()
            .map(|name| (name, default_partition_type()))
            .collect::<Vec<_>>();
        options = options.with_table_partition_cols(partition_cols);
    }

    block_on(options.validate_partitions(&state, &table_path))?;
    let schema = if let Some(schema) = overrides.schema {
        schema
    } else {
        block_on(options.infer_schema(&state, &table_path))?
    };

    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(schema);
    let provider = ListingTable::try_new(config)?
        .with_cache(state.runtime_env().cache_manager.get_file_statistic_cache());
    Ok(Arc::new(provider))
}

fn maybe_register_object_store(
    ctx: &SessionContext,
    path: &str,
    storage_options: Option<HashMap<String, String>>,
) -> Result<()> {
    let Some(storage_options) = storage_options else {
        return Ok(());
    };
    let url = match Url::parse(path) {
        Ok(url) => url,
        Err(_) => return Ok(()),
    };
    let opts_iter = storage_options
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()));
    let (store, _) = parse_url_opts(&url, opts_iter).map_err(|err| {
        DataFusionError::Plan(format!("Failed to parse storage options: {err}"))
    })?;
    let store_url = object_store_url_for_path(&url)?;
    ctx.runtime_env()
        .register_object_store(&store_url, Arc::from(store));
    Ok(())
}

fn object_store_url_for_path(url: &Url) -> Result<Url> {
    let authority = &url[Position::BeforeHost..Position::AfterPort];
    let base = format!("{}://{}", url.scheme(), authority);
    Url::parse(&base).map_err(|err| {
        DataFusionError::Plan(format!("Invalid object store URL for {url}: {err}"))
    })
}

fn schema_from_ipc(payload: Vec<u8>) -> Result<SchemaRef> {
    let reader = StreamReader::try_new(Cursor::new(payload), None)
        .map_err(|err| DataFusionError::Plan(format!("Failed to open schema IPC stream: {err}")))?;
    Ok(Arc::clone(&reader.schema()))
}

fn block_on<F, T>(future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    if let Ok(handle) = Handle::try_current() {
        return block_in_place(|| handle.block_on(future));
    }
    let runtime = Runtime::new()
        .map_err(|err| DataFusionError::Plan(format!("Failed to create Tokio runtime: {err}")))?;
    runtime.block_on(future)
}

fn infer_file_extension(
    table_path: &ListingTableUrl,
    options: &ListingOptions,
    path: &str,
) -> Result<String> {
    if table_path.is_collection() {
        return Ok(options.file_extension.clone());
    }
    let (extension, compression) =
        ListingTableConfig::infer_file_extension_and_compression_type(path)
            .unwrap_or((options.file_extension.clone(), None));
    if extension.is_empty() {
        return Ok(options.file_extension.clone());
    }
    if let Some(compression) = compression {
        let compression_type = FileCompressionType::from_str(&compression)?;
        return options.format.get_ext_with_compression(&compression_type);
    }
    Ok(extension)
}

fn wrap_with_limit(
    provider: Arc<dyn TableProvider>,
    limit: Option<usize>,
) -> Arc<dyn TableProvider> {
    if let Some(limit) = limit {
        Arc::new(LimitedTableProvider::new(provider, limit))
    } else {
        provider
    }
}

#[derive(Debug)]
struct LimitedTableProvider {
    inner: Arc<dyn TableProvider>,
    max_rows: usize,
}

impl LimitedTableProvider {
    fn new(inner: Arc<dyn TableProvider>, max_rows: usize) -> Self {
        Self { inner, max_rows }
    }
}

#[async_trait]
impl TableProvider for LimitedTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&datafusion_common::Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.inner.get_table_definition()
    }

    fn get_logical_plan(&self) -> Option<std::borrow::Cow<'_, datafusion_expr::LogicalPlan>> {
        self.inner.get_logical_plan()
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.inner.get_column_default(column)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let effective_limit = limit.map_or(self.max_rows, |value| value.min(self.max_rows));
        self.inner
            .scan(state, projection, filters, Some(effective_limit))
            .await
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.inner.statistics()
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: datafusion_expr::dml::InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }
}
