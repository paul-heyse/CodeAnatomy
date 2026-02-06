use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionImpl, TableProvider};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::Expr;

use crate::{
    async_runtime, delta_control_plane, macros::TableUdfSpec, table_udfs,
};

const READ_DELTA_TABLE_FUNCTION: &str = "read_delta";
const READ_DELTA_CDF_TABLE_FUNCTION: &str = "read_delta_cdf";
const DELTA_SNAPSHOT_TABLE_FUNCTION: &str = "delta_snapshot";
const DELTA_ADD_ACTIONS_TABLE_FUNCTION: &str = "delta_add_actions";

pub fn external_udtf_specs() -> Vec<TableUdfSpec> {
    table_udfs![
        READ_DELTA_TABLE_FUNCTION => read_delta_table_function;
        READ_DELTA_CDF_TABLE_FUNCTION => read_delta_cdf_table_function;
        DELTA_SNAPSHOT_TABLE_FUNCTION => delta_snapshot_table_function;
        DELTA_ADD_ACTIONS_TABLE_FUNCTION => delta_add_actions_table_function;
    ]
}

pub fn register_external_udtfs(ctx: &SessionContext) -> Result<()> {
    for spec in external_udtf_specs() {
        let table_fn = (spec.builder)(ctx)?;
        ctx.register_udtf(spec.name, Arc::clone(&table_fn));
        for alias in spec.aliases {
            ctx.register_udtf(alias, Arc::clone(&table_fn));
        }
    }
    Ok(())
}

fn read_delta_table_function(ctx: &SessionContext) -> Result<Arc<dyn TableFunctionImpl>> {
    Ok(Arc::new(ReadDeltaTableFunction { ctx: ctx.clone() }))
}

fn read_delta_cdf_table_function(ctx: &SessionContext) -> Result<Arc<dyn TableFunctionImpl>> {
    Ok(Arc::new(ReadDeltaCdfTableFunction { ctx: ctx.clone() }))
}

fn delta_snapshot_table_function(ctx: &SessionContext) -> Result<Arc<dyn TableFunctionImpl>> {
    Ok(Arc::new(DeltaSnapshotTableFunction { ctx: ctx.clone() }))
}

fn delta_add_actions_table_function(ctx: &SessionContext) -> Result<Arc<dyn TableFunctionImpl>> {
    Ok(Arc::new(DeltaAddActionsTableFunction { ctx: ctx.clone() }))
}

fn ensure_exact_args(args: &[Expr], expected: usize, name: &str) -> Result<()> {
    if args.len() != expected {
        return Err(DataFusionError::Plan(format!(
            "{name} expects {expected} arguments"
        )));
    }
    Ok(())
}

fn literal_string_arg(args: &[Expr], idx: usize, name: &str) -> Result<String> {
    let Some(expr) = args.get(idx) else {
        return Err(DataFusionError::Plan(format!(
            "{name} expects argument {} to be a string literal",
            idx + 1
        )));
    };
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => Ok(value.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "{name} expects argument {} to be a string literal",
            idx + 1
        ))),
    }
}

fn map_delta_error(name: &str, err: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(format!("{name} failed: {err}"))
}

fn snapshot_provider(
    snapshot: &crate::delta_protocol::DeltaSnapshotInfo,
) -> Result<Arc<dyn TableProvider>> {
    let reader_features = serde_json::to_string(&snapshot.reader_features).ok();
    let writer_features = serde_json::to_string(&snapshot.writer_features).ok();
    let table_properties = serde_json::to_string(&snapshot.table_properties).ok();
    let partition_columns = serde_json::to_string(&snapshot.partition_columns).ok();

    let mut table_uri_builder = StringBuilder::new();
    let mut version_builder = Int64Builder::new();
    let mut snapshot_timestamp_builder = Int64Builder::new();
    let mut min_reader_builder = Int32Builder::new();
    let mut min_writer_builder = Int32Builder::new();
    let mut reader_features_builder = StringBuilder::new();
    let mut writer_features_builder = StringBuilder::new();
    let mut table_properties_builder = StringBuilder::new();
    let mut partition_columns_builder = StringBuilder::new();
    let mut schema_json_builder = StringBuilder::new();

    table_uri_builder.append_value(snapshot.table_uri.as_str());
    version_builder.append_value(snapshot.version);
    if let Some(ts) = snapshot.snapshot_timestamp {
        snapshot_timestamp_builder.append_value(ts);
    } else {
        snapshot_timestamp_builder.append_null();
    }
    min_reader_builder.append_value(snapshot.min_reader_version);
    min_writer_builder.append_value(snapshot.min_writer_version);
    if let Some(value) = reader_features {
        reader_features_builder.append_value(value);
    } else {
        reader_features_builder.append_null();
    }
    if let Some(value) = writer_features {
        writer_features_builder.append_value(value);
    } else {
        writer_features_builder.append_null();
    }
    if let Some(value) = table_properties {
        table_properties_builder.append_value(value);
    } else {
        table_properties_builder.append_null();
    }
    if let Some(value) = partition_columns {
        partition_columns_builder.append_value(value);
    } else {
        partition_columns_builder.append_null();
    }
    schema_json_builder.append_value(snapshot.schema_json.as_str());

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_uri", DataType::Utf8, false),
        Field::new("version", DataType::Int64, false),
        Field::new("snapshot_timestamp", DataType::Int64, true),
        Field::new("min_reader_version", DataType::Int32, false),
        Field::new("min_writer_version", DataType::Int32, false),
        Field::new("reader_features_json", DataType::Utf8, true),
        Field::new("writer_features_json", DataType::Utf8, true),
        Field::new("table_properties_json", DataType::Utf8, true),
        Field::new("partition_columns_json", DataType::Utf8, true),
        Field::new("schema_json", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(table_uri_builder.finish()) as ArrayRef,
            Arc::new(version_builder.finish()) as ArrayRef,
            Arc::new(snapshot_timestamp_builder.finish()) as ArrayRef,
            Arc::new(min_reader_builder.finish()) as ArrayRef,
            Arc::new(min_writer_builder.finish()) as ArrayRef,
            Arc::new(reader_features_builder.finish()) as ArrayRef,
            Arc::new(writer_features_builder.finish()) as ArrayRef,
            Arc::new(table_properties_builder.finish()) as ArrayRef,
            Arc::new(partition_columns_builder.finish()) as ArrayRef,
            Arc::new(schema_json_builder.finish()) as ArrayRef,
        ],
    )?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    Ok(Arc::new(table))
}

fn add_actions_provider(
    snapshot: &crate::delta_protocol::DeltaSnapshotInfo,
    actions: &[crate::delta_control_plane::DeltaAddActionPayload],
) -> Result<Arc<dyn TableProvider>> {
    let mut table_uri_builder = StringBuilder::new();
    let mut version_builder = Int64Builder::new();
    let mut path_builder = StringBuilder::new();
    let mut size_builder = Int64Builder::new();
    let mut modification_time_builder = Int64Builder::new();
    let mut data_change_builder = BooleanBuilder::new();
    let mut stats_builder = StringBuilder::new();
    let mut partition_builder = StringBuilder::new();
    let mut tags_builder = StringBuilder::new();

    for action in actions {
        table_uri_builder.append_value(snapshot.table_uri.as_str());
        version_builder.append_value(snapshot.version);
        path_builder.append_value(action.path.as_str());
        size_builder.append_value(action.size);
        modification_time_builder.append_value(action.modification_time);
        data_change_builder.append_value(action.data_change);
        if let Some(stats) = action.stats.as_ref() {
            stats_builder.append_value(stats.as_str());
        } else {
            stats_builder.append_null();
        }
        match serde_json::to_string(&action.partition_values) {
            Ok(value) => partition_builder.append_value(value),
            Err(_) => partition_builder.append_null(),
        }
        match serde_json::to_string(&action.tags) {
            Ok(value) => tags_builder.append_value(value),
            Err(_) => tags_builder.append_null(),
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("table_uri", DataType::Utf8, false),
        Field::new("version", DataType::Int64, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
        Field::new("modification_time", DataType::Int64, false),
        Field::new("data_change", DataType::Boolean, false),
        Field::new("stats", DataType::Utf8, true),
        Field::new("partition_values_json", DataType::Utf8, true),
        Field::new("tags_json", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(table_uri_builder.finish()) as ArrayRef,
            Arc::new(version_builder.finish()) as ArrayRef,
            Arc::new(path_builder.finish()) as ArrayRef,
            Arc::new(size_builder.finish()) as ArrayRef,
            Arc::new(modification_time_builder.finish()) as ArrayRef,
            Arc::new(data_change_builder.finish()) as ArrayRef,
            Arc::new(stats_builder.finish()) as ArrayRef,
            Arc::new(partition_builder.finish()) as ArrayRef,
            Arc::new(tags_builder.finish()) as ArrayRef,
        ],
    )?;
    let table = MemTable::try_new(schema, vec![vec![batch]])?;
    Ok(Arc::new(table))
}

#[derive(Clone)]
struct ReadDeltaTableFunction {
    ctx: SessionContext,
}

impl fmt::Debug for ReadDeltaTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReadDeltaTableFunction")
    }
}

impl TableFunctionImpl for ReadDeltaTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        ensure_exact_args(args, 1, READ_DELTA_TABLE_FUNCTION)?;
        let table_uri = literal_string_arg(args, 0, READ_DELTA_TABLE_FUNCTION)?;
        let resolved = async_runtime::block_on(delta_control_plane::delta_provider_from_session(
            &self.ctx,
            table_uri.as_str(),
            None,
            None,
            None,
            None,
            delta_control_plane::DeltaScanOverrides::default(),
            None,
        ))?;
        let (provider, _, _, _, _) =
            resolved.map_err(|err| map_delta_error(READ_DELTA_TABLE_FUNCTION, err))?;
        Ok(Arc::new(provider))
    }
}

#[derive(Clone)]
struct ReadDeltaCdfTableFunction {
    ctx: SessionContext,
}

impl fmt::Debug for ReadDeltaCdfTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReadDeltaCdfTableFunction")
    }
}

impl TableFunctionImpl for ReadDeltaCdfTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let _ = &self.ctx;
        ensure_exact_args(args, 1, READ_DELTA_CDF_TABLE_FUNCTION)?;
        let table_uri = literal_string_arg(args, 0, READ_DELTA_CDF_TABLE_FUNCTION)?;
        let resolved = async_runtime::block_on(delta_control_plane::delta_cdf_provider(
            table_uri.as_str(),
            None,
            None,
            None,
            delta_control_plane::DeltaCdfScanOptions::default(),
            None,
        ))?;
        let (provider, _) =
            resolved.map_err(|err| map_delta_error(READ_DELTA_CDF_TABLE_FUNCTION, err))?;
        Ok(Arc::new(provider))
    }
}

#[derive(Clone)]
struct DeltaSnapshotTableFunction {
    ctx: SessionContext,
}

impl fmt::Debug for DeltaSnapshotTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DeltaSnapshotTableFunction")
    }
}

impl TableFunctionImpl for DeltaSnapshotTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let _ = &self.ctx;
        ensure_exact_args(args, 1, DELTA_SNAPSHOT_TABLE_FUNCTION)?;
        let table_uri = literal_string_arg(args, 0, DELTA_SNAPSHOT_TABLE_FUNCTION)?;
        let resolved = async_runtime::block_on(delta_control_plane::snapshot_info_with_gate(
            table_uri.as_str(),
            None,
            None,
            None,
            None,
        ))?;
        let snapshot =
            resolved.map_err(|err| map_delta_error(DELTA_SNAPSHOT_TABLE_FUNCTION, err))?;
        snapshot_provider(&snapshot)
    }
}

#[derive(Clone)]
struct DeltaAddActionsTableFunction {
    ctx: SessionContext,
}

impl fmt::Debug for DeltaAddActionsTableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DeltaAddActionsTableFunction")
    }
}

impl TableFunctionImpl for DeltaAddActionsTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let _ = &self.ctx;
        ensure_exact_args(args, 1, DELTA_ADD_ACTIONS_TABLE_FUNCTION)?;
        let table_uri = literal_string_arg(args, 0, DELTA_ADD_ACTIONS_TABLE_FUNCTION)?;
        let resolved = async_runtime::block_on(delta_control_plane::delta_add_actions(
            table_uri.as_str(),
            None,
            None,
            None,
            None,
        ))?;
        let (snapshot, actions) =
            resolved.map_err(|err| map_delta_error(DELTA_ADD_ACTIONS_TABLE_FUNCTION, err))?;
        add_actions_provider(&snapshot, actions.as_slice())
    }
}
