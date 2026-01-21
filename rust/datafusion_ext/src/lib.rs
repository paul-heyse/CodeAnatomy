//! DataFusion extension for native function registration.

use std::any::Any;
use std::ffi::CString;
use std::io::Cursor;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    ArrayRef,
    Int32Builder,
    Int32Array,
    Int64Array,
    Int64Builder,
    ListArray,
    StringArray,
    StringBuilder,
    StructArray,
};
use arrow::datatypes::DataType;
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::StreamReader;
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider};
use datafusion::execution::context::SessionContext;
use datafusion::physical_expr_adapter::{
    DefaultPhysicalExprAdapterFactory,
    PhysicalExprAdapter,
    PhysicalExprAdapterFactory,
};
use datafusion_common::{DataFusionError, Result};
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource_parquet::file_format::ParquetFormat;
use deltalake::delta_datafusion::{DeltaScanConfigBuilder, DeltaTableProvider};
use deltalake::{ensure_table_uri, DeltaTableBuilder};
use tokio::runtime::Runtime;
use datafusion_expr::{
    ColumnarValue,
    ScalarFunctionArgs,
    ScalarUDF,
    ScalarUDFImpl,
    Signature,
    Volatility,
};
use datafusion_python::context::PySessionContext;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyCapsule};

const ENC_UTF8: i32 = 1;
const ENC_UTF16: i32 = 2;
const ENC_UTF32: i32 = 3;

fn schema_from_ipc(schema_ipc: Vec<u8>) -> PyResult<SchemaRef> {
    let reader = StreamReader::try_new(Cursor::new(schema_ipc), None)
        .map_err(|err| PyValueError::new_err(format!("Failed to decode schema IPC: {err}")))?;
    Ok(reader.schema())
}

#[derive(Debug)]
struct FunctionParameter {
    name: String,
    dtype: String,
}

#[derive(Debug)]
struct RulePrimitive {
    name: String,
    params: Vec<FunctionParameter>,
    return_type: String,
    volatility: String,
    description: Option<String>,
    supports_named_args: bool,
}

#[derive(Debug)]
struct FunctionFactoryPolicy {
    primitives: Vec<RulePrimitive>,
    prefer_named_arguments: bool,
    allow_async: bool,
    domain_operator_hooks: Vec<String>,
}

const POLICY_SCHEMA_VERSION: i32 = 1;

#[derive(Debug, Default)]
struct SchemaEvolutionAdapterFactory;

impl PhysicalExprAdapterFactory for SchemaEvolutionAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Arc<dyn PhysicalExprAdapter> {
        DefaultPhysicalExprAdapterFactory
            .create(logical_file_schema, physical_file_schema)
    }
}

fn policy_from_ipc(payload: &[u8]) -> Result<FunctionFactoryPolicy> {
    let mut reader = StreamReader::try_new(Cursor::new(payload), None).map_err(|err| {
        DataFusionError::Plan(format!("Failed to open policy IPC stream: {err}"))
    })?;
    let batch = reader
        .next()
        .ok_or_else(|| DataFusionError::Plan("Policy IPC stream is empty.".into()))?
        .map_err(|err| DataFusionError::Plan(format!("Failed to read policy IPC batch: {err}")))?;
    if batch.num_rows() != 1 {
        return Err(DataFusionError::Plan(
            "Policy IPC payload must contain exactly one row.".into(),
        ));
    }
    let version = read_int32(&batch, "version")?;
    if version != POLICY_SCHEMA_VERSION {
        return Err(DataFusionError::Plan(format!(
            "Unsupported policy payload version: {version}."
        )));
    }
    let prefer_named_arguments = read_bool(&batch, "prefer_named_arguments")?;
    let allow_async = read_bool(&batch, "allow_async")?;
    let domain_operator_hooks = read_string_list(&batch, "domain_operator_hooks")?;
    let primitives = read_primitives(&batch, "primitives")?;
    Ok(FunctionFactoryPolicy {
        primitives,
        prefer_named_arguments,
        allow_async,
        domain_operator_hooks,
    })
}

fn read_int32(batch: &arrow::record_batch::RecordBatch, name: &str) -> Result<i32> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} column.")))?;
    let array = array
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} column type.")))?;
    if array.is_null(0) {
        return Err(DataFusionError::Plan(format!("{name} cannot be null.")));
    }
    Ok(array.value(0))
}

fn read_bool(batch: &arrow::record_batch::RecordBatch, name: &str) -> Result<bool> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} column.")))?;
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} column type.")))?;
    if array.is_null(0) {
        return Err(DataFusionError::Plan(format!("{name} cannot be null.")));
    }
    Ok(array.value(0))
}

fn read_string_list(batch: &arrow::record_batch::RecordBatch, name: &str) -> Result<Vec<String>> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} column.")))?;
    let array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} column type.")))?;
    if array.is_null(0) {
        return Ok(Vec::new());
    }
    let values = array.value(0);
    let strings = values
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} list type.")))?;
    let mut output = Vec::with_capacity(strings.len());
    for index in 0..strings.len() {
        if strings.is_null(index) {
            continue;
        }
        output.push(strings.value(index).to_string());
    }
    Ok(output)
}

fn read_primitives(
    batch: &arrow::record_batch::RecordBatch,
    name: &str,
) -> Result<Vec<RulePrimitive>> {
    let array = batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} column.")))?;
    let array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} column type.")))?;
    if array.is_null(0) {
        return Ok(Vec::new());
    }
    let values = array.value(0);
    let structs = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Plan("Invalid primitives list type.".into()))?;
    let names = string_field(structs, "name")?;
    let params = list_field(structs, "params")?;
    let return_types = string_field(structs, "return_type")?;
    let volatilities = string_field(structs, "volatility")?;
    let descriptions = string_field(structs, "description")?;
    let supports_named = bool_field(structs, "supports_named_args")?;
    let mut output = Vec::with_capacity(structs.len());
    for index in 0..structs.len() {
        if structs.is_null(index) {
            continue;
        }
        let primitive = RulePrimitive {
            name: read_string_value(&names, index)?,
            params: read_params(&params, index)?,
            return_type: read_string_value(&return_types, index)?,
            volatility: read_string_value(&volatilities, index)?,
            description: read_optional_string(&descriptions, index),
            supports_named_args: read_bool_value(&supports_named, index)?,
        };
        output.push(primitive);
    }
    Ok(output)
}

fn read_params(array: &ListArray, index: usize) -> Result<Vec<FunctionParameter>> {
    if array.is_null(index) {
        return Ok(Vec::new());
    }
    let values = array.value(index);
    let structs = values
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| DataFusionError::Plan("Invalid params list type.".into()))?;
    let names = string_field(structs, "name")?;
    let dtypes = string_field(structs, "dtype")?;
    let mut output = Vec::with_capacity(structs.len());
    for row in 0..structs.len() {
        if structs.is_null(row) {
            continue;
        }
        output.push(FunctionParameter {
            name: read_string_value(&names, row)?,
            dtype: read_string_value(&dtypes, row)?,
        });
    }
    Ok(output)
}

fn string_field<'a>(array: &'a StructArray, name: &str) -> Result<&'a StringArray> {
    let column = array
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} field.")))?;
    column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} field type.")))
}

fn list_field<'a>(array: &'a StructArray, name: &str) -> Result<&'a ListArray> {
    let column = array
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} field.")))?;
    column
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} field type.")))
}

fn bool_field<'a>(
    array: &'a StructArray,
    name: &str,
) -> Result<&'a arrow::array::BooleanArray> {
    let column = array
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing {name} field.")))?;
    column
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid {name} field type.")))
}

fn read_string_value(array: &StringArray, index: usize) -> Result<String> {
    if array.is_null(index) {
        return Err(DataFusionError::Plan("String value cannot be null.".into()));
    }
    Ok(array.value(index).to_string())
}

fn read_optional_string(array: &StringArray, index: usize) -> Option<String> {
    if array.is_null(index) {
        None
    } else {
        Some(array.value(index).to_string())
    }
}

fn read_bool_value(array: &arrow::array::BooleanArray, index: usize) -> Result<bool> {
    if array.is_null(index) {
        return Err(DataFusionError::Plan("Boolean value cannot be null.".into()));
    }
    Ok(array.value(index))
}

#[pyfunction]
fn install_function_factory(ctx: PyRef<PySessionContext>, policy_ipc: &PyBytes) -> PyResult<()> {
    let policy = policy_from_ipc(policy_ipc.as_bytes())
        .map_err(|err| PyValueError::new_err(format!("Invalid policy payload: {err}")))?;
    register_primitives(&ctx.ctx, &policy)
        .map_err(|err| PyRuntimeError::new_err(format!("FunctionFactory install failed: {err}")))?;
    Ok(())
}

#[pyfunction]
fn schema_evolution_adapter_factory(py: Python<'_>) -> PyResult<PyObject> {
    let factory: Arc<dyn PhysicalExprAdapterFactory> =
        Arc::new(SchemaEvolutionAdapterFactory::default());
    let name = CString::new("datafusion_ext.SchemaEvolutionAdapterFactory")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, factory, Some(name))?;
    Ok(capsule.into_py(py))
}

#[pyfunction]
fn parquet_listing_table_provider(
    py: Python<'_>,
    path: String,
    file_extension: String,
    table_partition_cols: Option<Vec<(String, String)>>,
    schema_ipc: Option<Vec<u8>>,
    partition_schema_ipc: Option<Vec<u8>>,
    parquet_pruning: Option<bool>,
    skip_metadata: Option<bool>,
    collect_statistics: Option<bool>,
) -> PyResult<PyObject> {
    let schema_ipc = schema_ipc.ok_or_else(|| {
        PyValueError::new_err("Parquet listing table provider requires schema_ipc.")
    })?;
    let schema = schema_from_ipc(schema_ipc)?;
    let mut format = ParquetFormat::default();
    if let Some(enable_pruning) = parquet_pruning {
        format = format.with_enable_pruning(enable_pruning);
    }
    if let Some(skip) = skip_metadata {
        format = format.with_skip_metadata(skip);
    }
    let mut options = ListingOptions::new(Arc::new(format)).with_file_extension(file_extension);
    if let Some(collect) = collect_statistics {
        options = options.with_collect_stat(collect);
    }
    if let Some(schema_ipc) = partition_schema_ipc {
        let schema = schema_from_ipc(schema_ipc)?;
        let mut cols = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            cols.push((field.name().to_string(), field.data_type().clone()));
        }
        options = options.with_table_partition_cols(cols);
    } else if let Some(table_partition_cols) = table_partition_cols {
        let mut cols = Vec::with_capacity(table_partition_cols.len());
        for (name, dtype) in table_partition_cols {
            let parsed: DataType = DataType::from_str(&dtype).map_err(|err| {
                PyValueError::new_err(format!(
                    "Unsupported partition column type {dtype:?}: {err}"
                ))
            })?;
            cols.push((name, parsed));
        }
        options = options.with_table_partition_cols(cols);
    }
    let table_path = ListingTableUrl::parse(path.as_str()).or_else(|_| {
        ListingTableUrl::parse(format!("file://{path}").as_str())
    });
    let table_path = table_path.map_err(|err| {
        PyValueError::new_err(format!("Invalid listing table path {path:?}: {err}"))
    })?;
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .with_schema(schema)
        .with_expr_adapter_factory(Arc::new(DefaultPhysicalExprAdapterFactory));
    let provider = ListingTable::try_new(config).map_err(|err| {
        PyRuntimeError::new_err(format!("ListingTable provider build failed: {err}"))
    })?;
    let ffi_provider = FFI_TableProvider::new(Arc::new(provider), true, None);
    let name = CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, ffi_provider, Some(name))?;
    Ok(capsule.into_py(py))
}

#[pyfunction]
fn delta_table_provider(
    py: Python<'_>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_ipc: Option<Vec<u8>>,
) -> PyResult<PyObject> {
    let table_url = ensure_table_uri(table_uri.as_str()).map_err(|err| {
        PyValueError::new_err(format!("Invalid Delta table URI {table_uri:?}: {err}"))
    })?;
    let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta table: {err}"))
    })?;
    if let Some(options) = storage_options {
        let options: HashMap<String, String> = options.into_iter().collect();
        builder = builder.with_storage_options(options);
    }
    if let Some(version) = version {
        builder = builder.with_version(version);
    }
    if let Some(timestamp) = timestamp {
        builder = builder.with_datestring(timestamp).map_err(|err| {
            PyValueError::new_err(format!("Invalid Delta timestamp: {err}"))
        })?;
    }
    let runtime = Runtime::new().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}"))
    })?;
    let table = runtime.block_on(builder.load()).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to load Delta table: {err}"))
    })?;
    let snapshot = table.snapshot().map_err(|err| {
        PyRuntimeError::new_err(format!("Delta table snapshot unavailable: {err}"))
    })?;
    let snapshot = snapshot.snapshot().clone();
    let log_store = table.log_store();
    let mut scan_builder = DeltaScanConfigBuilder::new();
    if let Some(name) = file_column_name {
        scan_builder = scan_builder.with_file_column_name(&name);
    }
    if let Some(pushdown) = enable_parquet_pushdown {
        scan_builder = scan_builder.with_parquet_pushdown(pushdown);
    }
    if let Some(schema_ipc) = schema_ipc {
        let schema = schema_from_ipc(schema_ipc)?;
        scan_builder = scan_builder.with_schema(schema);
    }
    let scan_config = scan_builder.build(&snapshot).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta scan config: {err}"))
    })?;
    let provider = DeltaTableProvider::try_new(snapshot, log_store, scan_config).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to build Delta table provider: {err}"))
    })?;
    let ffi_provider = FFI_TableProvider::new(Arc::new(provider), true, None);
    let name = CString::new("datafusion_table_provider")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, ffi_provider, Some(name))?;
    Ok(capsule.into_py(py))
}

#[pyfunction]
fn install_schema_evolution_adapter_factory(ctx: PyRef<PySessionContext>) -> PyResult<()> {
    let factory: Arc<dyn PhysicalExprAdapterFactory> =
        Arc::new(SchemaEvolutionAdapterFactory::default());
    ctx.ctx
        .register_physical_expr_adapter_factory(factory)
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Schema evolution adapter factory registration failed: {err}"
            ))
        })?;
    Ok(())
}

#[pyfunction]
fn registry_catalog_provider_factory(py: Python<'_>, schema_name: Option<String>) -> PyResult<PyObject> {
    let schema_name = schema_name.unwrap_or_else(|| "public".to_string());
    let schema_provider: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
    let catalog = Arc::new(MemoryCatalogProvider::new());
    catalog
        .register_schema(schema_name, schema_provider)
        .map_err(|err| PyRuntimeError::new_err(format!("Catalog registration failed: {err}")))?;
    let provider: Arc<dyn CatalogProvider> = catalog;
    let name = CString::new("datafusion_ext.RegistryCatalogProviderFactory")
        .map_err(|err| PyValueError::new_err(format!("Invalid capsule name: {err}")))?;
    let capsule = PyCapsule::new(py, provider, Some(name))?;
    Ok(capsule.into_py(py))
}

fn register_primitives(ctx: &SessionContext, policy: &FunctionFactoryPolicy) -> Result<()> {
    for primitive in &policy.primitives {
        let udf = build_udf(primitive, policy.prefer_named_arguments)?;
        ctx.register_udf(udf);
    }
    Ok(())
}

fn build_udf(primitive: &RulePrimitive, prefer_named: bool) -> Result<ScalarUDF> {
    let signature = primitive_signature(primitive, prefer_named)?;
    match primitive.name.as_str() {
        "cpg_score" => Ok(ScalarUDF::from(Arc::new(CpgScoreUdf { signature }))),
        "stable_hash64" => Ok(ScalarUDF::from(Arc::new(StableHash64Udf { signature }))),
        "stable_hash128" => Ok(ScalarUDF::from(Arc::new(StableHash128Udf { signature }))),
        "position_encoding_norm" => Ok(ScalarUDF::from(Arc::new(PositionEncodingUdf { signature }))),
        "col_to_byte" => Ok(ScalarUDF::from(Arc::new(ColToByteUdf { signature }))),
        name => Err(DataFusionError::Plan(format!(
            "Unsupported rule primitive: {name}"
        ))),
    }
}

fn primitive_signature(primitive: &RulePrimitive, prefer_named: bool) -> Result<Signature> {
    let arg_types: Result<Vec<DataType>> = primitive
        .params
        .iter()
        .map(|param| dtype_from_str(param.dtype.as_str()))
        .collect();
    let signature = Signature::exact(arg_types?, volatility_from_str(primitive.volatility.as_str())?);
    if prefer_named && primitive.supports_named_args {
        let names = primitive.params.iter().map(|param| param.name.clone()).collect();
        return signature.with_parameter_names(names).map_err(|err| {
            DataFusionError::Plan(format!("Invalid parameter names for {}: {err}", primitive.name))
        });
    }
    Ok(signature)
}

fn dtype_from_str(value: &str) -> Result<DataType> {
    match value {
        "string" => Ok(DataType::Utf8),
        "int64" => Ok(DataType::Int64),
        "int32" => Ok(DataType::Int32),
        "float64" => Ok(DataType::Float64),
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported dtype in primitive policy: {value}"
        ))),
    }
}

fn volatility_from_str(value: &str) -> Result<Volatility> {
    match value {
        "immutable" => Ok(Volatility::Immutable),
        "stable" => Ok(Volatility::Stable),
        "volatile" => Ok(Volatility::Volatile),
        _ => Err(DataFusionError::Plan(format!(
            "Unsupported volatility in primitive policy: {value}"
        ))),
    }
}

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

fn normalize_position_encoding(value: &str) -> i32 {
    let upper = value.trim().to_uppercase();
    if upper.is_empty() {
        return ENC_UTF32;
    }
    if upper.chars().all(|ch| ch.is_ascii_digit()) {
        if let Ok(parsed) = upper.parse::<i32>() {
            return match parsed {
                ENC_UTF8 | ENC_UTF16 | ENC_UTF32 => parsed,
                _ => ENC_UTF32,
            };
        }
    }
    if upper.contains("UTF8") {
        return ENC_UTF8;
    }
    if upper.contains("UTF16") {
        return ENC_UTF16;
    }
    ENC_UTF32
}

fn col_unit_from_text(value: &str) -> ColUnit {
    let upper = value.trim().to_uppercase();
    if upper.chars().all(|ch| ch.is_ascii_digit()) {
        if let Ok(parsed) = upper.parse::<i32>() {
            return ColUnit::from_encoding(parsed);
        }
    }
    if upper.contains("BYTE") {
        return ColUnit::Byte;
    }
    if upper.contains("UTF8") {
        return ColUnit::Utf8;
    }
    if upper.contains("UTF16") {
        return ColUnit::Utf16;
    }
    ColUnit::Utf32
}

#[derive(Clone, Copy)]
enum ColUnit {
    Byte,
    Utf8,
    Utf16,
    Utf32,
}

impl ColUnit {
    fn from_encoding(value: i32) -> Self {
        match value {
            ENC_UTF8 => ColUnit::Utf8,
            ENC_UTF16 => ColUnit::Utf16,
            ENC_UTF32 => ColUnit::Utf32,
            _ => ColUnit::Utf32,
        }
    }
}

fn clamp_offset(value: i64, limit: usize) -> usize {
    if value <= 0 {
        return 0;
    }
    let candidate = value as usize;
    candidate.min(limit)
}

fn normalize_offset(value: i64) -> usize {
    if value <= 0 {
        return 0;
    }
    value as usize
}

fn code_unit_offset_to_py_index(line: &str, offset: usize, unit: ColUnit) -> Result<usize> {
    match unit {
        ColUnit::Utf32 => Ok(offset.min(line.chars().count())),
        ColUnit::Utf8 => {
            let bytes = line.as_bytes();
            let prefix = &bytes[..offset.min(bytes.len())];
            let prefix_str = std::str::from_utf8(prefix)
                .map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Ok(prefix_str.chars().count())
        }
        ColUnit::Utf16 => {
            let units: Vec<u16> = line.encode_utf16().collect();
            let prefix = &units[..offset.min(units.len())];
            let prefix_str =
                String::from_utf16(prefix).map_err(|err| DataFusionError::Plan(err.to_string()))?;
            Ok(prefix_str.chars().count())
        }
        ColUnit::Byte => Ok(offset.min(line.as_bytes().len())),
    }
}

fn byte_offset_from_py_index(line: &str, py_index: usize) -> usize {
    let prefix: String = line.chars().take(py_index).collect();
    prefix.as_bytes().len()
}

struct CpgScoreUdf {
    signature: Signature,
}

impl ScalarUDFImpl for CpgScoreUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "cpg_score"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        Ok(ColumnarValue::Array(arrays[0].clone()))
    }
}

struct StableHash64Udf {
    signature: Signature,
}

impl ScalarUDFImpl for StableHash64Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "stable_hash64"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("stable_hash64 expects string input".into()))?;
        let mut builder = Int64Builder::with_capacity(input.len());
        for index in 0..input.len() {
            if input.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(hash64_value(input.value(index)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

struct StableHash128Udf {
    signature: Signature,
}

impl ScalarUDFImpl for StableHash128Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "stable_hash128"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("stable_hash128 expects string input".into()))?;
        let mut builder = StringBuilder::with_capacity(input.len(), input.len() * 16);
        for index in 0..input.len() {
            if input.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(hash128_value(input.value(index)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

struct PositionEncodingUdf {
    signature: Signature,
}

impl ScalarUDFImpl for PositionEncodingUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "position_encoding_norm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("position_encoding_norm expects string input".into())
            })?;
        let mut builder = Int32Builder::with_capacity(input.len());
        for index in 0..input.len() {
            if input.is_null(index) {
                builder.append_value(ENC_UTF32);
            } else {
                builder.append_value(normalize_position_encoding(input.value(index)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

struct ColToByteUdf {
    signature: Signature,
}

impl ScalarUDFImpl for ColToByteUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "col_to_byte"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let lines = arrays[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("col_to_byte expects string input".into()))?;
        let offsets = arrays[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Plan("col_to_byte expects int64 input".into()))?;
        let encodings = arrays[2]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Plan("col_to_byte expects string encoding".into()))?;
        let mut builder = Int64Builder::with_capacity(lines.len());
        for index in 0..lines.len() {
            if lines.is_null(index) || offsets.is_null(index) {
                builder.append_null();
                continue;
            }
            let line = lines.value(index);
            let offset = offsets.value(index);
            let unit = if encodings.is_null(index) {
                ColUnit::Utf32
            } else {
                col_unit_from_text(encodings.value(index))
            };
            if matches!(unit, ColUnit::Byte) {
                let max_len = line.as_bytes().len();
                let bytes = clamp_offset(offset, max_len);
                builder.append_value(bytes as i64);
                continue;
            }
            let py_index = code_unit_offset_to_py_index(line, normalize_offset(offset), unit)?;
            let byte_offset = byte_offset_from_py_index(line, py_index);
            builder.append_value(byte_offset as i64);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[pymodule]
fn datafusion_ext(_py: Python<'_>, module: &PyModule) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(install_function_factory, module)?)?;
    module.add_function(wrap_pyfunction!(schema_evolution_adapter_factory, module)?)?;
    module.add_function(wrap_pyfunction!(parquet_listing_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(delta_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(install_schema_evolution_adapter_factory, module)?)?;
    module.add_function(wrap_pyfunction!(registry_catalog_provider_factory, module)?)?;
    Ok(())
}
