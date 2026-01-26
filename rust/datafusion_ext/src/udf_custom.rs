use std::any::Any;
use std::borrow::Cow;
use std::hash::Hash;
use std::sync::Arc;

use arrow::array::{
    Array,
    ArrayRef,
    Int32Builder,
    Int32Array,
    Int64Array,
    Int64Builder,
    LargeStringArray,
    ListArray,
    MapBuilder,
    StringArray,
    StringBuilder,
    StringViewArray,
    StructArray,
};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use arrow::ipc::reader::StreamReader;
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue,
    Documentation,
    Expr,
    ReturnFieldArgs,
    ScalarFunctionArgs,
    ScalarUDF,
    ScalarUDFImpl,
    Signature,
    TypeSignature,
    Volatility,
    lit,
};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_python::context::PySessionContext;
use datafusion_python::expr::PyExpr;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods};

use crate::{udf_async, udf_docs};

const ENC_UTF8: i32 = 1;
const ENC_UTF16: i32 = 2;
const ENC_UTF32: i32 = 3;

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

#[pyfunction]
pub fn install_function_factory(
    ctx: PyRef<PySessionContext>,
    policy_ipc: &Bound<'_, PyBytes>,
) -> PyResult<()> {
    let policy = policy_from_ipc(policy_ipc.as_bytes())
        .map_err(|err| PyValueError::new_err(format!("Invalid policy payload: {err}")))?;
    touch_policy_fields(&policy);
    register_primitives(&ctx.ctx, &policy)
        .map_err(|err| PyRuntimeError::new_err(format!("FunctionFactory install failed: {err}")))?;
    install_sql_macro_factory(&ctx.ctx)
        .map_err(|err| PyRuntimeError::new_err(format!("FunctionFactory install failed: {err}")))?;
    Ok(())
}

fn install_sql_macro_factory(ctx: &SessionContext) -> Result<()> {
    crate::install_sql_macro_factory_native(ctx)
}

fn policy_from_ipc(payload: &[u8]) -> Result<FunctionFactoryPolicy> {
    let mut reader = StreamReader::try_new(std::io::Cursor::new(payload), None).map_err(|err| {
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

fn touch_policy_fields(policy: &FunctionFactoryPolicy) {
    let _ = &policy.allow_async;
    let _ = &policy.domain_operator_hooks;
    for primitive in &policy.primitives {
        let _ = &primitive.return_type;
        let _ = &primitive.description;
    }
}

fn register_primitives(ctx: &SessionContext, policy: &FunctionFactoryPolicy) -> Result<()> {
    for primitive in &policy.primitives {
        let udf = build_udf(primitive, policy.prefer_named_arguments)?;
        ctx.register_udf(udf);
    }
    if policy.allow_async {
        udf_async::register_async_udfs(ctx)?;
    }
    Ok(())
}

fn build_udf(primitive: &RulePrimitive, prefer_named: bool) -> Result<ScalarUDF> {
    let signature = primitive_signature(primitive, prefer_named)?;
    match primitive.name.as_str() {
        "cpg_score" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(CpgScoreUdf {
            signature: SignatureEqHash::new(signature),
        }))),
        "stable_hash64" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(StableHash64Udf {
            signature: SignatureEqHash::new(signature),
        }))),
        "stable_hash128" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(StableHash128Udf {
            signature: SignatureEqHash::new(signature),
        }))),
        "prefixed_hash64" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(PrefixedHash64Udf {
            signature: SignatureEqHash::new(signature),
        }))),
        "stable_id" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(StableIdUdf {
            signature: SignatureEqHash::new(signature),
        }))),
        "position_encoding_norm" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            PositionEncodingUdf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "col_to_byte" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(ColToByteUdf {
            signature: SignatureEqHash::new(signature),
        }))),
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
    let signature = signature_from_arg_types(
        arg_types?,
        volatility_from_str(primitive.volatility.as_str())?,
    );
    if prefer_named && primitive.supports_named_args {
        let names = primitive.params.iter().map(|param| param.name.clone()).collect();
        return signature.with_parameter_names(names).map_err(|err| {
            DataFusionError::Plan(format!("Invalid parameter names for {}: {err}", primitive.name))
        });
    }
    Ok(signature)
}

fn signature_from_arg_types(arg_types: Vec<DataType>, volatility: Volatility) -> Signature {
    let has_string = arg_types.iter().any(|dtype| matches!(dtype, DataType::Utf8));
    if arg_types.iter().all(|dtype| matches!(dtype, DataType::Utf8)) {
        Signature::string(arg_types.len(), volatility)
    } else if has_string {
        Signature::one_of(expand_string_signatures(&arg_types), volatility)
    } else {
        Signature::exact(arg_types, volatility)
    }
}

fn expand_string_signatures(arg_types: &[DataType]) -> Vec<TypeSignature> {
    let variants = [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View];
    let mut expanded: Vec<Vec<DataType>> = vec![Vec::new()];
    for dtype in arg_types {
        if matches!(dtype, DataType::Utf8) {
            let mut next = Vec::new();
            for existing in &expanded {
                for variant in &variants {
                    let mut updated = existing.clone();
                    updated.push(variant.clone());
                    next.push(updated);
                }
            }
            expanded = next;
        } else {
            for existing in &mut expanded {
                existing.push(dtype.clone());
            }
        }
    }
    expanded.into_iter().map(TypeSignature::Exact).collect()
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

fn signature_with_names(signature: Signature, names: &[&str]) -> Signature {
    let name_vec = names.iter().map(|name| (*name).to_string()).collect();
    signature
        .clone()
        .with_parameter_names(name_vec)
        .unwrap_or(signature)
}

fn string_int_string_signature(volatility: Volatility) -> Signature {
    let arg_types = vec![DataType::Utf8, DataType::Int64, DataType::Utf8];
    Signature::one_of(expand_string_signatures(&arg_types), volatility)
}

fn scalar_str<'a>(value: &'a ScalarValue, message: &str) -> Result<Option<&'a str>> {
    value
        .try_as_str()
        .ok_or_else(|| DataFusionError::Plan(message.to_string()))
}

fn literal_string_value(value: &ScalarValue) -> Option<Option<String>> {
    match value {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value) => {
            Some(value.clone())
        }
        _ => None,
    }
}

fn literal_string(expr: &Expr) -> Option<Option<String>> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    literal_string_value(value)
}

fn string_array_any<'a>(array: &'a ArrayRef, message: &str) -> Result<Cow<'a, StringArray>> {
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(Cow::Borrowed(values));
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(Cow::Owned(StringArray::from_iter(values.iter())));
    }
    if let Some(values) = array.as_any().downcast_ref::<StringViewArray>() {
        return Ok(Cow::Owned(StringArray::from_iter(values.iter())));
    }
    Err(DataFusionError::Plan(message.to_string()))
}

fn prefixed_hash64_value(prefix: &str, value: &str) -> String {
    format!("{prefix}:{}", hash64_value(value))
}

fn stable_id_value(prefix: &str, value: &str) -> String {
    format!("{prefix}:{}", hash128_value(value))
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

#[derive(Debug, Clone)]
struct SignatureEqHash {
    signature: Signature,
}

impl SignatureEqHash {
    fn new(signature: Signature) -> Self {
        Self { signature }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }
}

impl PartialEq for SignatureEqHash {
    fn eq(&self, other: &Self) -> bool {
        self.signature.type_signature == other.signature.type_signature
            && self.signature.volatility == other.signature.volatility
            && self.signature.parameter_names == other.signature.parameter_names
    }
}

impl Eq for SignatureEqHash {}

impl std::hash::Hash for SignatureEqHash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature.type_signature.hash(state);
        self.signature.volatility.hash(state);
        self.signature.parameter_names.hash(state);
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CpgScoreUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for CpgScoreUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "cpg_score"
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(udf_docs::cpg_score_doc())
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .map(|field| field.is_nullable())
            .unwrap_or(true);
        Ok(Arc::new(Field::new(self.name(), DataType::Float64, nullable)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [ColumnarValue::Scalar(value)] = args.args.as_slice() {
            return Ok(ColumnarValue::Scalar(value.clone()));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        Ok(ColumnarValue::Array(arrays[0].clone()))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrowMetadataUdf {
    signature: SignatureEqHash,
}

impl ArrowMetadataUdf {
    fn new() -> Self {
        let signature = signature_with_names(
            Signature::one_of(
                vec![TypeSignature::Any(1), TypeSignature::Any(2)],
                Volatility::Immutable,
            ),
            &["expr", "key"],
        );
        Self {
            signature: SignatureEqHash::new(signature),
        }
    }
}

impl Default for ArrowMetadataUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ArrowMetadataUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "arrow_metadata"
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(udf_docs::arrow_metadata_doc())
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let data_type = if args.arg_fields.len() == 2 {
            DataType::Utf8
        } else if args.arg_fields.len() == 1 {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            )
        } else {
            return Err(DataFusionError::Plan(
                "arrow_metadata requires 1 or 2 arguments".into(),
            ));
        };
        Ok(Arc::new(Field::new(self.name(), data_type, true)))
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() == 2 {
            Ok(DataType::Utf8)
        } else if arg_types.len() == 1 {
            Ok(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Utf8, true),
                    ])),
                    false,
                )),
                false,
            ))
        } else {
            Err(DataFusionError::Plan(
                "arrow_metadata requires 1 or 2 arguments".into(),
            ))
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let metadata = args.arg_fields[0].metadata();
        if args.args.len() == 2 {
            let key = match &args.args[1] {
                ColumnarValue::Scalar(value) => scalar_str(
                    value,
                    "Second argument to arrow_metadata must be a string literal key",
                )?
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "Second argument to arrow_metadata must be a string literal key".into(),
                    )
                })?,
                _ => {
                    return Err(DataFusionError::Plan(
                        "Second argument to arrow_metadata must be a string literal key".into(),
                    ))
                }
            };
            let value = metadata.get(key).cloned();
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(value)))
        } else if args.args.len() == 1 {
            let mut map_builder =
                MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
            let mut entries: Vec<_> = metadata.iter().collect();
            entries.sort_by_key(|(key, _)| *key);
            for (key, value) in entries {
                map_builder.keys().append_value(key);
                map_builder.values().append_value(value);
            }
            map_builder.append(true)?;
            let map_array = map_builder.finish();
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &map_array, 0,
            )?))
        } else {
            Err(DataFusionError::Plan(
                "arrow_metadata requires 1 or 2 arguments".into(),
            ))
        }
    }
}

pub fn arrow_metadata_udf() -> ScalarUDF {
    ScalarUDF::new_from_shared_impl(Arc::new(ArrowMetadataUdf::new()))
}

pub fn stable_hash64_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::string(1, Volatility::Stable),
        &["value"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(StableHash64Udf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn stable_hash128_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::string(1, Volatility::Stable),
        &["value"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(StableHash128Udf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn prefixed_hash64_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::string(2, Volatility::Stable),
        &["prefix", "value"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(PrefixedHash64Udf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn stable_id_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::string(2, Volatility::Stable),
        &["prefix", "value"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(StableIdUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn col_to_byte_udf() -> ScalarUDF {
    let signature = signature_with_names(
        string_int_string_signature(Volatility::Stable),
        &["line_text", "col", "col_unit"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(ColToByteUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

#[pyfunction]
#[pyo3(signature = (expr, key=None))]
pub fn arrow_metadata(expr: PyExpr, key: Option<&str>) -> PyExpr {
    let udf = arrow_metadata_udf();
    let mut args = vec![expr.into()];
    if let Some(key) = key {
        args.push(lit(key));
    }
    udf.call(args).into()
}

#[pyfunction]
pub fn stable_hash64(value: PyExpr) -> PyExpr {
    stable_hash64_udf().call(vec![value.into()]).into()
}

#[pyfunction]
pub fn stable_hash128(value: PyExpr) -> PyExpr {
    stable_hash128_udf().call(vec![value.into()]).into()
}

#[pyfunction]
pub fn prefixed_hash64(prefix: &str, value: PyExpr) -> PyExpr {
    prefixed_hash64_udf()
        .call(vec![lit(prefix), value.into()])
        .into()
}

#[pyfunction]
pub fn stable_id(prefix: &str, value: PyExpr) -> PyExpr {
    stable_id_udf().call(vec![lit(prefix), value.into()]).into()
}

#[pyfunction]
pub fn col_to_byte(line_text: PyExpr, col_index: PyExpr, col_unit: PyExpr) -> PyExpr {
    col_to_byte_udf()
        .call(vec![line_text.into(), col_index.into(), col_unit.into()])
        .into()
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StableHash64Udf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for StableHash64Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "stable_hash64"
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(udf_docs::stable_hash64_doc())
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .map(|field| field.is_nullable())
            .unwrap_or(true);
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, nullable)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let [expr] = args.as_slice() {
            if let Some(value) = literal_string(expr) {
                let hashed = value.as_deref().map(hash64_value);
                return Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Int64(
                    hashed,
                ))));
            }
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [ColumnarValue::Scalar(value)] = args.args.as_slice() {
            let hashed = scalar_str(value, "stable_hash64 expects string input")?
                .map(hash64_value);
            return Ok(ColumnarValue::Scalar(ScalarValue::Int64(hashed)));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = string_array_any(&arrays[0], "stable_hash64 expects string input")?;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct StableHash128Udf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for StableHash128Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "stable_hash128"
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(udf_docs::stable_hash128_doc())
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .map(|field| field.is_nullable())
            .unwrap_or(true);
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let [expr] = args.as_slice() {
            if let Some(value) = literal_string(expr) {
                let hashed = value.as_deref().map(hash128_value);
                return Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Utf8(
                    hashed,
                ))));
            }
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [ColumnarValue::Scalar(value)] = args.args.as_slice() {
            let hashed = scalar_str(value, "stable_hash128 expects string input")?
                .map(hash128_value);
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(hashed)));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = string_array_any(&arrays[0], "stable_hash128 expects string input")?;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct PrefixedHash64Udf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for PrefixedHash64Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "prefixed_hash64"
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(udf_docs::prefixed_hash64_doc())
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .get(0)
            .map(|field| field.is_nullable())
            .unwrap_or(true)
            || args
                .arg_fields
                .get(1)
                .map(|field| field.is_nullable())
                .unwrap_or(true);
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let [prefix_expr, value_expr] = args.as_slice() {
            let Some(prefix) = literal_string(prefix_expr) else {
                return Ok(ExprSimplifyResult::Original(args));
            };
            let Some(value) = literal_string(value_expr) else {
                return Ok(ExprSimplifyResult::Original(args));
            };
            let hashed = match (prefix.as_deref(), value.as_deref()) {
                (Some(prefix), Some(value)) => Some(prefixed_hash64_value(prefix, value)),
                _ => None,
            };
            return Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Utf8(
                hashed,
            ))));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [ColumnarValue::Scalar(prefix), ColumnarValue::Scalar(value)] = args.args.as_slice()
        {
            let prefix = scalar_str(prefix, "prefixed_hash64 expects string prefix input")?;
            let value = scalar_str(value, "prefixed_hash64 expects string value input")?;
            let hashed = match (prefix, value) {
                (Some(prefix), Some(value)) => Some(prefixed_hash64_value(prefix, value)),
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(hashed)));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let prefixes = string_array_any(
            &arrays[0],
            "prefixed_hash64 expects string prefix input",
        )?;
        let values =
            string_array_any(&arrays[1], "prefixed_hash64 expects string value input")?;
        let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 16);
        for index in 0..values.len() {
            if prefixes.is_null(index) || values.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(prefixed_hash64_value(
                    prefixes.value(index),
                    values.value(index),
                ));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StableIdUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for StableIdUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "stable_id"
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(udf_docs::stable_id_doc())
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .get(0)
            .map(|field| field.is_nullable())
            .unwrap_or(true)
            || args
                .arg_fields
                .get(1)
                .map(|field| field.is_nullable())
                .unwrap_or(true);
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let [prefix_expr, value_expr] = args.as_slice() {
            let Some(prefix) = literal_string(prefix_expr) else {
                return Ok(ExprSimplifyResult::Original(args));
            };
            let Some(value) = literal_string(value_expr) else {
                return Ok(ExprSimplifyResult::Original(args));
            };
            let hashed = match (prefix.as_deref(), value.as_deref()) {
                (Some(prefix), Some(value)) => Some(stable_id_value(prefix, value)),
                _ => None,
            };
            return Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Utf8(
                hashed,
            ))));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [ColumnarValue::Scalar(prefix), ColumnarValue::Scalar(value)] = args.args.as_slice()
        {
            let prefix = scalar_str(prefix, "stable_id expects string prefix input")?;
            let value = scalar_str(value, "stable_id expects string value input")?;
            let hashed = match (prefix, value) {
                (Some(prefix), Some(value)) => Some(stable_id_value(prefix, value)),
                _ => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(hashed)));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let prefixes =
            string_array_any(&arrays[0], "stable_id expects string prefix input")?;
        let values = string_array_any(&arrays[1], "stable_id expects string value input")?;
        let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 32);
        for index in 0..values.len() {
            if prefixes.is_null(index) || values.is_null(index) {
                builder.append_null();
            } else {
                builder.append_value(stable_id_value(
                    prefixes.value(index),
                    values.value(index),
                ));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct PositionEncodingUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for PositionEncodingUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "position_encoding_norm"
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(udf_docs::position_encoding_doc())
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, false)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [ColumnarValue::Scalar(value)] = args.args.as_slice() {
            let normalized = scalar_str(
                value,
                "position_encoding_norm expects string input",
            )?
            .map(normalize_position_encoding)
            .unwrap_or(ENC_UTF32);
            return Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(normalized))));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let input = string_array_any(
            &arrays[0],
            "position_encoding_norm expects string input",
        )?;
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

#[derive(Debug, PartialEq, Eq, Hash)]
struct ColToByteUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for ColToByteUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "col_to_byte"
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(udf_docs::col_to_byte_doc())
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .get(0)
            .map(|field| field.is_nullable())
            .unwrap_or(true)
            || args
                .arg_fields
                .get(1)
                .map(|field| field.is_nullable())
                .unwrap_or(true);
        Ok(Arc::new(Field::new(self.name(), DataType::Int64, nullable)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [
            ColumnarValue::Scalar(line),
            ColumnarValue::Scalar(offset),
            ColumnarValue::Scalar(encoding),
        ] = args.args.as_slice()
        {
            let line = scalar_str(line, "col_to_byte expects string input")?;
            let Some(line) = line else {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)));
            };
            let offset = match offset {
                ScalarValue::Int64(value) => value,
                _ => {
                    return Err(DataFusionError::Plan(
                        "col_to_byte expects int64 input".into(),
                    ))
                }
            };
            let Some(offset) = offset else {
                return Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)));
            };
            let encoding = scalar_str(encoding, "col_to_byte expects string encoding")?;
            let unit = match encoding {
                Some(value) => col_unit_from_text(value),
                None => ColUnit::Utf32,
            };
            let byte_offset = if matches!(unit, ColUnit::Byte) {
                clamp_offset(*offset, line.as_bytes().len()) as i64
            } else {
                let py_index =
                    code_unit_offset_to_py_index(line, normalize_offset(*offset), unit)?;
                byte_offset_from_py_index(line, py_index) as i64
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(
                byte_offset,
            ))));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let lines = string_array_any(&arrays[0], "col_to_byte expects string input")?;
        let offsets = arrays[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Plan("col_to_byte expects int64 input".into()))?;
        let encodings = string_array_any(&arrays[2], "col_to_byte expects string encoding")?;
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
