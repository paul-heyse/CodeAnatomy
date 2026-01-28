use std::any::Any;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Builder, Int32Array, Int32Builder,
    Int64Array, Int64Builder, LargeStringArray, ListArray, ListBuilder, MapArray, MapBuilder,
    StringArray, StringBuilder, StringViewArray, StructArray,
};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use arrow::ipc::reader::StreamReader;
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{
    lit, ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::sort_properties::ExprProperties;
use datafusion_macros::user_doc;
use datafusion_python::context::PySessionContext;
use datafusion_python::expr::PyExpr;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyBytesMethods, PyTuple};
use unicode_normalization::UnicodeNormalization;

#[cfg(feature = "async-udf")]
use crate::udf_async;

const ENC_UTF8: i32 = 1;
const ENC_UTF16: i32 = 2;
const ENC_UTF32: i32 = 3;
const PART_SEPARATOR: &str = "\u{001f}";
const NULL_SENTINEL: &str = "__NULL__";
const DEFAULT_SPAN_COL_UNIT: &str = "byte";
const DEFAULT_NORMALIZE_FORM: &str = "NFKC";

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
    let mut reader = StreamReader::try_new(std::io::Cursor::new(payload), None)
        .map_err(|err| DataFusionError::Plan(format!("Failed to open policy IPC stream: {err}")))?;
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

fn bool_field<'a>(array: &'a StructArray, name: &str) -> Result<&'a arrow::array::BooleanArray> {
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
        return Err(DataFusionError::Plan(
            "Boolean value cannot be null.".into(),
        ));
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
        #[cfg(feature = "async-udf")]
        {
            udf_async::register_async_udfs(ctx)?;
        }
        #[cfg(not(feature = "async-udf"))]
        {
            return Err(DataFusionError::Plan(
                "Async UDFs require the async-udf feature".into(),
            ));
        }
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
        "stable_hash128" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            StableHash128Udf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "prefixed_hash64" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            PrefixedHash64Udf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "stable_id" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(StableIdUdf {
            signature: SignatureEqHash::new(signature),
        }))),
        "stable_id_parts" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            StableIdPartsUdf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "prefixed_hash_parts64" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            PrefixedHashParts64Udf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "stable_hash_any" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            StableHashAnyUdf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "span_make" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(SpanMakeUdf {
            signature: SignatureEqHash::new(signature),
        }))),
        "span_len" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(SpanLenUdf {
            signature: SignatureEqHash::new(signature),
        }))),
        "interval_align_score" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            IntervalAlignScoreUdf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "span_overlaps" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(SpanOverlapsUdf {
            signature: SignatureEqHash::new(signature),
        }))),
        "span_contains" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(SpanContainsUdf {
            signature: SignatureEqHash::new(signature),
        }))),
        "span_id" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(SpanIdUdf {
            signature: SignatureEqHash::new(signature),
        }))),
        "utf8_normalize" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            Utf8NormalizeUdf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "utf8_null_if_blank" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            Utf8NullIfBlankUdf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "qname_normalize" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            QNameNormalizeUdf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "map_get_default" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            MapGetDefaultUdf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "map_normalize" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(MapNormalizeUdf {
            signature: SignatureEqHash::new(signature),
        }))),
        "list_compact" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(ListCompactUdf {
            signature: SignatureEqHash::new(signature),
        }))),
        "list_unique_sorted" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            ListUniqueSortedUdf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "struct_pick" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(StructPickUdf {
            signature: SignatureEqHash::new(signature),
        }))),
        "cdf_change_rank" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            CdfChangeRankUdf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "cdf_is_upsert" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(CdfIsUpsertUdf {
            signature: SignatureEqHash::new(signature),
        }))),
        "cdf_is_delete" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(CdfIsDeleteUdf {
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
        let names = primitive
            .params
            .iter()
            .map(|param| param.name.clone())
            .collect();
        return signature.with_parameter_names(names).map_err(|err| {
            DataFusionError::Plan(format!(
                "Invalid parameter names for {}: {err}",
                primitive.name
            ))
        });
    }
    Ok(signature)
}

fn signature_from_arg_types(arg_types: Vec<DataType>, volatility: Volatility) -> Signature {
    let has_string = arg_types
        .iter()
        .any(|dtype| matches!(dtype, DataType::Utf8));
    if arg_types
        .iter()
        .all(|dtype| matches!(dtype, DataType::Utf8))
    {
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
    match signature.clone().with_parameter_names(name_vec) {
        Ok(signature) => signature,
        Err(_) => signature,
    }
}

fn string_int_string_signature(volatility: Volatility) -> Signature {
    let arg_types = vec![DataType::Utf8, DataType::Int64, DataType::Utf8];
    Signature::one_of(expand_string_signatures(&arg_types), volatility)
}

fn user_defined_signature(volatility: Volatility) -> Signature {
    Signature::one_of(vec![TypeSignature::UserDefined], volatility)
}

fn variadic_any_signature(_min_args: usize, _max_args: usize, volatility: Volatility) -> Signature {
    user_defined_signature(volatility)
}

fn expect_arg_len(name: &str, arg_types: &[DataType], min: usize, max: usize) -> Result<usize> {
    let count = arg_types.len();
    if count < min || count > max {
        return Err(DataFusionError::Plan(format!(
            "{name} expects between {min} and {max} arguments"
        )));
    }
    Ok(count)
}

fn expect_arg_len_exact(name: &str, arg_types: &[DataType], expected: usize) -> Result<()> {
    if arg_types.len() != expected {
        return Err(DataFusionError::Plan(format!(
            "{name} expects {expected} arguments"
        )));
    }
    Ok(())
}

fn ensure_struct_arg(name: &str, arg_type: &DataType) -> Result<()> {
    if matches!(arg_type, DataType::Struct(_)) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} expects a struct input"
        )))
    }
}

fn ensure_map_arg(name: &str, arg_type: &DataType) -> Result<()> {
    if matches!(arg_type, DataType::Map(_, _)) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!("{name} expects a map input")))
    }
}

fn ensure_list_arg(name: &str, arg_type: &DataType) -> Result<()> {
    if matches!(arg_type, DataType::List(_)) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!("{name} expects a list input")))
    }
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

fn literal_scalar(expr: &Expr) -> Option<&ScalarValue> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    Some(value)
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

fn scalar_to_string(value: &ScalarValue) -> Result<Option<String>> {
    match value {
        ScalarValue::Null => Ok(None),
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
            Ok(v.clone())
        }
        ScalarValue::Int8(v) => Ok(v.map(|x| x.to_string())),
        ScalarValue::Int16(v) => Ok(v.map(|x| x.to_string())),
        ScalarValue::Int32(v) => Ok(v.map(|x| x.to_string())),
        ScalarValue::Int64(v) => Ok(v.map(|x| x.to_string())),
        ScalarValue::UInt8(v) => Ok(v.map(|x| x.to_string())),
        ScalarValue::UInt16(v) => Ok(v.map(|x| x.to_string())),
        ScalarValue::UInt32(v) => Ok(v.map(|x| x.to_string())),
        ScalarValue::UInt64(v) => Ok(v.map(|x| x.to_string())),
        ScalarValue::Float32(v) => Ok(v.map(|x| x.to_string())),
        ScalarValue::Float64(v) => Ok(v.map(|x| x.to_string())),
        ScalarValue::Boolean(v) => Ok(v.map(|x| x.to_string())),
        other => Ok(Some(other.to_string())),
    }
}

fn scalar_to_string_with_sentinel(value: &ScalarValue, sentinel: &str) -> Result<String> {
    Ok(scalar_to_string(value)?.unwrap_or_else(|| sentinel.to_string()))
}

fn scalar_to_i64(value: &ScalarValue, context: &str) -> Result<Option<i64>> {
    match value {
        ScalarValue::Null => Ok(None),
        ScalarValue::Int8(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::Int16(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::Int32(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::Int64(v) => Ok(*v),
        ScalarValue::UInt8(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::UInt16(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::UInt32(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::UInt64(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::Float32(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::Float64(v) => Ok(v.map(|x| x as i64)),
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
            if let Some(text) = v {
                let parsed = text.parse::<i64>().map_err(|err| {
                    DataFusionError::Plan(format!("{context}: failed to parse integer: {err}"))
                })?;
                Ok(Some(parsed))
            } else {
                Ok(None)
            }
        }
        other => Err(DataFusionError::Plan(format!(
            "{context}: unsupported integer conversion from {other:?}"
        ))),
    }
}

fn scalar_to_i32(value: &ScalarValue, context: &str) -> Result<Option<i32>> {
    scalar_to_i64(value, context).map(|opt| opt.map(|x| x as i32))
}

fn scalar_to_bool(value: &ScalarValue, context: &str) -> Result<Option<bool>> {
    match value {
        ScalarValue::Null => Ok(None),
        ScalarValue::Boolean(v) => Ok(*v),
        ScalarValue::Int8(v) => Ok(v.map(|x| x != 0)),
        ScalarValue::Int16(v) => Ok(v.map(|x| x != 0)),
        ScalarValue::Int32(v) => Ok(v.map(|x| x != 0)),
        ScalarValue::Int64(v) => Ok(v.map(|x| x != 0)),
        ScalarValue::UInt8(v) => Ok(v.map(|x| x != 0)),
        ScalarValue::UInt16(v) => Ok(v.map(|x| x != 0)),
        ScalarValue::UInt32(v) => Ok(v.map(|x| x != 0)),
        ScalarValue::UInt64(v) => Ok(v.map(|x| x != 0)),
        ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v) | ScalarValue::Utf8View(v) => {
            if let Some(text) = v {
                let normalized = text.trim().to_ascii_lowercase();
                if normalized.is_empty() {
                    return Ok(None);
                }
                if matches!(normalized.as_str(), "true" | "t" | "1" | "yes" | "y") {
                    return Ok(Some(true));
                }
                if matches!(normalized.as_str(), "false" | "f" | "0" | "no" | "n") {
                    return Ok(Some(false));
                }
                Err(DataFusionError::Plan(format!(
                    "{context}: unsupported boolean literal {text:?}"
                )))
            } else {
                Ok(None)
            }
        }
        other => Err(DataFusionError::Plan(format!(
            "{context}: unsupported boolean conversion from {other:?}"
        ))),
    }
}

fn columnar_to_strings(
    value: &ColumnarValue,
    num_rows: usize,
    sentinel: &str,
    context: &str,
) -> Result<Vec<String>> {
    match value {
        ColumnarValue::Scalar(v) => {
            let text = scalar_to_string_with_sentinel(v, sentinel)?;
            Ok(vec![text; num_rows])
        }
        ColumnarValue::Array(array) => {
            let mut out = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                let scalar = ScalarValue::try_from_array(array, row)?;
                out.push(scalar_to_string_with_sentinel(&scalar, sentinel)?);
            }
            if out.len() != num_rows {
                return Err(DataFusionError::Plan(format!(
                    "{context}: string conversion produced incorrect row count"
                )));
            }
            Ok(out)
        }
    }
}

fn columnar_to_optional_strings(
    value: &ColumnarValue,
    num_rows: usize,
    context: &str,
) -> Result<Vec<Option<String>>> {
    match value {
        ColumnarValue::Scalar(v) => {
            let converted = scalar_to_string(v)?;
            Ok(vec![converted; num_rows])
        }
        ColumnarValue::Array(array) => {
            let mut out = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                let scalar = ScalarValue::try_from_array(array, row)?;
                out.push(scalar_to_string(&scalar)?);
            }
            if out.len() != num_rows {
                return Err(DataFusionError::Plan(format!(
                    "{context}: string conversion produced incorrect row count"
                )));
            }
            Ok(out)
        }
    }
}

fn columnar_to_i64(
    value: &ColumnarValue,
    num_rows: usize,
    context: &str,
) -> Result<Vec<Option<i64>>> {
    match value {
        ColumnarValue::Scalar(v) => {
            let converted = scalar_to_i64(v, context)?;
            Ok(vec![converted; num_rows])
        }
        ColumnarValue::Array(array) => {
            let mut out = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                let scalar = ScalarValue::try_from_array(array, row)?;
                out.push(scalar_to_i64(&scalar, context)?);
            }
            Ok(out)
        }
    }
}

fn columnar_to_i32(
    value: &ColumnarValue,
    num_rows: usize,
    context: &str,
) -> Result<Vec<Option<i32>>> {
    match value {
        ColumnarValue::Scalar(v) => {
            let converted = scalar_to_i32(v, context)?;
            Ok(vec![converted; num_rows])
        }
        ColumnarValue::Array(array) => {
            let mut out = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                let scalar = ScalarValue::try_from_array(array, row)?;
                out.push(scalar_to_i32(&scalar, context)?);
            }
            Ok(out)
        }
    }
}

fn columnar_to_bool(
    value: &ColumnarValue,
    num_rows: usize,
    context: &str,
) -> Result<Vec<Option<bool>>> {
    match value {
        ColumnarValue::Scalar(v) => {
            let converted = scalar_to_bool(v, context)?;
            Ok(vec![converted; num_rows])
        }
        ColumnarValue::Array(array) => {
            let mut out = Vec::with_capacity(num_rows);
            for row in 0..num_rows {
                let scalar = ScalarValue::try_from_array(array, row)?;
                out.push(scalar_to_bool(&scalar, context)?);
            }
            Ok(out)
        }
    }
}

fn span_struct_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("bstart", DataType::Int64, true),
        Field::new("bend", DataType::Int64, true),
        Field::new("line_base", DataType::Int32, true),
        Field::new("col_unit", DataType::Utf8, true),
        Field::new("end_exclusive", DataType::Boolean, true),
    ]))
}

fn span_metadata_from_scalars(args: &ReturnFieldArgs) -> Result<HashMap<String, String>> {
    let mut metadata = HashMap::new();
    metadata.insert("semantic_type".to_string(), "Span".to_string());
    let line_base = match scalar_argument(args, 2) {
        Some(value) => scalar_to_i32(value, "span_make line_base metadata")?.unwrap_or(0),
        None => 0,
    };
    metadata.insert("line_base".to_string(), line_base.to_string());
    let col_unit = match scalar_argument(args, 3) {
        Some(value) => scalar_to_string(value)?
            .unwrap_or_else(|| DEFAULT_SPAN_COL_UNIT.to_string()),
        None => DEFAULT_SPAN_COL_UNIT.to_string(),
    };
    metadata.insert("col_unit".to_string(), col_unit);
    let end_exclusive = match scalar_argument(args, 4) {
        Some(value) => scalar_to_bool(value, "span_make end_exclusive metadata")?.unwrap_or(true),
        None => true,
    };
    metadata.insert("end_exclusive".to_string(), end_exclusive.to_string());
    Ok(metadata)
}

fn scalar_argument<'a>(args: &ReturnFieldArgs<'a>, index: usize) -> Option<&'a ScalarValue> {
    args.scalar_arguments.get(index).and_then(|value| *value)
}

fn scalar_argument_string(
    args: &ReturnFieldArgs,
    index: usize,
    context: &str,
) -> Result<Option<String>> {
    let Some(value) = scalar_argument(args, index) else {
        return Ok(None);
    };
    scalar_to_string(value).map_err(|err| {
        DataFusionError::Plan(format!("{context}: failed to read scalar argument: {err}"))
    })
}

fn scalar_columnar_value(value: &ColumnarValue, context: &str) -> Result<ScalarValue> {
    match value {
        ColumnarValue::Scalar(v) => Ok(v.clone()),
        ColumnarValue::Array(_) => Err(DataFusionError::Plan(context.to_string())),
    }
}

fn collapse_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalize_unicode_form(value: &str, form: &str) -> String {
    let normalized_form = form.trim().to_ascii_uppercase();
    match normalized_form.as_str() {
        "NFC" => value.nfc().collect(),
        "NFD" => value.nfd().collect(),
        "NFKD" => value.nfkd().collect(),
        _ => value.nfkc().collect(),
    }
}

fn normalize_text(value: &str, form: &str, casefold: bool, collapse_ws: bool) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let normalized = normalize_unicode_form(trimmed, form);
    let cased = if casefold {
        normalized.to_lowercase()
    } else {
        normalized
    };
    if collapse_ws {
        collapse_whitespace(&cased)
    } else {
        cased
    }
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

#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Pass-through CPG score function for compatibility.",
    syntax_example = "cpg_score(value)",
    argument(name = "value", description = "Score value to pass through.")
)]
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
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 65 {
            return Err(DataFusionError::Plan(
                "stable_id_parts expects between two and sixty-five arguments".into(),
            ));
        }
        Ok(vec![DataType::Utf8; arg_types.len()])
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .map(|field| field.is_nullable())
            .unwrap_or(true);
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Float64,
            nullable,
        )))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        if let Some(interval) = inputs.first() {
            return Ok((*interval).clone());
        }
        Interval::make_unbounded(&DataType::Null)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        if inputs.len() == 1 {
            return Ok(Some(vec![interval.clone()]));
        }
        Ok(Some(Vec::new()))
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        if let Some(props) = inputs.first() {
            return Ok(props.preserves_lex_ordering);
        }
        Ok(true)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "cpg_score expects one argument".into(),
            ));
        };
        match value {
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(value.clone())),
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::clone(array))),
        }
    }
}

#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Extract Arrow field metadata. When a key is provided, returns a single metadata value; otherwise returns a map of all metadata entries.",
    syntax_example = "arrow_metadata(expr [, key])",
    argument(
        name = "expr",
        description = "Expression whose field metadata should be inspected."
    ),
    argument(name = "key", description = "Optional metadata key to extract.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArrowMetadataUdf {
    signature: SignatureEqHash,
}

impl ArrowMetadataUdf {
    fn new() -> Self {
        let signature = signature_with_names(
            user_defined_signature(Volatility::Immutable),
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
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        match arg_types.len() {
            1 => Ok(vec![arg_types[0].clone()]),
            2 => Ok(vec![arg_types[0].clone(), DataType::Utf8]),
            _ => Err(DataFusionError::Plan(
                "arrow_metadata requires 1 or 2 arguments".into(),
            )),
        }
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
            let mut map_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
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

pub fn cpg_score_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::numeric(1, Volatility::Immutable),
        &["value"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(CpgScoreUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn stable_hash64_udf() -> ScalarUDF {
    let signature = signature_with_names(Signature::string(1, Volatility::Immutable), &["value"]);
    ScalarUDF::new_from_shared_impl(Arc::new(StableHash64Udf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn stable_hash128_udf() -> ScalarUDF {
    let signature = signature_with_names(Signature::string(1, Volatility::Immutable), &["value"]);
    ScalarUDF::new_from_shared_impl(Arc::new(StableHash128Udf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn prefixed_hash64_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::string(2, Volatility::Immutable),
        &["prefix", "value"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(PrefixedHash64Udf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn stable_id_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::string(2, Volatility::Immutable),
        &["prefix", "value"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(StableIdUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn stable_id_parts_udf() -> ScalarUDF {
    let signature = variadic_any_signature(2, 65, Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(StableIdPartsUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn prefixed_hash_parts64_udf() -> ScalarUDF {
    let signature = variadic_any_signature(2, 65, Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(PrefixedHashParts64Udf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn stable_hash_any_udf() -> ScalarUDF {
    let signature = variadic_any_signature(1, 3, Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(StableHashAnyUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn span_make_udf() -> ScalarUDF {
    let signature = variadic_any_signature(2, 5, Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(SpanMakeUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn span_len_udf() -> ScalarUDF {
    let signature = user_defined_signature(Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(SpanLenUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn interval_align_score_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::one_of(vec![TypeSignature::Any(4)], Volatility::Immutable),
        &["left_start", "left_end", "right_start", "right_end"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(IntervalAlignScoreUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn span_overlaps_udf() -> ScalarUDF {
    let signature = user_defined_signature(Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(SpanOverlapsUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn span_contains_udf() -> ScalarUDF {
    let signature = user_defined_signature(Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(SpanContainsUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn span_id_udf() -> ScalarUDF {
    let signature = variadic_any_signature(4, 5, Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(SpanIdUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn utf8_normalize_udf() -> ScalarUDF {
    let signature = variadic_any_signature(1, 4, Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(Utf8NormalizeUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn utf8_null_if_blank_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::one_of(
            expand_string_signatures(&[DataType::Utf8]),
            Volatility::Immutable,
        ),
        &["value"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(Utf8NullIfBlankUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn qname_normalize_udf() -> ScalarUDF {
    let signature = variadic_any_signature(1, 3, Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(QNameNormalizeUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn map_get_default_udf() -> ScalarUDF {
    let signature = user_defined_signature(Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(MapGetDefaultUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn map_normalize_udf() -> ScalarUDF {
    let signature = variadic_any_signature(1, 3, Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(MapNormalizeUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn list_compact_udf() -> ScalarUDF {
    let signature = user_defined_signature(Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(ListCompactUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn list_unique_sorted_udf() -> ScalarUDF {
    let signature = user_defined_signature(Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(ListUniqueSortedUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn struct_pick_udf() -> ScalarUDF {
    let signature = variadic_any_signature(2, 7, Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(StructPickUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn cdf_change_rank_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::one_of(
            expand_string_signatures(&[DataType::Utf8]),
            Volatility::Immutable,
        ),
        &["change_type"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(CdfChangeRankUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn cdf_is_upsert_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::one_of(
            expand_string_signatures(&[DataType::Utf8]),
            Volatility::Immutable,
        ),
        &["change_type"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(CdfIsUpsertUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn cdf_is_delete_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::one_of(
            expand_string_signatures(&[DataType::Utf8]),
            Volatility::Immutable,
        ),
        &["change_type"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(CdfIsDeleteUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn col_to_byte_udf() -> ScalarUDF {
    let signature = signature_with_names(
        string_int_string_signature(Volatility::Immutable),
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

fn push_optional_expr(args: &mut Vec<Expr>, value: Option<PyExpr>) {
    if let Some(expr) = value {
        args.push(expr.into());
    }
}

fn extend_expr_args_from_tuple(args: &mut Vec<Expr>, parts: &Bound<'_, PyTuple>) -> PyResult<()> {
    for item in parts.iter() {
        let expr: PyExpr = item.extract()?;
        args.push(expr.into());
    }
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (prefix, part1, *parts))]
pub fn stable_id_parts(
    prefix: &str,
    part1: PyExpr,
    parts: &Bound<'_, PyTuple>,
) -> PyResult<PyExpr> {
    let mut args: Vec<Expr> = vec![lit(prefix), part1.into()];
    extend_expr_args_from_tuple(&mut args, parts)?;
    Ok(stable_id_parts_udf().call(args).into())
}

#[pyfunction]
#[pyo3(signature = (prefix, part1, *parts))]
pub fn prefixed_hash_parts64(
    prefix: &str,
    part1: PyExpr,
    parts: &Bound<'_, PyTuple>,
) -> PyResult<PyExpr> {
    let mut args: Vec<Expr> = vec![lit(prefix), part1.into()];
    extend_expr_args_from_tuple(&mut args, parts)?;
    Ok(prefixed_hash_parts64_udf().call(args).into())
}

#[pyfunction]
#[pyo3(signature = (value, canonical=None, null_sentinel=None))]
pub fn stable_hash_any(
    value: PyExpr,
    canonical: Option<bool>,
    null_sentinel: Option<&str>,
) -> PyExpr {
    let mut args: Vec<Expr> = vec![value.into()];
    if let Some(flag) = canonical {
        args.push(lit(flag));
    }
    if let Some(sentinel) = null_sentinel {
        args.push(lit(sentinel));
    }
    stable_hash_any_udf().call(args).into()
}

#[pyfunction]
#[pyo3(signature = (bstart, bend, line_base=None, col_unit=None, end_exclusive=None))]
pub fn span_make(
    bstart: PyExpr,
    bend: PyExpr,
    line_base: Option<PyExpr>,
    col_unit: Option<PyExpr>,
    end_exclusive: Option<PyExpr>,
) -> PyExpr {
    let mut args: Vec<Expr> = vec![bstart.into(), bend.into()];
    push_optional_expr(&mut args, line_base);
    push_optional_expr(&mut args, col_unit);
    push_optional_expr(&mut args, end_exclusive);
    span_make_udf().call(args).into()
}

#[pyfunction]
pub fn span_len(span: PyExpr) -> PyExpr {
    span_len_udf().call(vec![span.into()]).into()
}

#[pyfunction]
pub fn span_overlaps(span_a: PyExpr, span_b: PyExpr) -> PyExpr {
    span_overlaps_udf()
        .call(vec![span_a.into(), span_b.into()])
        .into()
}

#[pyfunction]
pub fn span_contains(span_a: PyExpr, span_b: PyExpr) -> PyExpr {
    span_contains_udf()
        .call(vec![span_a.into(), span_b.into()])
        .into()
}

#[pyfunction]
pub fn interval_align_score(
    left_start: PyExpr,
    left_end: PyExpr,
    right_start: PyExpr,
    right_end: PyExpr,
) -> PyExpr {
    interval_align_score_udf()
        .call(vec![
            left_start.into(),
            left_end.into(),
            right_start.into(),
            right_end.into(),
        ])
        .into()
}

#[pyfunction]
#[pyo3(signature = (prefix, path, bstart, bend, kind=None))]
pub fn span_id(
    prefix: &str,
    path: PyExpr,
    bstart: PyExpr,
    bend: PyExpr,
    kind: Option<PyExpr>,
) -> PyExpr {
    let mut args: Vec<Expr> = vec![lit(prefix), path.into(), bstart.into(), bend.into()];
    push_optional_expr(&mut args, kind);
    span_id_udf().call(args).into()
}

#[pyfunction]
#[pyo3(signature = (value, form=None, casefold=None, collapse_ws=None))]
pub fn utf8_normalize(
    value: PyExpr,
    form: Option<&str>,
    casefold: Option<bool>,
    collapse_ws: Option<bool>,
) -> PyExpr {
    let mut args: Vec<Expr> = vec![value.into()];
    if let Some(form) = form {
        args.push(lit(form));
    }
    if let Some(flag) = casefold {
        args.push(lit(flag));
    }
    if let Some(flag) = collapse_ws {
        args.push(lit(flag));
    }
    utf8_normalize_udf().call(args).into()
}

#[pyfunction]
pub fn utf8_null_if_blank(value: PyExpr) -> PyExpr {
    utf8_null_if_blank_udf().call(vec![value.into()]).into()
}

#[pyfunction]
#[pyo3(signature = (symbol, module=None, lang=None))]
pub fn qname_normalize(symbol: PyExpr, module: Option<PyExpr>, lang: Option<PyExpr>) -> PyExpr {
    let mut args: Vec<Expr> = vec![symbol.into()];
    push_optional_expr(&mut args, module);
    push_optional_expr(&mut args, lang);
    qname_normalize_udf().call(args).into()
}

#[pyfunction]
pub fn map_get_default(map_expr: PyExpr, key: &str, default_value: PyExpr) -> PyExpr {
    map_get_default_udf()
        .call(vec![map_expr.into(), lit(key), default_value.into()])
        .into()
}

#[pyfunction]
#[pyo3(signature = (map_expr, key_case=None, sort_keys=None))]
pub fn map_normalize(map_expr: PyExpr, key_case: Option<&str>, sort_keys: Option<bool>) -> PyExpr {
    let mut args: Vec<Expr> = vec![map_expr.into()];
    if let Some(key_case) = key_case {
        args.push(lit(key_case));
    }
    if let Some(sort_keys) = sort_keys {
        args.push(lit(sort_keys));
    }
    map_normalize_udf().call(args).into()
}

#[pyfunction]
pub fn list_compact(list_expr: PyExpr) -> PyExpr {
    list_compact_udf().call(vec![list_expr.into()]).into()
}

#[pyfunction]
pub fn list_unique_sorted(list_expr: PyExpr) -> PyExpr {
    list_unique_sorted_udf().call(vec![list_expr.into()]).into()
}

#[pyfunction]
#[pyo3(signature = (
    struct_expr,
    field1,
    field2=None,
    field3=None,
    field4=None,
    field5=None,
    field6=None
))]
pub fn struct_pick(
    struct_expr: PyExpr,
    field1: &str,
    field2: Option<&str>,
    field3: Option<&str>,
    field4: Option<&str>,
    field5: Option<&str>,
    field6: Option<&str>,
) -> PyExpr {
    let mut args: Vec<Expr> = vec![struct_expr.into(), lit(field1)];
    if let Some(field) = field2 {
        args.push(lit(field));
    }
    if let Some(field) = field3 {
        args.push(lit(field));
    }
    if let Some(field) = field4 {
        args.push(lit(field));
    }
    if let Some(field) = field5 {
        args.push(lit(field));
    }
    if let Some(field) = field6 {
        args.push(lit(field));
    }
    struct_pick_udf().call(args).into()
}

#[pyfunction]
pub fn cdf_change_rank(change_type: PyExpr) -> PyExpr {
    cdf_change_rank_udf().call(vec![change_type.into()]).into()
}

#[pyfunction]
pub fn cdf_is_upsert(change_type: PyExpr) -> PyExpr {
    cdf_is_upsert_udf().call(vec![change_type.into()]).into()
}

#[pyfunction]
pub fn cdf_is_delete(change_type: PyExpr) -> PyExpr {
    cdf_is_delete_udf().call(vec![change_type.into()]).into()
}

#[pyfunction]
pub fn col_to_byte(line_text: PyExpr, col_index: PyExpr, col_unit: PyExpr) -> PyExpr {
    col_to_byte_udf()
        .call(vec![line_text.into(), col_index.into(), col_unit.into()])
        .into()
}

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Compute a stable 64-bit hash of a string using Blake2b.",
    syntax_example = "stable_hash64(value)",
    standard_argument(name = "value", prefix = "String")
)]
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
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 65 {
            return Err(DataFusionError::Plan(
                "prefixed_hash_parts64 expects between two and sixty-five arguments".into(),
            ));
        }
        Ok(vec![DataType::Utf8; arg_types.len()])
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
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "stable_hash64 expects one argument".into(),
            ));
        };
        match value {
            ColumnarValue::Scalar(value) => {
                let hashed =
                    scalar_str(value, "stable_hash64 expects string input")?.map(hash64_value);
                Ok(ColumnarValue::Scalar(ScalarValue::Int64(hashed)))
            }
            ColumnarValue::Array(array) => {
                let input = string_array_any(array, "stable_hash64 expects string input")?;
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
    }
}

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Compute a stable 128-bit hash of a string using Blake2b.",
    syntax_example = "stable_hash128(value)",
    standard_argument(name = "value", prefix = "String")
)]
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
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 3 {
            return Err(DataFusionError::Plan(
                "stable_hash_any expects between one and three arguments".into(),
            ));
        }
        let mut coerced = Vec::with_capacity(arg_types.len());
        coerced.push(DataType::Utf8);
        if arg_types.len() >= 2 {
            coerced.push(DataType::Boolean);
        }
        if arg_types.len() >= 3 {
            coerced.push(DataType::Utf8);
        }
        Ok(coerced)
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
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "stable_hash128 expects one argument".into(),
            ));
        };
        match value {
            ColumnarValue::Scalar(value) => {
                let hashed =
                    scalar_str(value, "stable_hash128 expects string input")?.map(hash128_value);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(hashed)))
            }
            ColumnarValue::Array(array) => {
                let input = string_array_any(array, "stable_hash128 expects string input")?;
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
    }
}

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Compute a stable 64-bit hash of a string and prefix the result with a namespace.",
    syntax_example = "prefixed_hash64(prefix, value)",
    argument(
        name = "prefix",
        description = "Namespace prefix to prepend to the hash."
    ),
    standard_argument(name = "value", prefix = "String")
)]
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
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        expect_arg_len_exact(self.name(), arg_types, 1)?;
        ensure_struct_arg(self.name(), &arg_types[0])?;
        Ok(vec![arg_types[0].clone()])
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
        let [prefix_value, input_value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "prefixed_hash64 expects two arguments".into(),
            ));
        };
        match (prefix_value, input_value) {
            (ColumnarValue::Scalar(prefix), ColumnarValue::Scalar(value)) => {
                let prefix = scalar_str(prefix, "prefixed_hash64 expects string prefix input")?;
                let value = scalar_str(value, "prefixed_hash64 expects string value input")?;
                let hashed = match (prefix, value) {
                    (Some(prefix), Some(value)) => Some(prefixed_hash64_value(prefix, value)),
                    _ => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(hashed)))
            }
            (ColumnarValue::Array(prefixes), ColumnarValue::Array(values)) => {
                let prefixes =
                    string_array_any(prefixes, "prefixed_hash64 expects string prefix input")?;
                let values =
                    string_array_any(values, "prefixed_hash64 expects string value input")?;
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
            (ColumnarValue::Array(prefixes), ColumnarValue::Scalar(value)) => {
                let prefixes =
                    string_array_any(prefixes, "prefixed_hash64 expects string prefix input")?;
                let value = scalar_str(value, "prefixed_hash64 expects string value input")?;
                let mut builder = StringBuilder::with_capacity(prefixes.len(), prefixes.len() * 16);
                for index in 0..prefixes.len() {
                    if prefixes.is_null(index) {
                        builder.append_null();
                        continue;
                    }
                    let Some(value) = value else {
                        builder.append_null();
                        continue;
                    };
                    builder.append_value(prefixed_hash64_value(prefixes.value(index), value));
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            (ColumnarValue::Scalar(prefix), ColumnarValue::Array(values)) => {
                let prefix = scalar_str(prefix, "prefixed_hash64 expects string prefix input")?;
                let values =
                    string_array_any(values, "prefixed_hash64 expects string value input")?;
                let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 16);
                for index in 0..values.len() {
                    if values.is_null(index) {
                        builder.append_null();
                        continue;
                    }
                    let Some(prefix) = prefix else {
                        builder.append_null();
                        continue;
                    };
                    builder.append_value(prefixed_hash64_value(prefix, values.value(index)));
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
        }
    }
}

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Compute a stable identifier by prefixing a 128-bit hash of a string.",
    syntax_example = "stable_id(prefix, value)",
    argument(
        name = "prefix",
        description = "Namespace prefix to prepend to the hash."
    ),
    standard_argument(name = "value", prefix = "String")
)]
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
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(4..=5).contains(&arg_types.len()) {
            return Err(DataFusionError::Plan(
                "span_id expects four or five arguments".into(),
            ));
        }
        Ok(vec![DataType::Utf8; arg_types.len()])
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
        let [prefix_value, input_value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "stable_id expects two arguments".into(),
            ));
        };
        match (prefix_value, input_value) {
            (ColumnarValue::Scalar(prefix), ColumnarValue::Scalar(value)) => {
                let prefix = scalar_str(prefix, "stable_id expects string prefix input")?;
                let value = scalar_str(value, "stable_id expects string value input")?;
                let hashed = match (prefix, value) {
                    (Some(prefix), Some(value)) => Some(stable_id_value(prefix, value)),
                    _ => None,
                };
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(hashed)))
            }
            (ColumnarValue::Array(prefixes), ColumnarValue::Array(values)) => {
                let prefixes = string_array_any(prefixes, "stable_id expects string prefix input")?;
                let values = string_array_any(values, "stable_id expects string value input")?;
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
            (ColumnarValue::Array(prefixes), ColumnarValue::Scalar(value)) => {
                let prefixes = string_array_any(prefixes, "stable_id expects string prefix input")?;
                let value = scalar_str(value, "stable_id expects string value input")?;
                let mut builder = StringBuilder::with_capacity(prefixes.len(), prefixes.len() * 32);
                for index in 0..prefixes.len() {
                    if prefixes.is_null(index) {
                        builder.append_null();
                        continue;
                    }
                    let Some(value) = value else {
                        builder.append_null();
                        continue;
                    };
                    builder.append_value(stable_id_value(prefixes.value(index), value));
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
            (ColumnarValue::Scalar(prefix), ColumnarValue::Array(values)) => {
                let prefix = scalar_str(prefix, "stable_id expects string prefix input")?;
                let values = string_array_any(values, "stable_id expects string value input")?;
                let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 32);
                for index in 0..values.len() {
                    if values.is_null(index) {
                        builder.append_null();
                        continue;
                    }
                    let Some(prefix) = prefix else {
                        builder.append_null();
                        continue;
                    };
                    builder.append_value(stable_id_value(prefix, values.value(index)));
                }
                Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
            }
        }
    }
}

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Compute a stable identifier from a prefix and variadic parts.",
    syntax_example = "stable_id_parts(prefix, part1, part2, ...)",
    argument(
        name = "prefix",
        description = "Namespace prefix to prepend to the hash."
    ),
    argument(
        name = "parts",
        description = "Variadic parts to join with a stable separator and hash."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct StableIdPartsUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for StableIdPartsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "stable_id_parts"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
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
        if args.len() < 2 {
            return Err(DataFusionError::Plan(
                "stable_id_parts expects at least two arguments".into(),
            ));
        }
        let mut scalars: Vec<&ScalarValue> = Vec::with_capacity(args.len());
        for expr in &args {
            let Some(value) = literal_scalar(expr) else {
                return Ok(ExprSimplifyResult::Original(args));
            };
            scalars.push(value);
        }
        let prefix = scalar_to_string(scalars[0])?;
        let Some(prefix) = prefix else {
            return Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Utf8(None))));
        };
        let mut parts: Vec<String> = Vec::with_capacity(scalars.len() - 1);
        for scalar in scalars.iter().skip(1) {
            parts.push(scalar_to_string_with_sentinel(scalar, NULL_SENTINEL)?);
        }
        let joined = parts.join(PART_SEPARATOR);
        let hashed = stable_id_value(&prefix, &joined);
        Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Utf8(
            Some(hashed),
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 {
            return Err(DataFusionError::Plan(
                "stable_id_parts expects at least two arguments".into(),
            ));
        }
        let num_rows = args.number_rows;
        let prefixes = columnar_to_optional_strings(
            &args.args[0],
            num_rows,
            "stable_id_parts expects string prefix input",
        )?;
        let mut part_columns: Vec<Vec<String>> = Vec::with_capacity(args.args.len() - 1);
        for value in args.args.iter().skip(1) {
            part_columns.push(columnar_to_strings(
                value,
                num_rows,
                NULL_SENTINEL,
                "stable_id_parts expects string-compatible part input",
            )?);
        }
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        for row in 0..num_rows {
            let Some(prefix) = prefixes[row].as_deref() else {
                builder.append_null();
                continue;
            };
            let mut joined = String::new();
            for (index, column) in part_columns.iter().enumerate() {
                if index > 0 {
                    joined.push_str(PART_SEPARATOR);
                }
                joined.push_str(&column[row]);
            }
            builder.append_value(stable_id_value(prefix, &joined));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Compute a prefixed stable 64-bit hash from variadic parts.",
    syntax_example = "prefixed_hash_parts64(prefix, part1, part2, ...)",
    argument(
        name = "prefix",
        description = "Namespace prefix to prepend to the hash."
    ),
    argument(
        name = "parts",
        description = "Variadic parts to join with a stable separator and hash."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct PrefixedHashParts64Udf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for PrefixedHashParts64Udf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "prefixed_hash_parts64"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
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
        if args.len() < 2 {
            return Err(DataFusionError::Plan(
                "prefixed_hash_parts64 expects at least two arguments".into(),
            ));
        }
        let mut scalars: Vec<&ScalarValue> = Vec::with_capacity(args.len());
        for expr in &args {
            let Some(value) = literal_scalar(expr) else {
                return Ok(ExprSimplifyResult::Original(args));
            };
            scalars.push(value);
        }
        let prefix = scalar_to_string(scalars[0])?;
        let Some(prefix) = prefix else {
            return Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Utf8(None))));
        };
        let mut parts: Vec<String> = Vec::with_capacity(scalars.len() - 1);
        for scalar in scalars.iter().skip(1) {
            parts.push(scalar_to_string_with_sentinel(scalar, NULL_SENTINEL)?);
        }
        let joined = parts.join(PART_SEPARATOR);
        let hashed = prefixed_hash64_value(&prefix, &joined);
        Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Utf8(
            Some(hashed),
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 {
            return Err(DataFusionError::Plan(
                "prefixed_hash_parts64 expects at least two arguments".into(),
            ));
        }
        let num_rows = args.number_rows;
        let prefixes = columnar_to_optional_strings(
            &args.args[0],
            num_rows,
            "prefixed_hash_parts64 expects string prefix input",
        )?;
        let mut part_columns: Vec<Vec<String>> = Vec::with_capacity(args.args.len() - 1);
        for value in args.args.iter().skip(1) {
            part_columns.push(columnar_to_strings(
                value,
                num_rows,
                NULL_SENTINEL,
                "prefixed_hash_parts64 expects string-compatible part input",
            )?);
        }
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 24);
        for row in 0..num_rows {
            let Some(prefix) = prefixes[row].as_deref() else {
                builder.append_null();
                continue;
            };
            let mut joined = String::new();
            for (index, column) in part_columns.iter().enumerate() {
                if index > 0 {
                    joined.push_str(PART_SEPARATOR);
                }
                joined.push_str(&column[row]);
            }
            builder.append_value(prefixed_hash64_value(prefix, &joined));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Compute a stable 128-bit hash from any value with optional canonicalization.",
    syntax_example = "stable_hash_any(value, canonical, null_sentinel)",
    argument(name = "value", description = "Value to hash."),
    argument(
        name = "canonical",
        description = "If true, apply default Unicode normalization and trimming."
    ),
    argument(
        name = "null_sentinel",
        description = "Sentinel string to use for null inputs."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct StableHashAnyUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for StableHashAnyUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "stable_hash_any"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
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
        if args.is_empty() {
            return Err(DataFusionError::Plan(
                "stable_hash_any expects at least one argument".into(),
            ));
        }
        let mut scalars: Vec<&ScalarValue> = Vec::with_capacity(args.len());
        for expr in &args {
            let Some(value) = literal_scalar(expr) else {
                return Ok(ExprSimplifyResult::Original(args));
            };
            scalars.push(value);
        }
        let canonical = scalars
            .get(1)
            .map(|value| scalar_to_bool(value, "stable_hash_any canonical flag"))
            .transpose()?
            .flatten()
            .unwrap_or(true);
        let null_sentinel = scalars
            .get(2)
            .map(|value| scalar_to_string(value))
            .transpose()?
            .flatten()
            .unwrap_or_else(|| NULL_SENTINEL.to_string());
        let value = scalar_to_string_with_sentinel(scalars[0], &null_sentinel)?;
        let canonical_value = if canonical {
            normalize_text(&value, DEFAULT_NORMALIZE_FORM, false, false)
        } else {
            value
        };
        let hashed = hash128_value(&canonical_value);
        Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Utf8(
            Some(hashed),
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Err(DataFusionError::Plan(
                "stable_hash_any expects at least one argument".into(),
            ));
        }
        let canonical = if args.args.len() >= 2 {
            let scalar = scalar_columnar_value(
                &args.args[1],
                "stable_hash_any canonical flag must be a scalar literal",
            )?;
            scalar_to_bool(&scalar, "stable_hash_any canonical flag")?.unwrap_or(true)
        } else {
            true
        };
        let null_sentinel = if args.args.len() >= 3 {
            let scalar = scalar_columnar_value(
                &args.args[2],
                "stable_hash_any null_sentinel must be a scalar literal",
            )?;
            scalar_to_string(&scalar)?.unwrap_or_else(|| NULL_SENTINEL.to_string())
        } else {
            NULL_SENTINEL.to_string()
        };
        let num_rows = args.number_rows;
        let values = columnar_to_strings(
            &args.args[0],
            num_rows,
            &null_sentinel,
            "stable_hash_any expects string-compatible input",
        )?;
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        for value in values {
            let canonical_value = if canonical {
                normalize_text(&value, DEFAULT_NORMALIZE_FORM, false, false)
            } else {
                value
            };
            builder.append_value(hash128_value(&canonical_value));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn span_int64_column<'a>(array: &'a StructArray, name: &str) -> Result<&'a Int64Array> {
    let column = array
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Plan(format!("Missing span field: {name}")))?;
    column
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Plan(format!("Span field {name} must be int64")))
}

fn span_bool_column<'a>(array: &'a StructArray, name: &str) -> Option<&'a BooleanArray> {
    let column = array.column_by_name(name)?;
    column.as_any().downcast_ref::<BooleanArray>()
}

fn span_end_exclusive(values: Option<&BooleanArray>, index: usize) -> bool {
    let Some(values) = values else {
        return true;
    };
    if values.is_null(index) {
        return true;
    }
    values.value(index)
}

fn adjusted_end(end: i64, exclusive: bool) -> i64 {
    if exclusive {
        end
    } else {
        end.saturating_add(1)
    }
}

#[user_doc(
    doc_section(label = "Span Functions"),
    description = "Create a normalized span struct from offsets and options.",
    syntax_example = "span_make(bstart, bend, line_base, col_unit, end_exclusive)",
    argument(name = "bstart", description = "Start byte offset."),
    argument(name = "bend", description = "End byte offset."),
    argument(name = "line_base", description = "Optional line base offset."),
    argument(name = "col_unit", description = "Optional column encoding unit."),
    argument(
        name = "end_exclusive",
        description = "Whether the end offset is exclusive."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct SpanMakeUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for SpanMakeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "span_make"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if !(2..=5).contains(&arg_types.len()) {
            return Err(DataFusionError::Plan(
                "span_make expects between two and five arguments".into(),
            ));
        }
        let mut coerced = Vec::with_capacity(arg_types.len());
        coerced.push(DataType::Int64);
        coerced.push(DataType::Int64);
        if arg_types.len() >= 3 {
            coerced.push(DataType::Int32);
        }
        if arg_types.len() >= 4 {
            coerced.push(DataType::Utf8);
        }
        if arg_types.len() >= 5 {
            coerced.push(DataType::Boolean);
        }
        Ok(coerced)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        let mut field = Field::new(self.name(), span_struct_type(), nullable);
        if let Some(first) = args.arg_fields.first() {
            if !first.metadata().is_empty() {
                field = field.with_metadata(first.metadata().clone());
                return Ok(Arc::new(field));
            }
        }
        let metadata = span_metadata_from_scalars(&args)?;
        if !metadata.is_empty() {
            field = field.with_metadata(metadata);
        }
        Ok(Arc::new(field))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(span_struct_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !(2..=5).contains(&args.args.len()) {
            return Err(DataFusionError::Plan(
                "span_make expects between two and five arguments".into(),
            ));
        }
        let num_rows = args.number_rows;
        let bstart_values = columnar_to_i64(
            &args.args[0],
            num_rows,
            "span_make bstart must be int64-compatible",
        )?;
        let bend_values = columnar_to_i64(
            &args.args[1],
            num_rows,
            "span_make bend must be int64-compatible",
        )?;
        let line_base_values = if args.args.len() >= 3 {
            columnar_to_i32(
                &args.args[2],
                num_rows,
                "span_make line_base must be int32-compatible",
            )?
        } else {
            vec![Some(0); num_rows]
        };
        let col_unit_values = if args.args.len() >= 4 {
            let values = columnar_to_optional_strings(
                &args.args[3],
                num_rows,
                "span_make col_unit must be string-compatible",
            )?;
            values
                .into_iter()
                .map(|value| value.unwrap_or_else(|| DEFAULT_SPAN_COL_UNIT.to_string()))
                .collect::<Vec<_>>()
        } else {
            vec![DEFAULT_SPAN_COL_UNIT.to_string(); num_rows]
        };
        let end_exclusive_values = if args.args.len() >= 5 {
            let values = columnar_to_bool(
                &args.args[4],
                num_rows,
                "span_make end_exclusive must be boolean-compatible",
            )?;
            values
                .into_iter()
                .map(|value| value.unwrap_or(true))
                .collect::<Vec<_>>()
        } else {
            vec![true; num_rows]
        };

        let mut bstart_builder = Int64Builder::with_capacity(num_rows);
        let mut bend_builder = Int64Builder::with_capacity(num_rows);
        let mut line_base_builder = Int32Builder::with_capacity(num_rows);
        let mut col_unit_builder = StringBuilder::with_capacity(num_rows, num_rows * 8);
        let mut end_exclusive_builder = BooleanBuilder::with_capacity(num_rows);
        for row in 0..num_rows {
            if let Some(value) = bstart_values[row] {
                bstart_builder.append_value(value);
            } else {
                bstart_builder.append_null();
            }
            if let Some(value) = bend_values[row] {
                bend_builder.append_value(value);
            } else {
                bend_builder.append_null();
            }
            if let Some(value) = line_base_values[row] {
                line_base_builder.append_value(value);
            } else {
                line_base_builder.append_null();
            }
            col_unit_builder.append_value(&col_unit_values[row]);
            end_exclusive_builder.append_value(end_exclusive_values[row]);
        }
        let return_field = args.return_field.as_ref();
        let span_fields = match return_field.data_type() {
            DataType::Struct(fields) => fields.clone(),
            other => {
                return Err(DataFusionError::Plan(format!(
                    "span_make return type must be struct, got {other:?}"
                )))
            }
        };
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(bstart_builder.finish()),
            Arc::new(bend_builder.finish()),
            Arc::new(line_base_builder.finish()),
            Arc::new(col_unit_builder.finish()),
            Arc::new(end_exclusive_builder.finish()),
        ];
        let span_array = StructArray::new(span_fields, arrays, None);
        Ok(ColumnarValue::Array(Arc::new(span_array) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Span Functions"),
    description = "Compute the length of a span.",
    syntax_example = "span_len(span)",
    argument(name = "span", description = "Span struct.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct SpanLenUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for SpanLenUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "span_len"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [span_value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "span_len expects one argument".into(),
            ));
        };
        let span_array = span_value.to_array(args.number_rows)?;
        let span_struct = span_array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Plan("span_len expects a struct input".into()))?;
        let bstart = span_int64_column(span_struct, "bstart")?;
        let bend = span_int64_column(span_struct, "bend")?;
        let mut builder = Int64Builder::with_capacity(span_struct.len());
        for row in 0..span_struct.len() {
            if bstart.is_null(row) || bend.is_null(row) {
                builder.append_null();
                continue;
            }
            builder.append_value(bend.value(row) - bstart.value(row));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Interval Functions"),
    description = "Compute alignment score for interval pairs.",
    syntax_example = "interval_align_score(left_start, left_end, right_start, right_end)",
    argument(name = "left_start", description = "Left interval start position."),
    argument(name = "left_end", description = "Left interval end position."),
    argument(name = "right_start", description = "Right interval start position."),
    argument(name = "right_end", description = "Right interval end position.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct IntervalAlignScoreUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for IntervalAlignScoreUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "interval_align_score"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .iter()
            .any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Float64, nullable)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left_start, left_end, right_start, right_end] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "interval_align_score expects four arguments".into(),
            ));
        };
        let num_rows = args.number_rows;
        let left_start_values =
            columnar_to_i64(left_start, num_rows, "interval_align_score left_start")?;
        let left_end_values =
            columnar_to_i64(left_end, num_rows, "interval_align_score left_end")?;
        let right_start_values =
            columnar_to_i64(right_start, num_rows, "interval_align_score right_start")?;
        let right_end_values =
            columnar_to_i64(right_end, num_rows, "interval_align_score right_end")?;
        let mut builder = Float64Builder::with_capacity(num_rows);
        for row in 0..num_rows {
            if left_start_values[row].is_none()
                || left_end_values[row].is_none()
                || right_start_values[row].is_none()
                || right_end_values[row].is_none()
            {
                builder.append_null();
                continue;
            }
            let length = right_end_values[row].unwrap() - right_start_values[row].unwrap();
            builder.append_value(-(length as f64));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Span Functions"),
    description = "Determine whether two spans overlap.",
    syntax_example = "span_overlaps(span_a, span_b)",
    argument(name = "span_a", description = "First span struct."),
    argument(name = "span_b", description = "Second span struct.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct SpanOverlapsUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for SpanOverlapsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "span_overlaps"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(
                "span_overlaps expects two arguments".into(),
            ));
        }
        for arg_type in arg_types {
            if !matches!(arg_type, DataType::Struct(_)) {
                return Err(DataFusionError::Plan(
                    "span_overlaps expects struct inputs".into(),
                ));
            }
        }
        Ok(vec![arg_types[0].clone(), arg_types[1].clone()])
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, true)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left_value, right_value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "span_overlaps expects two arguments".into(),
            ));
        };
        let left_array = left_value.to_array(args.number_rows)?;
        let right_array = right_value.to_array(args.number_rows)?;
        let left_struct = left_array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Plan("span_overlaps expects struct inputs".into()))?;
        let right_struct = right_array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Plan("span_overlaps expects struct inputs".into()))?;

        let left_start = span_int64_column(left_struct, "bstart")?;
        let left_end = span_int64_column(left_struct, "bend")?;
        let right_start = span_int64_column(right_struct, "bstart")?;
        let right_end = span_int64_column(right_struct, "bend")?;
        let left_exclusive = span_bool_column(left_struct, "end_exclusive");
        let right_exclusive = span_bool_column(right_struct, "end_exclusive");

        let len = args.number_rows;
        if left_struct.len() != len || right_struct.len() != len {
            return Err(DataFusionError::Plan(
                "span_overlaps input lengths must match".into(),
            ));
        }
        let mut builder = BooleanBuilder::with_capacity(len);
        for row in 0..len {
            if left_start.is_null(row)
                || left_end.is_null(row)
                || right_start.is_null(row)
                || right_end.is_null(row)
            {
                builder.append_null();
                continue;
            }
            let left_adjusted_end =
                adjusted_end(left_end.value(row), span_end_exclusive(left_exclusive, row));
            let right_adjusted_end = adjusted_end(
                right_end.value(row),
                span_end_exclusive(right_exclusive, row),
            );
            let overlaps = left_start.value(row) < right_adjusted_end
                && right_start.value(row) < left_adjusted_end;
            builder.append_value(overlaps);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Span Functions"),
    description = "Determine whether one span fully contains another.",
    syntax_example = "span_contains(span_a, span_b)",
    argument(name = "span_a", description = "Container span struct."),
    argument(name = "span_b", description = "Contained span struct.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct SpanContainsUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for SpanContainsUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "span_contains"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(
                "span_contains expects two arguments".into(),
            ));
        }
        for arg_type in arg_types {
            if !matches!(arg_type, DataType::Struct(_)) {
                return Err(DataFusionError::Plan(
                    "span_contains expects struct inputs".into(),
                ));
            }
        }
        Ok(vec![arg_types[0].clone(), arg_types[1].clone()])
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, true)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left_value, right_value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "span_contains expects two arguments".into(),
            ));
        };
        let left_array = left_value.to_array(args.number_rows)?;
        let right_array = right_value.to_array(args.number_rows)?;
        let left_struct = left_array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Plan("span_contains expects struct inputs".into()))?;
        let right_struct = right_array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Plan("span_contains expects struct inputs".into()))?;

        let left_start = span_int64_column(left_struct, "bstart")?;
        let left_end = span_int64_column(left_struct, "bend")?;
        let right_start = span_int64_column(right_struct, "bstart")?;
        let right_end = span_int64_column(right_struct, "bend")?;
        let left_exclusive = span_bool_column(left_struct, "end_exclusive");
        let right_exclusive = span_bool_column(right_struct, "end_exclusive");

        let len = args.number_rows;
        if left_struct.len() != len || right_struct.len() != len {
            return Err(DataFusionError::Plan(
                "span_contains input lengths must match".into(),
            ));
        }
        let mut builder = BooleanBuilder::with_capacity(len);
        for row in 0..len {
            if left_start.is_null(row)
                || left_end.is_null(row)
                || right_start.is_null(row)
                || right_end.is_null(row)
            {
                builder.append_null();
                continue;
            }
            let left_adjusted_end =
                adjusted_end(left_end.value(row), span_end_exclusive(left_exclusive, row));
            let right_adjusted_end = adjusted_end(
                right_end.value(row),
                span_end_exclusive(right_exclusive, row),
            );
            let contains = left_start.value(row) <= right_start.value(row)
                && right_adjusted_end <= left_adjusted_end;
            builder.append_value(contains);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Span Functions"),
    description = "Create a stable span identifier from prefix, path, and offsets.",
    syntax_example = "span_id(prefix, path, bstart, bend, kind)",
    argument(name = "prefix", description = "Namespace prefix for the identifier."),
    argument(name = "path", description = "Path or document identifier."),
    argument(name = "bstart", description = "Start byte offset."),
    argument(name = "bend", description = "End byte offset."),
    argument(name = "kind", description = "Optional span kind discriminator.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct SpanIdUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for SpanIdUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "span_id"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
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

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !(4..=5).contains(&args.args.len()) {
            return Err(DataFusionError::Plan(
                "span_id expects four or five arguments".into(),
            ));
        }
        let num_rows = args.number_rows;
        let prefixes = columnar_to_optional_strings(
            &args.args[0],
            num_rows,
            "span_id expects string prefix input",
        )?;
        let mut part_columns: Vec<Vec<String>> = Vec::with_capacity(args.args.len() - 1);
        for value in args.args.iter().skip(1) {
            part_columns.push(columnar_to_strings(
                value,
                num_rows,
                NULL_SENTINEL,
                "span_id expects string-compatible part input",
            )?);
        }
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 40);
        for row in 0..num_rows {
            let Some(prefix) = prefixes[row].as_deref() else {
                builder.append_null();
                continue;
            };
            let mut joined = String::new();
            for (index, column) in part_columns.iter().enumerate() {
                if index > 0 {
                    joined.push_str(PART_SEPARATOR);
                }
                joined.push_str(&column[row]);
            }
            builder.append_value(stable_id_value(prefix, &joined));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Normalize UTF-8 text with Unicode normalization, case folding, and whitespace collapse.",
    syntax_example = "utf8_normalize(value, form, casefold, collapse_ws)",
    standard_argument(name = "value", prefix = "String"),
    argument(
        name = "form",
        description = "Unicode normalization form (NFC, NFD, NFKC, NFKD)."
    ),
    argument(
        name = "casefold",
        description = "Whether to lower-case the normalized text."
    ),
    argument(
        name = "collapse_ws",
        description = "Whether to collapse consecutive whitespace."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct Utf8NormalizeUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for Utf8NormalizeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "utf8_normalize"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 4 {
            return Err(DataFusionError::Plan(
                "utf8_normalize expects between one and four arguments".into(),
            ));
        }
        let mut coerced = Vec::with_capacity(arg_types.len());
        coerced.push(DataType::Utf8);
        if arg_types.len() >= 2 {
            coerced.push(DataType::Utf8);
        }
        if arg_types.len() >= 3 {
            coerced.push(DataType::Boolean);
        }
        if arg_types.len() >= 4 {
            coerced.push(DataType::Boolean);
        }
        Ok(coerced)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if args.is_empty() {
            return Err(DataFusionError::Plan(
                "utf8_normalize expects at least one argument".into(),
            ));
        }
        let mut scalars: Vec<&ScalarValue> = Vec::with_capacity(args.len());
        for expr in &args {
            let Some(value) = literal_scalar(expr) else {
                return Ok(ExprSimplifyResult::Original(args));
            };
            scalars.push(value);
        }
        let form = scalars
            .get(1)
            .map(|value| scalar_to_string(value))
            .transpose()?
            .flatten()
            .unwrap_or_else(|| DEFAULT_NORMALIZE_FORM.to_string());
        let casefold = scalars
            .get(2)
            .map(|value| scalar_to_bool(value, "utf8_normalize casefold flag"))
            .transpose()?
            .flatten()
            .unwrap_or(true);
        let collapse_ws = scalars
            .get(3)
            .map(|value| scalar_to_bool(value, "utf8_normalize collapse_ws flag"))
            .transpose()?
            .flatten()
            .unwrap_or(true);
        let value = scalar_to_string(scalars[0])?;
        let Some(value) = value else {
            return Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Utf8(None))));
        };
        let normalized = normalize_text(&value, &form, casefold, collapse_ws);
        let result = if normalized.is_empty() {
            None
        } else {
            Some(normalized)
        };
        Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Utf8(
            result,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.is_empty() {
            return Err(DataFusionError::Plan(
                "utf8_normalize expects at least one argument".into(),
            ));
        }
        let form = if args.args.len() >= 2 {
            let scalar = scalar_columnar_value(
                &args.args[1],
                "utf8_normalize form must be a scalar literal",
            )?;
            scalar_to_string(&scalar)?.unwrap_or_else(|| DEFAULT_NORMALIZE_FORM.to_string())
        } else {
            DEFAULT_NORMALIZE_FORM.to_string()
        };
        let casefold = if args.args.len() >= 3 {
            let scalar = scalar_columnar_value(
                &args.args[2],
                "utf8_normalize casefold must be a scalar literal",
            )?;
            scalar_to_bool(&scalar, "utf8_normalize casefold flag")?.unwrap_or(true)
        } else {
            true
        };
        let collapse_ws = if args.args.len() >= 4 {
            let scalar = scalar_columnar_value(
                &args.args[3],
                "utf8_normalize collapse_ws must be a scalar literal",
            )?;
            scalar_to_bool(&scalar, "utf8_normalize collapse_ws flag")?.unwrap_or(true)
        } else {
            true
        };
        let values = columnar_to_optional_strings(
            &args.args[0],
            args.number_rows,
            "utf8_normalize expects string-compatible input",
        )?;
        let mut builder = StringBuilder::with_capacity(args.number_rows, args.number_rows * 16);
        for value in values {
            let Some(value) = value else {
                builder.append_null();
                continue;
            };
            let normalized = normalize_text(&value, &form, casefold, collapse_ws);
            if normalized.is_empty() {
                builder.append_null();
            } else {
                builder.append_value(normalized);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Trim text and return null when the result is blank.",
    syntax_example = "utf8_null_if_blank(value)",
    standard_argument(name = "value", prefix = "String")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct Utf8NullIfBlankUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for Utf8NullIfBlankUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "utf8_null_if_blank"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 3 {
            return Err(DataFusionError::Plan(
                "qname_normalize expects between one and three arguments".into(),
            ));
        }
        Ok(vec![DataType::Utf8; arg_types.len()])
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "utf8_null_if_blank expects one argument".into(),
            ));
        };
        let values = columnar_to_optional_strings(
            value,
            args.number_rows,
            "utf8_null_if_blank expects string-compatible input",
        )?;
        let mut builder = StringBuilder::with_capacity(args.number_rows, args.number_rows * 8);
        for value in values {
            let Some(value) = value else {
                builder.append_null();
                continue;
            };
            let trimmed = value.trim();
            if trimmed.is_empty() {
                builder.append_null();
            } else {
                builder.append_value(trimmed);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Normalize and compose qualified names from symbol and optional module.",
    syntax_example = "qname_normalize(symbol, module, lang)",
    argument(name = "symbol", description = "Symbol or identifier."),
    argument(name = "module", description = "Optional module or namespace."),
    argument(name = "lang", description = "Optional language discriminator.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct QNameNormalizeUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for QNameNormalizeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "qname_normalize"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        expect_arg_len_exact(self.name(), arg_types, 3)?;
        ensure_map_arg(self.name(), &arg_types[0])?;
        Ok(vec![arg_types[0].clone(), DataType::Utf8, DataType::Utf8])
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !(1..=3).contains(&args.args.len()) {
            return Err(DataFusionError::Plan(
                "qname_normalize expects between one and three arguments".into(),
            ));
        }
        let symbols = columnar_to_optional_strings(
            &args.args[0],
            args.number_rows,
            "qname_normalize symbol must be string-compatible",
        )?;
        let modules = if args.args.len() >= 2 {
            columnar_to_optional_strings(
                &args.args[1],
                args.number_rows,
                "qname_normalize module must be string-compatible",
            )?
        } else {
            vec![None; args.number_rows]
        };
        let _langs = if args.args.len() >= 3 {
            columnar_to_optional_strings(
                &args.args[2],
                args.number_rows,
                "qname_normalize lang must be string-compatible",
            )?
        } else {
            vec![None; args.number_rows]
        };
        let mut builder = StringBuilder::with_capacity(args.number_rows, args.number_rows * 16);
        for row in 0..args.number_rows {
            let Some(symbol) = symbols[row].as_deref() else {
                builder.append_null();
                continue;
            };
            let normalized_symbol = normalize_text(symbol, DEFAULT_NORMALIZE_FORM, true, true);
            if normalized_symbol.is_empty() {
                builder.append_null();
                continue;
            }
            let normalized_module = modules[row]
                .as_deref()
                .map(|module| normalize_text(module, DEFAULT_NORMALIZE_FORM, true, true))
                .filter(|module| !module.is_empty());
            if normalized_symbol.contains('.') || normalized_module.is_none() {
                builder.append_value(normalized_symbol);
            } else if let Some(module) = normalized_module {
                builder.append_value(format!("{module}.{normalized_symbol}"));
            } else {
                builder.append_value(normalized_symbol);
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

fn normalized_map_type() -> DataType {
    let entry_fields = Fields::from(vec![
        Field::new("keys", DataType::Utf8, false),
        Field::new("values", DataType::Utf8, true),
    ]);
    let entry_field = Arc::new(Field::new("entries", DataType::Struct(entry_fields), false));
    DataType::Map(entry_field, false)
}

fn normalize_key_case(key: &str, key_case: &str) -> String {
    let normalized_case = key_case.trim().to_ascii_lowercase();
    match normalized_case.as_str() {
        "upper" => key.to_uppercase(),
        "none" => key.to_string(),
        _ => key.to_lowercase(),
    }
}

#[user_doc(
    doc_section(label = "Nested Functions"),
    description = "Extract a map value by key with a default fallback.",
    syntax_example = "map_get_default(map_expr, key, default_value)",
    argument(name = "map_expr", description = "Map expression."),
    argument(name = "key", description = "Key to extract (scalar literal)."),
    argument(
        name = "default_value",
        description = "Default value when the key is missing."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct MapGetDefaultUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for MapGetDefaultUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_get_default"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, true)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return Err(DataFusionError::Plan(
                "map_get_default expects exactly three arguments".into(),
            ));
        }
        let key_scalar = scalar_columnar_value(
            &args.args[1],
            "map_get_default key must be a scalar literal",
        )?;
        let Some(key) = scalar_to_string(&key_scalar)? else {
            return Err(DataFusionError::Plan(
                "map_get_default key cannot be null".into(),
            ));
        };

        let map_array = args.args[0].to_array(args.number_rows)?;
        let map_values = map_array
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| DataFusionError::Plan("map_get_default expects a map input".into()))?;

        let default_values = columnar_to_optional_strings(
            &args.args[2],
            args.number_rows,
            "map_get_default default must be string-compatible",
        )?;

        let mut builder = StringBuilder::with_capacity(args.number_rows, args.number_rows * 8);
        for row in 0..args.number_rows {
            let default_value = default_values[row].as_deref();
            if map_values.is_null(row) {
                if let Some(value) = default_value {
                    builder.append_value(value);
                } else {
                    builder.append_null();
                }
                continue;
            }
            let entries = map_values.value(row);
            let entries_struct =
                entries
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Plan("map_get_default map entries must be a struct".into())
                    })?;
            if entries_struct.num_columns() < 2 {
                return Err(DataFusionError::Plan(
                    "map_get_default map entries must include key and value columns".into(),
                ));
            }
            let keys = entries_struct.column(0);
            let values = entries_struct.column(1);
            let mut selected_value: Option<String> = None;
            for entry_index in 0..entries_struct.len() {
                let entry_key = ScalarValue::try_from_array(keys.as_ref(), entry_index)?;
                let entry_key_text = scalar_to_string(&entry_key)?;
                if entry_key_text.as_deref() != Some(key.as_str()) {
                    continue;
                }
                let entry_value = ScalarValue::try_from_array(values.as_ref(), entry_index)?;
                selected_value = scalar_to_string(&entry_value)?;
                if selected_value.is_some() {
                    break;
                }
            }
            if let Some(value) = selected_value.as_deref() {
                builder.append_value(value);
            } else if let Some(value) = default_value {
                builder.append_value(value);
            } else {
                builder.append_null();
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Nested Functions"),
    description = "Normalize map keys and optionally sort them deterministically.",
    syntax_example = "map_normalize(map_expr, key_case, sort_keys)",
    argument(name = "map_expr", description = "Map expression."),
    argument(
        name = "key_case",
        description = "Key case normalization: lower, upper, none."
    ),
    argument(
        name = "sort_keys",
        description = "Whether to sort keys deterministically."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct MapNormalizeUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for MapNormalizeUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_normalize"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let count = expect_arg_len(self.name(), arg_types, 1, 3)?;
        ensure_map_arg(self.name(), &arg_types[0])?;
        let mut coerced = Vec::with_capacity(count);
        coerced.push(arg_types[0].clone());
        if count >= 2 {
            coerced.push(DataType::Utf8);
        }
        if count >= 3 {
            coerced.push(DataType::Boolean);
        }
        Ok(coerced)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .map(|field| field.is_nullable())
            .unwrap_or(true);
        let mut field = Field::new(self.name(), normalized_map_type(), nullable);
        if let Some(first) = args.arg_fields.first() {
            if !first.metadata().is_empty() {
                field = field.with_metadata(first.metadata().clone());
            }
        }
        Ok(Arc::new(field))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(normalized_map_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !(1..=3).contains(&args.args.len()) {
            return Err(DataFusionError::Plan(
                "map_normalize expects between one and three arguments".into(),
            ));
        }
        let key_case = if args.args.len() >= 2 {
            let scalar = scalar_columnar_value(
                &args.args[1],
                "map_normalize key_case must be a scalar literal",
            )?;
            scalar_to_string(&scalar)?.unwrap_or_else(|| "lower".to_string())
        } else {
            "lower".to_string()
        };
        let sort_keys = if args.args.len() >= 3 {
            let scalar = scalar_columnar_value(
                &args.args[2],
                "map_normalize sort_keys must be a scalar literal",
            )?;
            scalar_to_bool(&scalar, "map_normalize sort_keys flag")?.unwrap_or(true)
        } else {
            true
        };

        let map_array = args.args[0].to_array(args.number_rows)?;
        let maps = map_array
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| DataFusionError::Plan("map_normalize expects a map input".into()))?;

        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for row in 0..args.number_rows {
            if maps.is_null(row) {
                builder.append(false)?;
                continue;
            }
            let entries = maps.value(row);
            let entries_struct =
                entries
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        DataFusionError::Plan("map_normalize map entries must be a struct".into())
                    })?;
            if entries_struct.num_columns() < 2 {
                return Err(DataFusionError::Plan(
                    "map_normalize map entries must include key and value columns".into(),
                ));
            }
            let keys = entries_struct.column(0);
            let values = entries_struct.column(1);
            let mut normalized_entries: Vec<(String, Option<String>)> =
                Vec::with_capacity(entries_struct.len());
            for entry_index in 0..entries_struct.len() {
                let entry_key = ScalarValue::try_from_array(keys.as_ref(), entry_index)?;
                let Some(entry_key_text) = scalar_to_string(&entry_key)? else {
                    continue;
                };
                let normalized_key = normalize_key_case(&entry_key_text, &key_case);
                let entry_value = ScalarValue::try_from_array(values.as_ref(), entry_index)?;
                let normalized_value = scalar_to_string(&entry_value)?;
                normalized_entries.push((normalized_key, normalized_value));
            }
            let entries_to_write = if sort_keys {
                let mut sorted_entries: BTreeMap<String, Option<String>> = BTreeMap::new();
                for (key, value) in normalized_entries {
                    sorted_entries.insert(key, value);
                }
                sorted_entries.into_iter().collect::<Vec<_>>()
            } else {
                normalized_entries
            };
            for (key, value) in entries_to_write {
                builder.keys().append_value(key);
                if let Some(value) = value {
                    builder.values().append_value(value);
                } else {
                    builder.values().append_null();
                }
            }
            builder.append(true)?;
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Nested Functions"),
    description = "Remove null entries from a list and coerce values to strings.",
    syntax_example = "list_compact(list_expr)",
    argument(name = "list_expr", description = "List expression.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct ListCompactUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for ListCompactUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "list_compact"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        expect_arg_len_exact(self.name(), arg_types, 1)?;
        ensure_list_arg(self.name(), &arg_types[0])?;
        Ok(vec![arg_types[0].clone()])
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .map(|field| field.is_nullable())
            .unwrap_or(true);
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let mut field = Field::new(self.name(), DataType::List(item_field), nullable);
        if let Some(first) = args.arg_fields.first() {
            if !first.metadata().is_empty() {
                field = field.with_metadata(first.metadata().clone());
            }
        }
        Ok(Arc::new(field))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        Ok(DataType::List(item_field))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "list_compact expects one argument".into(),
            ));
        };
        let list_array = value.to_array(args.number_rows)?;
        let lists = list_array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("list_compact expects a list input".into()))?;
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in 0..args.number_rows {
            if lists.is_null(row) {
                builder.append(false);
                continue;
            }
            let entries = lists.value(row);
            for entry_index in 0..entries.len() {
                let entry_value = ScalarValue::try_from_array(entries.as_ref(), entry_index)?;
                let Some(entry_text) = scalar_to_string(&entry_value)? else {
                    continue;
                };
                builder.values().append_value(entry_text);
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Nested Functions"),
    description = "Compact a list, remove duplicates, and sort deterministically.",
    syntax_example = "list_unique_sorted(list_expr)",
    argument(name = "list_expr", description = "List expression.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct ListUniqueSortedUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for ListUniqueSortedUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "list_unique_sorted"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        expect_arg_len_exact(self.name(), arg_types, 1)?;
        ensure_list_arg(self.name(), &arg_types[0])?;
        Ok(vec![arg_types[0].clone()])
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .map(|field| field.is_nullable())
            .unwrap_or(true);
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let mut field = Field::new(self.name(), DataType::List(item_field), nullable);
        if let Some(first) = args.arg_fields.first() {
            if !first.metadata().is_empty() {
                field = field.with_metadata(first.metadata().clone());
            }
        }
        Ok(Arc::new(field))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        let item_field = Arc::new(Field::new("item", DataType::Utf8, true));
        Ok(DataType::List(item_field))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "list_unique_sorted expects one argument".into(),
            ));
        };
        let list_array = value.to_array(args.number_rows)?;
        let lists = list_array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("list_unique_sorted expects a list input".into())
            })?;
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in 0..args.number_rows {
            if lists.is_null(row) {
                builder.append(false);
                continue;
            }
            let entries = lists.value(row);
            let mut unique_values: BTreeSet<String> = BTreeSet::new();
            for entry_index in 0..entries.len() {
                let entry_value = ScalarValue::try_from_array(entries.as_ref(), entry_index)?;
                let Some(entry_text) = scalar_to_string(&entry_value)? else {
                    continue;
                };
                unique_values.insert(entry_text);
            }
            for entry in unique_values {
                builder.values().append_value(entry);
            }
            builder.append(true);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Nested Functions"),
    description = "Select a subset of fields from a struct using scalar field names.",
    syntax_example = "struct_pick(struct_expr, field1, field2, ...)",
    argument(name = "struct_expr", description = "Struct expression."),
    argument(name = "fields", description = "Scalar field names to select.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct StructPickUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for StructPickUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "struct_pick"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() < 2 || arg_types.len() > 7 {
            return Err(DataFusionError::Plan(
                "struct_pick expects between two and seven arguments".into(),
            ));
        }
        ensure_struct_arg(self.name(), &arg_types[0])?;
        let mut coerced = Vec::with_capacity(arg_types.len());
        coerced.push(arg_types[0].clone());
        for _ in 1..arg_types.len() {
            coerced.push(DataType::Utf8);
        }
        Ok(coerced)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() < 2 {
            return Err(DataFusionError::Plan(
                "struct_pick expects a struct and at least one field name".into(),
            ));
        }
        let struct_field = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan("struct_pick requires a struct argument".into())
        })?;
        let DataType::Struct(struct_fields) = struct_field.data_type() else {
            return Err(DataFusionError::Plan(
                "struct_pick first argument must be a struct".into(),
            ));
        };
        let parent_nullable = struct_field.is_nullable();
        let mut selected_fields: Vec<Field> = Vec::with_capacity(args.arg_fields.len() - 1);
        for index in 1..args.arg_fields.len() {
            let field_name = scalar_argument_string(&args, index, "struct_pick field name")?
                .ok_or_else(|| {
                    DataFusionError::Plan("struct_pick field names must be scalar literals".into())
                })?;
            let trimmed = field_name.trim();
            if trimmed.is_empty() {
                return Err(DataFusionError::Plan(
                    "struct_pick field names cannot be empty".into(),
                ));
            }
            let field = struct_fields
                .iter()
                .find(|field| field.name() == trimmed)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "struct_pick field not found in struct: {trimmed}"
                    ))
                })?;
            let mut selected = field.as_ref().clone();
            if parent_nullable {
                selected = selected.with_nullable(true);
            }
            selected_fields.push(selected);
        }
        let output_type = DataType::Struct(Fields::from(selected_fields));
        let mut field = Field::new(self.name(), output_type, parent_nullable);
        if !struct_field.metadata().is_empty() {
            field = field.with_metadata(struct_field.metadata().clone());
        }
        Ok(Arc::new(field))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Err(DataFusionError::Plan(
            "struct_pick requires return_field_from_args".into(),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() < 2 {
            return Err(DataFusionError::Plan(
                "struct_pick expects a struct and at least one field name".into(),
            ));
        }
        let struct_array = args.args[0].to_array(args.number_rows)?;
        let struct_values = struct_array
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Plan("struct_pick expects a struct input".into()))?;
        let DataType::Struct(struct_fields) = struct_values.data_type() else {
            return Err(DataFusionError::Plan(
                "struct_pick struct input must have a struct type".into(),
            ));
        };
        let parent_nullable = struct_values.nulls().is_some();
        let return_field = args.return_field.as_ref();
        let DataType::Struct(return_fields) = return_field.data_type() else {
            return Err(DataFusionError::Plan(
                "struct_pick return type must be a struct".into(),
            ));
        };
        let mut selected_fields: Vec<Field> = Vec::with_capacity(return_fields.len());
        let mut selected_arrays: Vec<ArrayRef> = Vec::with_capacity(return_fields.len());
        for field in return_fields {
            let field_name = field.name();
            let field = struct_fields
                .iter()
                .find(|candidate| candidate.name() == field_name)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "struct_pick field not found in struct: {field_name}"
                    ))
                })?;
            let mut selected = field.as_ref().clone();
            if parent_nullable {
                selected = selected.with_nullable(true);
            }
            selected_fields.push(selected);
            let column = struct_values.column_by_name(field_name).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "struct_pick field not found in struct data: {field_name}"
                ))
            })?;
            selected_arrays.push(column.clone());
        }
        let nulls = struct_values.nulls().cloned();
        let picked = StructArray::new(Fields::from(selected_fields), selected_arrays, nulls);
        Ok(ColumnarValue::Array(Arc::new(picked) as ArrayRef))
    }
}

fn cdf_rank(change_type: &str) -> i32 {
    let normalized = change_type.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "update_postimage" => 3,
        "insert" => 2,
        "delete" => 1,
        "update_preimage" => 0,
        _ => -1,
    }
}

#[user_doc(
    doc_section(label = "Delta Functions"),
    description = "Rank Delta change types deterministically.",
    syntax_example = "cdf_change_rank(change_type)",
    argument(name = "change_type", description = "Delta change type string.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct CdfChangeRankUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for CdfChangeRankUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "cdf_change_rank"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, true)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "cdf_change_rank expects one argument".into(),
            ));
        };
        let values = columnar_to_optional_strings(
            value,
            args.number_rows,
            "cdf_change_rank expects string-compatible input",
        )?;
        let mut builder = Int32Builder::with_capacity(args.number_rows);
        for value in values {
            let Some(value) = value else {
                builder.append_null();
                continue;
            };
            builder.append_value(cdf_rank(&value));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Delta Functions"),
    description = "Return true when the Delta change type represents an upsert.",
    syntax_example = "cdf_is_upsert(change_type)",
    argument(name = "change_type", description = "Delta change type string.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct CdfIsUpsertUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for CdfIsUpsertUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "cdf_is_upsert"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, true)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "cdf_is_upsert expects one argument".into(),
            ));
        };
        let values = columnar_to_optional_strings(
            value,
            args.number_rows,
            "cdf_is_upsert expects string-compatible input",
        )?;
        let mut builder = BooleanBuilder::with_capacity(args.number_rows);
        for value in values {
            let Some(value) = value else {
                builder.append_null();
                continue;
            };
            let rank = cdf_rank(&value);
            builder.append_value(rank == 3 || rank == 2);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[user_doc(
    doc_section(label = "Delta Functions"),
    description = "Return true when the Delta change type represents a delete.",
    syntax_example = "cdf_is_delete(change_type)",
    argument(name = "change_type", description = "Delta change type string.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct CdfIsDeleteUdf {
    signature: SignatureEqHash,
}

impl ScalarUDFImpl for CdfIsDeleteUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "cdf_is_delete"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Boolean, true)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "cdf_is_delete expects one argument".into(),
            ));
        };
        let values = columnar_to_optional_strings(
            value,
            args.number_rows,
            "cdf_is_delete expects string-compatible input",
        )?;
        let mut builder = BooleanBuilder::with_capacity(args.number_rows);
        for value in values {
            let Some(value) = value else {
                builder.append_null();
                continue;
            };
            builder.append_value(cdf_rank(&value) == 1);
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

#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Normalize a position encoding name to its numeric code.",
    syntax_example = "position_encoding_norm(value)",
    standard_argument(name = "value", prefix = "String")
)]
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
        self.doc()
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
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "position_encoding_norm expects one argument".into(),
            ));
        };
        match value {
            ColumnarValue::Scalar(value) => {
                let normalized = scalar_str(value, "position_encoding_norm expects string input")?
                    .map(normalize_position_encoding)
                    .unwrap_or(ENC_UTF32);
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(normalized))))
            }
            ColumnarValue::Array(array) => {
                let input = string_array_any(array, "position_encoding_norm expects string input")?;
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
    }
}

#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Convert a column offset to a byte offset for a line and encoding.",
    syntax_example = "col_to_byte(line_text, col, col_unit)",
    argument(
        name = "line_text",
        description = "Line text to compute offsets within."
    ),
    argument(name = "col", description = "Column offset within the line."),
    argument(
        name = "col_unit",
        description = "Encoding unit (BYTE, UTF8, UTF16, UTF32)."
    )
)]
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
        self.doc()
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
        let [line_value, offset_value, encoding_value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "col_to_byte expects three arguments".into(),
            ));
        };
        if let (
            ColumnarValue::Scalar(line),
            ColumnarValue::Scalar(offset),
            ColumnarValue::Scalar(encoding),
        ) = (line_value, offset_value, encoding_value)
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
                let py_index = code_unit_offset_to_py_index(line, normalize_offset(*offset), unit)?;
                byte_offset_from_py_index(line, py_index) as i64
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(byte_offset))));
        }
        let len = args
            .args
            .iter()
            .find_map(|value| match value {
                ColumnarValue::Array(array) => Some(array.len()),
                ColumnarValue::Scalar(_) => None,
            })
            .unwrap_or(args.number_rows);
        let line_array = match line_value {
            ColumnarValue::Array(array) => {
                Some(string_array_any(array, "col_to_byte expects string input")?)
            }
            ColumnarValue::Scalar(_) => None,
        };
        let offset_array = match offset_value {
            ColumnarValue::Array(array) => {
                Some(array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    DataFusionError::Plan("col_to_byte expects int64 input".into())
                })?)
            }
            ColumnarValue::Scalar(_) => None,
        };
        let encoding_array = match encoding_value {
            ColumnarValue::Array(array) => Some(string_array_any(
                array,
                "col_to_byte expects string encoding",
            )?),
            ColumnarValue::Scalar(_) => None,
        };
        let line_scalar = match line_value {
            ColumnarValue::Scalar(value) => scalar_str(value, "col_to_byte expects string input")?,
            ColumnarValue::Array(_) => None,
        };
        let offset_scalar = match offset_value {
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => *value,
            ColumnarValue::Scalar(_) => {
                return Err(DataFusionError::Plan(
                    "col_to_byte expects int64 input".into(),
                ))
            }
            ColumnarValue::Array(_) => None,
        };
        let encoding_scalar = match encoding_value {
            ColumnarValue::Scalar(value) => {
                scalar_str(value, "col_to_byte expects string encoding")?
            }
            ColumnarValue::Array(_) => None,
        };
        let mut builder = Int64Builder::with_capacity(len);
        for index in 0..len {
            let line = if let Some(lines) = line_array.as_ref() {
                if lines.is_null(index) {
                    None
                } else {
                    Some(lines.value(index))
                }
            } else {
                line_scalar
            };
            let offset = if let Some(offsets) = offset_array {
                if offsets.is_null(index) {
                    None
                } else {
                    Some(offsets.value(index))
                }
            } else {
                offset_scalar
            };
            let encoding = if let Some(encodings) = encoding_array.as_ref() {
                if encodings.is_null(index) {
                    None
                } else {
                    Some(encodings.value(index))
                }
            } else {
                encoding_scalar
            };
            let Some(line) = line else {
                builder.append_null();
                continue;
            };
            let Some(offset) = offset else {
                builder.append_null();
                continue;
            };
            let unit = match encoding {
                Some(value) => col_unit_from_text(value),
                None => ColUnit::Utf32,
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
