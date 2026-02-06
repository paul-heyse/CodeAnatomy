use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use blake2::digest::{Update, VariableOutput};
use blake2::Blake2bVar;
use dashmap::DashMap;
use datafusion::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

use crate::compat::{
    ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use crate::udf_config::CodeAnatomyUdfConfig;

static PATTERN_CACHE: LazyLock<DashMap<String, Regex>> = LazyLock::new(DashMap::new);

pub(crate) const ENC_UTF8: i32 = 1;
pub(crate) const ENC_UTF16: i32 = 2;
pub(crate) const ENC_UTF32: i32 = 3;
pub(crate) const PART_SEPARATOR: &str = "\u{001f}";
pub(crate) const NULL_SENTINEL: &str = "__NULL__";
pub(crate) const DEFAULT_NORMALIZE_FORM: &str = "NFKC";

pub(crate) fn signature_with_names(signature: Signature, names: &[&str]) -> Signature {
    let name_vec = names.iter().map(|name| (*name).to_string()).collect();
    match signature.clone().with_parameter_names(name_vec) {
        Ok(signature) => signature,
        Err(_) => signature,
    }
}

pub(crate) fn field_from_first_arg_typed(
    args: ReturnFieldArgs,
    name: &str,
    dtype: DataType,
) -> Result<FieldRef> {
    let field = args
        .arg_fields
        .first()
        .ok_or_else(|| DataFusionError::Plan(format!("{name} expects at least one argument")))?;
    let mut output = Field::new(name, dtype.clone(), field.is_nullable());
    if field.data_type() == &dtype && !field.metadata().is_empty() {
        output = output.with_metadata(field.metadata().clone());
    }
    Ok(Arc::new(output))
}

pub(crate) fn string_int_string_signature(volatility: Volatility) -> Signature {
    let arg_types = vec![DataType::Utf8, DataType::Int64, DataType::Utf8];
    Signature::one_of(expand_string_signatures(&arg_types), volatility)
}

pub(crate) fn user_defined_signature(volatility: Volatility) -> Signature {
    Signature::one_of(vec![TypeSignature::UserDefined], volatility)
}

pub(crate) fn variadic_any_signature(
    _min_args: usize,
    _max_args: usize,
    volatility: Volatility,
) -> Signature {
    user_defined_signature(volatility)
}

pub(crate) fn expand_string_signatures(arg_types: &[DataType]) -> Vec<TypeSignature> {
    let variants = [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View];
    let mut expanded: Vec<Vec<DataType>> = vec![Vec::new()];
    for dtype in arg_types {
        if is_string_type(dtype) {
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

pub(crate) fn is_string_type(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

pub(crate) fn expect_arg_len(
    name: &str,
    arg_types: &[DataType],
    min: usize,
    max: usize,
) -> Result<usize> {
    let count = arg_types.len();
    if count < min || count > max {
        return Err(DataFusionError::Plan(format!(
            "{name} expects between {min} and {max} arguments"
        )));
    }
    Ok(count)
}

pub(crate) fn expect_arg_len_exact(
    name: &str,
    arg_types: &[DataType],
    expected: usize,
) -> Result<()> {
    if arg_types.len() != expected {
        return Err(DataFusionError::Plan(format!(
            "{name} expects {expected} arguments"
        )));
    }
    Ok(())
}

pub(crate) fn ensure_struct_arg(name: &str, arg_type: &DataType) -> Result<()> {
    if matches!(arg_type, DataType::Struct(_)) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} expects a struct input"
        )))
    }
}

pub(crate) fn ensure_map_arg(name: &str, arg_type: &DataType) -> Result<()> {
    if matches!(arg_type, DataType::Map(_, _)) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!("{name} expects a map input")))
    }
}

pub(crate) fn ensure_list_arg(name: &str, arg_type: &DataType) -> Result<()> {
    if matches!(arg_type, DataType::List(_)) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "{name} expects a list input"
        )))
    }
}

pub(crate) fn scalar_str<'a>(value: &'a ScalarValue, message: &str) -> Result<Option<&'a str>> {
    value
        .try_as_str()
        .ok_or_else(|| DataFusionError::Plan(message.to_string()))
}

pub(crate) fn literal_string_value(value: &ScalarValue) -> Option<Option<String>> {
    match value {
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value) => {
            Some(value.clone())
        }
        _ => None,
    }
}

pub(crate) fn literal_scalar(expr: &Expr) -> Option<&ScalarValue> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    Some(value)
}

pub(crate) fn literal_string(expr: &Expr) -> Option<Option<String>> {
    let Expr::Literal(value, _) = expr else {
        return None;
    };
    literal_string_value(value)
}

pub(crate) fn eval_udf_on_literals(
    udf: &dyn ScalarUDFImpl,
    args: &[Expr],
) -> Result<Option<ScalarValue>> {
    let mut scalars = Vec::with_capacity(args.len());
    for arg in args {
        let Some(value) = literal_scalar(arg) else {
            return Ok(None);
        };
        scalars.push(value);
    }
    let arg_fields: Vec<FieldRef> = scalars
        .iter()
        .enumerate()
        .map(|(index, value)| {
            Arc::new(Field::new(
                format!("arg{index}"),
                value.data_type(),
                value.is_null(),
            ))
        })
        .collect();
    let scalar_args: Vec<Option<&ScalarValue>> = scalars.iter().map(|value| Some(*value)).collect();
    let return_field = udf.return_field_from_args(ReturnFieldArgs {
        arg_fields: &arg_fields,
        scalar_arguments: &scalar_args,
    })?;
    let mut columnar_args = Vec::with_capacity(scalars.len());
    for value in scalars {
        columnar_args.push(ColumnarValue::Scalar(value.clone()));
    }
    let eval_args = ScalarFunctionArgs {
        args: columnar_args,
        arg_fields,
        number_rows: 1,
        return_field,
        config_options: Arc::new(ConfigOptions::default()),
    };
    let result = udf.invoke_with_args(eval_args)?;
    let scalar = match result {
        ColumnarValue::Scalar(value) => value,
        ColumnarValue::Array(array) => array_to_scalar(array, udf.name())?,
    };
    Ok(Some(scalar))
}

pub(crate) fn string_array_any<'a>(
    array: &'a ArrayRef,
    message: &str,
) -> Result<Cow<'a, StringArray>> {
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

pub(crate) fn scalar_to_string(value: &ScalarValue) -> Result<Option<String>> {
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

pub(crate) fn scalar_to_string_with_sentinel(
    value: &ScalarValue,
    sentinel: &str,
) -> Result<String> {
    Ok(scalar_to_string(value)?.unwrap_or_else(|| sentinel.to_string()))
}

pub(crate) fn scalar_to_i64(value: &ScalarValue, context: &str) -> Result<Option<i64>> {
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

pub(crate) fn scalar_to_i32(value: &ScalarValue, context: &str) -> Result<Option<i32>> {
    scalar_to_i64(value, context).map(|opt| opt.map(|x| x as i32))
}

pub(crate) fn scalar_to_bool(value: &ScalarValue, context: &str) -> Result<Option<bool>> {
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

pub(crate) fn columnar_to_strings(
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

pub(crate) fn columnar_to_optional_strings(
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

pub(crate) fn columnar_to_i64(
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

pub(crate) fn columnar_to_i32(
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

pub(crate) fn columnar_to_bool(
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

pub(crate) fn all_scalars(values: &[ColumnarValue]) -> bool {
    values
        .iter()
        .all(|value| matches!(value, ColumnarValue::Scalar(_)))
}

pub(crate) fn array_to_scalar(array: ArrayRef, context: &str) -> Result<ScalarValue> {
    ScalarValue::try_from_array(array.as_ref(), 0).map_err(|err| {
        DataFusionError::Plan(format!("{context}: failed to read scalar output: {err}"))
    })
}

pub(crate) fn columnar_result(
    array: ArrayRef,
    all_scalar: bool,
    context: &str,
) -> Result<ColumnarValue> {
    if all_scalar {
        Ok(ColumnarValue::Scalar(array_to_scalar(array, context)?))
    } else {
        Ok(ColumnarValue::Array(array))
    }
}

pub(crate) fn span_struct_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("bstart", DataType::Int64, true),
        Field::new("bend", DataType::Int64, true),
        Field::new("line_base", DataType::Int32, true),
        Field::new("col_unit", DataType::Utf8, true),
        Field::new("end_exclusive", DataType::Boolean, true),
    ]))
}

pub(crate) fn span_metadata_from_scalars(
    args: &ReturnFieldArgs,
    defaults: &CodeAnatomyUdfConfig,
) -> Result<HashMap<String, String>> {
    let mut metadata = HashMap::new();
    metadata.insert("semantic_type".to_string(), "Span".to_string());
    let line_base = match scalar_argument(args, 2) {
        Some(value) => scalar_to_i32(value, "span_make line_base metadata")?
            .unwrap_or(defaults.span_default_line_base),
        None => defaults.span_default_line_base,
    };
    metadata.insert("line_base".to_string(), line_base.to_string());
    let col_unit = match scalar_argument(args, 3) {
        Some(value) => {
            scalar_to_string(value)?.unwrap_or_else(|| defaults.span_default_col_unit.clone())
        }
        None => defaults.span_default_col_unit.clone(),
    };
    metadata.insert("col_unit".to_string(), col_unit);
    let end_exclusive = match scalar_argument(args, 4) {
        Some(value) => scalar_to_bool(value, "span_make end_exclusive metadata")?
            .unwrap_or(defaults.span_default_end_exclusive),
        None => defaults.span_default_end_exclusive,
    };
    metadata.insert("end_exclusive".to_string(), end_exclusive.to_string());
    Ok(metadata)
}

pub(crate) fn scalar_argument<'a>(
    args: &ReturnFieldArgs<'a>,
    index: usize,
) -> Option<&'a ScalarValue> {
    args.scalar_arguments.get(index).and_then(|value| *value)
}

pub(crate) fn scalar_argument_string(
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

pub(crate) fn semantic_type_from_prefix(prefix: &str) -> Option<&'static str> {
    let normalized = prefix.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "node" | "node_id" | "nodeid" => Some("NodeId"),
        "edge" | "edge_id" | "edgeid" => Some("EdgeId"),
        "span" | "span_id" | "spanid" => Some("SpanId"),
        _ => None,
    }
}

pub(crate) fn normalize_semantic_type(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let normalized = trimmed.to_ascii_lowercase();
    let canonical = match normalized.as_str() {
        "node" | "node_id" | "nodeid" => "NodeId",
        "edge" | "edge_id" | "edgeid" => "EdgeId",
        "span" | "span_id" | "spanid" => "SpanId",
        _ => trimmed,
    };
    Some(canonical.to_string())
}

pub(crate) fn scalar_columnar_value(value: &ColumnarValue, context: &str) -> Result<ScalarValue> {
    match value {
        ColumnarValue::Scalar(v) => Ok(v.clone()),
        ColumnarValue::Array(_) => Err(DataFusionError::Plan(context.to_string())),
    }
}

fn collapse_whitespace(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let regex = PATTERN_CACHE
        .entry(r"\s+".to_string())
        .or_insert_with(|| Regex::new(r"\s+").expect("valid whitespace regex"));
    regex.replace_all(trimmed, " ").to_string()
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

pub(crate) fn normalize_text(value: &str, form: &str, casefold: bool, collapse_ws: bool) -> String {
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

pub(crate) fn prefixed_hash64_value(prefix: &str, value: &str) -> String {
    format!("{prefix}:{}", hash64_value(value))
}

pub(crate) fn stable_id_value(prefix: &str, value: &str) -> String {
    format!("{prefix}:{}", hash128_value(value))
}

pub(crate) fn hash64_value(value: &str) -> i64 {
    let mut hasher = Blake2bVar::new(8).expect("blake2b supports 8-byte output");
    hasher.update(value.as_bytes());
    let mut out = [0u8; 8];
    hasher.finalize_variable(&mut out).expect("hash output");
    let unsigned = u64::from_be_bytes(out);
    let masked = unsigned & ((1_u64 << 63) - 1);
    masked as i64
}

pub(crate) fn hash128_value(value: &str) -> String {
    let mut hasher = Blake2bVar::new(16).expect("blake2b supports 16-byte output");
    hasher.update(value.as_bytes());
    let mut out = [0u8; 16];
    hasher.finalize_variable(&mut out).expect("hash output");
    hex::encode(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whitespace_regex_cache_reused() {
        PATTERN_CACHE.clear();
        assert_eq!(PATTERN_CACHE.len(), 0);
        let _ = normalize_text("a  b", DEFAULT_NORMALIZE_FORM, false, true);
        assert_eq!(PATTERN_CACHE.len(), 1);
        let _ = normalize_text("c   d", DEFAULT_NORMALIZE_FORM, false, true);
        assert_eq!(PATTERN_CACHE.len(), 1);
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SignatureEqHash {
    signature: Signature,
}

impl SignatureEqHash {
    pub(crate) fn new(signature: Signature) -> Self {
        Self { signature }
    }

    pub(crate) fn signature(&self) -> &Signature {
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
