use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ListArray, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields};
use arrow::ipc::reader::StreamReader;
use datafusion::config::ConfigOptions;
use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result};

use crate::compat::{ScalarUDF, Signature, Volatility};
use crate::udf::common::{expand_string_signatures, is_string_type, SignatureEqHash};
use crate::udf::{
    cdf::{CdfChangeRankUdf, CdfIsDeleteUdf, CdfIsUpsertUdf},
    collection::{ListCompactUdf, ListUniqueSortedUdf, MapGetDefaultUdf, MapNormalizeUdf},
    hash::{
        PrefixedHash64Udf, PrefixedHashParts64Udf, StableHash128Udf, StableHash64Udf,
        StableHashAnyUdf, StableIdPartsUdf, StableIdUdf,
    },
    metadata::{ArrowMetadataUdf, CpgScoreUdf},
    position::{CanonicalizeByteSpanUdf, ColToByteUdf, PositionEncodingUdf},
    span::{
        IntervalAlignScoreUdf, SpanContainsUdf, SpanIdUdf, SpanLenUdf, SpanMakeUdf, SpanOverlapsUdf,
    },
    string::{QNameNormalizeUdf, SemanticTagUdf, Utf8NormalizeUdf, Utf8NullIfBlankUdf},
    struct_ops::StructPickUdf,
};
#[cfg(feature = "async-udf")]
use crate::udf_async;
use crate::udf_config::{CodeAnatomyUdfConfig, UdfConfigValue};

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

pub fn install_function_factory_native(ctx: &SessionContext, policy_ipc: &[u8]) -> Result<()> {
    let policy = policy_from_ipc(policy_ipc)?;
    touch_policy_fields(&policy);
    register_primitives(ctx, &policy)?;
    install_sql_macro_factory(ctx)
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
        .downcast_ref::<arrow::array::Int32Array>()
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
            name: read_string_value(names, index)?,
            params: read_params(params, index)?,
            return_type: read_string_value(return_types, index)?,
            volatility: read_string_value(volatilities, index)?,
            description: read_optional_string(descriptions, index),
            supports_named_args: read_bool_value(supports_named, index)?,
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
            name: read_string_value(names, row)?,
            dtype: read_string_value(dtypes, row)?,
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
        "arrow_metadata" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            ArrowMetadataUdf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
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
            policy: CodeAnatomyUdfConfig::default(),
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
                policy: CodeAnatomyUdfConfig::default(),
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
            policy: CodeAnatomyUdfConfig::default(),
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
        "canonicalize_byte_span" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(
            CanonicalizeByteSpanUdf {
                signature: SignatureEqHash::new(signature),
            },
        ))),
        "semantic_tag" => Ok(ScalarUDF::new_from_shared_impl(Arc::new(SemanticTagUdf {
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
    if arg_types.is_empty() {
        return Signature::nullary(volatility);
    }
    if arg_types
        .iter()
        .all(|dtype| matches!(dtype, DataType::Null))
    {
        return Signature::any(arg_types.len(), volatility);
    }
    let has_string = arg_types.iter().any(is_string_type);
    if arg_types.iter().all(is_string_type) {
        Signature::string(arg_types.len(), volatility)
    } else if has_string {
        Signature::one_of(expand_string_signatures(&arg_types), volatility)
    } else {
        Signature::exact(arg_types, volatility)
    }
}

fn strip_named_type(value: &str) -> &str {
    let trimmed = value.trim();
    trimmed
        .split_once(':')
        .map(|(_, dtype)| dtype.trim())
        .unwrap_or(trimmed)
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

fn dtype_from_str(value: &str) -> Result<DataType> {
    let text = normalize_type(value)?;
    parse_type_signature(&text).map_err(|err| {
        DataFusionError::Plan(format!(
            "Unsupported dtype in primitive policy: {value} ({err})"
        ))
    })
}

fn normalize_type(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(DataFusionError::Plan(
            "Unsupported dtype in primitive policy: empty string".into(),
        ));
    }
    Ok(trimmed.to_lowercase())
}

fn parse_type_signature(text: &str) -> Result<DataType> {
    if let Some(simple) = simple_type_from_str(text) {
        return Ok(simple);
    }
    if let Some(container) = parse_container_type(text)? {
        return Ok(container);
    }
    if let Some(decimal) = parse_decimal(text)? {
        return Ok(decimal);
    }
    Err(DataFusionError::Plan(format!(
        "Unsupported dtype in primitive policy: {text}"
    )))
}

fn simple_type_from_str(text: &str) -> Option<DataType> {
    match text {
        "null" => Some(DataType::Null),
        "bool" | "boolean" => Some(DataType::Boolean),
        "int8" => Some(DataType::Int8),
        "int16" => Some(DataType::Int16),
        "int32" => Some(DataType::Int32),
        "int64" => Some(DataType::Int64),
        "uint8" => Some(DataType::UInt8),
        "uint16" => Some(DataType::UInt16),
        "uint32" => Some(DataType::UInt32),
        "uint64" => Some(DataType::UInt64),
        "float16" => Some(DataType::Float16),
        "float32" | "float" => Some(DataType::Float32),
        "float64" | "double" => Some(DataType::Float64),
        "binary" => Some(DataType::Binary),
        "largebinary" | "large_binary" => Some(DataType::LargeBinary),
        "string" | "utf8" => Some(DataType::Utf8),
        "large_string" | "large_utf8" => Some(DataType::LargeUtf8),
        "utf8view" | "utf8_view" => Some(DataType::Utf8View),
        "timestamp" => Some(DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        "date32" => Some(DataType::Date32),
        "date64" => Some(DataType::Date64),
        "time32" => Some(DataType::Time32(arrow::datatypes::TimeUnit::Second)),
        "time64" => Some(DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)),
        "interval" => Some(DataType::Interval(
            arrow::datatypes::IntervalUnit::MonthDayNano,
        )),
        _ => None,
    }
}

fn parse_container_type(text: &str) -> Result<Option<DataType>> {
    let value = strip_named_type(text);
    let Some((prefix, inner)) = value.split_once('<') else {
        return Ok(None);
    };
    let inner = inner
        .strip_suffix('>')
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid type signature: {value}")))?;
    let inner = inner.trim();
    match prefix {
        "list" => Ok(Some(DataType::List(Arc::new(Field::new(
            "item",
            parse_type_signature(inner)?,
            true,
        ))))),
        "large_list" => Ok(Some(DataType::LargeList(Arc::new(Field::new(
            "item",
            parse_type_signature(inner)?,
            true,
        ))))),
        "map" => {
            let mut parts = inner.split(',');
            let Some(key) = parts.next() else {
                return Err(DataFusionError::Plan(format!(
                    "Invalid map type signature: {value}"
                )));
            };
            let Some(value) = parts.next() else {
                return Err(DataFusionError::Plan(format!(
                    "Invalid map type signature: {value}"
                )));
            };
            let key = parse_type_signature(key.trim())?;
            let value = parse_type_signature(value.trim())?;
            Ok(Some(DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", key, false),
                        Field::new("value", value, true),
                    ])),
                    false,
                )),
                false,
            )))
        }
        "struct" => {
            let mut fields = Vec::new();
            for field in inner.split(',') {
                let parts: Vec<&str> = field.split(':').collect();
                if parts.len() != 2 {
                    return Err(DataFusionError::Plan(format!(
                        "Invalid struct type signature: {value}"
                    )));
                }
                let name = parts[0].trim();
                let dtype = parse_type_signature(parts[1].trim())?;
                fields.push(Field::new(name, dtype, true));
            }
            Ok(Some(DataType::Struct(Fields::from(fields))))
        }
        _ => Ok(None),
    }
}

fn parse_decimal(text: &str) -> Result<Option<DataType>> {
    let Some(value) = text.strip_prefix("decimal") else {
        return Ok(None);
    };
    let args = value
        .trim()
        .strip_prefix('(')
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid type signature: {text}")))?;
    let (precision, scale) = parse_decimal_args(args)?;
    Ok(Some(DataType::Decimal128(precision, scale)))
}

fn parse_decimal_args(args: &str) -> Result<(u8, i8)> {
    let mut parts = args.split(',');
    let precision = parts
        .next()
        .ok_or_else(|| DataFusionError::Plan("Decimal precision missing".into()))?
        .trim()
        .parse::<u8>()
        .map_err(|err| DataFusionError::Plan(format!("Invalid decimal precision: {err}")))?;
    let scale = parts
        .next()
        .ok_or_else(|| DataFusionError::Plan("Decimal scale missing".into()))?
        .trim()
        .parse::<i8>()
        .map_err(|err| DataFusionError::Plan(format!("Invalid decimal scale: {err}")))?;
    Ok((precision, scale))
}

pub fn udf_config_payload(config: &ConfigOptions) -> HashMap<String, UdfConfigValue> {
    let policy = CodeAnatomyUdfConfig::from_config(config);
    let mut payload = HashMap::new();
    payload.insert(
        "utf8_normalize_form".to_string(),
        UdfConfigValue::String(policy.utf8_normalize_form.clone()),
    );
    payload.insert(
        "utf8_normalize_casefold".to_string(),
        UdfConfigValue::Bool(policy.utf8_normalize_casefold),
    );
    payload.insert(
        "utf8_normalize_collapse_ws".to_string(),
        UdfConfigValue::Bool(policy.utf8_normalize_collapse_ws),
    );
    payload.insert(
        "span_default_line_base".to_string(),
        UdfConfigValue::Int(policy.span_default_line_base as i64),
    );
    payload.insert(
        "span_default_col_unit".to_string(),
        UdfConfigValue::String(policy.span_default_col_unit.clone()),
    );
    payload.insert(
        "span_default_end_exclusive".to_string(),
        UdfConfigValue::Bool(policy.span_default_end_exclusive),
    );
    payload.insert(
        "map_normalize_key_case".to_string(),
        UdfConfigValue::String(policy.map_normalize_key_case.clone()),
    );
    payload.insert(
        "map_normalize_sort_keys".to_string(),
        UdfConfigValue::Bool(policy.map_normalize_sort_keys),
    );
    payload
}
