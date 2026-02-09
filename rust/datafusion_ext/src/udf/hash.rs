use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::lit;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_macros::user_doc;

use crate::compat::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use crate::udf::common::{
    columnar_to_optional_strings, columnar_to_strings, hash128_value, hash64_value, literal_scalar,
    literal_string, normalize_text, prefixed_hash64_value, scalar_argument_string,
    scalar_columnar_value, scalar_str, scalar_to_bool, scalar_to_string,
    scalar_to_string_with_sentinel, semantic_type_from_prefix, signature_with_names,
    stable_id_value, string_array_any, variadic_any_signature, SignatureEqHash,
    DEFAULT_NORMALIZE_FORM, NULL_SENTINEL, PART_SEPARATOR,
};

#[user_doc(
    doc_section(label = "Hashing Functions"),
    description = "Compute a stable 64-bit hash of a string using Blake2b.",
    syntax_example = "stable_hash64(value)",
    standard_argument(name = "value", prefix = "String")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct StableHash64Udf {
    pub(crate) signature: SignatureEqHash,
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
        if arg_types.len() != 1 {
            return Err(DataFusionError::Plan(
                "stable_hash64 expects one argument".into(),
            ));
        }
        Ok(vec![DataType::Utf8])
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
pub(crate) struct StableHash128Udf {
    pub(crate) signature: SignatureEqHash,
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
        let mut field = Field::new(self.name(), DataType::Utf8, nullable);
        if let Some(prefix) = scalar_argument_string(&args, 0, "stable_id_parts")? {
            if let Some(semantic_type) = semantic_type_from_prefix(&prefix) {
                let mut metadata = HashMap::new();
                metadata.insert("semantic_type".to_string(), semantic_type.to_string());
                field = field.with_metadata(metadata);
            }
        }
        Ok(Arc::new(field))
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
        description = "String namespace prefix to prepend to the hash."
    ),
    argument(name = "value", description = "String value to hash.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct PrefixedHash64Udf {
    pub(crate) signature: SignatureEqHash,
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
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(
                "prefixed_hash64 expects two arguments".into(),
            ));
        }
        Ok(vec![DataType::Utf8, DataType::Utf8])
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
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
pub(crate) struct StableIdUdf {
    pub(crate) signature: SignatureEqHash,
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
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(
                "stable_id expects two arguments".into(),
            ));
        }
        Ok(vec![DataType::Utf8, DataType::Utf8])
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .map(|field| field.is_nullable())
            .unwrap_or(true)
            || args
                .arg_fields
                .get(1)
                .map(|field| field.is_nullable())
                .unwrap_or(true);
        let mut field = Field::new(self.name(), DataType::Utf8, nullable);
        if let Some(prefix) = scalar_argument_string(&args, 0, "stable_id")? {
            if let Some(semantic_type) = semantic_type_from_prefix(&prefix) {
                let mut metadata = HashMap::new();
                metadata.insert("semantic_type".to_string(), semantic_type.to_string());
                field = field.with_metadata(metadata);
            }
        }
        Ok(Arc::new(field))
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
pub(crate) struct StableIdPartsUdf {
    pub(crate) signature: SignatureEqHash,
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
        let prefix = scalar_argument_string(&args, 0, "span_id")?;
        let semantic_type = prefix
            .as_deref()
            .and_then(semantic_type_from_prefix)
            .unwrap_or("SpanId");
        let mut metadata = HashMap::new();
        metadata.insert("semantic_type".to_string(), semantic_type.to_string());
        Ok(Arc::new(
            Field::new(self.name(), DataType::Utf8, nullable).with_metadata(metadata),
        ))
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
pub(crate) struct PrefixedHashParts64Udf {
    pub(crate) signature: SignatureEqHash,
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
        let mut field = Field::new(self.name(), DataType::Utf8, nullable);
        let prefix = scalar_argument_string(&args, 0, "span_id")?;
        let semantic = prefix
            .as_deref()
            .and_then(semantic_type_from_prefix)
            .unwrap_or("SpanId");
        let mut metadata = HashMap::new();
        metadata.insert("semantic_type".to_string(), semantic.to_string());
        field = field.with_metadata(metadata);
        Ok(Arc::new(field))
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
pub(crate) struct StableHashAnyUdf {
    pub(crate) signature: SignatureEqHash,
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
