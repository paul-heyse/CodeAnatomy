use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Builder, Int64Array, Int64Builder, StructArray};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_macros::user_doc;

use crate::compat::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, TypeSignature, Volatility,
};
use crate::udf::common::{
    all_scalars, columnar_result, columnar_to_i64, columnar_to_optional_strings,
    expand_string_signatures, scalar_str, signature_with_names, string_array_any,
    string_int_string_signature, SignatureEqHash, ENC_UTF16, ENC_UTF32, ENC_UTF8,
};

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
        ColUnit::Byte => Ok(offset.min(line.len())),
    }
}

fn byte_offset_from_py_index(line: &str, py_index: usize) -> usize {
    let prefix: String = line.chars().take(py_index).collect();
    prefix.len()
}

fn canonicalize_side(line_start: i64, line_text: &str, col: i64, unit: ColUnit) -> Result<i64> {
    let offset = if matches!(unit, ColUnit::Byte) {
        clamp_offset(col, line_text.len()) as i64
    } else {
        let py_index = code_unit_offset_to_py_index(line_text, normalize_offset(col), unit)?;
        byte_offset_from_py_index(line_text, py_index) as i64
    };
    Ok(line_start.saturating_add(offset))
}

fn canonicalize_span_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("bstart", DataType::Int64, true),
        Field::new("bend", DataType::Int64, true),
    ]))
}

#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Normalize a position encoding name to its numeric code.",
    syntax_example = "position_encoding_norm(value)",
    standard_argument(name = "value", prefix = "String")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct PositionEncodingUdf {
    pub(crate) signature: SignatureEqHash,
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
pub(crate) struct ColToByteUdf {
    pub(crate) signature: SignatureEqHash,
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
            .first()
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
                clamp_offset(*offset, line.len()) as i64
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
                let max_len = line.len();
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

#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Canonicalize line/column spans to byte start/end offsets.",
    syntax_example = "canonicalize_byte_span(start_line_start_byte, start_line_text, start_col, end_line_start_byte, end_line_text, end_col, col_unit)",
    argument(
        name = "start_line_start_byte",
        description = "Start-line byte offset from file start."
    ),
    argument(
        name = "start_line_text",
        description = "Start-line text used for encoding-aware conversion."
    ),
    argument(name = "start_col", description = "Start column offset."),
    argument(
        name = "end_line_start_byte",
        description = "End-line byte offset from file start."
    ),
    argument(
        name = "end_line_text",
        description = "End-line text used for encoding-aware conversion."
    ),
    argument(name = "end_col", description = "End column offset."),
    argument(
        name = "col_unit",
        description = "Encoding unit (BYTE, UTF8, UTF16, UTF32)."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct CanonicalizeByteSpanUdf {
    pub(crate) signature: SignatureEqHash,
}

impl ScalarUDFImpl for CanonicalizeByteSpanUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "canonicalize_byte_span"
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        self.signature.signature()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 7 {
            return Err(DataFusionError::Plan(
                "canonicalize_byte_span expects seven arguments".into(),
            ));
        }
        Ok(vec![
            DataType::Int64,
            DataType::Utf8,
            DataType::Int64,
            DataType::Int64,
            DataType::Utf8,
            DataType::Int64,
            DataType::Utf8,
        ])
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            canonicalize_span_type(),
            nullable,
        )))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(canonicalize_span_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 7 {
            return Err(DataFusionError::Plan(
                "canonicalize_byte_span expects seven arguments".into(),
            ));
        }
        let all_scalar = all_scalars(&args.args);
        let num_rows = if all_scalar { 1 } else { args.number_rows };
        let start_line_start_values = columnar_to_i64(
            &args.args[0],
            num_rows,
            "canonicalize_byte_span start_line_start_byte must be int64-compatible",
        )?;
        let start_line_text_values = columnar_to_optional_strings(
            &args.args[1],
            num_rows,
            "canonicalize_byte_span start_line_text must be string-compatible",
        )?;
        let start_col_values = columnar_to_i64(
            &args.args[2],
            num_rows,
            "canonicalize_byte_span start_col must be int64-compatible",
        )?;
        let end_line_start_values = columnar_to_i64(
            &args.args[3],
            num_rows,
            "canonicalize_byte_span end_line_start_byte must be int64-compatible",
        )?;
        let end_line_text_values = columnar_to_optional_strings(
            &args.args[4],
            num_rows,
            "canonicalize_byte_span end_line_text must be string-compatible",
        )?;
        let end_col_values = columnar_to_i64(
            &args.args[5],
            num_rows,
            "canonicalize_byte_span end_col must be int64-compatible",
        )?;
        let col_unit_values = columnar_to_optional_strings(
            &args.args[6],
            num_rows,
            "canonicalize_byte_span col_unit must be string-compatible",
        )?;

        let mut bstart_builder = Int64Builder::with_capacity(num_rows);
        let mut bend_builder = Int64Builder::with_capacity(num_rows);
        for row in 0..num_rows {
            let unit = col_unit_values[row].as_deref().map(col_unit_from_text);
            if let (Some(line_start), Some(line_text), Some(col), Some(unit)) = (
                start_line_start_values[row],
                start_line_text_values[row].as_deref(),
                start_col_values[row],
                unit,
            ) {
                bstart_builder.append_value(canonicalize_side(line_start, line_text, col, unit)?);
            } else {
                bstart_builder.append_null();
            }
            if let (Some(line_start), Some(line_text), Some(col), Some(unit)) = (
                end_line_start_values[row],
                end_line_text_values[row].as_deref(),
                end_col_values[row],
                unit,
            ) {
                bend_builder.append_value(canonicalize_side(line_start, line_text, col, unit)?);
            } else {
                bend_builder.append_null();
            }
        }
        let array = Arc::new(StructArray::new(
            Fields::from(vec![
                Field::new("bstart", DataType::Int64, true),
                Field::new("bend", DataType::Int64, true),
            ]),
            vec![
                Arc::new(bstart_builder.finish()) as ArrayRef,
                Arc::new(bend_builder.finish()) as ArrayRef,
            ],
            None,
        )) as ArrayRef;
        columnar_result(array, all_scalar, "canonicalize_byte_span")
    }
}

pub fn position_encoding_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::one_of(vec![TypeSignature::Any(1)], Volatility::Immutable),
        &["value"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(PositionEncodingUdf {
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

pub fn canonicalize_byte_span_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::one_of(
            expand_string_signatures(&[
                DataType::Int64,
                DataType::Utf8,
                DataType::Int64,
                DataType::Int64,
                DataType::Utf8,
                DataType::Int64,
                DataType::Utf8,
            ]),
            Volatility::Immutable,
        ),
        &[
            "start_line_start_byte",
            "start_line_text",
            "start_col",
            "end_line_start_byte",
            "end_line_text",
            "end_col",
            "col_unit",
        ],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(CanonicalizeByteSpanUdf {
        signature: SignatureEqHash::new(signature),
    }))
}
