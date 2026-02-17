use std::any::Any;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Builder, Int32Builder, Int64Array,
    Int64Builder, StringBuilder, StructArray,
};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::lit;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_macros::user_doc;

use crate::compat::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use crate::udf::common::{
    all_scalars, columnar_result, columnar_to_bool, columnar_to_i32, columnar_to_i64,
    columnar_to_optional_strings, columnar_to_strings, eval_udf_on_literals, signature_with_names,
    span_metadata_from_scalars, span_struct_type, stable_id_value, user_defined_signature,
    variadic_any_signature, SignatureEqHash, NULL_SENTINEL, PART_SEPARATOR,
};
use crate::udf_config::CodeAnatomyUdfConfig;

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
    argument(
        name = "line_base",
        description = "Optional line base offset. Defaults to codeanatomy_udf.span_default_line_base (0)."
    ),
    argument(
        name = "col_unit",
        description = "Optional column encoding unit. Defaults to codeanatomy_udf.span_default_col_unit (byte)."
    ),
    argument(
        name = "end_exclusive",
        description = "Whether the end offset is exclusive. Defaults to codeanatomy_udf.span_default_end_exclusive (true)."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SpanMakeUdf {
    pub(crate) signature: SignatureEqHash,
    pub(crate) policy: CodeAnatomyUdfConfig,
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
        let metadata = span_metadata_from_scalars(&args, &self.policy)?;
        if !metadata.is_empty() {
            field = field.with_metadata(metadata);
        }
        Ok(Arc::new(field))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(span_struct_type())
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let Some(value) = eval_udf_on_literals(self, &args)? {
            return Ok(ExprSimplifyResult::Simplified(lit(value)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !(2..=5).contains(&args.args.len()) {
            return Err(DataFusionError::Plan(
                "span_make expects between two and five arguments".into(),
            ));
        }
        let all_scalar = all_scalars(&args.args);
        let num_rows = if all_scalar { 1 } else { args.number_rows };
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
            vec![Some(self.policy.span_default_line_base); num_rows]
        };
        let col_unit_values = if args.args.len() >= 4 {
            let values = columnar_to_optional_strings(
                &args.args[3],
                num_rows,
                "span_make col_unit must be string-compatible",
            )?;
            values
                .into_iter()
                .map(|value| value.unwrap_or_else(|| self.policy.span_default_col_unit.clone()))
                .collect::<Vec<_>>()
        } else {
            vec![self.policy.span_default_col_unit.clone(); num_rows]
        };
        let end_exclusive_values = if args.args.len() >= 5 {
            let values = columnar_to_bool(
                &args.args[4],
                num_rows,
                "span_make end_exclusive must be boolean-compatible",
            )?;
            values
                .into_iter()
                .map(|value| value.unwrap_or(self.policy.span_default_end_exclusive))
                .collect::<Vec<_>>()
        } else {
            vec![self.policy.span_default_end_exclusive; num_rows]
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
        columnar_result(Arc::new(span_array) as ArrayRef, all_scalar, "span_make")
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        let policy = CodeAnatomyUdfConfig::from_config(config);
        if policy == self.policy {
            return None;
        }
        let inner = Arc::new(Self {
            signature: self.signature.clone(),
            policy,
        });
        Some(ScalarUDF::new_from_shared_impl(inner))
    }
}

#[user_doc(
    doc_section(label = "Span Functions"),
    description = "Compute the length of a span.",
    syntax_example = "span_len(span)",
    argument(name = "span", description = "Span struct.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SpanLenUdf {
    pub(crate) signature: SignatureEqHash,
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

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let Some(value) = eval_udf_on_literals(self, &args)? {
            return Ok(ExprSimplifyResult::Simplified(lit(value)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [span_value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "span_len expects one argument".into(),
            ));
        };
        let all_scalar = all_scalars(&args.args);
        let num_rows = if all_scalar { 1 } else { args.number_rows };
        let span_array = span_value.to_array(num_rows)?;
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
        columnar_result(
            Arc::new(builder.finish()) as ArrayRef,
            all_scalar,
            "span_len",
        )
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
pub(crate) struct IntervalAlignScoreUdf {
    pub(crate) signature: SignatureEqHash,
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
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Float64,
            nullable,
        )))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let Some(value) = eval_udf_on_literals(self, &args)? {
            return Ok(ExprSimplifyResult::Simplified(lit(value)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left_start, left_end, right_start, right_end] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "interval_align_score expects four arguments".into(),
            ));
        };
        let all_scalar = all_scalars(&args.args);
        let num_rows = if all_scalar { 1 } else { args.number_rows };
        let left_start_values =
            columnar_to_i64(left_start, num_rows, "interval_align_score left_start")?;
        let left_end_values = columnar_to_i64(left_end, num_rows, "interval_align_score left_end")?;
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
        columnar_result(
            Arc::new(builder.finish()) as ArrayRef,
            all_scalar,
            "interval_align_score",
        )
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
pub(crate) struct SpanOverlapsUdf {
    pub(crate) signature: SignatureEqHash,
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

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let Some(value) = eval_udf_on_literals(self, &args)? {
            return Ok(ExprSimplifyResult::Simplified(lit(value)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left_value, right_value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "span_overlaps expects two arguments".into(),
            ));
        };
        let all_scalar = all_scalars(&args.args);
        let num_rows = if all_scalar { 1 } else { args.number_rows };
        let left_array = left_value.to_array(num_rows)?;
        let right_array = right_value.to_array(num_rows)?;
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

        let len = num_rows;
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
        columnar_result(
            Arc::new(builder.finish()) as ArrayRef,
            all_scalar,
            "span_overlaps",
        )
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
pub(crate) struct SpanContainsUdf {
    pub(crate) signature: SignatureEqHash,
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

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let Some(value) = eval_udf_on_literals(self, &args)? {
            return Ok(ExprSimplifyResult::Simplified(lit(value)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [left_value, right_value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "span_contains expects two arguments".into(),
            ));
        };
        let all_scalar = all_scalars(&args.args);
        let num_rows = if all_scalar { 1 } else { args.number_rows };
        let left_array = left_value.to_array(num_rows)?;
        let right_array = right_value.to_array(num_rows)?;
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

        let len = num_rows;
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
        columnar_result(
            Arc::new(builder.finish()) as ArrayRef,
            all_scalar,
            "span_contains",
        )
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
pub(crate) struct SpanIdUdf {
    pub(crate) signature: SignatureEqHash,
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

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let Some(value) = eval_udf_on_literals(self, &args)? {
            return Ok(ExprSimplifyResult::Simplified(lit(value)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !(4..=5).contains(&args.args.len()) {
            return Err(DataFusionError::Plan(
                "span_id expects four or five arguments".into(),
            ));
        }
        let all_scalar = all_scalars(&args.args);
        let num_rows = if all_scalar { 1 } else { args.number_rows };
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
        columnar_result(
            Arc::new(builder.finish()) as ArrayRef,
            all_scalar,
            "span_id",
        )
    }
}

pub fn span_make_udf() -> ScalarUDF {
    let signature = variadic_any_signature(Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(SpanMakeUdf {
        signature: SignatureEqHash::new(signature),
        policy: CodeAnatomyUdfConfig::default(),
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
    let signature = variadic_any_signature(Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(SpanIdUdf {
        signature: SignatureEqHash::new(signature),
    }))
}
