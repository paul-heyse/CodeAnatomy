use std::any::Any;
use std::sync::Arc;

use arrow::array::{MapBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::lit;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::sort_properties::ExprProperties;
use datafusion_macros::user_doc;

use crate::compat::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use crate::udf::common::{
    eval_udf_on_literals, field_from_first_arg_typed, scalar_str, signature_with_names,
    user_defined_signature, SignatureEqHash,
};

#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Pass-through CPG score function for compatibility.",
    syntax_example = "cpg_score(value)",
    argument(name = "value", description = "Score value to pass through.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct CpgScoreUdf {
    pub(crate) signature: SignatureEqHash,
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
        field_from_first_arg_typed(args, self.name(), DataType::Float64)
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

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let Some(value) = eval_udf_on_literals(self, &args)? {
            return Ok(ExprSimplifyResult::Simplified(lit(value)));
        }
        Ok(ExprSimplifyResult::Original(args))
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
pub(crate) struct ArrowMetadataUdf {
    pub(crate) signature: SignatureEqHash,
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
