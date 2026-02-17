use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion_common::{DataFusionError, Result};
use datafusion_macros::user_doc;

use crate::compat::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use crate::udf::common::{
    all_scalars, columnar_result, ensure_struct_arg, scalar_argument_string,
    variadic_any_signature, SignatureEqHash,
};

#[user_doc(
    doc_section(label = "Nested Functions"),
    description = "Select a subset of fields from a struct using scalar field names.",
    syntax_example = "struct_pick(struct_expr, field1, field2, ...)",
    argument(name = "struct_expr", description = "Struct expression."),
    argument(name = "fields", description = "Scalar field names to select.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct StructPickUdf {
    pub(crate) signature: SignatureEqHash,
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
        let all_scalar = all_scalars(&args.args);
        let num_rows = if all_scalar { 1 } else { args.number_rows };
        let struct_array = args.args[0].to_array(num_rows)?;
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
        columnar_result(Arc::new(picked) as ArrayRef, all_scalar, "struct_pick")
    }
}

pub fn struct_pick_udf() -> ScalarUDF {
    let signature = variadic_any_signature(Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(StructPickUdf {
        signature: SignatureEqHash::new(signature),
    }))
}
