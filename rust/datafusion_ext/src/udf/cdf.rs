use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Int32Builder};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result};
use datafusion_macros::user_doc;

use crate::compat::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use crate::udf::common::{
    columnar_to_optional_strings, expand_string_signatures, signature_with_names, SignatureEqHash,
};

pub(crate) const CDF_RANK_UPDATE_POSTIMAGE: i32 = 3;
pub(crate) const CDF_RANK_INSERT: i32 = 2;
pub(crate) const CDF_RANK_DELETE: i32 = 1;
pub(crate) const CDF_RANK_UPDATE_PREIMAGE: i32 = 0;
pub(crate) const CDF_RANK_UNKNOWN: i32 = -1;

fn cdf_rank(change_type: &str) -> i32 {
    let normalized = change_type.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "update_postimage" => CDF_RANK_UPDATE_POSTIMAGE,
        "insert" => CDF_RANK_INSERT,
        "delete" => CDF_RANK_DELETE,
        "update_preimage" => CDF_RANK_UPDATE_PREIMAGE,
        _ => CDF_RANK_UNKNOWN,
    }
}

#[user_doc(
    doc_section(label = "Delta Functions"),
    description = "Rank Delta change types deterministically.",
    syntax_example = "cdf_change_rank(change_type)",
    argument(name = "change_type", description = "Delta change type string.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct CdfChangeRankUdf {
    pub(crate) signature: SignatureEqHash,
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
pub(crate) struct CdfIsUpsertUdf {
    pub(crate) signature: SignatureEqHash,
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
            builder.append_value(rank == CDF_RANK_UPDATE_POSTIMAGE || rank == CDF_RANK_INSERT);
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
pub(crate) struct CdfIsDeleteUdf {
    pub(crate) signature: SignatureEqHash,
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
            builder.append_value(cdf_rank(&value) == CDF_RANK_DELETE);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
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
