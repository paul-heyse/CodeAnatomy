use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringBuilder};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::lit;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_macros::user_doc;

use crate::compat::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use crate::udf::common::{
    columnar_to_optional_strings, ensure_map_arg, expand_string_signatures, expect_arg_len_exact,
    field_from_first_arg_typed, literal_scalar, normalize_semantic_type, normalize_text,
    scalar_argument_string, scalar_columnar_value, scalar_to_bool, scalar_to_string,
    signature_with_names, variadic_any_signature, SignatureEqHash, DEFAULT_NORMALIZE_FORM,
};
use crate::udf_config::CodeAnatomyUdfConfig;

#[user_doc(
    doc_section(label = "Metadata Functions"),
    description = "Attach semantic type metadata to an expression.",
    syntax_example = "semantic_tag(semantic_type, value)",
    argument(
        name = "semantic_type",
        description = "Semantic type label (for example: NodeId, EdgeId, SpanId)."
    ),
    argument(name = "value", description = "Expression to tag.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SemanticTagUdf {
    pub(crate) signature: SignatureEqHash,
}

impl ScalarUDFImpl for SemanticTagUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "semantic_tag"
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
                "semantic_tag expects two arguments".into(),
            ));
        }
        Ok(vec![DataType::Utf8, arg_types[1].clone()])
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        if args.arg_fields.len() != 2 {
            return Err(DataFusionError::Plan(
                "semantic_tag expects two arguments".into(),
            ));
        }
        let semantic = scalar_argument_string(&args, 0, "semantic_tag")?
            .and_then(|value| normalize_semantic_type(&value))
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "semantic_tag requires a non-empty semantic_type literal".into(),
                )
            })?;
        let base_field = &args.arg_fields[1];
        let mut field = Field::new(
            self.name(),
            base_field.data_type().clone(),
            base_field.is_nullable(),
        );
        let mut metadata = base_field.metadata().clone();
        metadata.insert("semantic_type".to_string(), semantic);
        field = field.with_metadata(metadata);
        Ok(Arc::new(field))
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.len() != 2 {
            return Err(DataFusionError::Plan(
                "semantic_tag expects two arguments".into(),
            ));
        }
        Ok(arg_types[1].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(DataFusionError::Plan(
                "semantic_tag expects two arguments".into(),
            ));
        }
        match &args.args[1] {
            ColumnarValue::Scalar(value) => Ok(ColumnarValue::Scalar(value.clone())),
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(array.clone())),
        }
    }
}

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Normalize UTF-8 text with Unicode normalization, case folding, and whitespace collapse.",
    syntax_example = "utf8_normalize(value, form, casefold, collapse_ws)",
    standard_argument(name = "value", prefix = "String"),
    argument(
        name = "form",
        description = "Unicode normalization form (NFC, NFD, NFKC, NFKD). Defaults to codeanatomy_udf.utf8_normalize_form (NFKC)."
    ),
    argument(
        name = "casefold",
        description = "Whether to lower-case the normalized text. Defaults to codeanatomy_udf.utf8_normalize_casefold (true)."
    ),
    argument(
        name = "collapse_ws",
        description = "Whether to collapse consecutive whitespace. Defaults to codeanatomy_udf.utf8_normalize_collapse_ws (true)."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct Utf8NormalizeUdf {
    pub(crate) signature: SignatureEqHash,
    pub(crate) policy: CodeAnatomyUdfConfig,
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        field_from_first_arg_typed(args, self.name(), DataType::Utf8)
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
            .unwrap_or_else(|| self.policy.utf8_normalize_form.clone());
        let casefold = scalars
            .get(2)
            .map(|value| scalar_to_bool(value, "utf8_normalize casefold flag"))
            .transpose()?
            .flatten()
            .unwrap_or(self.policy.utf8_normalize_casefold);
        let collapse_ws = scalars
            .get(3)
            .map(|value| scalar_to_bool(value, "utf8_normalize collapse_ws flag"))
            .transpose()?
            .flatten()
            .unwrap_or(self.policy.utf8_normalize_collapse_ws);
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
            scalar_to_string(&scalar)?.unwrap_or_else(|| self.policy.utf8_normalize_form.clone())
        } else {
            self.policy.utf8_normalize_form.clone()
        };
        let casefold = if args.args.len() >= 3 {
            let scalar = scalar_columnar_value(
                &args.args[2],
                "utf8_normalize casefold must be a scalar literal",
            )?;
            scalar_to_bool(&scalar, "utf8_normalize casefold flag")?
                .unwrap_or(self.policy.utf8_normalize_casefold)
        } else {
            self.policy.utf8_normalize_casefold
        };
        let collapse_ws = if args.args.len() >= 4 {
            let scalar = scalar_columnar_value(
                &args.args[3],
                "utf8_normalize collapse_ws must be a scalar literal",
            )?;
            scalar_to_bool(&scalar, "utf8_normalize collapse_ws flag")?
                .unwrap_or(self.policy.utf8_normalize_collapse_ws)
        } else {
            self.policy.utf8_normalize_collapse_ws
        };
        if let ColumnarValue::Scalar(value) = &args.args[0] {
            let normalized = scalar_to_string(value)?
                .map(|value| normalize_text(&value, &form, casefold, collapse_ws))
                .filter(|value| !value.is_empty());
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(normalized)));
        }
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
    doc_section(label = "String Functions"),
    description = "Trim text and return null when the result is blank.",
    syntax_example = "utf8_null_if_blank(value)",
    standard_argument(name = "value", prefix = "String")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct Utf8NullIfBlankUdf {
    pub(crate) signature: SignatureEqHash,
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        field_from_first_arg_typed(args, self.name(), DataType::Utf8)
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
        if let ColumnarValue::Scalar(scalar) = value {
            let trimmed = scalar_to_string(scalar)?.map(|value| value.trim().to_string());
            let normalized = trimmed
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string());
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(normalized)));
        }
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
pub(crate) struct QNameNormalizeUdf {
    pub(crate) signature: SignatureEqHash,
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        field_from_first_arg_typed(args, self.name(), DataType::Utf8)
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
        if args
            .args
            .iter()
            .all(|value| matches!(value, ColumnarValue::Scalar(_)))
        {
            let symbol = scalar_to_string(&scalar_columnar_value(
                &args.args[0],
                "qname_normalize symbol literal",
            )?)?;
            let module = if args.args.len() >= 2 {
                scalar_to_string(&scalar_columnar_value(
                    &args.args[1],
                    "qname_normalize module literal",
                )?)?
            } else {
                None
            };
            let normalized = match symbol.as_deref() {
                Some(symbol) => {
                    let normalized_symbol =
                        normalize_text(symbol, DEFAULT_NORMALIZE_FORM, true, true);
                    if normalized_symbol.is_empty() {
                        None
                    } else {
                        let normalized_module = module
                            .as_deref()
                            .map(|module| {
                                normalize_text(module, DEFAULT_NORMALIZE_FORM, true, true)
                            })
                            .filter(|module| !module.is_empty());
                        if normalized_symbol.contains('.') || normalized_module.is_none() {
                            Some(normalized_symbol)
                        } else if let Some(module) = normalized_module {
                            Some(format!("{module}.{normalized_symbol}"))
                        } else {
                            Some(normalized_symbol)
                        }
                    }
                }
                None => None,
            };
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(normalized)));
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

pub fn semantic_tag_udf() -> ScalarUDF {
    let signature = signature_with_names(
        Signature::one_of(vec![TypeSignature::Any(2)], Volatility::Immutable),
        &["semantic_type", "value"],
    );
    ScalarUDF::new_from_shared_impl(Arc::new(SemanticTagUdf {
        signature: SignatureEqHash::new(signature),
    }))
}

pub fn utf8_normalize_udf() -> ScalarUDF {
    let signature = variadic_any_signature(1, 4, Volatility::Immutable);
    ScalarUDF::new_from_shared_impl(Arc::new(Utf8NormalizeUdf {
        signature: SignatureEqHash::new(signature),
        policy: CodeAnatomyUdfConfig::default(),
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
