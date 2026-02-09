use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ListArray, ListBuilder, MapArray, MapBuilder, StringBuilder, StructArray,
};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::lit;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_macros::user_doc;

use crate::compat::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};
use crate::udf::common::{
    all_scalars, columnar_result, columnar_to_optional_strings, ensure_list_arg, ensure_map_arg,
    eval_udf_on_literals, expect_arg_len, expect_arg_len_exact, field_from_first_arg_typed,
    scalar_columnar_value, scalar_to_bool, scalar_to_string, user_defined_signature,
    variadic_any_signature, SignatureEqHash,
};
use crate::udf_config::CodeAnatomyUdfConfig;

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
pub(crate) struct MapGetDefaultUdf {
    pub(crate) signature: SignatureEqHash,
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

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        field_from_first_arg_typed(args, self.name(), DataType::Utf8)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let Some(value) = eval_udf_on_literals(self, &args)? {
            return Ok(ExprSimplifyResult::Simplified(lit(value)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 3 {
            return Err(DataFusionError::Plan(
                "map_get_default expects exactly three arguments".into(),
            ));
        }
        let all_scalar = all_scalars(&args.args);
        let num_rows = if all_scalar { 1 } else { args.number_rows };
        let key_scalar = scalar_columnar_value(
            &args.args[1],
            "map_get_default key must be a scalar literal",
        )?;
        let Some(key) = scalar_to_string(&key_scalar)? else {
            return Err(DataFusionError::Plan(
                "map_get_default key cannot be null".into(),
            ));
        };

        let map_array = args.args[0].to_array(num_rows)?;
        let map_values = map_array
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| DataFusionError::Plan("map_get_default expects a map input".into()))?;

        let default_values = columnar_to_optional_strings(
            &args.args[2],
            num_rows,
            "map_get_default default must be string-compatible",
        )?;

        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 8);
        for (row, default_value) in default_values.iter().enumerate().take(num_rows) {
            let default_value = default_value.as_deref();
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
        columnar_result(
            Arc::new(builder.finish()) as ArrayRef,
            all_scalar,
            "map_get_default",
        )
    }
}

#[user_doc(
    doc_section(label = "Nested Functions"),
    description = "Normalize map keys and optionally sort them deterministically.",
    syntax_example = "map_normalize(map_expr, key_case, sort_keys)",
    argument(name = "map_expr", description = "Map expression."),
    argument(
        name = "key_case",
        description = "Key case normalization: lower, upper, none. Defaults to codeanatomy_udf.map_normalize_key_case (lower)."
    ),
    argument(
        name = "sort_keys",
        description = "Whether to sort keys deterministically. Defaults to codeanatomy_udf.map_normalize_sort_keys (true)."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct MapNormalizeUdf {
    pub(crate) signature: SignatureEqHash,
    pub(crate) policy: CodeAnatomyUdfConfig,
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

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let Some(value) = eval_udf_on_literals(self, &args)? {
            return Ok(ExprSimplifyResult::Simplified(lit(value)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !(1..=3).contains(&args.args.len()) {
            return Err(DataFusionError::Plan(
                "map_normalize expects between one and three arguments".into(),
            ));
        }
        let all_scalar = all_scalars(&args.args);
        let num_rows = if all_scalar { 1 } else { args.number_rows };
        let key_case = if args.args.len() >= 2 {
            let scalar = scalar_columnar_value(
                &args.args[1],
                "map_normalize key_case must be a scalar literal",
            )?;
            scalar_to_string(&scalar)?.unwrap_or_else(|| self.policy.map_normalize_key_case.clone())
        } else {
            self.policy.map_normalize_key_case.clone()
        };
        let sort_keys = if args.args.len() >= 3 {
            let scalar = scalar_columnar_value(
                &args.args[2],
                "map_normalize sort_keys must be a scalar literal",
            )?;
            scalar_to_bool(&scalar, "map_normalize sort_keys flag")?
                .unwrap_or(self.policy.map_normalize_sort_keys)
        } else {
            self.policy.map_normalize_sort_keys
        };

        let map_array = args.args[0].to_array(num_rows)?;
        let maps = map_array
            .as_any()
            .downcast_ref::<MapArray>()
            .ok_or_else(|| DataFusionError::Plan("map_normalize expects a map input".into()))?;

        let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
        for row in 0..num_rows {
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
        columnar_result(
            Arc::new(builder.finish()) as ArrayRef,
            all_scalar,
            "map_normalize",
        )
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
    doc_section(label = "Nested Functions"),
    description = "Remove null entries from a list and coerce values to strings.",
    syntax_example = "list_compact(list_expr)",
    argument(name = "list_expr", description = "List expression.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ListCompactUdf {
    pub(crate) signature: SignatureEqHash,
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

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let Some(value) = eval_udf_on_literals(self, &args)? {
            return Ok(ExprSimplifyResult::Simplified(lit(value)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "list_compact expects one argument".into(),
            ));
        };
        let all_scalar = all_scalars(&args.args);
        let num_rows = if all_scalar { 1 } else { args.number_rows };
        let list_array = value.to_array(num_rows)?;
        let lists = list_array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("list_compact expects a list input".into()))?;
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in 0..num_rows {
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
        columnar_result(
            Arc::new(builder.finish()) as ArrayRef,
            all_scalar,
            "list_compact",
        )
    }
}

#[user_doc(
    doc_section(label = "Nested Functions"),
    description = "Compact a list, remove duplicates, and sort deterministically.",
    syntax_example = "list_unique_sorted(list_expr)",
    argument(name = "list_expr", description = "List expression.")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ListUniqueSortedUdf {
    pub(crate) signature: SignatureEqHash,
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

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        if let Some(value) = eval_udf_on_literals(self, &args)? {
            return Ok(ExprSimplifyResult::Simplified(lit(value)));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_slice() else {
            return Err(DataFusionError::Plan(
                "list_unique_sorted expects one argument".into(),
            ));
        };
        let all_scalar = all_scalars(&args.args);
        let num_rows = if all_scalar { 1 } else { args.number_rows };
        let list_array = value.to_array(num_rows)?;
        let lists = list_array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Plan("list_unique_sorted expects a list input".into())
            })?;
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in 0..num_rows {
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
        columnar_result(
            Arc::new(builder.finish()) as ArrayRef,
            all_scalar,
            "list_unique_sorted",
        )
    }
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
        policy: CodeAnatomyUdfConfig::default(),
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
