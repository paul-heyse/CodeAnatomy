use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, Int64Builder, LargeStringArray, ListArray,
    ListBuilder, StringArray, StringBuilder, StringViewArray,
};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator,
    Signature, TypeSignature, Volatility,
};
use datafusion_functions_aggregate::first_last::{first_value_udaf, last_value_udaf};
use datafusion_macros::user_doc;

use crate::{aggregate_udfs, macros::AggregateUdfSpec};

const LIST_UNIQUE_NAME: &str = "list_unique";
const COUNT_DISTINCT_NAME: &str = "count_distinct_agg";
const COLLECT_SET_NAME: &str = "collect_set";
const COUNT_IF_NAME: &str = "count_if";
const ANY_VALUE_DET_NAME: &str = "any_value_det";
const ARG_MAX_NAME: &str = "arg_max";
const ARG_MIN_NAME: &str = "arg_min";
const ASOF_SELECT_NAME: &str = "asof_select";
const STRING_AGG_NAME: &str = "string_agg";

pub fn builtin_udafs() -> Vec<AggregateUDF> {
    builtin_udaf_specs()
        .into_iter()
        .map(|spec| {
            let mut udaf = (spec.builder)();
            if !spec.aliases.is_empty() {
                udaf = udaf.with_aliases(spec.aliases.iter().copied());
            }
            udaf
        })
        .collect()
}

fn builtin_udaf_specs() -> Vec<AggregateUdfSpec> {
    aggregate_udfs![
        "list_unique" => list_unique_udaf;
        "collect_set" => collect_set_udaf;
        "count_distinct_agg" => count_distinct_udaf;
        "count_if" => count_if_udaf;
        "any_value_det" => any_value_det_udaf;
        "arg_max" => arg_max_udaf;
        "asof_select" => asof_select_udaf;
        "arg_min" => arg_min_udaf;
        "first_value" => first_value_base_udaf, aliases: ["first_value_agg"];
        "last_value" => last_value_base_udaf, aliases: ["last_value_agg"];
        "string_agg" => string_agg_base_udaf;
    ]
}

fn list_unique_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(ListUniqueUdaf::new()))
}

fn collect_set_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(CollectSetUdaf::new()))
}

fn count_distinct_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(CountDistinctUdaf::new()))
}

fn first_value_base_udaf() -> AggregateUDF {
    first_value_udaf().as_ref().clone()
}

fn last_value_base_udaf() -> AggregateUDF {
    last_value_udaf().as_ref().clone()
}

fn string_agg_base_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(StringAggDetUdaf::new()))
}

#[user_doc(
    doc_section(label = "Built-in Functions"),
    description = "Aggregate values into a list and remove duplicates.",
    syntax_example = "list_unique(value)",
    standard_argument(name = "value", prefix = "String")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct ListUniqueUdaf {
    signature: Signature,
}

impl ListUniqueUdaf {
    fn new() -> Self {
        let signature = string_signature(Volatility::Immutable)
            .with_parameter_names(vec!["value".to_string()])
            .unwrap_or_else(|_| string_signature(Volatility::Immutable));
        Self { signature }
    }
}

impl AggregateUDFImpl for ListUniqueUdaf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        LIST_UNIQUE_NAME
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn supports_null_handling_clause(&self) -> bool {
        true
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Utf8,
            true,
        ))))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new_list(
            format_state_name(args.name, "list_unique"),
            Field::new_list_field(DataType::Utf8, true),
            true,
        )
        .into()])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ListUniqueAccumulator::new(acc_args.ignore_nulls)))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(ListUniqueGroupsAccumulator::new(
            args.ignore_nulls,
        )))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ListUniqueSlidingAccumulator::new(
            args.ignore_nulls,
        )))
    }
}

#[user_doc(
    doc_section(label = "Built-in Functions"),
    description = "Aggregate values into a deterministic set (unique, sorted).",
    syntax_example = "collect_set(value)",
    standard_argument(name = "value", prefix = "String")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct CollectSetUdaf {
    signature: Signature,
}

impl CollectSetUdaf {
    fn new() -> Self {
        let signature = signature_with_names(string_signature(Volatility::Immutable), &["value"]);
        Self { signature }
    }
}

impl AggregateUDFImpl for CollectSetUdaf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        COLLECT_SET_NAME
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn supports_null_handling_clause(&self) -> bool {
        true
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new_list_field(
            DataType::Utf8,
            true,
        ))))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new_list(
            format_state_name(args.name, "collect_set"),
            Field::new_list_field(DataType::Utf8, true),
            true,
        )
        .into()])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ListUniqueAccumulator::new(acc_args.ignore_nulls)))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(ListUniqueGroupsAccumulator::new(
            args.ignore_nulls,
        )))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ListUniqueSlidingAccumulator::new(
            args.ignore_nulls,
        )))
    }
}

#[user_doc(
    doc_section(label = "Built-in Functions"),
    description = "Count distinct values in the aggregate window.",
    syntax_example = "count_distinct_agg(value)",
    standard_argument(name = "value", prefix = "String")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct CountDistinctUdaf {
    signature: Signature,
}

impl CountDistinctUdaf {
    fn new() -> Self {
        let signature = string_signature(Volatility::Immutable)
            .with_parameter_names(vec!["value".to_string()])
            .unwrap_or_else(|_| string_signature(Volatility::Immutable));
        Self { signature }
    }
}

fn string_signature(volatility: Volatility) -> Signature {
    let signatures = vec![
        TypeSignature::Exact(vec![DataType::Utf8]),
        TypeSignature::Exact(vec![DataType::LargeUtf8]),
        TypeSignature::Exact(vec![DataType::Utf8View]),
    ];
    Signature::one_of(signatures, volatility)
}

fn signature_with_names(signature: Signature, names: &[&str]) -> Signature {
    let parameter_names = names.iter().map(|name| (*name).to_string()).collect();
    signature
        .clone()
        .with_parameter_names(parameter_names)
        .unwrap_or(signature)
}

impl AggregateUDFImpl for CountDistinctUdaf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        COUNT_DISTINCT_NAME
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn supports_null_handling_clause(&self) -> bool {
        true
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new_list(
            format_state_name(args.name, "count_distinct"),
            Field::new_list_field(DataType::Utf8, true),
            true,
        )
        .into()])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountDistinctAccumulator::new(
            acc_args.ignore_nulls,
        )))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(CountDistinctGroupsAccumulator::new(
            args.ignore_nulls,
        )))
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountDistinctSlidingAccumulator::new(
            args.ignore_nulls,
        )))
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(0)))
    }
}

fn count_if_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(CountIfUdaf::new()))
}

fn boolean_signature(volatility: Volatility) -> Signature {
    Signature::one_of(
        vec![TypeSignature::Exact(vec![DataType::Boolean])],
        volatility,
    )
}

fn boolean_array<'a>(array: &'a ArrayRef, message: &str) -> Result<&'a BooleanArray> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| DataFusionError::Plan(message.to_string()))
}

fn ensure_count_capacity(counts: &mut Vec<i64>, total_num_groups: usize) {
    if total_num_groups > counts.len() {
        counts.resize(total_num_groups, 0);
    }
}

fn build_count_array(counts: &[i64]) -> Int64Array {
    let mut builder = Int64Builder::with_capacity(counts.len());
    for count in counts {
        builder.append_value(*count);
    }
    builder.finish()
}

#[user_doc(
    doc_section(label = "Built-in Functions"),
    description = "Count rows where the predicate evaluates to true.",
    syntax_example = "count_if(predicate)",
    standard_argument(name = "predicate", prefix = "Boolean")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct CountIfUdaf {
    signature: Signature,
}

impl CountIfUdaf {
    fn new() -> Self {
        let signature =
            signature_with_names(boolean_signature(Volatility::Immutable), &["predicate"]);
        Self { signature }
    }
}

impl AggregateUDFImpl for CountIfUdaf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        COUNT_IF_NAME
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn supports_null_handling_clause(&self) -> bool {
        true
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Field::new(
            format_state_name(args.name, "count_if"),
            DataType::Int64,
            false,
        )
        .into()])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountIfAccumulator::default()))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(CountIfGroupsAccumulator::default()))
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(0)))
    }
}

#[derive(Debug, Default)]
struct CountIfAccumulator {
    count: i64,
}

impl Accumulator for CountIfAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [predicate] = values else {
            return Err(DataFusionError::Plan(
                "count_if expects a single predicate argument".into(),
            ));
        };
        let predicate = boolean_array(predicate, "count_if expects boolean input")?;
        for row in 0..predicate.len() {
            if predicate.is_null(row) || !predicate.value(row) {
                continue;
            }
            self.count += 1;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [state] = states else {
            return Err(DataFusionError::Plan(
                "count_if expects a single state value".into(),
            ));
        };
        let state = state
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Plan("count_if expects int64 state".into()))?;
        for row in 0..state.len() {
            if state.is_null(row) {
                continue;
            }
            self.count += state.value(row);
        }
        Ok(())
    }

    /// Support retraction for bounded window frames.
    ///
    /// When a row exits the window frame, retract_batch is called
    /// with the rows leaving the frame. For count_if, we decrement
    /// the count for rows that matched the predicate.
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [predicate] = values else {
            return Err(DataFusionError::Plan(
                "count_if expects a single predicate argument".into(),
            ));
        };
        let predicate = boolean_array(predicate, "count_if expects boolean input")?;
        for row in 0..predicate.len() {
            if predicate.is_null(row) || !predicate.value(row) {
                continue;
            }
            self.count -= 1;
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Debug, Default)]
struct CountIfGroupsAccumulator {
    counts: Vec<i64>,
}

impl GroupsAccumulator for CountIfGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let [predicate] = values else {
            return Err(DataFusionError::Plan(
                "count_if expects a single predicate argument".into(),
            ));
        };
        let predicate = boolean_array(predicate, "count_if expects boolean input")?;
        ensure_count_capacity(&mut self.counts, total_num_groups);
        for (row, group_index) in group_indices.iter().enumerate() {
            if let Some(filter) = opt_filter {
                if filter.is_null(row) || !filter.value(row) {
                    continue;
                }
            }
            if predicate.is_null(row) || !predicate.value(row) {
                continue;
            }
            if let Some(count) = self.counts.get_mut(*group_index) {
                *count += 1;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let total_groups = self.counts.len();
        let (emit_count, retain) = match emit_to {
            EmitTo::All => (total_groups, false),
            EmitTo::First(count) => (count.min(total_groups), true),
        };
        let array = build_count_array(&self.counts[..emit_count]);
        if retain {
            self.counts.drain(0..emit_count);
        } else {
            self.counts.clear();
        }
        Ok(Arc::new(array))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let array = self.evaluate(emit_to)?;
        Ok(vec![array])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let [state] = values else {
            return Err(DataFusionError::Plan(
                "count_if expects a single state value".into(),
            ));
        };
        let state = state
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| DataFusionError::Plan("count_if expects int64 state".into()))?;
        ensure_count_capacity(&mut self.counts, total_num_groups);
        for (row, group_index) in group_indices.iter().enumerate() {
            if let Some(filter) = opt_filter {
                if filter.is_null(row) || !filter.value(row) {
                    continue;
                }
            }
            if state.is_null(row) {
                continue;
            }
            if let Some(count) = self.counts.get_mut(*group_index) {
                *count += state.value(row);
            }
        }
        Ok(())
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let [predicate] = values else {
            return Err(DataFusionError::Plan(
                "count_if expects a single predicate argument".into(),
            ));
        };
        let predicate = boolean_array(predicate, "count_if expects boolean input")?;
        let mut builder = Int64Builder::with_capacity(predicate.len());
        for row in 0..predicate.len() {
            if let Some(filter) = opt_filter {
                if filter.is_null(row) || !filter.value(row) {
                    builder.append_value(0);
                    continue;
                }
            }
            if predicate.is_null(row) || !predicate.value(row) {
                builder.append_value(0);
            } else {
                builder.append_value(1);
            }
        }
        Ok(vec![Arc::new(builder.finish())])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of_val(self) + (self.counts.len() * std::mem::size_of::<i64>())
    }
}

fn any_value_det_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(AnyValueDetUdaf::new()))
}

fn arg_max_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(ArgMaxUdaf::new()))
}

fn asof_select_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(AsofSelectUdaf::new()))
}

fn arg_min_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(ArgMinUdaf::new()))
}

fn arg_best_signature(volatility: Volatility) -> Signature {
    signature_with_names(
        Signature::one_of(vec![TypeSignature::UserDefined], volatility),
        &["value", "order_key"],
    )
}

fn arg_best_coerce_types(name: &str, arg_types: &[DataType]) -> Result<Vec<DataType>> {
    if arg_types.len() != 2 {
        return Err(DataFusionError::Plan(format!(
            "{name} expects exactly two arguments"
        )));
    }
    Ok(vec![DataType::Utf8, DataType::Int64])
}

fn arg_best_state_fields(args: StateFieldsArgs, label: &str) -> Vec<FieldRef> {
    let value_suffix = format!("{label}_value");
    let key_suffix = format!("{label}_key");
    vec![
        Field::new(
            format_state_name(args.name, value_suffix.as_str()),
            DataType::Utf8,
            true,
        )
        .into(),
        Field::new(
            format_state_name(args.name, key_suffix.as_str()),
            DataType::Int64,
            true,
        )
        .into(),
    ]
}

fn scalar_to_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value) => {
            value.clone()
        }
        other => Some(other.to_string()),
    }
}

fn scalar_to_i64(value: &ScalarValue, context: &str) -> Result<Option<i64>> {
    match value {
        ScalarValue::Null => Ok(None),
        ScalarValue::Int8(value) => Ok(value.map(i64::from)),
        ScalarValue::Int16(value) => Ok(value.map(i64::from)),
        ScalarValue::Int32(value) => Ok(value.map(i64::from)),
        ScalarValue::Int64(value) => Ok(*value),
        ScalarValue::UInt8(value) => Ok(value.map(i64::from)),
        ScalarValue::UInt16(value) => Ok(value.map(i64::from)),
        ScalarValue::UInt32(value) => Ok(value.map(i64::from)),
        ScalarValue::UInt64(value) => Ok(value.map(|value| value as i64)),
        ScalarValue::Float32(value) => Ok(value.map(|value| value as i64)),
        ScalarValue::Float64(value) => Ok(value.map(|value| value as i64)),
        ScalarValue::TimestampSecond(value, _)
        | ScalarValue::TimestampMillisecond(value, _)
        | ScalarValue::TimestampMicrosecond(value, _)
        | ScalarValue::TimestampNanosecond(value, _) => Ok(*value),
        ScalarValue::Date32(value) => Ok(value.map(i64::from)),
        ScalarValue::Date64(value) => Ok(*value),
        ScalarValue::DurationSecond(value)
        | ScalarValue::DurationMillisecond(value)
        | ScalarValue::DurationMicrosecond(value)
        | ScalarValue::DurationNanosecond(value) => Ok(*value),
        ScalarValue::Utf8(value) | ScalarValue::LargeUtf8(value) | ScalarValue::Utf8View(value) => {
            if let Some(text) = value {
                let parsed = text.parse::<i64>().map_err(|err| {
                    DataFusionError::Plan(format!(
                        "{context}: failed to parse order_key as int64: {err}"
                    ))
                })?;
                Ok(Some(parsed))
            } else {
                Ok(None)
            }
        }
        other => Err(DataFusionError::Plan(format!(
            "{context}: unsupported order_key type: {other:?}"
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ArgBestMode {
    Min,
    Max,
}

#[derive(Debug)]
struct ArgBestAccumulator {
    mode: ArgBestMode,
    best_value: Option<String>,
    best_key: Option<i64>,
}

impl ArgBestAccumulator {
    fn new(mode: ArgBestMode) -> Self {
        Self {
            mode,
            best_value: None,
            best_key: None,
        }
    }

    fn update_candidate(&mut self, value: Option<String>, key: Option<i64>) {
        let Some(key) = key else {
            return;
        };
        let Some(value) = value else {
            return;
        };
        match self.best_key {
            None => {
                self.best_key = Some(key);
                self.best_value = Some(value);
            }
            Some(best_key) => {
                let better_key = match self.mode {
                    ArgBestMode::Min => key < best_key,
                    ArgBestMode::Max => key > best_key,
                };
                if better_key {
                    self.best_key = Some(key);
                    self.best_value = Some(value);
                    return;
                }
                if key != best_key {
                    return;
                }
                let replace = match &self.best_value {
                    None => true,
                    Some(best_value) => value.as_str() < best_value.as_str(),
                };
                if replace {
                    self.best_key = Some(key);
                    self.best_value = Some(value);
                }
            }
        }
    }

    fn update_from_arrays(
        &mut self,
        values: &ArrayRef,
        keys: &ArrayRef,
        context: &str,
    ) -> Result<()> {
        if values.len() != keys.len() {
            return Err(DataFusionError::Plan(format!(
                "{context}: value and order_key lengths must match"
            )));
        }
        for row in 0..values.len() {
            let value = ScalarValue::try_from_array(values.as_ref(), row)?;
            let key = ScalarValue::try_from_array(keys.as_ref(), row)?;
            let value_text = scalar_to_string(&value);
            let key_value = scalar_to_i64(&key, context)?;
            self.update_candidate(value_text, key_value);
        }
        Ok(())
    }
}

impl Accumulator for ArgBestAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [values, order_key] = values else {
            return Err(DataFusionError::Plan(
                "arg_* aggregators expect value and order_key arguments".into(),
            ));
        };
        self.update_from_arrays(values, order_key, "arg_* aggregators")
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8(self.best_value.clone()))
    }

    fn size(&self) -> usize {
        let value_size = self
            .best_value
            .as_ref()
            .map(|value| value.len())
            .unwrap_or(0);
        size_of_val(self) + value_size
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Utf8(self.best_value.clone()),
            ScalarValue::Int64(self.best_key),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [values, order_key] = states else {
            return Err(DataFusionError::Plan(
                "arg_* aggregators expect value and order_key state".into(),
            ));
        };
        self.update_from_arrays(values, order_key, "arg_* aggregators state")
    }
}

#[user_doc(
    doc_section(label = "Built-in Functions"),
    description = "Return a deterministic value by selecting the row with the smallest order key.",
    syntax_example = "any_value_det(value, order_key)",
    argument(name = "value", description = "Value to return."),
    argument(
        name = "order_key",
        description = "Ordering key used for deterministic selection."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct AnyValueDetUdaf {
    signature: Signature,
}

impl AnyValueDetUdaf {
    fn new() -> Self {
        Self {
            signature: arg_best_signature(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for AnyValueDetUdaf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        ANY_VALUE_DET_NAME
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn supports_null_handling_clause(&self) -> bool {
        true
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        arg_best_coerce_types(self.name(), arg_types)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(arg_best_state_fields(args, ANY_VALUE_DET_NAME))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArgBestAccumulator::new(ArgBestMode::Min)))
    }

    fn create_sliding_accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(AnyValueDetSlidingAccumulator::new()))
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8(None))
    }
}

/// Sliding accumulator for any_value_det that supports retract_batch.
///
/// `any_value_det` captures the row with the smallest order key (deterministic
/// first value). For bounded window frames, retraction is a semantic no-op
/// because the "first value" contract means the captured value does not change
/// when later rows leave the frame. The accumulator delegates to ArgBestAccumulator
/// for the forward path and reports retract support so the DataFusion window
/// executor can use the sliding code path.
#[derive(Debug)]
struct AnyValueDetSlidingAccumulator {
    inner: ArgBestAccumulator,
}

impl AnyValueDetSlidingAccumulator {
    fn new() -> Self {
        Self {
            inner: ArgBestAccumulator::new(ArgBestMode::Min),
        }
    }
}

impl Accumulator for AnyValueDetSlidingAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.inner.evaluate()
    }

    fn size(&self) -> usize {
        self.inner.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.inner.state()
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)
    }

    /// Retract for any_value_det is a no-op. The semantic contract is
    /// "first seen wins" -- the value with the smallest order key is
    /// captured and does not change when later rows exit the window frame.
    fn retract_batch(&mut self, _values: &[ArrayRef]) -> Result<()> {
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// Custom string_agg aggregate that supports retract_batch for bounded
/// window frames. Uses a Vec-based internal representation so individual
/// values can be removed during retraction.
#[user_doc(
    doc_section(label = "Built-in Functions"),
    description = "Concatenate string values with a separator.",
    syntax_example = "string_agg(value, separator)",
    argument(name = "value", description = "String value to aggregate."),
    argument(
        name = "separator",
        description = "Separator string placed between concatenated values."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct StringAggDetUdaf {
    signature: Signature,
}

impl StringAggDetUdaf {
    fn new() -> Self {
        let signature = Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::LargeUtf8, DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8View, DataType::Utf8]),
            ],
            Volatility::Immutable,
        );
        let signature = signature_with_names(signature, &["value", "separator"]);
        Self { signature }
    }
}

impl AggregateUDFImpl for StringAggDetUdaf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        STRING_AGG_NAME
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Field::new_list(
                format_state_name(args.name, "string_agg_values"),
                Field::new_list_field(DataType::Utf8, true),
                true,
            )
            .into(),
            Field::new(
                format_state_name(args.name, "string_agg_sep"),
                DataType::Utf8,
                true,
            )
            .into(),
        ])
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(StringAggDetAccumulator::default()))
    }

    fn create_sliding_accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(StringAggDetAccumulator::default()))
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8(None))
    }
}

/// Accumulator for string_agg that maintains a Vec of values for retract
/// support. On evaluate, joins them with the separator.
#[derive(Debug, Default)]
struct StringAggDetAccumulator {
    values: Vec<String>,
    separator: Option<String>,
}

impl Accumulator for StringAggDetAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() != 2 {
            return Err(DataFusionError::Plan(
                "string_agg expects value and separator arguments".into(),
            ));
        }
        let arr = string_array_any(&values[0], "string_agg expects string value")?;
        let sep_arr = string_array_any(&values[1], "string_agg expects string separator")?;

        // Capture separator from first non-null row if not yet set.
        if self.separator.is_none() {
            for i in 0..sep_arr.len() {
                if !sep_arr.is_null(i) {
                    self.separator = Some(sep_arr.value(i).to_string());
                    break;
                }
            }
        }

        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            self.values.push(arr.value(i).to_string());
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.values.is_empty() {
            return Ok(ScalarValue::Utf8(None));
        }
        let sep = self.separator.as_deref().unwrap_or(",");
        Ok(ScalarValue::Utf8(Some(self.values.join(sep))))
    }

    fn size(&self) -> usize {
        let values_size: usize = self.values.iter().map(|v| v.len()).sum();
        let sep_size = self.separator.as_ref().map_or(0, |s| s.len());
        size_of_val(self) + values_size + sep_size
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        // State: list of individual values + separator string.
        let mut builder = ListBuilder::new(StringBuilder::new());
        for v in &self.values {
            builder.values().append_value(v);
        }
        builder.append(true);
        let list_array = builder.finish();
        Ok(vec![
            ScalarValue::List(Arc::new(list_array)),
            ScalarValue::Utf8(self.separator.clone()),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.len() != 2 {
            return Err(DataFusionError::Plan(
                "string_agg expects list and separator state".into(),
            ));
        }
        let list_array = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("string_agg expects list state".into()))?;
        let sep_array =
            string_array_any(&states[1], "string_agg expects string separator state")?;

        // Capture separator from merge state if not yet set.
        if self.separator.is_none() {
            for i in 0..sep_array.len() {
                if !sep_array.is_null(i) {
                    self.separator = Some(sep_array.value(i).to_string());
                    break;
                }
            }
        }

        for row in 0..list_array.len() {
            if list_array.is_null(row) {
                continue;
            }
            let inner = list_array.value(row);
            let inner_strings =
                string_array_any(&inner, "string_agg expects string values in state")?;
            for i in 0..inner_strings.len() {
                if inner_strings.is_null(i) {
                    continue;
                }
                self.values.push(inner_strings.value(i).to_string());
            }
        }
        Ok(())
    }

    /// Retract values from the accumulator for bounded window frame retraction.
    ///
    /// Removes the first occurrence of each retracting value from the Vec.
    /// This preserves insertion order for the remaining values.
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = string_array_any(&values[0], "string_agg expects string value")?;
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }
            let val = arr.value(i);
            if let Some(pos) = self.values.iter().position(|v| v == val) {
                self.values.remove(pos);
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[user_doc(
    doc_section(label = "Built-in Functions"),
    description = "Return the value associated with the maximum order key.",
    syntax_example = "arg_max(value, order_key)",
    argument(name = "value", description = "Value to return."),
    argument(
        name = "order_key",
        description = "Ordering key used to select the maximum value."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArgMaxUdaf {
    signature: Signature,
}

impl ArgMaxUdaf {
    fn new() -> Self {
        Self {
            signature: arg_best_signature(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ArgMaxUdaf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        ARG_MAX_NAME
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn supports_null_handling_clause(&self) -> bool {
        true
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        arg_best_coerce_types(self.name(), arg_types)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(arg_best_state_fields(args, ARG_MAX_NAME))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArgBestAccumulator::new(ArgBestMode::Max)))
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8(None))
    }
}

#[user_doc(
    doc_section(label = "Built-in Functions"),
    description = "Return the value associated with the latest as-of order key.",
    syntax_example = "asof_select(value, order_key)",
    argument(name = "value", description = "Value to return."),
    argument(
        name = "order_key",
        description = "Ordering key used to select the latest value."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct AsofSelectUdaf {
    signature: Signature,
}

impl AsofSelectUdaf {
    fn new() -> Self {
        Self {
            signature: arg_best_signature(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for AsofSelectUdaf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        ASOF_SELECT_NAME
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn supports_null_handling_clause(&self) -> bool {
        true
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        arg_best_coerce_types(self.name(), arg_types)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(arg_best_state_fields(args, ASOF_SELECT_NAME))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArgBestAccumulator::new(ArgBestMode::Max)))
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8(None))
    }
}

#[user_doc(
    doc_section(label = "Built-in Functions"),
    description = "Return the value associated with the minimum order key.",
    syntax_example = "arg_min(value, order_key)",
    argument(name = "value", description = "Value to return."),
    argument(
        name = "order_key",
        description = "Ordering key used to select the minimum value."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct ArgMinUdaf {
    signature: Signature,
}

impl ArgMinUdaf {
    fn new() -> Self {
        Self {
            signature: arg_best_signature(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ArgMinUdaf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        ARG_MIN_NAME
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn supports_null_handling_clause(&self) -> bool {
        true
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        arg_best_coerce_types(self.name(), arg_types)
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(arg_best_state_fields(args, ARG_MIN_NAME))
    }

    fn accumulator(&self, _acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArgBestAccumulator::new(ArgBestMode::Min)))
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Utf8(None))
    }
}

#[derive(Debug, Default)]
struct DistinctStringSet {
    values: BTreeSet<String>,
    has_null: bool,
}

impl DistinctStringSet {
    fn update_from_array(
        &mut self,
        array: &ArrayRef,
        message: &str,
        track_null: bool,
    ) -> Result<()> {
        let values = string_array_any(array, message)?;
        for index in 0..values.len() {
            if values.is_null(index) {
                if track_null {
                    self.has_null = true;
                }
                continue;
            }
            self.values.insert(values.value(index).to_string());
        }
        Ok(())
    }

    fn merge_from_list_array(
        &mut self,
        array: &ListArray,
        message: &str,
        track_null: bool,
    ) -> Result<()> {
        for index in 0..array.len() {
            if array.is_null(index) {
                continue;
            }
            let values = array.value(index);
            self.update_from_array(&values, message, track_null)?;
        }
        Ok(())
    }

    fn build_list_array(&self, include_null: bool) -> ListArray {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for value in &self.values {
            builder.values().append_value(value);
        }
        if include_null && self.has_null {
            builder.values().append_null();
        }
        builder.append(true);
        builder.finish()
    }

    fn size(&self) -> usize {
        let values_len: usize = self.values.iter().map(|value| value.len()).sum();
        size_of_val(self) + values_len
    }
}

fn ensure_group_capacity(groups: &mut Vec<DistinctStringSet>, total_num_groups: usize) {
    if total_num_groups > groups.len() {
        groups.resize_with(total_num_groups, DistinctStringSet::default);
    }
}

fn update_groups_from_values(
    groups: &mut [DistinctStringSet],
    values: &StringArray,
    group_indices: &[usize],
    opt_filter: Option<&BooleanArray>,
    track_null: bool,
) -> Result<()> {
    for (row_index, group_index) in group_indices.iter().enumerate() {
        if let Some(filter) = opt_filter {
            if !filter.value(row_index) {
                continue;
            }
        }
        let group = groups
            .get_mut(*group_index)
            .ok_or_else(|| DataFusionError::Plan("group index out of range".into()))?;
        if values.is_null(row_index) {
            if track_null {
                group.has_null = true;
            }
            continue;
        }
        group.values.insert(values.value(row_index).to_string());
    }
    Ok(())
}

fn update_groups_from_list_array(
    groups: &mut [DistinctStringSet],
    list_array: &ListArray,
    group_indices: &[usize],
    opt_filter: Option<&BooleanArray>,
    message: &str,
    track_null: bool,
) -> Result<()> {
    for (row_index, group_index) in group_indices.iter().enumerate() {
        if let Some(filter) = opt_filter {
            if !filter.value(row_index) {
                continue;
            }
        }
        if list_array.is_null(row_index) {
            continue;
        }
        let values = list_array.value(row_index);
        let group = groups
            .get_mut(*group_index)
            .ok_or_else(|| DataFusionError::Plan("group index out of range".into()))?;
        group.update_from_array(&values, message, track_null)?;
    }
    Ok(())
}

fn build_group_list_array(groups: &[DistinctStringSet], include_null: bool) -> ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for group in groups {
        for value in &group.values {
            builder.values().append_value(value);
        }
        if include_null && group.has_null {
            builder.values().append_null();
        }
        builder.append(true);
    }
    builder.finish()
}

fn build_group_count_array(groups: &[DistinctStringSet], include_null: bool) -> Int64Array {
    let mut builder = Int64Builder::with_capacity(groups.len());
    for group in groups {
        let mut count = group.values.len() as i64;
        if include_null && group.has_null {
            count += 1;
        }
        builder.append_value(count);
    }
    builder.finish()
}

#[derive(Debug)]
struct ListUniqueAccumulator {
    state: DistinctStringSet,
    include_null: bool,
}

impl ListUniqueAccumulator {
    fn new(ignore_nulls: bool) -> Self {
        Self {
            state: DistinctStringSet::default(),
            include_null: !ignore_nulls,
        }
    }
}

impl Accumulator for ListUniqueAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [values] = values else {
            return Err(DataFusionError::Plan(
                "list_unique expects a single input value".into(),
            ));
        };
        self.state.update_from_array(
            values,
            "list_unique expects string input",
            self.include_null,
        )
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let list_array = self.state.build_list_array(self.include_null);
        Ok(ScalarValue::List(Arc::new(list_array)))
    }

    fn size(&self) -> usize {
        self.state.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let list_array = self.state.build_list_array(self.include_null);
        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [state] = states else {
            return Err(DataFusionError::Plan(
                "list_unique expects a single state value".into(),
            ));
        };
        let list_array = state
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("list_unique expects list state".into()))?;
        self.state.merge_from_list_array(
            list_array,
            "list_unique expects string state",
            self.include_null,
        )
    }
}

#[derive(Debug)]
struct ListUniqueGroupsAccumulator {
    groups: Vec<DistinctStringSet>,
    include_null: bool,
}

impl ListUniqueGroupsAccumulator {
    fn new(ignore_nulls: bool) -> Self {
        Self {
            groups: Vec::new(),
            include_null: !ignore_nulls,
        }
    }
}

impl GroupsAccumulator for ListUniqueGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let [values] = values else {
            return Err(DataFusionError::Plan(
                "list_unique expects a single input value".into(),
            ));
        };
        ensure_group_capacity(&mut self.groups, total_num_groups);
        let values = string_array_any(values, "list_unique expects string input")?;
        update_groups_from_values(
            &mut self.groups,
            &values,
            group_indices,
            opt_filter,
            self.include_null,
        )
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let total_groups = self.groups.len();
        let (emit_count, retain) = match emit_to {
            EmitTo::All => (total_groups, false),
            EmitTo::First(count) => (count.min(total_groups), true),
        };
        let array = build_group_list_array(&self.groups[..emit_count], self.include_null);
        if retain {
            self.groups.drain(0..emit_count);
        } else {
            self.groups.clear();
        }
        Ok(Arc::new(array))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let array = self.evaluate(emit_to)?;
        Ok(vec![array])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let [values] = values else {
            return Err(DataFusionError::Plan(
                "list_unique expects a single state value".into(),
            ));
        };
        let list_array = values
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("list_unique expects list state".into()))?;
        ensure_group_capacity(&mut self.groups, total_num_groups);
        update_groups_from_list_array(
            &mut self.groups,
            list_array,
            group_indices,
            opt_filter,
            "list_unique expects string state",
            self.include_null,
        )
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let [values] = values else {
            return Err(DataFusionError::Plan(
                "list_unique expects a single input value".into(),
            ));
        };
        let values = string_array_any(values, "list_unique expects string input")?;
        let state = list_state_from_values(&values, opt_filter, self.include_null);
        Ok(vec![Arc::new(state)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.groups.iter().map(DistinctStringSet::size).sum()
    }
}

#[derive(Debug)]
struct CountDistinctAccumulator {
    state: DistinctStringSet,
    include_null: bool,
}

impl CountDistinctAccumulator {
    fn new(ignore_nulls: bool) -> Self {
        Self {
            state: DistinctStringSet::default(),
            include_null: !ignore_nulls,
        }
    }
}

impl Accumulator for CountDistinctAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [values] = values else {
            return Err(DataFusionError::Plan(
                "count_distinct_agg expects a single input value".into(),
            ));
        };
        self.state.update_from_array(
            values,
            "count_distinct_agg expects string input",
            self.include_null,
        )
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut count = self.state.values.len() as i64;
        if self.include_null && self.state.has_null {
            count += 1;
        }
        Ok(ScalarValue::Int64(Some(count)))
    }

    fn size(&self) -> usize {
        self.state.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let list_array = self.state.build_list_array(self.include_null);
        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [state] = states else {
            return Err(DataFusionError::Plan(
                "count_distinct_agg expects a single state value".into(),
            ));
        };
        let list_array = state
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("count_distinct_agg expects list state".into()))?;
        self.state.merge_from_list_array(
            list_array,
            "count_distinct_agg expects string state",
            self.include_null,
        )
    }
}

#[derive(Debug)]
struct CountDistinctGroupsAccumulator {
    groups: Vec<DistinctStringSet>,
    include_null: bool,
}

impl CountDistinctGroupsAccumulator {
    fn new(ignore_nulls: bool) -> Self {
        Self {
            groups: Vec::new(),
            include_null: !ignore_nulls,
        }
    }
}

impl GroupsAccumulator for CountDistinctGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let [values] = values else {
            return Err(DataFusionError::Plan(
                "count_distinct_agg expects a single input value".into(),
            ));
        };
        ensure_group_capacity(&mut self.groups, total_num_groups);
        let values = string_array_any(values, "count_distinct_agg expects string input")?;
        update_groups_from_values(
            &mut self.groups,
            &values,
            group_indices,
            opt_filter,
            self.include_null,
        )
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let total_groups = self.groups.len();
        let (emit_count, retain) = match emit_to {
            EmitTo::All => (total_groups, false),
            EmitTo::First(count) => (count.min(total_groups), true),
        };
        let array = build_group_count_array(&self.groups[..emit_count], self.include_null);
        if retain {
            self.groups.drain(0..emit_count);
        } else {
            self.groups.clear();
        }
        Ok(Arc::new(array))
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let total_groups = self.groups.len();
        let (emit_count, retain) = match emit_to {
            EmitTo::All => (total_groups, false),
            EmitTo::First(count) => (count.min(total_groups), true),
        };
        let array = build_group_list_array(&self.groups[..emit_count], self.include_null);
        if retain {
            self.groups.drain(0..emit_count);
        } else {
            self.groups.clear();
        }
        Ok(vec![Arc::new(array)])
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        let [values] = values else {
            return Err(DataFusionError::Plan(
                "count_distinct_agg expects a single state value".into(),
            ));
        };
        let list_array = values
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("count_distinct_agg expects list state".into()))?;
        ensure_group_capacity(&mut self.groups, total_num_groups);
        update_groups_from_list_array(
            &mut self.groups,
            list_array,
            group_indices,
            opt_filter,
            "count_distinct_agg expects string state",
            self.include_null,
        )
    }

    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let [values] = values else {
            return Err(DataFusionError::Plan(
                "count_distinct_agg expects a single input value".into(),
            ));
        };
        let values = string_array_any(values, "count_distinct_agg expects string input")?;
        let state = list_state_from_values(&values, opt_filter, self.include_null);
        Ok(vec![Arc::new(state)])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.groups.iter().map(DistinctStringSet::size).sum()
    }
}

#[derive(Debug, Default)]
struct DistinctStringCounts {
    values: BTreeMap<String, usize>,
    null_count: usize,
}

impl DistinctStringCounts {
    fn update_from_array(
        &mut self,
        array: &ArrayRef,
        message: &str,
        track_null: bool,
    ) -> Result<()> {
        let values = string_array_any(array, message)?;
        for index in 0..values.len() {
            if values.is_null(index) {
                if track_null {
                    self.null_count = self.null_count.saturating_add(1);
                }
                continue;
            }
            let entry = self
                .values
                .entry(values.value(index).to_string())
                .or_insert(0);
            *entry += 1;
        }
        Ok(())
    }

    fn retract_from_array(
        &mut self,
        array: &ArrayRef,
        message: &str,
        track_null: bool,
    ) -> Result<()> {
        let values = string_array_any(array, message)?;
        for index in 0..values.len() {
            if values.is_null(index) {
                if track_null {
                    if self.null_count == 0 {
                        return Err(DataFusionError::Plan(
                            "distinct string null count underflow".into(),
                        ));
                    }
                    self.null_count -= 1;
                }
                continue;
            }
            let key = values.value(index);
            let Some(entry) = self.values.get_mut(key) else {
                return Err(DataFusionError::Plan(
                    "distinct string count underflow".into(),
                ));
            };
            if *entry == 0 {
                return Err(DataFusionError::Plan(
                    "distinct string count underflow".into(),
                ));
            }
            *entry -= 1;
            if *entry == 0 {
                self.values.remove(key);
            }
        }
        Ok(())
    }

    fn merge_from_list_array(
        &mut self,
        array: &ListArray,
        message: &str,
        track_null: bool,
    ) -> Result<()> {
        for index in 0..array.len() {
            if array.is_null(index) {
                continue;
            }
            let values = array.value(index);
            self.update_from_array(&values, message, track_null)?;
        }
        Ok(())
    }

    fn build_list_array(&self, include_null: bool, include_duplicates: bool) -> ListArray {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for (value, count) in &self.values {
            let repeat = if include_duplicates { *count } else { 1 };
            for _ in 0..repeat {
                builder.values().append_value(value);
            }
        }
        if include_null && self.null_count > 0 {
            for _ in 0..self.null_count {
                if include_duplicates {
                    builder.values().append_null();
                }
            }
            if !include_duplicates {
                builder.values().append_null();
            }
        }
        builder.append(true);
        builder.finish()
    }

    fn size(&self) -> usize {
        let values_len: usize = self.values.keys().map(|value| value.len()).sum();
        size_of_val(self) + values_len
    }
}

#[derive(Debug)]
struct ListUniqueSlidingAccumulator {
    state: DistinctStringCounts,
    include_null: bool,
}

impl ListUniqueSlidingAccumulator {
    fn new(ignore_nulls: bool) -> Self {
        Self {
            state: DistinctStringCounts::default(),
            include_null: !ignore_nulls,
        }
    }
}

impl Accumulator for ListUniqueSlidingAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [values] = values else {
            return Err(DataFusionError::Plan(
                "list_unique expects a single input value".into(),
            ));
        };
        self.state.update_from_array(
            values,
            "list_unique expects string input",
            self.include_null,
        )
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let list_array = self.state.build_list_array(self.include_null, false);
        Ok(ScalarValue::List(Arc::new(list_array)))
    }

    fn size(&self) -> usize {
        self.state.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let list_array = self.state.build_list_array(self.include_null, true);
        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [state] = states else {
            return Err(DataFusionError::Plan(
                "list_unique expects a single state value".into(),
            ));
        };
        let list_array = state
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("list_unique expects list state".into()))?;
        self.state.merge_from_list_array(
            list_array,
            "list_unique expects string state",
            self.include_null,
        )
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [values] = values else {
            return Err(DataFusionError::Plan(
                "list_unique expects a single input value".into(),
            ));
        };
        self.state.retract_from_array(
            values,
            "list_unique expects string input",
            self.include_null,
        )
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct CountDistinctSlidingAccumulator {
    state: DistinctStringCounts,
    include_null: bool,
}

impl CountDistinctSlidingAccumulator {
    fn new(ignore_nulls: bool) -> Self {
        Self {
            state: DistinctStringCounts::default(),
            include_null: !ignore_nulls,
        }
    }
}

impl Accumulator for CountDistinctSlidingAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [values] = values else {
            return Err(DataFusionError::Plan(
                "count_distinct_agg expects a single input value".into(),
            ));
        };
        self.state.update_from_array(
            values,
            "count_distinct_agg expects string input",
            self.include_null,
        )
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut count = self.state.values.len() as i64;
        if self.include_null && self.state.null_count > 0 {
            count += 1;
        }
        Ok(ScalarValue::Int64(Some(count)))
    }

    fn size(&self) -> usize {
        self.state.size()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let list_array = self.state.build_list_array(self.include_null, true);
        Ok(vec![ScalarValue::List(Arc::new(list_array))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [state] = states else {
            return Err(DataFusionError::Plan(
                "count_distinct_agg expects a single state value".into(),
            ));
        };
        let list_array = state
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Plan("count_distinct_agg expects list state".into()))?;
        self.state.merge_from_list_array(
            list_array,
            "count_distinct_agg expects string state",
            self.include_null,
        )
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [values] = values else {
            return Err(DataFusionError::Plan(
                "count_distinct_agg expects a single input value".into(),
            ));
        };
        self.state.retract_from_array(
            values,
            "count_distinct_agg expects string input",
            self.include_null,
        )
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

fn list_state_from_values(
    values: &StringArray,
    opt_filter: Option<&BooleanArray>,
    include_null: bool,
) -> ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for index in 0..values.len() {
        let mut include = true;
        if let Some(filter) = opt_filter {
            if filter.is_null(index) || !filter.value(index) {
                include = false;
            }
        }
        if !include {
            builder.append(true);
            continue;
        }
        if values.is_null(index) {
            if include_null {
                builder.values().append_null();
            }
            builder.append(true);
            continue;
        }
        builder.values().append_value(values.value(index));
        builder.append(true);
    }
    builder.finish()
}

fn string_array_any<'a>(array: &'a ArrayRef, message: &str) -> Result<Cow<'a, StringArray>> {
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
