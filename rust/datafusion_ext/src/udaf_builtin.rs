use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Int64Array, Int64Builder, ListArray, ListBuilder, StringArray,
    StringBuilder,
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

use crate::function_types::FunctionKind;
use crate::registry::metadata::FunctionMetadata;
use crate::udf::common::{signature_with_names, string_array_any};
use crate::{aggregate_udfs, macros::AggregateUdfSpec};

const LIST_UNIQUE_NAME: &str = "list_unique";
const COUNT_DISTINCT_NAME: &str = "count_distinct_agg";
const COLLECT_SET_NAME: &str = "collect_set";
const COUNT_IF_NAME: &str = "count_if";
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
    crate::udaf_arg_best::any_value_det_udaf()
}

fn arg_max_udaf() -> AggregateUDF {
    crate::udaf_arg_best::arg_max_udaf()
}

fn asof_select_udaf() -> AggregateUDF {
    crate::udaf_arg_best::asof_select_udaf()
}

fn arg_min_udaf() -> AggregateUDF {
    crate::udaf_arg_best::arg_min_udaf()
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
        let sep_array = string_array_any(&states[1], "string_agg expects string separator state")?;

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

pub(crate) fn function_metadata(name: &str) -> Option<FunctionMetadata> {
    let (rewrite_tags, has_groups_accumulator, has_retract_batch): (
        &'static [&'static str],
        bool,
        bool,
    ) = match name {
        "list_unique" => (&["list"], true, true),
        "collect_set" => (&["aggregate", "deterministic", "list"], true, true),
        "count_if" => (&["aggregate", "deterministic"], true, true),
        "any_value_det" => (&["aggregate", "deterministic"], false, true),
        "arg_max" => (&["aggregate", "deterministic"], false, false),
        "asof_select" => (&["aggregate", "asof"], false, false),
        "arg_min" => (&["aggregate", "deterministic"], false, false),
        "first_value_agg" => (&["aggregate"], false, false),
        "last_value_agg" => (&["aggregate"], false, false),
        "count_distinct_agg" => (&["aggregate"], true, true),
        "string_agg" => (&["aggregate", "string"], false, true),
        _ => return None,
    };

    Some(FunctionMetadata {
        name: None,
        kind: FunctionKind::Aggregate,
        rewrite_tags,
        has_simplify: false,
        has_coerce_types: false,
        has_short_circuits: false,
        has_groups_accumulator,
        has_retract_batch,
        has_reverse_expr: false,
        has_sort_options: false,
    })
}
