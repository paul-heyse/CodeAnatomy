use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{
    Array,
    ArrayRef,
    BooleanArray,
    Int64Array,
    Int64Builder,
    LargeStringArray,
    ListArray,
    ListBuilder,
    StringArray,
    StringBuilder,
    StringViewArray,
};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator,
    AggregateUDF,
    AggregateUDFImpl,
    Documentation,
    EmitTo,
    GroupsAccumulator,
    Signature,
    TypeSignature,
    Volatility,
};
use datafusion_functions_aggregate::first_last::{first_value_udaf, last_value_udaf};
use datafusion_functions_aggregate::string_agg::string_agg_udaf;

use crate::udf_docs;

const LIST_UNIQUE_NAME: &str = "list_unique";
const COUNT_DISTINCT_NAME: &str = "count_distinct_agg";

pub fn builtin_udafs() -> Vec<AggregateUDF> {
    vec![
        list_unique_udaf(),
        count_distinct_udaf(),
        first_value_udaf()
            .as_ref()
            .clone()
            .with_aliases(["first_value_agg"]),
        last_value_udaf()
            .as_ref()
            .clone()
            .with_aliases(["last_value_agg"]),
        string_agg_udaf().as_ref().clone(),
    ]
}

fn list_unique_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(ListUniqueUdaf::new()))
}

fn count_distinct_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(CountDistinctUdaf::new()))
}

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
        Some(udf_docs::list_unique_doc())
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
        Ok(Box::new(ListUniqueAccumulator::new(
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
        Ok(Box::new(ListUniqueGroupsAccumulator::new(
            args.ignore_nulls,
        )))
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ListUniqueSlidingAccumulator::new(
            args.ignore_nulls,
        )))
    }
}

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
        Some(udf_docs::count_distinct_agg_doc())
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

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(CountDistinctSlidingAccumulator::new(
            args.ignore_nulls,
        )))
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(0)))
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
    groups: &mut Vec<DistinctStringSet>,
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
    groups: &mut Vec<DistinctStringSet>,
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
        self.state
            .update_from_array(values, "list_unique expects string input", self.include_null)
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
            .ok_or_else(|| {
                DataFusionError::Plan("list_unique expects list state".into())
            })?;
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
            .ok_or_else(|| {
                DataFusionError::Plan("list_unique expects list state".into())
            })?;
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
            .ok_or_else(|| {
                DataFusionError::Plan("count_distinct_agg expects list state".into())
            })?;
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
            .ok_or_else(|| {
                DataFusionError::Plan("count_distinct_agg expects list state".into())
            })?;
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
            let entry = self.values.entry(values.value(index).to_string()).or_insert(0);
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
            .ok_or_else(|| {
                DataFusionError::Plan("list_unique expects list state".into())
            })?;
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
            .ok_or_else(|| {
                DataFusionError::Plan("count_distinct_agg expects list state".into())
            })?;
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
