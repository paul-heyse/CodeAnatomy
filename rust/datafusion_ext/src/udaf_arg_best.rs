use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, Signature, TypeSignature,
    Volatility,
};
use datafusion_macros::user_doc;

use crate::udf::common::{scalar_to_i64, scalar_to_string, signature_with_names};

const ANY_VALUE_DET_NAME: &str = "any_value_det";
const ARG_MAX_NAME: &str = "arg_max";
const ARG_MIN_NAME: &str = "arg_min";
const ASOF_SELECT_NAME: &str = "asof_select";

pub(crate) fn any_value_det_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(AnyValueDetUdaf::new()))
}

pub(crate) fn arg_max_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(ArgMaxUdaf::new()))
}

pub(crate) fn asof_select_udaf() -> AggregateUDF {
    AggregateUDF::new_from_shared_impl(Arc::new(AsofSelectUdaf::new()))
}

pub(crate) fn arg_min_udaf() -> AggregateUDF {
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
            let value_text = scalar_to_string(&value)?;
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
