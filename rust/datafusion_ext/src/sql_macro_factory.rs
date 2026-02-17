use std::any::Any;
use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, FieldRef};
use async_trait::async_trait;
use datafusion::catalog::TableFunctionImpl;
use datafusion::execution::context::{FunctionFactory, RegisterFunction};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_doc::DocSection;
use datafusion_expr::expr::{AggregateFunctionParams, Cast, WindowFunctionParams};
use datafusion_expr::function::{
    AccumulatorArgs, AggregateFunctionSimplification, StateFieldsArgs, WindowFunctionSimplification,
};
use datafusion_expr::logical_plan::OperateFunctionArg;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::udf_eq::UdfEq;
use datafusion_expr::utils::AggregateOrderSensitivity;
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, Expr, GroupsAccumulator,
    PartitionEvaluator, ReturnFieldArgs, ReversedUDAF, ScalarUDF, ScalarUDFImpl, SetMonotonicity,
    Signature, StatisticsArgs, Volatility, WindowFrame, WindowFunctionDefinition, WindowUDF,
    WindowUDFImpl,
};
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::sort_properties::ExprProperties;
use datafusion_functions_window_common::expr::ExpressionArgs;
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

use crate::function_types::FunctionKind;

const DOC_SECTION_SQL_MACRO: DocSection = DocSection {
    include: true,
    label: "SQL Macros",
    description: Some("Functions defined via CREATE FUNCTION."),
};

fn kind_from_language(language: Option<&str>) -> Result<Option<FunctionKind>> {
    let Some(language) = language else {
        return Ok(None);
    };
    let value = language.to_ascii_lowercase();
    let kind = match value.as_str() {
        "sql" | "datafusion" => return Ok(None),
        "scalar" | "udf" => FunctionKind::Scalar,
        "aggregate" | "udaf" | "agg" => FunctionKind::Aggregate,
        "window" | "udwf" => FunctionKind::Window,
        "table" | "udtf" => FunctionKind::Table,
        _ => {
            return Err(DataFusionError::Plan(format!(
                "Unsupported CREATE FUNCTION language: {value}"
            )));
        }
    };
    Ok(Some(kind))
}

fn infer_kind(body: &Expr) -> FunctionKind {
    match body {
        Expr::AggregateFunction(_) => FunctionKind::Aggregate,
        Expr::WindowFunction(window) => match window.fun {
            WindowFunctionDefinition::AggregateUDF(_) => FunctionKind::Aggregate,
            WindowFunctionDefinition::WindowUDF(_) => FunctionKind::Window,
        },
        _ => FunctionKind::Scalar,
    }
}

fn ensure_kind(body: &Expr, kind: FunctionKind, name: &str) -> Result<()> {
    match kind {
        FunctionKind::Scalar => Ok(()),
        FunctionKind::Aggregate => match body {
            Expr::AggregateFunction(_) => Ok(()),
            Expr::WindowFunction(window)
                if matches!(window.fun, WindowFunctionDefinition::AggregateUDF(_)) =>
            {
                Ok(())
            }
            _ => Err(DataFusionError::Plan(format!(
                "CREATE FUNCTION {name} expected aggregate body"
            ))),
        },
        FunctionKind::Window => match body {
            Expr::WindowFunction(window)
                if matches!(window.fun, WindowFunctionDefinition::WindowUDF(_)) =>
            {
                Ok(())
            }
            Expr::Literal(..) => Ok(()),
            _ => Err(DataFusionError::Plan(format!(
                "CREATE FUNCTION {name} expected window function body"
            ))),
        },
        FunctionKind::Table => Ok(()),
    }
}

#[derive(Debug, Default)]
pub struct SqlMacroFunctionFactory;

#[async_trait]
impl FunctionFactory for SqlMacroFunctionFactory {
    async fn create(
        &self,
        state: &SessionState,
        statement: datafusion_expr::logical_plan::CreateFunction,
    ) -> Result<RegisterFunction> {
        build_register_function(state, statement)
    }
}

pub fn with_sql_macro_factory(state: &SessionState) -> SessionState {
    SessionStateBuilder::new_from_existing(state.clone())
        .with_function_factory(Some(Arc::new(SqlMacroFunctionFactory)))
        .build()
}

fn build_register_function(
    state: &SessionState,
    statement: datafusion_expr::logical_plan::CreateFunction,
) -> Result<RegisterFunction> {
    let datafusion_expr::logical_plan::CreateFunction {
        name,
        args,
        return_type,
        params,
        ..
    } = statement;
    let body = params
        .function_body
        .ok_or_else(|| DataFusionError::Plan("CREATE FUNCTION missing body".into()))?;
    let arg_list = args.unwrap_or_default();
    let arg_types = arg_list
        .iter()
        .map(|arg| arg.data_type.clone())
        .collect::<Vec<_>>();
    let parameter_names = extract_parameter_names(&arg_list).unwrap_or_default();
    let kind_override =
        kind_from_language(params.language.as_ref().map(|ident| ident.value.as_str()))?;
    let kind = kind_override.unwrap_or_else(|| infer_kind(&body));
    ensure_kind(&body, kind, &name)?;

    match kind {
        FunctionKind::Scalar => build_scalar_macro(
            name,
            body,
            &arg_list,
            arg_types,
            parameter_names,
            params.behavior,
            return_type,
        ),
        FunctionKind::Aggregate => build_aggregate_alias(
            name,
            body,
            &arg_types,
            &parameter_names,
            params.behavior,
            return_type,
        ),
        FunctionKind::Window => build_window_alias(
            state,
            name,
            body,
            &arg_types,
            &parameter_names,
            params.behavior,
            return_type,
        ),
        FunctionKind::Table => build_table_alias(state, name, body),
    }
}

fn build_scalar_macro(
    name: String,
    body: Expr,
    arg_list: &[OperateFunctionArg],
    arg_types: Vec<DataType>,
    parameter_names: Vec<String>,
    requested_volatility: Option<Volatility>,
    return_type: Option<DataType>,
) -> Result<RegisterFunction> {
    let return_type = return_type
        .ok_or_else(|| DataFusionError::Plan("CREATE FUNCTION missing return type".into()))?;
    let volatility = requested_volatility.unwrap_or(Volatility::Volatile);
    let mut signature = Signature::exact(arg_types, volatility);
    if !parameter_names.is_empty() {
        signature = signature
            .with_parameter_names(parameter_names.clone())
            .map_err(|err| {
                DataFusionError::Plan(format!("CREATE FUNCTION invalid parameter names: {err}"))
            })?;
    }
    let documentation = sql_macro_documentation(&name, arg_list, &return_type);
    let udf_impl = SqlMacroUdf {
        name,
        signature,
        return_type,
        template: body,
        parameter_names,
        documentation,
    };
    Ok(RegisterFunction::Scalar(Arc::new(ScalarUDF::from(
        udf_impl,
    ))))
}

fn build_aggregate_alias(
    name: String,
    body: Expr,
    arg_types: &[DataType],
    parameter_names: &[String],
    requested_volatility: Option<Volatility>,
    return_type: Option<DataType>,
) -> Result<RegisterFunction> {
    match body {
        Expr::AggregateFunction(aggregate) => {
            validate_aggregate_params(&aggregate.params, &name)?;
            register_aggregate_alias(
                name,
                Arc::clone(&aggregate.func),
                &aggregate.params.args,
                arg_types,
                parameter_names,
                requested_volatility,
                return_type,
            )
        }
        Expr::WindowFunction(window)
            if matches!(window.fun, WindowFunctionDefinition::AggregateUDF(_)) =>
        {
            let WindowFunctionDefinition::AggregateUDF(udaf) = window.fun else {
                unreachable!("guarded by matches!");
            };
            validate_window_params(&window.params, &name)?;
            register_aggregate_alias(
                name,
                udaf,
                &window.params.args,
                arg_types,
                parameter_names,
                requested_volatility,
                return_type,
            )
        }
        _ => Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} expected aggregate body"
        ))),
    }
}

fn register_aggregate_alias(
    name: String,
    udaf: Arc<AggregateUDF>,
    args: &[Expr],
    arg_types: &[DataType],
    parameter_names: &[String],
    requested_volatility: Option<Volatility>,
    return_type: Option<DataType>,
) -> Result<RegisterFunction> {
    validate_placeholder_args(args, parameter_names, arg_types.len(), &name)?;
    validate_volatility(&name, requested_volatility, udaf.signature())?;
    let signature = apply_parameter_names(&name, udaf.signature(), parameter_names)?;
    let computed = udaf.return_type(arg_types)?;
    validate_return_type(&name, return_type, computed)?;
    let renamed = RenamedAggregateUdf::new(name, signature, Arc::clone(udaf.inner()));
    Ok(RegisterFunction::Aggregate(Arc::new(
        AggregateUDF::new_from_shared_impl(Arc::new(renamed)),
    )))
}

fn build_window_alias(
    state: &SessionState,
    name: String,
    body: Expr,
    arg_types: &[DataType],
    parameter_names: &[String],
    requested_volatility: Option<Volatility>,
    return_type: Option<DataType>,
) -> Result<RegisterFunction> {
    match body {
        Expr::WindowFunction(window) => {
            let WindowFunctionDefinition::WindowUDF(udwf) = window.fun else {
                return Err(DataFusionError::Plan(format!(
                    "CREATE FUNCTION {name} requires a window UDF body"
                )));
            };
            validate_window_params(&window.params, &name)?;
            validate_placeholder_args(
                &window.params.args,
                parameter_names,
                arg_types.len(),
                &name,
            )?;
            register_window_alias(
                name,
                udwf,
                arg_types,
                parameter_names,
                requested_volatility,
                return_type,
            )
        }
        Expr::Literal(..) => {
            let target = extract_window_target(&body, &name)?;
            let udwf = state.window_functions().get(&target).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "CREATE FUNCTION {name} references unknown window function {target}"
                ))
            })?;
            register_window_alias(
                name,
                Arc::clone(udwf),
                arg_types,
                parameter_names,
                requested_volatility,
                return_type,
            )
        }
        _ => Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} expected window function body"
        ))),
    }
}

fn register_window_alias(
    name: String,
    udwf: Arc<WindowUDF>,
    arg_types: &[DataType],
    parameter_names: &[String],
    requested_volatility: Option<Volatility>,
    return_type: Option<DataType>,
) -> Result<RegisterFunction> {
    validate_volatility(&name, requested_volatility, udwf.signature())?;
    let signature = apply_parameter_names(&name, udwf.signature(), parameter_names)?;
    validate_window_return_type(&name, return_type, &udwf, arg_types, parameter_names)?;
    let renamed = RenamedWindowUdf::new(name, signature, Arc::clone(udwf.inner()));
    Ok(RegisterFunction::Window(Arc::new(
        WindowUDF::new_from_shared_impl(Arc::new(renamed)),
    )))
}

fn build_table_alias(state: &SessionState, name: String, body: Expr) -> Result<RegisterFunction> {
    let target = extract_table_target(&body, &name)?;
    let table_function = state.table_functions().get(&target).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} references unknown table function {target}"
        ))
    })?;
    Ok(RegisterFunction::Table(
        name,
        Arc::clone(table_function.function()) as Arc<dyn TableFunctionImpl>,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SqlMacroUdf {
    name: String,
    signature: Signature,
    return_type: DataType,
    template: Expr,
    parameter_names: Vec<String>,
    documentation: Documentation,
}

impl ScalarUDFImpl for SqlMacroUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&self.documentation)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type.clone(),
            nullable,
        )))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(
        &self,
        _args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<datafusion_expr::ColumnarValue> {
        Err(DataFusionError::Internal(
            "SQL macro UDF should be simplified before execution".into(),
        ))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        let transformed = self.template.clone().transform_up(|expr| {
            if let Expr::Placeholder(placeholder) = &expr {
                let index = resolve_placeholder_index(&placeholder.id, &self.parameter_names)?;
                let replacement = args.get(index).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "CREATE FUNCTION placeholder {} is out of range",
                        placeholder.id
                    ))
                })?;
                return Ok(Transformed::yes(replacement.clone()));
            }
            Ok(Transformed::no(expr))
        })?;
        let casted = Expr::Cast(Cast::new(
            Box::new(transformed.data),
            self.return_type.clone(),
        ));
        Ok(ExprSimplifyResult::Simplified(casted))
    }

    fn short_circuits(&self) -> bool {
        self.template.short_circuits()
    }

    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        if matches!(self.template, Expr::Placeholder(_)) {
            if let Some(interval) = inputs.first().copied() {
                return Ok(interval.clone());
            }
        }
        Interval::make_unbounded(&self.return_type)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        if matches!(self.template, Expr::Placeholder(_)) && inputs.len() == 1 {
            return Ok(Some(vec![interval.clone()]));
        }
        Ok(Some(Vec::new()))
    }

    fn preserves_lex_ordering(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        Ok(matches!(self.template, Expr::Placeholder(_)))
    }
}

fn sql_macro_documentation(
    name: &str,
    arg_list: &[OperateFunctionArg],
    return_type: &DataType,
) -> Documentation {
    let args_display = arg_list
        .iter()
        .enumerate()
        .map(|(idx, arg)| {
            arg.name
                .as_ref()
                .map(|ident| ident.value.clone())
                .unwrap_or_else(|| format!("arg{}", idx + 1))
        })
        .collect::<Vec<_>>()
        .join(", ");
    let syntax = if args_display.is_empty() {
        format!("{name}()")
    } else {
        format!("{name}({args_display})")
    };
    let description = format!("SQL macro defined via CREATE FUNCTION returning {return_type}.");
    let mut builder = Documentation::builder(DOC_SECTION_SQL_MACRO, description, syntax);
    for (idx, arg) in arg_list.iter().enumerate() {
        let arg_name = arg
            .name
            .as_ref()
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| format!("arg{}", idx + 1));
        let mut arg_desc = format!("Type: {}", arg.data_type);
        if let Some(default_expr) = &arg.default_expr {
            arg_desc.push_str(&format!(". Default: {default_expr}"));
        }
        builder = builder.with_argument(arg_name, arg_desc);
    }
    builder.build()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RenamedAggregateUdf {
    name: String,
    signature: Signature,
    inner: UdfEq<Arc<dyn AggregateUDFImpl>>,
}

impl RenamedAggregateUdf {
    fn new(name: String, signature: Signature, inner: Arc<dyn AggregateUDFImpl>) -> Self {
        Self {
            name,
            signature,
            inner: UdfEq::from(inner),
        }
    }
}

impl AggregateUDFImpl for RenamedAggregateUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        let field = self.inner.return_field(arg_fields)?;
        if field.name() == self.name.as_str() {
            return Ok(field);
        }
        Ok(Arc::new(
            field.as_ref().clone().with_name(self.name.clone()),
        ))
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(acc_args)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.inner.state_fields(args)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        self.inner.groups_accumulator_supported(args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        self.inner.create_groups_accumulator(args)
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.create_sliding_accumulator(args)
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        let inner = Arc::clone(&*self.inner);
        let updated = inner.with_beneficial_ordering(beneficial_ordering)?;
        Ok(updated.map(|inner| {
            Arc::new(RenamedAggregateUdf::new(
                self.name.clone(),
                self.signature.clone(),
                inner,
            )) as Arc<dyn AggregateUDFImpl>
        }))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn simplify(&self) -> Option<AggregateFunctionSimplification> {
        self.inner.simplify()
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_expr()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn is_descending(&self) -> Option<bool> {
        self.inner.is_descending()
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.inner.value_from_stats(statistics_args)
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.inner.default_value(data_type)
    }

    fn supports_null_handling_clause(&self) -> bool {
        self.inner.supports_null_handling_clause()
    }

    fn supports_within_group_clause(&self) -> bool {
        self.inner.supports_within_group_clause()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.set_monotonicity(data_type)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RenamedWindowUdf {
    name: String,
    signature: Signature,
    inner: UdfEq<Arc<dyn WindowUDFImpl>>,
}

impl RenamedWindowUdf {
    fn new(name: String, signature: Signature, inner: Arc<dyn WindowUDFImpl>) -> Self {
        Self {
            name,
            signature,
            inner: UdfEq::from(inner),
        }
    }
}

impl WindowUDFImpl for RenamedWindowUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn expressions(&self, expr_args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>> {
        self.inner.expressions(expr_args)
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        self.inner.partition_evaluator(partition_evaluator_args)
    }

    fn simplify(&self) -> Option<WindowFunctionSimplification> {
        self.inner.simplify()
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        self.inner.field(field_args)
    }

    fn sort_options(&self) -> Option<SortOptions> {
        self.inner.sort_options()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDWF {
        self.inner.reverse_expr()
    }
}

fn apply_parameter_names(
    name: &str,
    signature: &Signature,
    parameter_names: &[String],
) -> Result<Signature> {
    if parameter_names.is_empty() {
        return Ok(signature.clone());
    }
    signature
        .clone()
        .with_parameter_names(parameter_names.to_vec())
        .map_err(|err| {
            DataFusionError::Plan(format!(
                "CREATE FUNCTION {name} invalid parameter names: {err}"
            ))
        })
}

fn validate_placeholder_args(
    args: &[Expr],
    parameter_names: &[String],
    expected_len: usize,
    name: &str,
) -> Result<()> {
    if args.len() != expected_len {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} expected {expected_len} arguments, got {}",
            args.len()
        )));
    }
    for (index, expr) in args.iter().enumerate() {
        let Expr::Placeholder(placeholder) = expr else {
            return Err(DataFusionError::Plan(format!(
                "CREATE FUNCTION {name} arguments must be placeholders"
            )));
        };
        let resolved = resolve_placeholder_index(&placeholder.id, parameter_names)?;
        if resolved != index {
            return Err(DataFusionError::Plan(format!(
                "CREATE FUNCTION {name} arguments must be ordered placeholders"
            )));
        }
    }
    Ok(())
}

fn validate_aggregate_params(params: &AggregateFunctionParams, name: &str) -> Result<()> {
    if params.distinct {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} cannot include DISTINCT in aggregate body"
        )));
    }
    if params.filter.is_some() {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} cannot include FILTER in aggregate body"
        )));
    }
    if !params.order_by.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} cannot include ORDER BY in aggregate body"
        )));
    }
    if params.null_treatment.is_some() {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} cannot include NULL TREATMENT in aggregate body"
        )));
    }
    Ok(())
}

fn validate_window_params(params: &WindowFunctionParams, name: &str) -> Result<()> {
    if !params.partition_by.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} cannot include PARTITION BY in window body"
        )));
    }
    if !params.order_by.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} cannot include ORDER BY in window body"
        )));
    }
    if params.filter.is_some() {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} cannot include FILTER in window body"
        )));
    }
    if params.null_treatment.is_some() {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} cannot include NULL TREATMENT in window body"
        )));
    }
    if params.distinct {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} cannot include DISTINCT in window body"
        )));
    }
    if params.window_frame != WindowFrame::new(None) {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} cannot include window frames in body"
        )));
    }
    Ok(())
}

fn validate_volatility(
    name: &str,
    requested: Option<Volatility>,
    signature: &Signature,
) -> Result<()> {
    if let Some(requested) = requested {
        if requested != signature.volatility {
            return Err(DataFusionError::Plan(format!(
                "CREATE FUNCTION {name} volatility {requested:?} does not match underlying volatility {:?}",
                signature.volatility
            )));
        }
    }
    Ok(())
}

fn validate_return_type(name: &str, expected: Option<DataType>, actual: DataType) -> Result<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    if expected != actual {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} return type {expected:?} does not match underlying return type {actual:?}",
        )));
    }
    Ok(())
}

fn build_arg_fields(parameter_names: &[String], arg_types: &[DataType]) -> Vec<FieldRef> {
    arg_types
        .iter()
        .enumerate()
        .map(|(idx, dtype)| {
            let name = parameter_names
                .get(idx)
                .cloned()
                .unwrap_or_else(|| format!("arg{}", idx + 1));
            Arc::new(Field::new(name, dtype.clone(), true))
        })
        .collect()
}

fn validate_window_return_type(
    name: &str,
    expected: Option<DataType>,
    udwf: &WindowUDF,
    arg_types: &[DataType],
    parameter_names: &[String],
) -> Result<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let arg_fields = build_arg_fields(parameter_names, arg_types);
    let field = udwf.field(WindowUDFFieldArgs::new(&arg_fields, name))?;
    if field.data_type() != &expected {
        return Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} return type {expected:?} does not match underlying return type {:?}",
            field.data_type(),
        )));
    }
    Ok(())
}

fn extract_table_target(expr: &Expr, name: &str) -> Result<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => Ok(value.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} requires a string literal table function target"
        ))),
    }
}

fn extract_window_target(expr: &Expr, name: &str) -> Result<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(value)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(value)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(value)), _) => Ok(value.clone()),
        _ => Err(DataFusionError::Plan(format!(
            "CREATE FUNCTION {name} requires a string literal window function target"
        ))),
    }
}

fn extract_parameter_names(args: &[OperateFunctionArg]) -> Option<Vec<String>> {
    let mut names: Vec<String> = Vec::with_capacity(args.len());
    for arg in args {
        let name = arg.name.as_ref()?.value.clone();
        names.push(name);
    }
    Some(names)
}

fn resolve_placeholder_index(id: &str, parameter_names: &[String]) -> Result<usize> {
    let raw = id
        .strip_prefix('$')
        .ok_or_else(|| DataFusionError::Plan(format!("Invalid placeholder identifier: {id}")))?;
    if let Ok(index) = raw.parse::<usize>() {
        return index
            .checked_sub(1)
            .ok_or_else(|| DataFusionError::Plan(format!("Invalid placeholder index: {id}")));
    }
    let needle = raw.to_ascii_lowercase();
    for (idx, name) in parameter_names.iter().enumerate() {
        if name.to_ascii_lowercase() == needle {
            return Ok(idx);
        }
    }
    Err(DataFusionError::Plan(format!(
        "Unknown placeholder name: {id}"
    )))
}
