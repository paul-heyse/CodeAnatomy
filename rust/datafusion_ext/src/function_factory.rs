use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::DataType;
use datafusion::execution::context::{FunctionFactory, RegisterFunction};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion_common::{DataFusionError, Result};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{Expr, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion_expr::logical_plan::OperateFunctionArg;

#[derive(Debug, Default)]
pub struct SqlMacroFunctionFactory;

#[async_trait]
impl FunctionFactory for SqlMacroFunctionFactory {
    async fn create(
        &self,
        _state: &SessionState,
        statement: datafusion_expr::logical_plan::CreateFunction,
    ) -> Result<RegisterFunction> {
        let body = statement
            .params
            .function_body
            .ok_or_else(|| DataFusionError::Plan("CREATE FUNCTION missing body".into()))?;
        let return_type = statement
            .return_type
            .ok_or_else(|| DataFusionError::Plan("CREATE FUNCTION missing return type".into()))?;
        let volatility = statement
            .params
            .behavior
            .unwrap_or(Volatility::Volatile);
        let args = statement.args.unwrap_or_default();
        let arg_types = args.iter().map(|arg| arg.data_type.clone()).collect::<Vec<_>>();
        let mut signature = Signature::exact(arg_types, volatility);
        let parameter_names = extract_parameter_names(&args);
        if let Some(names) = parameter_names.as_ref() {
            signature = signature.with_parameter_names(names.clone()).map_err(|err| {
                DataFusionError::Plan(format!(
                    "CREATE FUNCTION invalid parameter names: {err}"
                ))
            })?;
        }
        let udf_impl = SqlMacroUdf {
            name: statement.name,
            signature,
            return_type,
            template: body,
            parameter_names: parameter_names.unwrap_or_default(),
        };
        Ok(RegisterFunction::Scalar(Arc::new(ScalarUDF::from(udf_impl))))
    }
}

pub fn with_sql_macro_factory(state: &SessionState) -> SessionState {
    SessionStateBuilder::new_from_existing(state.clone())
        .with_function_factory(Some(Arc::new(SqlMacroFunctionFactory::default())))
        .build()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SqlMacroUdf {
    name: String,
    signature: Signature,
    return_type: DataType,
    template: Expr,
    parameter_names: Vec<String>,
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
        if transformed.transformed {
            Ok(ExprSimplifyResult::Simplified(transformed.data))
        } else {
            Ok(ExprSimplifyResult::Original(args))
        }
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
    let raw = id.strip_prefix('$').ok_or_else(|| {
        DataFusionError::Plan(format!("Invalid placeholder identifier: {id}"))
    })?;
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
