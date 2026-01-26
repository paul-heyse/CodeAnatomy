use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion_expr::{
    ColumnarValue,
    Documentation,
    ReturnFieldArgs,
    ScalarFunctionArgs,
    ScalarUDF,
    ScalarUDFImpl,
    Signature,
    Volatility,
};

use crate::udf_docs;

pub const ASYNC_ECHO_NAME: &str = "async_echo";

#[derive(Debug, PartialEq, Eq, Hash)]
struct AsyncEchoUdf {
    signature: Signature,
}

impl AsyncEchoUdf {
    fn new() -> Self {
        let signature = Signature::string(1, Volatility::Stable)
            .with_parameter_names(vec!["value".to_string()])
            .unwrap_or_else(|_| Signature::string(1, Volatility::Stable));
        Self {
            signature,
        }
    }
}

impl ScalarUDFImpl for AsyncEchoUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        ASYNC_ECHO_NAME
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(udf_docs::async_echo_doc())
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = args
            .arg_fields
            .first()
            .ok_or_else(|| DataFusionError::Plan("async_echo expects one argument".into()))?;
        let mut output = Field::new(self.name(), field.data_type().clone(), field.is_nullable());
        if !field.metadata().is_empty() {
            output = output.with_metadata(field.metadata().clone());
        }
        Ok(Arc::new(output))
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let arg_type = arg_types.first().ok_or_else(|| {
            DataFusionError::Plan("async_echo expects one argument".into())
        })?;
        Ok(arg_type.clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(DataFusionError::Internal(
            "async_echo must be executed via async invocation".into(),
        ))
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for AsyncEchoUdf {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let value = args.args.first().ok_or_else(|| {
            DataFusionError::Plan("async_echo expects exactly one argument".into())
        })?;
        Ok(value.clone())
    }
}

pub fn async_echo_udf() -> ScalarUDF {
    let inner = Arc::new(AsyncEchoUdf::new()) as Arc<dyn AsyncScalarUDFImpl>;
    AsyncScalarUDF::new(inner).into_scalar_udf()
}

pub fn register_async_udfs(ctx: &SessionContext) -> Result<()> {
    ctx.register_udf(async_echo_udf());
    Ok(())
}
