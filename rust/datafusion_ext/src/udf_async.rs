use std::any::Any;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use arrow::datatypes::{DataType, Field, FieldRef};
use async_trait::async_trait;
use datafusion::config::ConfigOptions;
use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion_expr_common::interval_arithmetic::Interval;
use datafusion_expr_common::sort_properties::ExprProperties;
use datafusion_macros::user_doc;
use tokio::time;

use crate::async_runtime;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AsyncUdfPolicy {
    pub ideal_batch_size: Option<usize>,
    pub timeout: Option<Duration>,
}

impl Default for AsyncUdfPolicy {
    fn default() -> Self {
        Self {
            ideal_batch_size: None,
            timeout: None,
        }
    }
}

static ASYNC_UDF_POLICY: OnceLock<RwLock<AsyncUdfPolicy>> = OnceLock::new();
fn async_udf_policy_lock() -> &'static RwLock<AsyncUdfPolicy> {
    ASYNC_UDF_POLICY.get_or_init(|| RwLock::new(AsyncUdfPolicy::default()))
}

pub fn set_async_udf_policy(
    ideal_batch_size: Option<usize>,
    timeout_ms: Option<u64>,
) -> Result<()> {
    let timeout = timeout_ms.map(Duration::from_millis);
    let policy = AsyncUdfPolicy {
        ideal_batch_size,
        timeout,
    };
    let lock = async_udf_policy_lock();
    let mut guard = lock
        .write()
        .map_err(|_| DataFusionError::Execution("Async UDF policy lock poisoned".into()))?;
    *guard = policy;
    Ok(())
}

pub fn async_udf_policy() -> AsyncUdfPolicy {
    let lock = async_udf_policy_lock();
    match lock.read() {
        Ok(guard) => *guard,
        Err(_) => AsyncUdfPolicy::default(),
    }
}

pub const ASYNC_ECHO_NAME: &str = "async_echo";

#[user_doc(
    doc_section(label = "Async Functions"),
    description = "Echo a string value using the async UDF execution path. Requires allow_async policy.",
    syntax_example = "async_echo(value)",
    standard_argument(name = "value", prefix = "String")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
struct AsyncEchoUdf {
    signature: Signature,
    policy: AsyncUdfPolicy,
}

impl AsyncEchoUdf {
    fn new() -> Self {
        Self::new_with_policy(async_udf_policy())
    }

    fn new_with_policy(policy: AsyncUdfPolicy) -> Self {
        let signature = Signature::string(1, Volatility::Immutable)
            .with_parameter_names(vec!["value".to_string()])
            .unwrap_or_else(|_| Signature::string(1, Volatility::Immutable));
        Self { signature, policy }
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
        self.doc()
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
        let arg_type = arg_types
            .first()
            .ok_or_else(|| DataFusionError::Plan("async_echo expects one argument".into()))?;
        Ok(arg_type.clone())
    }

    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        if let Some(interval) = inputs.first() {
            return Ok((*interval).clone());
        }
        Interval::make_unbounded(&DataType::Null)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        if inputs.len() == 1 {
            return Ok(Some(vec![interval.clone()]));
        }
        Ok(Some(Vec::new()))
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> Result<bool> {
        if let Some(props) = inputs.first() {
            return Ok(props.preserves_lex_ordering);
        }
        Ok(true)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(DataFusionError::Internal(
            "async_echo must be executed via async invocation".into(),
        ))
    }

    fn with_updated_config(&self, config: &ConfigOptions) -> Option<ScalarUDF> {
        let mut policy = self.policy;
        if policy.ideal_batch_size.is_none() {
            let size = config.execution.batch_size;
            if size > 0 {
                policy.ideal_batch_size = Some(size);
            }
        }
        if policy == self.policy {
            return None;
        }
        let inner = Arc::new(Self::new_with_policy(policy)) as Arc<dyn AsyncScalarUDFImpl>;
        Some(AsyncScalarUDF::new(inner).into_scalar_udf())
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for AsyncEchoUdf {
    fn ideal_batch_size(&self) -> Option<usize> {
        self.policy.ideal_batch_size
    }

    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let policy = self.policy;
        let handle = async_runtime::runtime_handle()?.spawn(async move {
            let value = args.args.first().ok_or_else(|| {
                DataFusionError::Plan("async_echo expects exactly one argument".into())
            })?;
            Ok(value.clone())
        });
        let result = if let Some(timeout) = policy.timeout {
            let mut handle = handle;
            tokio::select! {
                result = &mut handle => result,
                _ = time::sleep(timeout) => {
                    handle.abort();
                    return Err(DataFusionError::Execution(
                        "async_echo timed out".to_string(),
                    ));
                }
            }
        } else {
            handle.await
        };
        match result {
            Ok(value) => value,
            Err(err) => Err(DataFusionError::Execution(format!(
                "async_echo failed: {err}"
            ))),
        }
    }
}

pub fn async_echo_udf() -> ScalarUDF {
    let inner = Arc::new(AsyncEchoUdf::new()) as Arc<dyn AsyncScalarUDFImpl>;
    AsyncScalarUDF::new(inner).into_scalar_udf()
}

pub fn register_async_udfs(ctx: &SessionContext) -> Result<()> {
    let state = ctx.state();
    let config_options = state.config_options();
    let mut policy = async_udf_policy();
    if policy.ideal_batch_size.is_none() {
        let size = config_options.execution.batch_size;
        if size > 0 {
            policy.ideal_batch_size = Some(size);
        }
    }
    let inner = Arc::new(AsyncEchoUdf::new_with_policy(policy)) as Arc<dyn AsyncScalarUDFImpl>;
    ctx.register_udf(AsyncScalarUDF::new(inner).into_scalar_udf());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{AsyncEchoUdf, AsyncUdfPolicy};
    use datafusion::config::ConfigOptions;
    use datafusion_expr::ScalarUDFImpl;

    #[test]
    fn async_echo_updates_batch_size_from_config() {
        let base = AsyncEchoUdf::new_with_policy(AsyncUdfPolicy {
            ideal_batch_size: None,
            timeout: None,
        });
        let mut config = ConfigOptions::new();
        config.execution.batch_size = 256;
        let updated = base.with_updated_config(&config);
        assert!(updated.is_some(), "expected config-specialized UDF");
        let udf = updated.expect("updated UDF");
        let async_udf = udf.as_async().expect("async udf");
        assert_eq!(async_udf.ideal_batch_size(), Some(256));
    }
}
