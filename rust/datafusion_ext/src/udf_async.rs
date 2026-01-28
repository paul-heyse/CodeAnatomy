use std::any::Any;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use arrow::datatypes::{DataType, Field, FieldRef};
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion_expr::{
    ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};
use datafusion_macros::user_doc;
use tokio::runtime::Runtime;
use tokio::time;

#[derive(Clone, Copy, Debug)]
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
static ASYNC_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn async_udf_policy_lock() -> &'static RwLock<AsyncUdfPolicy> {
    ASYNC_UDF_POLICY.get_or_init(|| RwLock::new(AsyncUdfPolicy::default()))
}

fn async_runtime() -> &'static Runtime {
    ASYNC_RUNTIME.get_or_init(|| Runtime::new().expect("async udf runtime"))
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
}

impl AsyncEchoUdf {
    fn new() -> Self {
        let signature = Signature::string(1, Volatility::Immutable)
            .with_parameter_names(vec!["value".to_string()])
            .unwrap_or_else(|_| Signature::string(1, Volatility::Immutable));
        Self { signature }
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

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Err(DataFusionError::Internal(
            "async_echo must be executed via async invocation".into(),
        ))
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for AsyncEchoUdf {
    fn ideal_batch_size(&self) -> Option<usize> {
        async_udf_policy().ideal_batch_size
    }

    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let policy = async_udf_policy();
        let handle = async_runtime().spawn(async move {
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
    ctx.register_udf(async_echo_udf());
    Ok(())
}
