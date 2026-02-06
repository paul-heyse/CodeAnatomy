use std::future::Future;
use std::sync::OnceLock;

use datafusion_common::{DataFusionError, Result};
use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

static SHARED_RUNTIME: OnceLock<std::result::Result<Runtime, String>> = OnceLock::new();

pub fn shared_runtime() -> Result<&'static Runtime> {
    match SHARED_RUNTIME.get_or_init(|| Runtime::new().map_err(|err| err.to_string())) {
        Ok(runtime) => Ok(runtime),
        Err(message) => Err(DataFusionError::Execution(format!(
            "Failed to initialize shared Tokio runtime: {message}"
        ))),
    }
}

pub fn runtime_handle() -> Result<Handle> {
    match Handle::try_current() {
        Ok(handle) => Ok(handle),
        Err(_) => Ok(shared_runtime()?.handle().clone()),
    }
}

pub fn spawn<F, T>(future: F) -> Result<JoinHandle<T>>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    Ok(runtime_handle()?.spawn(future))
}

pub fn block_on<F, T>(future: F) -> Result<T>
where
    F: Future<Output = T>,
{
    Ok(shared_runtime()?.block_on(future))
}
