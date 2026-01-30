use std::future::Future;
use std::sync::OnceLock;

use tokio::runtime::{Handle, Runtime};
use tokio::task::JoinHandle;

static SHARED_RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn shared_runtime() -> &'static Runtime {
    SHARED_RUNTIME.get_or_init(|| Runtime::new().expect("shared tokio runtime"))
}

pub fn runtime_handle() -> Handle {
    Handle::try_current().unwrap_or_else(|_| shared_runtime().handle().clone())
}

pub fn spawn<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    runtime_handle().spawn(future)
}

pub fn block_on<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    shared_runtime().block_on(future)
}
