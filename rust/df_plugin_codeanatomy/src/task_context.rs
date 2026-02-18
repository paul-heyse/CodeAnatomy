use std::sync::{Arc, OnceLock};

use datafusion::execution::TaskContextProvider;
use datafusion::execution::context::SessionContext;

pub(crate) fn global_task_ctx_provider() -> Arc<dyn TaskContextProvider> {
    static TASK_CTX_PROVIDER: OnceLock<Arc<SessionContext>> = OnceLock::new();
    let provider = TASK_CTX_PROVIDER.get_or_init(|| Arc::new(SessionContext::new()));
    Arc::clone(provider) as Arc<dyn TaskContextProvider>
}
