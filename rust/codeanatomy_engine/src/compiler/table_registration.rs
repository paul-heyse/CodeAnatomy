//! Shared helpers for idempotent table/view registration.

use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use datafusion_common::Result;
use std::sync::Arc;

/// Register a table provider after replacing an existing table of the same name.
///
/// DataFusion registration can fail when the same logical name is reused across
/// multiple compile passes in a single process. This helper normalizes the
/// replacement behavior for planner and cache-boundary flows.
pub fn register_or_replace_table(
    ctx: &SessionContext,
    name: &str,
    provider: Arc<dyn TableProvider>,
) -> Result<()> {
    let _ = ctx.deregister_table(name);
    ctx.register_table(name, provider)?;
    Ok(())
}
