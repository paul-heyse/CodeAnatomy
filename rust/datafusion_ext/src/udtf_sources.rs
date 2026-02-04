use datafusion::execution::context::SessionContext;
use datafusion_common::Result;

pub fn register_external_udtfs(_ctx: &SessionContext) -> Result<()> {
    Ok(())
}
