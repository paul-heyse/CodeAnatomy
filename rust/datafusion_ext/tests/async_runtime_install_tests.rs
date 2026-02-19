use datafusion::execution::context::SessionContext;
use datafusion_common::Result;

#[cfg(feature = "async-udf")]
#[test]
fn register_all_with_policy_async_installs_without_deadlock() -> Result<()> {
    let ctx = SessionContext::new();
    datafusion_ext::udf_registry::register_all_with_policy(&ctx, true, Some(1000), Some(64))?;
    let state = ctx.state();
    assert!(
        state.scalar_functions().contains_key("async_echo"),
        "expected async_echo UDF to be registered after async runtime install"
    );
    Ok(())
}
