use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_ext::relation_planner::CodeAnatomyRelationPlanner;

#[test]
fn relation_planner_pilot_supports_explain_path() {
    let state = SessionStateBuilder::new()
        .with_relation_planners(vec![Arc::new(CodeAnatomyRelationPlanner)])
        .build();
    let ctx = SessionContext::new_with_state(state);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");

    let batches = runtime
        .block_on(async {
            let df = ctx.sql("SELECT 1 AS id").await?;
            df.explain(true, false)?.collect().await
        })
        .expect("explain query should run");

    assert!(!batches.is_empty());
}
