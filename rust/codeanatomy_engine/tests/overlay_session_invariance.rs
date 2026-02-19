use codeanatomy_engine::rules::overlay::build_overlaid_session;
use codeanatomy_engine::rules::registry::CpgRuleSet;
use datafusion::prelude::SessionContext;
use std::fs;
use std::path::PathBuf;

#[tokio::test]
async fn overlay_session_does_not_mutate_base_state() {
    let base_ctx = SessionContext::new();
    let initial_state = base_ctx.state();
    let initial_analyzer_count = initial_state.analyzer().rules.len();

    let ruleset = CpgRuleSet::new(vec![], vec![], vec![]);
    let _overlay_ctx = build_overlaid_session(&base_ctx, &ruleset)
        .await
        .expect("overlay build");

    let after_state = base_ctx.state();
    assert_eq!(initial_analyzer_count, after_state.analyzer().rules.len());
}

fn source_path(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel)
}

#[test]
fn orchestration_routes_rule_overlay_through_overlaid_ruleset() {
    let source = fs::read_to_string(source_path("src/executor/orchestration.rs")).unwrap();
    assert!(source.contains("spec.rule_overlay"));
    assert!(source.contains("build_overlaid_ruleset("));
}
