use std::fs;
use std::path::PathBuf;

fn source_path(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel)
}

#[test]
fn test_session_factory_uses_builder_native_planning_surface_path() {
    let source = fs::read_to_string(source_path("src/session/factory.rs")).unwrap();

    assert!(source.contains("apply_to_builder(builder, &planning_surface)"));
    assert!(!source.contains("register_expr_planner("));
    assert!(!source.contains("register_function_rewrite("));
}

#[test]
fn test_planning_surface_module_contains_post_build_rewrite_exception_only() {
    let source = fs::read_to_string(source_path("src/session/planning_surface.rs")).unwrap();

    assert!(source.contains("install_rewrites"));
    assert!(source.contains("builder = builder.with_query_planner(planner);"));
}
