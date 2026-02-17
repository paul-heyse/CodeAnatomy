#[test]
fn plan_compiler_has_phase_instrumentation() {
    let source = include_str!("../src/compiler/plan_compiler.rs");
    assert!(source.contains("#[cfg_attr(feature = \"tracing\", instrument(skip(self)))]"));
}

#[test]
fn optimizer_pipeline_has_phase_instrumentation() {
    let source = include_str!("../src/compiler/optimizer_pipeline.rs");
    assert!(source.contains(
        "#[cfg_attr(feature = \"tracing\", instrument(skip(ctx, unoptimized_plan, config)))]"
    ));
}

#[test]
fn plan_bundle_has_phase_instrumentation() {
    let source = include_str!("../src/compiler/plan_bundle.rs");
    assert!(source.contains("capture_plan_bundle_runtime"));
    assert!(source.contains("build_plan_bundle_artifact"));
    assert!(source.contains("build_plan_bundle_artifact_with_warnings"));
    assert!(source.contains("cfg_attr(feature = \"tracing\", instrument"));
}

#[test]
fn pushdown_probe_extract_has_instrumentation() {
    let source = include_str!("../src/compiler/pushdown_probe_extract.rs");
    assert!(source.contains("extract_input_filter_predicates"));
    assert!(source.contains("verify_pushdown_contracts"));
    assert!(source.contains("cfg_attr(feature = \"tracing\", instrument"));
}
