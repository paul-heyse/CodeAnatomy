#[test]
fn compile_contract_uses_decomposed_phase_functions() {
    let source = include_str!("../src/compiler/compile_contract.rs");
    assert!(source.contains("build_task_schedule_phase"));
    assert!(source.contains("pushdown_probe_phase"));
    assert!(source.contains("compile_artifacts_phase"));
}

#[test]
fn compile_phases_module_defines_phase_outputs() {
    let source = include_str!("../src/compiler/compile_phases.rs");
    assert!(source.contains("pub(crate) struct TaskSchedulePhaseResult"));
    assert!(source.contains("pub(crate) struct PushdownProbePhaseResult"));
    assert!(source.contains("pub(crate) struct ArtifactPhaseResult"));
}
