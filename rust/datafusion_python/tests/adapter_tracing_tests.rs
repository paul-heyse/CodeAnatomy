#[test]
fn dataset_exec_is_instrumented() {
    let source = include_str!("../src/dataset_exec.rs");
    assert!(source.contains("#[instrument(skip(self, context))]"));
}

#[test]
fn udaf_bridge_is_instrumented() {
    let source = include_str!("../src/udaf.rs");
    assert!(source.contains("#[instrument(skip(accum))]"));
    assert!(source.contains("#[instrument(skip(capsule))]"));
}

#[test]
fn udwf_bridge_is_instrumented() {
    let source = include_str!("../src/udwf.rs");
    assert!(source.contains("#[instrument(skip(evaluator))]"));
}
