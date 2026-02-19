use pyo3::prelude::*;
use pyo3::types::PyModule;
use std::path::Path;

#[test]
fn plan_bundle_bridge_entrypoints_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "plan_bundle_bridge_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        for name in [
            "capture_plan_bundle_runtime",
            "build_plan_bundle_artifact_with_warnings",
            "arrow_stream_to_batches",
        ] {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
    });
    let session_utils = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("codeanatomy_ext")
            .join("session_utils.rs"),
    )
    .expect("read session_utils.rs");
    assert!(
        !session_utils.contains("fn capture_plan_bundle_runtime("),
        "session_utils.rs should not own plan bundle bridge bodies"
    );
}
