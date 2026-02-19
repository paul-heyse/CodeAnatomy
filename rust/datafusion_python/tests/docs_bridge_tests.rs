use pyo3::prelude::*;
use pyo3::types::PyModule;
use std::path::Path;

#[test]
fn docs_bridge_entrypoint_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "docs_bridge_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        assert!(
            module.hasattr("udf_docs_snapshot").expect("hasattr"),
            "missing udf_docs_snapshot"
        );
    });
    let udf_registration = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("codeanatomy_ext")
            .join("udf_registration.rs"),
    )
    .expect("read udf_registration.rs");
    assert!(
        !udf_registration.contains("fn udf_docs_snapshot("),
        "udf_registration.rs should not own docs bridge bodies"
    );
}
