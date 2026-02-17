use std::path::Path;

use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn legacy_module_deleted_and_classes_exposed_from_owned_modules() {
    let legacy_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("codeanatomy_ext")
        .join("legacy.rs");
    assert!(!legacy_path.exists(), "legacy.rs must be deleted");

    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "legacy_cutover_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");

        for class_name in [
            "DeltaCdfOptions",
            "DeltaRuntimeEnvOptions",
            "DeltaSessionRuntimePolicyOptions",
        ] {
            assert!(
                module.hasattr(class_name).expect("hasattr"),
                "missing class {class_name}"
            );
        }
    });
}
