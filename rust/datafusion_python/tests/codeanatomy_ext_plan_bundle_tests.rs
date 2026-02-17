use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn plan_bundle_surface_registers() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "plan_bundle_surface_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        for name in [
            "capture_plan_bundle_runtime",
            "build_plan_bundle_artifact_with_warnings",
        ] {
            let _ = module.getattr(name);
        }
    });
}
