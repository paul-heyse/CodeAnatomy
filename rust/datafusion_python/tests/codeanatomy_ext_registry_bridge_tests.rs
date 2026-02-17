use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn registry_bridge_surface_registers() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "registry_bridge_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        assert!(
            module
                .hasattr("registry_catalog_provider_factory")
                .expect("hasattr"),
            "missing registry_catalog_provider_factory"
        );
    });
}
