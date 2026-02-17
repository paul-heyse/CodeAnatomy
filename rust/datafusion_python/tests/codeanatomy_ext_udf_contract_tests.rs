use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn udf_contract_surface_registers() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "udf_contract_surface_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        for name in ["install_codeanatomy_runtime", "udf_docs_snapshot", "registry_snapshot"] {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
    });
}
