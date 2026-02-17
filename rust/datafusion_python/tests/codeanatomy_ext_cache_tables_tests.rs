use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn cache_table_surface_registers() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "cache_table_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        for name in ["register_cache_tables", "runtime_execution_metrics_snapshot"] {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
    });
}
