use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn schema_evolution_surface_registers() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "schema_evolution_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        for name in [
            "schema_evolution_adapter_factory",
            "parquet_listing_table_provider",
            "install_schema_evolution_adapter_factory",
        ] {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
    });
}
