use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn delta_mutation_request_surface_registers() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "delta_mutation_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        let expected = [
            "delta_write_ipc_request",
            "delta_delete_request",
            "delta_update_request",
            "delta_merge_request",
        ];
        for name in expected {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
        for removed in ["delta_write_ipc", "delta_delete", "delta_update", "delta_merge"] {
            assert!(!module.hasattr(removed).expect("hasattr"));
        }
    });
}
