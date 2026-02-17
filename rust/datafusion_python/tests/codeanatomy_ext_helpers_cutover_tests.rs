use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn helper_cutover_surfaces_register_without_legacy_forwarders() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "helpers_cutover_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");

        for name in [
            "delta_table_provider_from_session",
            "delta_table_provider_with_files",
            "delta_write_ipc_request",
            "delta_optimize_compact_request_payload",
        ] {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
    });
}
