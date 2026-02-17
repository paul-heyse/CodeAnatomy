use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn helper_dependent_surfaces_register() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "helpers_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");

        // These entrypoints all depend on shared ctx/payload conversion helpers.
        for name in [
            "session_context_contract_probe",
            "delta_table_provider_from_session",
            "delta_write_ipc_request",
            "delta_optimize_compact_request_payload",
        ] {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
    });
}
