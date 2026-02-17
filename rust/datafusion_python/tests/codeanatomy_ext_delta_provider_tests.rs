use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn delta_provider_surface_registers() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "delta_provider_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        let expected = [
            "delta_cdf_table_provider",
            "delta_table_provider_from_session",
            "delta_table_provider_with_files",
            "delta_scan_config_from_session",
            "delta_snapshot_info",
            "validate_protocol_gate",
            "delta_add_actions",
            "delta_data_checker",
            "DeltaCdfOptions",
        ];
        for name in expected {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
    });
}
