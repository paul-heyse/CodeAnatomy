use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn delta_maintenance_request_surface_registers() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "delta_maintenance_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        let expected = [
            "delta_optimize_compact_request_payload",
            "delta_vacuum_request_payload",
            "delta_restore_request_payload",
            "delta_set_properties_request_payload",
            "delta_add_features_request_payload",
            "delta_add_constraints_request_payload",
            "delta_drop_constraints_request_payload",
            "delta_create_checkpoint_request_payload",
            "delta_cleanup_metadata_request_payload",
        ];
        for name in expected {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
        let removed = [
            "delta_optimize_compact",
            "delta_vacuum",
            "delta_restore",
            "delta_set_properties",
            "delta_add_features",
            "delta_add_constraints",
            "delta_drop_constraints",
            "delta_create_checkpoint",
            "delta_cleanup_metadata",
        ];
        for name in removed {
            assert!(!module.hasattr(name).expect("hasattr"));
        }
    });
}
