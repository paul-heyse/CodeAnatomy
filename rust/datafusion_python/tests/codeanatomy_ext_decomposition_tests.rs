use pyo3::prelude::*;
use pyo3::types::PyModule;

fn build_module(py: Python<'_>) -> Bound<'_, PyModule> {
    let module = PyModule::new(py, "codeanatomy_ext_test").expect("create module");
    datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
        .expect("init internal module");
    module
}

#[test]
fn codeanatomy_ext_registers_decomposed_surfaces() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = build_module(py);
        let expected = [
            "replay_substrait_plan",
            "install_function_factory",
            "load_df_plugin",
            "register_cache_tables",
            "delta_table_provider_from_session",
            "delta_write_ipc_request",
            "delta_optimize_compact_request_payload",
        ];
        for name in expected {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
    });
}

#[test]
fn codeanatomy_ext_hard_cuts_positional_delta_entrypoints() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = build_module(py);
        let removed = [
            "delta_write_ipc",
            "delta_delete",
            "delta_update",
            "delta_merge",
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
            assert!(
                !module.hasattr(name).expect("hasattr"),
                "expected {name} to be removed"
            );
        }
    });
}
