use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn udf_registration_surface_registers() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "udf_registration_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        for name in [
            "install_function_factory",
            "derive_function_factory_policy",
            "install_codeanatomy_udf_config",
            "register_codeanatomy_udfs",
            "registry_snapshot",
            "registry_snapshot_msgpack",
            "udf_docs_snapshot",
            "capabilities_snapshot",
            "install_codeanatomy_runtime",
            "install_codeanatomy_policy_config",
            "install_codeanatomy_physical_config",
            "install_planner_rules",
            "install_physical_rules",
            "install_expr_planners",
            "derive_cache_policies",
            "extract_tree_sitter_batch",
            "interval_align_table",
        ] {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
    });
}
