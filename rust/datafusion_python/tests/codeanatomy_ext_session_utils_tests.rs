use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn session_utils_surface_registers() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "session_utils_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        for name in [
            "replay_substrait_plan",
            "lineage_from_substrait",
            "extract_lineage_json",
            "build_extraction_session",
            "register_dataset_provider",
            "session_context_contract_probe",
            "arrow_stream_to_batches",
            "udf_expr",
            "install_delta_table_factory",
            "delta_session_context",
            "install_delta_plan_codecs",
            "install_tracing",
            "table_logical_plan",
            "table_dfschema_tree",
            "install_schema_evolution_adapter_factory",
            "registry_catalog_provider_factory",
            "schema_evolution_adapter_factory",
            "parquet_listing_table_provider",
        ] {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
    });
}
