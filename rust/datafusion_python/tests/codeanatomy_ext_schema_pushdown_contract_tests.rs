use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};

fn build_module(py: Python<'_>) -> Bound<'_, PyModule> {
    let module = PyModule::new(py, "schema_pushdown_contract_test").expect("create module");
    datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
        .expect("init internal module");
    module
}

#[test]
fn schema_pushdown_contract_requires_explicit_schema_ipc() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = build_module(py);
        let ctx = module
            .call_method0("delta_session_context")
            .expect("build delta session context");

        let kwargs = PyDict::new(py);
        kwargs.set_item("ctx", &ctx).expect("set ctx");
        kwargs
            .set_item("path", "file:///tmp/schema_pushdown_contract")
            .expect("set path");
        kwargs
            .set_item("file_extension", ".parquet")
            .expect("set file_extension");
        kwargs
            .set_item("table_name", "contract_table")
            .expect("set table_name");
        kwargs
            .set_item("table_definition", py.None())
            .expect("set table_definition");
        kwargs
            .set_item("table_partition_cols", py.None())
            .expect("set table_partition_cols");
        kwargs
            .set_item("schema_ipc", py.None())
            .expect("set schema_ipc");
        kwargs
            .set_item("partition_schema_ipc", py.None())
            .expect("set partition_schema_ipc");
        kwargs
            .set_item("file_sort_order", py.None())
            .expect("set file_sort_order");
        kwargs
            .set_item("key_fields", py.None())
            .expect("set key_fields");
        kwargs
            .set_item("expr_adapter_factory", py.None())
            .expect("set expr_adapter_factory");
        kwargs
            .set_item("parquet_pruning", py.None())
            .expect("set parquet_pruning");
        kwargs
            .set_item("skip_metadata", py.None())
            .expect("set skip_metadata");
        kwargs
            .set_item("collect_statistics", py.None())
            .expect("set collect_statistics");

        let err = module
            .call_method("parquet_listing_table_provider", (), Some(&kwargs))
            .expect_err("missing schema_ipc must fail");
        assert!(
            err.to_string().contains("requires schema_ipc"),
            "unexpected error: {err}"
        );
    });
}

#[test]
fn schema_pushdown_contract_exports_adapter_factory_capsule() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = build_module(py);
        let factory = module
            .call_method0("schema_evolution_adapter_factory")
            .expect("schema evolution adapter factory");
        let type_name = factory.get_type().name().expect("type name");
        assert_eq!(type_name, "PyCapsule");
    });
}
