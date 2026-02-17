use pyo3::prelude::*;
use pyo3::types::PyModule;

#[test]
fn plugin_bridge_surface_registers() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new(py, "plugin_bridge_test").expect("create module");
        datafusion_python::codeanatomy_ext::init_internal_module(py, &module)
            .expect("init internal module");
        for name in [
            "create_df_plugin_table_provider",
            "plugin_library_path",
            "plugin_manifest",
            "load_df_plugin",
            "register_df_plugin_udfs",
            "register_df_plugin_table_functions",
            "register_df_plugin_table_providers",
            "register_df_plugin",
        ] {
            assert!(module.hasattr(name).expect("hasattr"), "missing {name}");
        }
    });
}
