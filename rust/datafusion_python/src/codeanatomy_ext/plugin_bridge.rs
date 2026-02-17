//! Plugin bridge and manifest validation surface.

use pyo3::prelude::*;

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(super::legacy::load_df_plugin, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::register_df_plugin_udfs, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::register_df_plugin_table_functions, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::create_df_plugin_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::register_df_plugin_table_providers, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::register_df_plugin, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::plugin_library_path, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::plugin_manifest, module)?)?;
    Ok(())
}

pub(crate) fn register_internal_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(super::legacy::create_df_plugin_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::plugin_library_path, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::plugin_manifest, module)?)?;
    Ok(())
}
