//! CodeAnatomy DataFusion Python extension surface.

use pyo3::prelude::*;

pub(crate) mod cache_tables;
pub(crate) mod delta_maintenance;
pub(crate) mod delta_mutations;
pub(crate) mod delta_provider;
pub(crate) mod helpers;
pub(crate) mod plugin_bridge;
pub(crate) mod registry_bridge;
pub(crate) mod rust_pivot;
pub(crate) mod schema_evolution;
pub(crate) mod session_utils;
pub(crate) mod udf_registration;

fn register_shared_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    delta_provider::register_classes(module)?;
    session_utils::register_classes(module)?;
    Ok(())
}

pub fn init_module(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = py;
    session_utils::register_functions(module)?;
    udf_registration::register_functions(module)?;
    plugin_bridge::register_functions(module)?;
    cache_tables::register_functions(module)?;
    rust_pivot::register_functions(module)?;
    schema_evolution::register_functions(module)?;
    registry_bridge::register_functions(module)?;
    delta_provider::register_functions(module)?;
    delta_mutations::register_functions(module)?;
    delta_maintenance::register_functions(module)?;
    register_shared_classes(module)
}

pub fn init_internal_module(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = py;
    session_utils::register_internal_functions(module)?;
    udf_registration::register_functions(module)?;
    plugin_bridge::register_internal_functions(module)?;
    schema_evolution::register_functions(module)?;
    rust_pivot::register_functions(module)?;
    registry_bridge::register_functions(module)?;
    delta_provider::register_functions(module)?;
    delta_mutations::register_functions(module)?;
    delta_maintenance::register_functions(module)?;
    cache_tables::register_functions(module)?;
    register_shared_classes(module)
}
