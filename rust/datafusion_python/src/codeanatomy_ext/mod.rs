//! CodeAnatomy DataFusion Python extension surface.

use pyo3::prelude::*;

mod legacy;

pub(crate) mod cache_tables;
pub(crate) mod delta_maintenance;
pub(crate) mod delta_mutations;
pub(crate) mod delta_provider;
pub(crate) mod helpers;
pub(crate) mod plugin_bridge;
pub(crate) mod session_utils;
pub(crate) mod udf_registration;

pub fn init_module(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = py;
    session_utils::register_functions(module)?;
    udf_registration::register_functions(module)?;
    plugin_bridge::register_functions(module)?;
    cache_tables::register_functions(module)?;
    delta_provider::register_functions(module)?;
    delta_mutations::register_functions(module)?;
    delta_maintenance::register_functions(module)?;
    legacy::register_shared_classes(module)
}

pub fn init_internal_module(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = py;
    session_utils::register_internal_functions(module)?;
    udf_registration::register_functions(module)?;
    plugin_bridge::register_internal_functions(module)?;
    delta_provider::register_functions(module)?;
    delta_mutations::register_functions(module)?;
    delta_maintenance::register_functions(module)?;
    cache_tables::register_functions(module)?;
    legacy::register_shared_classes(module)
}
