//! Cache-table registration surface.

use pyo3::prelude::*;

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(super::legacy::register_cache_tables, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::runtime_execution_metrics_snapshot, module)?)?;
    Ok(())
}
