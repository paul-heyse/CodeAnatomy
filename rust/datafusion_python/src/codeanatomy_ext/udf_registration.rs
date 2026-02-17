//! UDF registration and config bridge surface.

use pyo3::prelude::*;

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(super::legacy::install_function_factory, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::derive_function_factory_policy, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_codeanatomy_udf_config, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::register_codeanatomy_udfs, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::registry_snapshot_py, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::udf_docs_snapshot, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::capabilities_snapshot, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_codeanatomy_runtime, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_codeanatomy_policy_config, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_codeanatomy_physical_config, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_planner_rules, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_physical_rules, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_expr_planners, module)?)?;
    Ok(())
}
