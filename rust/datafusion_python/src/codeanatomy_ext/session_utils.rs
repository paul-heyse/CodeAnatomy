//! Session/runtime utility bridge surface.

use pyo3::prelude::*;

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(super::legacy::replay_substrait_plan, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::lineage_from_substrait, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::build_extraction_session, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::register_dataset_provider, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::session_context_contract_probe, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::arrow_stream_to_batches, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::udf_expr, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::schema_evolution_adapter_factory, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::parquet_listing_table_provider, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_tracing, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::table_logical_plan, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::table_dfschema_tree, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_schema_evolution_adapter_factory, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::registry_catalog_provider_factory, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_delta_table_factory, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::delta_session_context, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_delta_plan_codecs, module)?)?;
    Ok(())
}

pub(crate) fn register_internal_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(super::legacy::replay_substrait_plan, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::lineage_from_substrait, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::build_extraction_session, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::register_dataset_provider, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::session_context_contract_probe, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::arrow_stream_to_batches, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::udf_expr, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_delta_table_factory, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::delta_session_context, module)?)?;
    module.add_function(wrap_pyfunction!(super::legacy::install_delta_plan_codecs, module)?)?;
    Ok(())
}
