//! Substrait replay and lineage bridge surface.

use codeanatomy_engine::compiler::lineage::{
    extract_lineage as extract_lineage_native,
    lineage_from_substrait as lineage_from_substrait_native,
};
use datafusion::execution::context::SessionContext;
#[cfg(feature = "substrait")]
use datafusion::prelude::DataFrame;
#[cfg(feature = "substrait")]
use datafusion_ext::async_runtime;
#[cfg(feature = "substrait")]
use pyo3::exceptions::PyValueError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::dataframe::PyDataFrame;
use crate::sql::logical::PyLogicalPlan;

#[cfg(feature = "substrait")]
use super::helpers::extract_session_ctx;
use super::helpers::json_to_py;

#[pyfunction]
pub(crate) fn replay_substrait_plan(
    ctx: &Bound<'_, PyAny>,
    payload_bytes: &Bound<'_, PyBytes>,
) -> PyResult<PyDataFrame> {
    #[cfg(feature = "substrait")]
    {
        use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
        use datafusion_substrait::substrait::proto::Plan;
        use prost::Message;

        let plan = Plan::decode(payload_bytes.as_bytes()).map_err(|err| {
            PyValueError::new_err(format!("Failed to decode Substrait payload: {err}"))
        })?;
        let state = extract_session_ctx(ctx)?.state();
        let runtime = async_runtime::shared_runtime().map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to acquire shared Tokio runtime: {err}"))
        })?;
        let logical_plan = runtime
            .block_on(from_substrait_plan(&state, &plan))
            .map_err(|err| PyRuntimeError::new_err(format!("Substrait replay failed: {err}")))?;
        Ok(PyDataFrame::new(DataFrame::new(state, logical_plan)))
    }
    #[cfg(not(feature = "substrait"))]
    {
        let _ = (ctx, payload_bytes);
        Err(PyRuntimeError::new_err(
            "Substrait replay requires datafusion-python built with the `substrait` feature.",
        ))
    }
}

#[pyfunction]
pub(crate) fn lineage_from_substrait(
    py: Python<'_>,
    payload_bytes: &Bound<'_, PyBytes>,
) -> PyResult<Py<PyAny>> {
    let ctx = SessionContext::new();
    let report = lineage_from_substrait_native(&ctx, payload_bytes.as_bytes())
        .map_err(|err| PyRuntimeError::new_err(format!("Lineage extraction failed: {err}")))?;
    let payload = serde_json::to_value(report).map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to encode lineage payload: {err}"))
    })?;
    json_to_py(py, &payload)
}

#[pyfunction]
pub(crate) fn extract_lineage_json(plan: PyLogicalPlan) -> PyResult<String> {
    let logical_plan = plan.plan();
    let report = extract_lineage_native(logical_plan.as_ref())
        .map_err(|err| PyRuntimeError::new_err(format!("Lineage extraction failed: {err}")))?;
    serde_json::to_string(&report)
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to encode lineage payload: {err}")))
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(replay_substrait_plan, module)?)?;
    module.add_function(wrap_pyfunction!(lineage_from_substrait, module)?)?;
    module.add_function(wrap_pyfunction!(extract_lineage_json, module)?)?;
    Ok(())
}

pub(crate) fn register_internal_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_functions(module)
}
