//! Plan bundle capture/artifact bridge surface.

use arrow::array::RecordBatchReader;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use codeanatomy_engine::compiler::plan_bundle;
use datafusion_ext::async_runtime;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyList};
use serde::Deserialize;
use tracing::instrument;

use crate::dataframe::PyDataFrame;

use super::helpers::{extract_session_ctx, json_to_py, parse_python_payload};

#[derive(Debug, Clone, Deserialize)]
struct PlanBundleBuildPayload {
    deterministic_inputs: Option<bool>,
    no_volatile_udfs: Option<bool>,
    deterministic_optimizer: Option<bool>,
    stats_quality: Option<String>,
    capture_substrait: Option<bool>,
    capture_sql: Option<bool>,
    capture_delta_codec: Option<bool>,
}

#[pyfunction]
#[instrument(level = "info", skip_all)]
#[pyo3(signature = (ctx, payload, df=None))]
pub(crate) fn capture_plan_bundle_runtime(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    payload: &Bound<'_, PyAny>,
    df: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let parsed: PlanBundleBuildPayload =
        parse_python_payload(py, payload, "plan bundle runtime capture")?;
    let Some(df_obj) = df else {
        return Err(PyValueError::new_err(
            "capture_plan_bundle_runtime requires a datafusion.DataFrame argument.",
        ));
    };

    let session_ctx = extract_session_ctx(ctx)?;
    let py_df = df_obj.extract::<PyDataFrame>().map_err(|err| {
        PyValueError::new_err(format!(
            "plan bundle bridge expected datafusion.DataFrame argument: {err}"
        ))
    })?;
    let dataframe = py_df.inner_df().as_ref().clone();
    let runtime = async_runtime::shared_runtime().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to acquire shared Tokio runtime: {err}"))
    })?;
    let (runtime_bundle, artifact, warnings) = runtime.block_on(async {
        let runtime_bundle = plan_bundle::capture_plan_bundle_runtime(&session_ctx, &dataframe)
            .await
            .map_err(|err| {
                PyRuntimeError::new_err(format!("Plan bundle runtime capture failed: {err}"))
            })?;
        let (artifact, warnings) = plan_bundle::build_plan_bundle_artifact_with_warnings(
            plan_bundle::PlanBundleArtifactBuildRequest {
                ctx: &session_ctx,
                runtime: &runtime_bundle,
                rulepack_fingerprint: [0_u8; 32],
                provider_identities: Vec::new(),
                optimizer_traces: Vec::new(),
                pushdown_report: None,
                deterministic_inputs: parsed.deterministic_inputs.unwrap_or(true),
                no_volatile_udfs: parsed.no_volatile_udfs.unwrap_or(true),
                deterministic_optimizer: parsed.deterministic_optimizer.unwrap_or(true),
                stats_quality: parsed.stats_quality.clone(),
                capture_substrait: parsed.capture_substrait.unwrap_or(true),
                capture_sql: parsed.capture_sql.unwrap_or(false),
                capture_delta_codec: parsed.capture_delta_codec.unwrap_or(false),
                planning_surface_hash: [0_u8; 32],
            },
        )
        .await
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Plan bundle runtime capture failed: {err}"))
        })?;
        Ok::<_, PyErr>((runtime_bundle, artifact, warnings))
    })?;
    let response = serde_json::json!({
        "captured": true,
        "deterministic_inputs": parsed.deterministic_inputs.unwrap_or(true),
        "no_volatile_udfs": parsed.no_volatile_udfs.unwrap_or(true),
        "deterministic_optimizer": parsed.deterministic_optimizer.unwrap_or(true),
        "stats_quality": parsed.stats_quality,
        "capture_substrait": parsed.capture_substrait.unwrap_or(true),
        "capture_sql": parsed.capture_sql.unwrap_or(false),
        "capture_delta_codec": parsed.capture_delta_codec.unwrap_or(false),
        "logical_plan": format!("{:?}", runtime_bundle.p0_logical),
        "optimized_plan": format!("{:?}", runtime_bundle.p1_optimized),
        "physical_plan": format!("{:?}", runtime_bundle.p2_physical),
        "required_udfs": artifact.required_udfs,
        "referenced_tables": artifact.referenced_tables,
        "artifact_version": artifact.artifact_version,
        "has_substrait_bytes": artifact.substrait_bytes.is_some(),
        "has_sql_text": artifact.sql_text.is_some(),
        "warnings": warnings,
    });
    json_to_py(py, &response)
}

#[pyfunction]
#[pyo3(signature = (ctx, payload, df=None))]
pub(crate) fn build_plan_bundle_artifact_with_warnings(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    payload: &Bound<'_, PyAny>,
    df: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let parsed: PlanBundleBuildPayload =
        parse_python_payload(py, payload, "plan bundle artifact build")?;
    let Some(df_obj) = df else {
        return Err(PyValueError::new_err(
            "build_plan_bundle_artifact_with_warnings requires a datafusion.DataFrame argument.",
        ));
    };

    let session_ctx = extract_session_ctx(ctx)?;
    let py_df = df_obj.extract::<PyDataFrame>().map_err(|err| {
        PyValueError::new_err(format!(
            "plan bundle bridge expected datafusion.DataFrame argument: {err}"
        ))
    })?;
    let dataframe = py_df.inner_df().as_ref().clone();
    let runtime = async_runtime::shared_runtime().map_err(|err| {
        PyRuntimeError::new_err(format!("Failed to acquire shared Tokio runtime: {err}"))
    })?;
    let result = runtime.block_on(async {
        let runtime_bundle = plan_bundle::capture_plan_bundle_runtime(&session_ctx, &dataframe)
            .await
            .map_err(|err| {
                PyRuntimeError::new_err(format!("Plan bundle runtime capture failed: {err}"))
            })?;
        plan_bundle::build_plan_bundle_artifact_with_warnings(
            plan_bundle::PlanBundleArtifactBuildRequest {
                ctx: &session_ctx,
                runtime: &runtime_bundle,
                rulepack_fingerprint: [0_u8; 32],
                provider_identities: Vec::new(),
                optimizer_traces: Vec::new(),
                pushdown_report: None,
                deterministic_inputs: parsed.deterministic_inputs.unwrap_or(true),
                no_volatile_udfs: parsed.no_volatile_udfs.unwrap_or(true),
                deterministic_optimizer: parsed.deterministic_optimizer.unwrap_or(true),
                stats_quality: parsed.stats_quality.clone(),
                capture_substrait: parsed.capture_substrait.unwrap_or(true),
                capture_sql: parsed.capture_sql.unwrap_or(false),
                capture_delta_codec: parsed.capture_delta_codec.unwrap_or(false),
                planning_surface_hash: [0_u8; 32],
            },
        )
        .await
        .map_err(|err| PyRuntimeError::new_err(format!("Plan bundle artifact build failed: {err}")))
    })?;
    let (artifact, warnings) = result;
    let response = serde_json::json!({
        "artifact": artifact,
        "warnings": warnings,
    });
    json_to_py(py, &response)
}

#[pyfunction]
pub(crate) fn arrow_stream_to_batches(py: Python<'_>, obj: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let bound = obj.bind(py);
    let mut reader = ArrowArrayStreamReader::from_pyarrow_bound(&bound).map_err(|err| {
        PyValueError::new_err(format!(
            "Expected an object supporting __arrow_c_stream__: {err}"
        ))
    })?;
    let schema = reader.schema();
    let mut batches = Vec::new();
    while let Some(batch) = reader.next() {
        let batch = batch
            .map_err(|err| PyRuntimeError::new_err(format!("Arrow stream batch error: {err}")) )?;
        batches.push(batch);
    }
    let mut py_batches = Vec::with_capacity(batches.len());
    for batch in batches {
        py_batches.push(batch.to_pyarrow(py)?);
    }
    let py_batches = PyList::new(py, py_batches)?;
    let schema_py = schema.to_pyarrow(py)?;
    let reader = py
        .import("pyarrow")?
        .getattr("RecordBatchReader")?
        .call_method1("from_batches", (schema_py, py_batches))?;
    Ok(reader.into())
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(capture_plan_bundle_runtime, module)?)?;
    module.add_function(wrap_pyfunction!(build_plan_bundle_artifact_with_warnings, module)?)?;
    module.add_function(wrap_pyfunction!(arrow_stream_to_batches, module)?)?;
    Ok(())
}

pub(crate) fn register_internal_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_functions(module)
}
