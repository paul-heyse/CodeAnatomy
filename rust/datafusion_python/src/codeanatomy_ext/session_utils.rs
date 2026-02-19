//! Session/runtime utility bridge surface.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatchReader;
use arrow::ffi_stream::ArrowArrayStreamReader;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use datafusion::config::ConfigOptions;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::{FairSpillPool, MemoryLimit};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::optimizer::OptimizerConfig;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
#[cfg(feature = "substrait")]
use datafusion::prelude::DataFrame;
use datafusion_common::Result;
use datafusion_expr::lit;
use datafusion_expr::Expr;
use datafusion_ext::udf_expr as udf_expr_mod;
use deltalake::delta_datafusion::{
    DeltaLogicalCodec, DeltaPhysicalCodec, DeltaSessionConfig, DeltaTableFactory,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList, PyTuple};
use serde::Deserialize;
use tokio::runtime::Runtime;

use codeanatomy_engine::compiler::lineage::{
    extract_lineage as extract_lineage_native,
    lineage_from_substrait as lineage_from_substrait_native,
};
use codeanatomy_engine::compiler::plan_bundle;
use codeanatomy_engine::session::extraction::{
    build_extraction_session as build_extraction_session_native, ExtractionConfig,
};

use crate::context::{PyRuntimeEnvBuilder, PySessionContext};
use crate::dataframe::PyDataFrame;
use crate::delta_control_plane::{
    delta_provider_from_session_request as delta_provider_from_session_native,
    DeltaProviderFromSessionRequest,
};
use crate::delta_observability::add_action_payloads;
use crate::expr::PyExpr;
use crate::sql::logical::PyLogicalPlan;
use crate::utils::py_obj_to_scalar_value;

use super::helpers::{
    delta_gate_from_params, extract_session_ctx, json_to_py, parse_python_payload,
    runtime as build_runtime, scan_config_to_pydict, scan_overrides_from_params,
    snapshot_to_pydict, table_version_from_options,
};

#[derive(Clone)]
struct SessionContextContract {
    ctx: SessionContext,
}

fn session_context_contract(ctx: &Bound<'_, PyAny>) -> PyResult<SessionContextContract> {
    if let Ok(session) = ctx.extract::<Bound<'_, PySessionContext>>() {
        return Ok(SessionContextContract {
            ctx: session.borrow().ctx().clone(),
        });
    }
    if let Ok(inner) = ctx.getattr("ctx") {
        if let Ok(session) = inner.extract::<Bound<'_, PySessionContext>>() {
            return Ok(SessionContextContract {
                ctx: session.borrow().ctx().clone(),
            });
        }
    }
    Err(PyValueError::new_err(
        "ctx must be datafusion.SessionContext or expose a compatible .ctx attribute",
    ))
}

fn optional_session_context_contract(
    ctx: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<SessionContextContract>> {
    ctx.map(session_context_contract).transpose()
}

#[derive(Debug, Clone, Deserialize)]
struct ExtractionSessionPayload {
    parallelism: Option<usize>,
    memory_limit_bytes: Option<u64>,
    batch_size: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
struct DatasetProviderRequestPayload {
    table_name: String,
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    predicate: Option<String>,
    overwrite: Option<bool>,
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_force_view_types: Option<bool>,
    wrap_partition_values: Option<bool>,
    schema_ipc: Option<Vec<u8>>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
}

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
        let runtime = build_runtime()?;
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

#[pyfunction]
pub(crate) fn build_extraction_session(
    py: Python<'_>,
    config_payload: &Bound<'_, PyAny>,
) -> PyResult<PySessionContext> {
    let payload: ExtractionSessionPayload =
        parse_python_payload(py, config_payload, "extraction session config")?;
    let mut config = ExtractionConfig::default();
    if let Some(parallelism) = payload.parallelism {
        if parallelism == 0 {
            return Err(PyValueError::new_err(
                "extraction session config parallelism must be >= 1",
            ));
        }
        config.parallelism = parallelism;
    }
    if let Some(batch_size) = payload.batch_size {
        if batch_size == 0 {
            return Err(PyValueError::new_err(
                "extraction session config batch_size must be >= 1",
            ));
        }
        config.batch_size = batch_size;
    }
    if let Some(memory_limit_bytes) = payload.memory_limit_bytes {
        config.memory_limit_bytes = Some(usize::try_from(memory_limit_bytes).map_err(|_| {
            PyValueError::new_err(format!(
                "extraction session config memory_limit_bytes exceeds usize: {memory_limit_bytes}"
            ))
        })?);
    }
    let ctx = build_extraction_session_native(&config).map_err(|err| {
        PyRuntimeError::new_err(format!("Extraction session build failed: {err}"))
    })?;
    Ok(PySessionContext { ctx })
}

#[pyfunction]
pub(crate) fn register_dataset_provider(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
    request_payload: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let request: DatasetProviderRequestPayload =
        parse_python_payload(py, request_payload, "dataset provider registration")?;
    let gate = delta_gate_from_params(
        request.min_reader_version,
        request.min_writer_version,
        request.required_reader_features,
        request.required_writer_features,
    );
    let overrides = scan_overrides_from_params(
        request.file_column_name,
        request.enable_parquet_pushdown,
        request.schema_force_view_types,
        request.wrap_partition_values,
        request.schema_ipc,
    )?;
    let runtime = build_runtime()?;
    let table_version = table_version_from_options(request.version, request.timestamp.clone())?;
    let session_ctx = extract_session_ctx(ctx)?;
    let (provider, snapshot, scan_config, add_actions, predicate_error) = runtime
        .block_on(delta_provider_from_session_native(
            DeltaProviderFromSessionRequest {
                session_ctx: &session_ctx,
                table_uri: &request.table_uri,
                storage_options: request.storage_options,
                table_version,
                predicate: request.predicate.clone(),
                overrides,
                gate,
            },
        ))
        .map_err(|err| PyRuntimeError::new_err(format!("Dataset provider build failed: {err}")))?;

    let table_name = request.table_name;
    if request.overwrite.unwrap_or(true) {
        let _ = session_ctx.deregister_table(table_name.as_str());
    }
    session_ctx
        .register_table(table_name.as_str(), provider)
        .map_err(|err| {
            PyRuntimeError::new_err(format!(
                "Failed to register dataset provider for table {table_name:?}: {err}"
            ))
        })?;

    let payload = PyDict::new(py);
    payload.set_item("table_name", table_name)?;
    payload.set_item("registered", true)?;
    payload.set_item("snapshot", snapshot_to_pydict(py, &snapshot)?)?;
    payload.set_item("scan_config", scan_config_to_pydict(py, &scan_config)?)?;
    if let Some(add_actions) = add_actions {
        let add_payload = add_action_payloads(&add_actions);
        payload.set_item("add_actions", json_to_py(py, &add_payload)?)?;
    }
    if let Some(error) = predicate_error {
        payload.set_item("predicate_error", error)?;
    }
    Ok(payload.into())
}

#[pyfunction]
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
    let runtime = build_runtime()?;
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
    let runtime = build_runtime()?;
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
            .map_err(|err| PyRuntimeError::new_err(format!("Arrow stream batch error: {err}")))?;
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

#[pyfunction]
#[pyo3(signature = (name, *args, ctx=None))]
pub(crate) fn udf_expr(
    py: Python<'_>,
    name: String,
    args: &Bound<'_, PyTuple>,
    ctx: Option<&Bound<'_, PyAny>>,
) -> PyResult<PyExpr> {
    let mut expr_args: Vec<Expr> = Vec::with_capacity(args.len());
    for item in args.iter() {
        if let Ok(expr) = item.extract::<PyExpr>() {
            expr_args.push(expr.into());
            continue;
        }
        let scalar = py_obj_to_scalar_value(py, item.unbind())?;
        expr_args.push(lit(scalar));
    }
    let ctx_contract = optional_session_context_contract(ctx)?;
    let expr = if let Some(ctx_ref) = ctx_contract.as_ref() {
        let state = ctx_ref.ctx.state();
        let config_options = state.config_options();
        if let Some(registry) = state.function_registry() {
            udf_expr_mod::expr_from_registry_or_specs(
                registry,
                name.as_str(),
                expr_args,
                Some(config_options),
            )
        } else {
            udf_expr_mod::expr_from_name(name.as_str(), expr_args, Some(config_options))
        }
    } else {
        udf_expr_mod::expr_from_name(name.as_str(), expr_args, None)
    }
    .map_err(|err| PyValueError::new_err(format!("UDF expression failed: {err}")))?;
    Ok(expr.into())
}

#[derive(Debug)]
struct TracingMarkerRule;

impl PhysicalOptimizerRule for TracingMarkerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan)
    }

    fn name(&self) -> &str {
        "codeanatomy_tracing_marker"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[pyfunction]
pub(crate) fn install_tracing(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    let state_ref = extract_session_ctx(ctx)?.state_ref();
    let mut state = state_ref.write();
    let new_state = SessionStateBuilder::new_from_existing(state.clone())
        .with_physical_optimizer_rule(Arc::new(TracingMarkerRule))
        .build();
    *state = new_state;
    Ok(())
}

#[pyfunction]
pub(crate) fn table_logical_plan(ctx: &Bound<'_, PyAny>, table_name: String) -> PyResult<String> {
    let runtime = Runtime::new()
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}")))?;
    let df = runtime
        .block_on(extract_session_ctx(ctx)?.table(table_name.as_str()))
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to resolve table {table_name:?}: {err}"))
        })?;
    Ok(format!("{:?}", df.logical_plan()))
}

#[pyfunction]
pub(crate) fn table_dfschema_tree(ctx: &Bound<'_, PyAny>, table_name: String) -> PyResult<String> {
    let runtime = Runtime::new()
        .map_err(|err| PyRuntimeError::new_err(format!("Failed to create Tokio runtime: {err}")))?;
    let df = runtime
        .block_on(extract_session_ctx(ctx)?.table(table_name.as_str()))
        .map_err(|err| {
            PyRuntimeError::new_err(format!("Failed to resolve table {table_name:?}: {err}"))
        })?;
    Ok(df.schema().to_string())
}

#[pyfunction]
pub(crate) fn install_delta_table_factory(ctx: &Bound<'_, PyAny>, alias: String) -> PyResult<()> {
    let state_ref = extract_session_ctx(ctx)?.state_ref();
    let mut state = state_ref.write();
    let factories = state.table_factories_mut();
    factories.insert(alias, Arc::new(DeltaTableFactory {}));
    Ok(())
}

#[pyclass]
#[derive(Clone)]
pub(crate) struct DeltaRuntimeEnvOptions {
    #[pyo3(get, set)]
    max_spill_size: Option<usize>,
    #[pyo3(get, set)]
    max_temp_directory_size: Option<u64>,
}

#[pymethods]
impl DeltaRuntimeEnvOptions {
    #[new]
    fn new() -> Self {
        Self {
            max_spill_size: None,
            max_temp_directory_size: None,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub(crate) struct DeltaSessionRuntimePolicyOptions {
    #[pyo3(get, set)]
    memory_limit: Option<u64>,
    #[pyo3(get, set)]
    metadata_cache_limit: Option<u64>,
    #[pyo3(get, set)]
    temp_directory: Option<String>,
    #[pyo3(get, set)]
    max_temp_directory_size: Option<u64>,
}

#[pymethods]
impl DeltaSessionRuntimePolicyOptions {
    #[new]
    fn new() -> Self {
        Self {
            memory_limit: None,
            metadata_cache_limit: None,
            temp_directory: None,
            max_temp_directory_size: None,
        }
    }
}

fn usize_from_u64(value: u64, field: &str) -> PyResult<usize> {
    usize::try_from(value).map_err(|_| {
        PyValueError::new_err(format!("{field} exceeds platform usize capacity: {value}"))
    })
}

fn apply_delta_runtime_options(
    mut builder: RuntimeEnvBuilder,
    options: Option<PyRef<DeltaRuntimeEnvOptions>>,
) -> RuntimeEnvBuilder {
    if let Some(options) = options {
        if let Some(size) = options.max_spill_size {
            builder = builder.with_memory_pool(Arc::new(FairSpillPool::new(size)));
        }
        if let Some(size) = options.max_temp_directory_size {
            builder = builder.with_max_temp_directory_size(size);
        }
    }
    builder
}

fn apply_runtime_policy_options(
    mut builder: RuntimeEnvBuilder,
    options: Option<PyRef<DeltaSessionRuntimePolicyOptions>>,
) -> PyResult<RuntimeEnvBuilder> {
    if let Some(options) = options {
        if let Some(limit) = options.memory_limit {
            builder = builder.with_memory_limit(usize_from_u64(limit, "memory_limit")?, 1.0);
        }
        if let Some(limit) = options.metadata_cache_limit {
            builder =
                builder.with_metadata_cache_limit(usize_from_u64(limit, "metadata_cache_limit")?);
        }
        if let Some(path) = options.temp_directory.as_deref() {
            builder = builder.with_temp_file_path(path);
        }
        if let Some(limit) = options.max_temp_directory_size {
            builder = builder.with_max_temp_directory_size(limit);
        }
    }
    Ok(builder)
}

#[pyfunction]
#[pyo3(signature = (settings = None, runtime = None, delta_runtime = None, runtime_policy = None))]
pub(crate) fn delta_session_context(
    settings: Option<Vec<(String, String)>>,
    runtime: Option<PyRef<PyRuntimeEnvBuilder>>,
    delta_runtime: Option<PyRef<DeltaRuntimeEnvOptions>>,
    runtime_policy: Option<PyRef<DeltaSessionRuntimePolicyOptions>>,
) -> PyResult<PySessionContext> {
    let mut config: SessionConfig = DeltaSessionConfig::default().into();
    if let Some(entries) = settings {
        for (key, value) in entries {
            if key.starts_with("datafusion.runtime.") {
                continue;
            }
            config = config.set_str(&key, &value);
        }
    }
    let runtime_builder = runtime
        .map(|builder| builder.builder.clone())
        .unwrap_or_else(RuntimeEnvBuilder::new);
    let runtime_builder = apply_runtime_policy_options(runtime_builder, runtime_policy)?;
    let runtime_builder = apply_delta_runtime_options(runtime_builder, delta_runtime);
    let runtime_env = Arc::new(
        runtime_builder
            .build()
            .map_err(|err| PyRuntimeError::new_err(format!("RuntimeEnv build failed: {err}")))?,
    );
    let planner = deltalake::delta_datafusion::planner::DeltaPlanner::new();
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_runtime_env(runtime_env)
        .with_query_planner(planner)
        .build();
    Ok(PySessionContext {
        ctx: SessionContext::new_with_state(state),
    })
}

pub(crate) fn install_delta_plan_codecs_inner(ctx: &SessionContext) -> PyResult<()> {
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    config.set_extension(Arc::new(DeltaLogicalCodec {}));
    config.set_extension(Arc::new(DeltaPhysicalCodec {}));
    Ok(())
}

#[pyfunction]
pub(crate) fn install_delta_plan_codecs(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    install_delta_plan_codecs_inner(&extract_session_ctx(ctx)?)
}

fn saturating_i64_from_usize(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn memory_limit_payload(memory_limit: MemoryLimit) -> (&'static str, Option<i64>) {
    match memory_limit {
        MemoryLimit::Finite(value) => ("finite", Some(saturating_i64_from_usize(value))),
        MemoryLimit::Infinite => ("infinite", None),
        MemoryLimit::Unknown => ("unknown", None),
    }
}

#[pyfunction]
pub(crate) fn session_context_contract_probe(
    py: Python<'_>,
    ctx: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let runtime_env = Arc::clone(extract_session_ctx(ctx)?.state().runtime_env());
    let metadata_entries = runtime_env
        .cache_manager
        .get_file_metadata_cache()
        .list_entries();
    let metadata_cache_hits: i64 = metadata_entries
        .values()
        .map(|entry| saturating_i64_from_usize(entry.hits))
        .sum();
    let list_files_cache_entries = runtime_env
        .cache_manager
        .get_list_files_cache()
        .map(|cache| saturating_i64_from_usize(cache.len()));
    let statistics_cache_entries = runtime_env
        .cache_manager
        .get_file_statistic_cache()
        .map(|cache| saturating_i64_from_usize(cache.len()));
    let metadata_cache_limit =
        saturating_i64_from_usize(runtime_env.cache_manager.get_metadata_cache_limit());
    let metadata_cache_entries = saturating_i64_from_usize(metadata_entries.len());
    let memory_reserved = saturating_i64_from_usize(runtime_env.memory_pool.reserved());
    let (memory_limit_kind, memory_limit_bytes) =
        memory_limit_payload(runtime_env.memory_pool.memory_limit());

    let payload = serde_json::json!({
        "ok": true,
        "runtime": {
            "memory_reserved_bytes": memory_reserved,
            "memory_limit_kind": memory_limit_kind,
            "memory_limit_bytes": memory_limit_bytes,
            "metadata_cache_limit_bytes": metadata_cache_limit,
            "metadata_cache_entries": metadata_cache_entries,
            "metadata_cache_hits": metadata_cache_hits,
            "list_files_cache_entries": list_files_cache_entries,
            "statistics_cache_entries": statistics_cache_entries,
        }
    });
    json_to_py(py, &payload)
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(replay_substrait_plan, module)?)?;
    module.add_function(wrap_pyfunction!(lineage_from_substrait, module)?)?;
    module.add_function(wrap_pyfunction!(extract_lineage_json, module)?)?;
    module.add_function(wrap_pyfunction!(build_extraction_session, module)?)?;
    module.add_function(wrap_pyfunction!(register_dataset_provider, module)?)?;
    module.add_function(wrap_pyfunction!(capture_plan_bundle_runtime, module)?)?;
    module.add_function(wrap_pyfunction!(
        build_plan_bundle_artifact_with_warnings,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(session_context_contract_probe, module)?)?;
    module.add_function(wrap_pyfunction!(arrow_stream_to_batches, module)?)?;
    module.add_function(wrap_pyfunction!(udf_expr, module)?)?;
    module.add_function(wrap_pyfunction!(install_tracing, module)?)?;
    module.add_function(wrap_pyfunction!(table_logical_plan, module)?)?;
    module.add_function(wrap_pyfunction!(table_dfschema_tree, module)?)?;
    module.add_function(wrap_pyfunction!(install_delta_table_factory, module)?)?;
    module.add_function(wrap_pyfunction!(delta_session_context, module)?)?;
    module.add_function(wrap_pyfunction!(install_delta_plan_codecs, module)?)?;
    Ok(())
}

pub(crate) fn register_internal_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(replay_substrait_plan, module)?)?;
    module.add_function(wrap_pyfunction!(lineage_from_substrait, module)?)?;
    module.add_function(wrap_pyfunction!(extract_lineage_json, module)?)?;
    module.add_function(wrap_pyfunction!(build_extraction_session, module)?)?;
    module.add_function(wrap_pyfunction!(register_dataset_provider, module)?)?;
    module.add_function(wrap_pyfunction!(capture_plan_bundle_runtime, module)?)?;
    module.add_function(wrap_pyfunction!(
        build_plan_bundle_artifact_with_warnings,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(session_context_contract_probe, module)?)?;
    module.add_function(wrap_pyfunction!(arrow_stream_to_batches, module)?)?;
    module.add_function(wrap_pyfunction!(udf_expr, module)?)?;
    module.add_function(wrap_pyfunction!(install_delta_table_factory, module)?)?;
    module.add_function(wrap_pyfunction!(delta_session_context, module)?)?;
    module.add_function(wrap_pyfunction!(install_delta_plan_codecs, module)?)?;
    Ok(())
}

pub(crate) fn register_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<DeltaRuntimeEnvOptions>()?;
    module.add_class::<DeltaSessionRuntimePolicyOptions>()?;
    Ok(())
}
