//! Delta session/factory and codec bridge surface.

use std::sync::Arc;
use std::time::Duration;

use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use deltalake::delta_datafusion::{
    DeltaLogicalCodec, DeltaPhysicalCodec, DeltaSessionConfig, DeltaTableFactory,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tracing::instrument;

use crate::context::{PyRuntimeEnvBuilder, PySessionContext};

use super::helpers::extract_session_ctx;

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
    list_files_cache_limit: Option<u64>,
    #[pyo3(get, set)]
    list_files_cache_ttl_seconds: Option<u64>,
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
            list_files_cache_limit: None,
            list_files_cache_ttl_seconds: None,
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
        let mut cache_manager = CacheManagerConfig::default();
        let mut cache_manager_configured = false;
        if let Some(limit) = options.metadata_cache_limit {
            cache_manager =
                cache_manager.with_metadata_cache_limit(usize_from_u64(limit, "metadata_cache_limit")?);
            cache_manager_configured = true;
        }
        if let Some(limit) = options.list_files_cache_limit {
            cache_manager = cache_manager.with_list_files_cache_limit(usize_from_u64(
                limit,
                "list_files_cache_limit",
            )?);
            cache_manager_configured = true;
        }
        if let Some(ttl_seconds) = options.list_files_cache_ttl_seconds {
            cache_manager =
                cache_manager.with_list_files_cache_ttl(Some(Duration::from_secs(ttl_seconds)));
            cache_manager_configured = true;
        }
        if cache_manager_configured {
            builder = builder.with_cache_manager(cache_manager);
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
pub(crate) fn install_delta_table_factory(ctx: &Bound<'_, PyAny>, alias: String) -> PyResult<()> {
    let state_ref = extract_session_ctx(ctx)?.state_ref();
    let mut state = state_ref.write();
    let factories = state.table_factories_mut();
    factories.insert(alias, Arc::new(DeltaTableFactory {}));
    Ok(())
}

#[pyfunction]
#[instrument(skip(settings, runtime, delta_runtime, runtime_policy))]
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

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(install_delta_table_factory, module)?)?;
    module.add_function(wrap_pyfunction!(delta_session_context, module)?)?;
    module.add_function(wrap_pyfunction!(install_delta_plan_codecs, module)?)?;
    Ok(())
}

pub(crate) fn register_internal_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_functions(module)
}

pub(crate) fn register_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<DeltaRuntimeEnvOptions>()?;
    module.add_class::<DeltaSessionRuntimePolicyOptions>()?;
    Ok(())
}
