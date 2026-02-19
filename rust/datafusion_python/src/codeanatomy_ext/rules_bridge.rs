//! Planner/physical rule and runtime policy bridge surface.

use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Result;
use datafusion_ext::physical_rules::{
    ensure_physical_config, install_physical_rules as install_physical_rules_native,
};
use datafusion_ext::planner_rules::{
    ensure_policy_config, install_policy_rules as install_policy_rules_native,
};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use tracing::instrument;

use super::helpers::extract_session_ctx;

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
#[pyo3(signature = (ctx, allow_ddl = None, allow_dml = None, allow_statements = None))]
pub(crate) fn install_codeanatomy_policy_config(
    ctx: &Bound<'_, PyAny>,
    allow_ddl: Option<bool>,
    allow_dml: Option<bool>,
    allow_statements: Option<bool>,
) -> PyResult<()> {
    let state_ref = extract_session_ctx(ctx)?.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    let policy = ensure_policy_config(config.options_mut()).map_err(|err| {
        PyRuntimeError::new_err(format!("Planner policy configuration failed: {err}"))
    })?;
    if let Some(value) = allow_ddl {
        policy.allow_ddl = value;
    }
    if let Some(value) = allow_dml {
        policy.allow_dml = value;
    }
    if let Some(value) = allow_statements {
        policy.allow_statements = value;
    }
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (ctx, enabled = None))]
pub(crate) fn install_codeanatomy_physical_config(
    ctx: &Bound<'_, PyAny>,
    enabled: Option<bool>,
) -> PyResult<()> {
    let state_ref = extract_session_ctx(ctx)?.state_ref();
    let mut state = state_ref.write();
    let config = state.config_mut();
    let physical = ensure_physical_config(config.options_mut()).map_err(|err| {
        PyRuntimeError::new_err(format!("Physical policy configuration failed: {err}"))
    })?;
    if let Some(value) = enabled {
        physical.enabled = value;
    }
    Ok(())
}

#[pyfunction]
pub(crate) fn install_planner_rules(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    install_policy_rules_native(&extract_session_ctx(ctx)?)
        .map_err(|err| PyRuntimeError::new_err(format!("Planner rule install failed: {err}")))
}

#[pyfunction]
pub(crate) fn install_physical_rules(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    install_physical_rules_native(&extract_session_ctx(ctx)?)
        .map_err(|err| PyRuntimeError::new_err(format!("Physical rule install failed: {err}")))
}

#[pyfunction]
#[instrument(skip(ctx))]
pub(crate) fn install_expr_planners(
    ctx: &Bound<'_, PyAny>,
    planner_names: Vec<String>,
) -> PyResult<()> {
    let names: Vec<&str> = planner_names.iter().map(String::as_str).collect();
    datafusion_ext::install_expr_planners_native(&extract_session_ctx(ctx)?, &names)
        .map_err(|err| PyRuntimeError::new_err(format!("ExprPlanner install failed: {err}")))
}

#[pyfunction]
#[instrument(level = "info", skip_all)]
pub(crate) fn install_relation_planner(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    datafusion_ext::install_relation_planner_native(&extract_session_ctx(ctx)?)
        .map_err(|err| PyRuntimeError::new_err(format!("RelationPlanner install failed: {err}")))
}

#[pyfunction]
#[instrument(level = "info", skip_all)]
pub(crate) fn install_type_planner(ctx: &Bound<'_, PyAny>) -> PyResult<()> {
    datafusion_ext::install_type_planner_native(&extract_session_ctx(ctx)?)
        .map_err(|err| PyRuntimeError::new_err(format!("TypePlanner install failed: {err}")))
}

pub(crate) fn register_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(install_tracing, module)?)?;
    module.add_function(wrap_pyfunction!(install_codeanatomy_policy_config, module)?)?;
    module.add_function(wrap_pyfunction!(
        install_codeanatomy_physical_config,
        module
    )?)?;
    module.add_function(wrap_pyfunction!(install_planner_rules, module)?)?;
    module.add_function(wrap_pyfunction!(install_physical_rules, module)?)?;
    module.add_function(wrap_pyfunction!(install_expr_planners, module)?)?;
    module.add_function(wrap_pyfunction!(install_relation_planner, module)?)?;
    module.add_function(wrap_pyfunction!(install_type_planner, module)?)?;
    Ok(())
}

pub(crate) fn register_internal_functions(module: &Bound<'_, PyModule>) -> PyResult<()> {
    register_functions(module)
}
