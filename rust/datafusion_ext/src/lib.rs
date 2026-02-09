//! DataFusion extension for native function registration.

pub mod async_runtime;
pub mod compat;
pub mod config_macros;
pub mod delta_control_plane;
pub mod delta_maintenance;
pub mod delta_mutations;
pub mod delta_observability;
pub mod delta_protocol;
pub mod error_conversion;
pub mod errors;
pub mod expr_planner;
pub mod function_factory;
pub mod function_rewrite;
pub mod generated;
pub mod macros;
pub mod physical_rules;
pub mod planner_rules;
pub mod registry_snapshot;
pub mod udaf_builtin;
pub mod udf;
#[cfg(feature = "async-udf")]
pub mod udf_async;
pub mod udf_config;
pub mod udf_docs;
pub mod udf_expr;
pub mod udf_registry;
pub mod udtf_builtin;
pub mod udtf_sources;
pub mod udwf_builtin;

pub use generated::delta_types::{DeltaAppTransaction, DeltaCommitOptions, DeltaFeatureGate};

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::registry::FunctionRegistry;

pub fn install_sql_macro_factory_native(ctx: &SessionContext) -> Result<()> {
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    let new_state = function_factory::with_sql_macro_factory(&state);
    *state = new_state;
    Ok(())
}

pub fn install_expr_planners_native(ctx: &SessionContext, planner_names: &[&str]) -> Result<()> {
    if planner_names.is_empty() {
        return Err(DataFusionError::Plan(
            "ExprPlanner installation requires at least one planner name.".into(),
        ));
    }
    let mut unknown: Vec<String> = Vec::new();
    let mut install_domain = false;
    for name in planner_names {
        match *name {
            "codeanatomy_domain" => {
                install_domain = true;
            }
            _ => unknown.push((*name).to_string()),
        }
    }
    if !unknown.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Unsupported ExprPlanner names: {}",
            unknown.join(", ")
        )));
    }
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    state.register_expr_planner(Arc::new(
        datafusion_functions_nested::planner::NestedFunctionPlanner,
    ))?;
    state.register_expr_planner(Arc::new(
        datafusion_functions_nested::planner::FieldAccessPlanner,
    ))?;
    if install_domain {
        state.register_expr_planner(Arc::new(expr_planner::CodeAnatomyDomainPlanner::default()))?;
        state.register_function_rewrite(Arc::new(
            function_rewrite::CodeAnatomyOperatorRewrite::default(),
        ))?;
    }
    Ok(())
}

// ---- Planning-Surface Builder Helpers ----
//
// These functions expose planner/rewrite components for use in
// PlanningSurfaceSpec construction. They allow codeanatomy_engine
// to assemble a typed planning surface without needing direct
// dependencies on datafusion_functions_nested.

use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::planner::ExprPlanner;

/// Return the domain expression planners for builder-path registration.
///
/// Includes NestedFunctionPlanner, FieldAccessPlanner, and the
/// CodeAnatomy domain planner. These are the same planners installed
/// by `install_expr_planners_native` with `codeanatomy_domain`.
pub fn domain_expr_planners() -> Vec<Arc<dyn ExprPlanner>> {
    vec![
        Arc::new(datafusion_functions_nested::planner::NestedFunctionPlanner),
        Arc::new(datafusion_functions_nested::planner::FieldAccessPlanner),
        Arc::new(expr_planner::CodeAnatomyDomainPlanner::default()),
    ]
}

/// Return the domain function rewrites for builder-path registration.
///
/// These are the same rewrites installed by `install_expr_planners_native`
/// with `codeanatomy_domain`.
pub fn domain_function_rewrites() -> Vec<Arc<dyn FunctionRewrite + Send + Sync>> {
    vec![Arc::new(
        function_rewrite::CodeAnatomyOperatorRewrite::default(),
    )]
}
