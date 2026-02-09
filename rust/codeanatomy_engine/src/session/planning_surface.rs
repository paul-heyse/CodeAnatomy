//! Unified planning-surface builder for deterministic session construction.
//!
//! `PlanningSurfaceSpec` captures all planning-time registrations as a single
//! typed object. Both profile and non-profile session-state builders route
//! through this module so planning behavior is explicit, testable, and
//! deterministic.
//!
//! Post-build mutation is isolated to `install_rewrites()` — the only operation
//! that cannot be expressed through `SessionStateBuilder` in DataFusion 51.

use std::sync::Arc;

use datafusion::catalog::TableProviderFactory;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::context::{FunctionFactory, QueryPlanner, SessionContext};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_common::config::TableOptions;
use datafusion_common::Result;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::registry::FunctionRegistry;

/// Typed specification of everything that configures the planning surface
/// of a DataFusion session.
///
/// All fields default to empty/disabled so callers only populate what they
/// need. This replaces ad-hoc state writes and duplicated builder logic
/// across `build_session_state*` builder paths.
#[derive(Clone, Default)]
pub struct PlanningSurfaceSpec {
    /// Whether to enable DataFusion's default features (catalog, file formats).
    pub enable_default_features: bool,
    /// File format factories to register with the session state builder.
    pub file_formats: Vec<Arc<dyn FileFormatFactory>>,
    /// Table options controlling format-level behavior (pushdown, page index).
    pub table_options: Option<TableOptions>,
    /// Named table provider factories (e.g. "delta" -> DeltaTableFactory).
    pub table_factories: Vec<(String, Arc<dyn TableProviderFactory>)>,
    /// Expression planners for domain-specific operator resolution.
    pub expr_planners: Vec<Arc<dyn ExprPlanner>>,
    /// Optional SQL macro function factory for `CREATE FUNCTION` support.
    pub function_factory: Option<Arc<dyn FunctionFactory>>,
    /// Optional custom query planner (e.g. DeltaPlanner).
    pub query_planner: Option<Arc<dyn QueryPlanner + Send + Sync>>,
    /// Whether Delta extension codecs are installed for plan serialization.
    pub delta_codec_enabled: bool,
    /// Function rewrites that must be installed post-build (no builder API).
    pub function_rewrites: Vec<Arc<dyn FunctionRewrite + Send + Sync>>,
}

/// Apply a `PlanningSurfaceSpec` to a `SessionStateBuilder`.
///
/// This is the canonical path for configuring planning-time registrations.
/// All fields that have builder-native APIs are applied here. Function
/// rewrites are NOT applied here — use `install_rewrites()` after the
/// `SessionContext` is created.
pub fn apply_to_builder(
    mut builder: SessionStateBuilder,
    spec: &PlanningSurfaceSpec,
) -> SessionStateBuilder {
    if spec.enable_default_features {
        builder = builder.with_default_features();
    }
    if !spec.file_formats.is_empty() {
        builder = builder.with_file_formats(spec.file_formats.clone());
    }
    if let Some(options) = spec.table_options.clone() {
        builder = builder.with_table_options(options);
    }
    if !spec.expr_planners.is_empty() {
        builder = builder.with_expr_planners(spec.expr_planners.clone());
    }
    if let Some(factory) = spec.function_factory.clone() {
        builder = builder.with_function_factory(Some(factory));
    }
    if let Some(planner) = spec.query_planner.clone() {
        builder = builder.with_query_planner(planner);
    }
    for (name, factory) in &spec.table_factories {
        builder = builder.with_table_factory(name.clone(), Arc::clone(factory));
    }
    builder
}

/// Install function rewrites on an already-constructed `SessionContext`.
///
/// This is the only post-build mutation allowed. DataFusion 51 does not
/// expose a builder API for function rewrites, so we register them through
/// the `FunctionRegistry` trait on `SessionState` after context creation.
pub fn install_rewrites(
    ctx: &SessionContext,
    rewrites: &[Arc<dyn FunctionRewrite + Send + Sync>],
) -> Result<()> {
    if rewrites.is_empty() {
        return Ok(());
    }
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();
    for rewrite in rewrites {
        state.register_function_rewrite(Arc::clone(rewrite))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_spec_is_empty() {
        let spec = PlanningSurfaceSpec::default();
        assert!(!spec.enable_default_features);
        assert!(spec.file_formats.is_empty());
        assert!(spec.table_options.is_none());
        assert!(spec.table_factories.is_empty());
        assert!(spec.expr_planners.is_empty());
        assert!(spec.function_factory.is_none());
        assert!(spec.query_planner.is_none());
        assert!(!spec.delta_codec_enabled);
        assert!(spec.function_rewrites.is_empty());
    }
}
