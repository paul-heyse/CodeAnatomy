//! Tracing integration for engine execution.
//!
//! This module is intentionally split into feature-gated integration parts so
//! the default build (without the `tracing` feature) remains zero-overhead.

use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion::execution::session_state::SessionState;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_common::Result;

use crate::spec::runtime::TracingConfig;

mod context;
pub use context::{ExecutionSpanInfo, TraceRuleContext};

#[cfg(feature = "tracing")]
mod bootstrap;
#[cfg(feature = "tracing")]
mod exec_instrumentation;
#[cfg(feature = "tracing")]
mod object_store;
#[cfg(feature = "tracing")]
mod preview;
#[cfg(feature = "tracing")]
mod rule_instrumentation;

#[cfg(feature = "tracing")]
pub fn init_otel_tracing(config: &TracingConfig) -> Result<()> {
    bootstrap::init_otel_tracing(config)
}

#[cfg(not(feature = "tracing"))]
pub fn init_otel_tracing(_config: &TracingConfig) -> Result<()> {
    Ok(())
}

#[cfg(feature = "tracing")]
pub fn flush_otel_tracing() -> Result<()> {
    bootstrap::flush_otel_tracing()
}

#[cfg(not(feature = "tracing"))]
pub fn flush_otel_tracing() -> Result<()> {
    Ok(())
}

#[cfg(feature = "tracing")]
pub fn instrument_session_state(
    state: SessionState,
    config: &TracingConfig,
    trace_ctx: &TraceRuleContext,
) -> SessionState {
    rule_instrumentation::instrument_session_state(state, config, trace_ctx)
}

#[cfg(not(feature = "tracing"))]
pub fn instrument_session_state(
    state: SessionState,
    _config: &TracingConfig,
    _trace_ctx: &TraceRuleContext,
) -> SessionState {
    state
}

#[cfg(feature = "tracing")]
pub fn append_execution_instrumentation_rule(
    physical_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    config: &TracingConfig,
    trace_ctx: &TraceRuleContext,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    exec_instrumentation::append_execution_instrumentation_rule(physical_rules, config, trace_ctx)
}

#[cfg(not(feature = "tracing"))]
pub fn append_execution_instrumentation_rule(
    physical_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    _config: &TracingConfig,
    _trace_ctx: &TraceRuleContext,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    physical_rules
}

#[cfg(feature = "tracing")]
pub fn register_instrumented_file_store(
    ctx: &SessionContext,
    config: &TracingConfig,
) -> Result<()> {
    object_store::register_instrumented_file_store(ctx, config)
}

#[cfg(not(feature = "tracing"))]
pub fn register_instrumented_file_store(
    _ctx: &SessionContext,
    _config: &TracingConfig,
) -> Result<()> {
    Ok(())
}

#[cfg(feature = "tracing")]
pub fn register_instrumented_stores_for_locations(
    ctx: &SessionContext,
    config: &TracingConfig,
    locations: &[String],
) -> Result<()> {
    object_store::register_instrumented_stores_for_locations(ctx, config, locations)
}

#[cfg(not(feature = "tracing"))]
pub fn register_instrumented_stores_for_locations(
    _ctx: &SessionContext,
    _config: &TracingConfig,
    _locations: &[String],
) -> Result<()> {
    Ok(())
}

#[cfg(feature = "tracing")]
pub fn execution_span(info: &ExecutionSpanInfo, config: &TracingConfig) -> tracing::Span {
    context::execution_span(info, config)
}

#[cfg(feature = "tracing")]
pub fn record_envelope_hash(span: &tracing::Span, envelope_hash: &[u8; 32]) {
    context::record_envelope_hash(span, envelope_hash);
}

#[cfg(feature = "tracing")]
pub fn record_warning_summary(
    span: &tracing::Span,
    warning_count_total: u64,
    warning_counts_by_code: &std::collections::BTreeMap<String, u64>,
) {
    context::record_warning_summary(span, warning_count_total, warning_counts_by_code);
}
