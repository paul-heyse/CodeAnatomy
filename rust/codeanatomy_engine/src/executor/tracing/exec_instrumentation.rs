//! Physical execution instrumentation rule wiring.

use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion_tracing::{instrument_with_info_spans, InstrumentationOptions};
use tracing::field;

use crate::spec::runtime::TracingConfig;

use super::context::TraceRuleContext;
use super::preview::build_preview_formatter;

/// Append DataFusion execution instrumentation rule as the final physical rule.
///
/// Ordering matters: this must be the last physical optimizer rule so all
/// upstream optimizer rewrites run against unwrapped execution nodes.
pub fn append_execution_instrumentation_rule(
    mut physical_rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    config: &TracingConfig,
    trace_ctx: &TraceRuleContext,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    if !config.enabled {
        return physical_rules;
    }

    let rule_mode = format!("{:?}", config.rule_mode);
    let mut builder = InstrumentationOptions::builder()
        .record_metrics(config.record_metrics)
        .preview_limit(config.preview_limit)
        .add_custom_field("trace.spec_hash", trace_ctx.spec_hash.clone())
        .add_custom_field("trace.rulepack", trace_ctx.rulepack_fingerprint.clone())
        .add_custom_field("trace.profile", trace_ctx.profile_name.clone())
        .add_custom_field("trace.rule_mode", rule_mode)
        .add_custom_field(
            "trace.custom_fields_json",
            trace_ctx.custom_fields_json.clone(),
        );

    if config.preview_limit > 0 {
        builder = builder.preview_fn(build_preview_formatter(config));
    }

    let options = builder.build();
    let instrumentation_rule = instrument_with_info_spans!(
        options: options,
        trace.spec_hash = field::Empty,
        trace.rulepack = field::Empty,
        trace.profile = field::Empty,
        trace.rule_mode = field::Empty,
        trace.custom_fields_json = field::Empty,
    );
    physical_rules.push(instrumentation_rule);
    physical_rules
}
