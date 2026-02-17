//! Analyzer/logical/physical phase instrumentation wiring.
//!
//! DataFusion 51 does not expose native rule-phase instrumentation macros.
//! This module ports the same sentinel + wrapper architecture so we can keep
//! deterministic `Disabled` / `PhaseOnly` / `Full` semantics.

use std::cell::RefCell;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use tracing::Span;

use crate::spec::runtime::{RuleTraceMode, TracingConfig};

use super::context::TraceRuleContext;

mod sentinel_names {
    pub const ANALYZER: &str = "__trace_analyzer_phase";
    pub const OPTIMIZER: &str = "__trace_optimizer_phase";
    pub const PHYSICAL_OPTIMIZER: &str = "__trace_physical_optimizer_phase";
}

mod phase_names {
    pub const ANALYZE_LOGICAL_PLAN: &str = "analyze_logical_plan";
    pub const OPTIMIZE_LOGICAL_PLAN: &str = "optimize_logical_plan";
    pub const OPTIMIZE_PHYSICAL_PLAN: &str = "optimize_physical_plan";
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum InstrumentationLevel {
    #[default]
    Disabled,
    PhaseOnly,
    Full,
}

impl InstrumentationLevel {
    fn phase_span_enabled(self) -> bool {
        matches!(self, Self::PhaseOnly | Self::Full)
    }

    fn rule_spans_enabled(self) -> bool {
        matches!(self, Self::Full)
    }
}

#[derive(Clone, Debug)]
struct RuleInstrumentationOptions {
    plan_diff: bool,
    analyzer: InstrumentationLevel,
    optimizer: InstrumentationLevel,
    physical_optimizer: InstrumentationLevel,
}

impl RuleInstrumentationOptions {
    fn full() -> Self {
        Self {
            plan_diff: false,
            analyzer: InstrumentationLevel::Full,
            optimizer: InstrumentationLevel::Full,
            physical_optimizer: InstrumentationLevel::Full,
        }
    }

    fn phase_only() -> Self {
        Self {
            plan_diff: false,
            analyzer: InstrumentationLevel::PhaseOnly,
            optimizer: InstrumentationLevel::PhaseOnly,
            physical_optimizer: InstrumentationLevel::PhaseOnly,
        }
    }

    fn with_plan_diff(mut self) -> Self {
        self.plan_diff = true;
        self
    }
}

trait FormatPlan {
    fn format_for_diff(&self) -> String;
}

impl FormatPlan for LogicalPlan {
    fn format_for_diff(&self) -> String {
        self.display_indent_schema().to_string()
    }
}

impl FormatPlan for Arc<dyn ExecutionPlan> {
    fn format_for_diff(&self) -> String {
        displayable(self.as_ref()).indent(true).to_string()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlanningPhase {
    Analyzer,
    Optimizer,
    PhysicalOptimizer,
}

struct PlanningContext {
    phase: PlanningPhase,
    _entered: tracing::span::EnteredSpan,
    plan_before: Option<String>,
    effective_rules: Vec<String>,
}

thread_local! {
    static PLANNING_CONTEXT: RefCell<Option<PlanningContext>> = const { RefCell::new(None) };
}

fn record_modified_rule_in_context(rule_name: &str) {
    PLANNING_CONTEXT.with(|cell| {
        if let Some(ref mut ctx) = *cell.borrow_mut() {
            ctx.effective_rules.push(rule_name.to_string());
        }
    });
}

fn close_phase_span<P: FormatPlan>(ctx: PlanningContext, plan_after: &P) {
    let current = Span::current();
    if current.is_disabled() {
        return;
    }
    if !ctx.effective_rules.is_empty() {
        let effective_rules = ctx.effective_rules.join(", ");
        current.record("datafusion.effective_rules", effective_rules.as_str());
    }
    if let Some(before) = &ctx.plan_before {
        let after = plan_after.format_for_diff();
        if before != &after {
            let diff = generate_plan_diff(before, &after);
            current.record("datafusion.plan_diff", diff.as_str());
        }
    }
}

fn generate_plan_diff(before: &str, after: &str) -> String {
    let mut diff = String::new();
    for line in before.lines() {
        diff.push('-');
        diff.push_str(line);
        diff.push('\n');
    }
    for line in after.lines() {
        diff.push('+');
        diff.push_str(line);
        diff.push('\n');
    }
    diff
}

fn detect_and_record_modification(
    before_str: &str,
    after_str: &str,
    span: &Span,
    record_diff: bool,
    rule_name: &str,
) {
    if before_str == after_str {
        return;
    }
    if record_diff {
        let diff = generate_plan_diff(before_str, after_str);
        span.record("datafusion.plan_diff", diff.as_str());
    }
    let modified_name = format!("{rule_name} (modified)");
    span.record("otel.name", modified_name.as_str());
    record_modified_rule_in_context(rule_name);
}

type RuleSpanCreateFn = dyn Fn(&str) -> Span + Send + Sync;
type PhaseSpanCreateFn = dyn Fn(&str) -> Span + Send + Sync;

#[derive(Clone)]
struct TraceSpanFields {
    spec_hash: String,
    rulepack: String,
    profile: String,
    custom_fields_json: String,
}

impl From<&TraceRuleContext> for TraceSpanFields {
    fn from(value: &TraceRuleContext) -> Self {
        Self {
            spec_hash: value.spec_hash.clone(),
            rulepack: value.rulepack_fingerprint.clone(),
            profile: value.profile_name.clone(),
            custom_fields_json: value.custom_fields_json.clone(),
        }
    }
}

fn record_trace_fields(span: &Span, trace_fields: &TraceSpanFields) {
    span.record("trace.spec_hash", trace_fields.spec_hash.as_str());
    span.record("trace.rulepack", trace_fields.rulepack.as_str());
    span.record("trace.profile", trace_fields.profile.as_str());
    span.record(
        "trace.custom_fields_json",
        trace_fields.custom_fields_json.as_str(),
    );
}

fn make_rule_span(rule_name: &str, trace_fields: &TraceSpanFields) -> Span {
    let span = tracing::info_span!(
        "datafusion.rule",
        otel.name = tracing::field::Empty,
        datafusion.rule = tracing::field::Empty,
        datafusion.plan_diff = tracing::field::Empty,
        trace.spec_hash = tracing::field::Empty,
        trace.rulepack = tracing::field::Empty,
        trace.profile = tracing::field::Empty,
        trace.custom_fields_json = tracing::field::Empty,
    );
    span.record("otel.name", rule_name);
    span.record("datafusion.rule", rule_name);
    record_trace_fields(&span, trace_fields);
    span
}

fn make_phase_span(phase_name: &str, trace_fields: &TraceSpanFields) -> Span {
    let span = tracing::info_span!(
        "datafusion.phase",
        otel.name = tracing::field::Empty,
        datafusion.phase = tracing::field::Empty,
        datafusion.plan_diff = tracing::field::Empty,
        datafusion.effective_rules = tracing::field::Empty,
        trace.spec_hash = tracing::field::Empty,
        trace.rulepack = tracing::field::Empty,
        trace.profile = tracing::field::Empty,
        trace.custom_fields_json = tracing::field::Empty,
    );
    span.record("otel.name", phase_name);
    span.record("datafusion.phase", phase_name);
    record_trace_fields(&span, trace_fields);
    span
}

fn make_span_creators(
    trace_ctx: &TraceRuleContext,
) -> (Arc<RuleSpanCreateFn>, Arc<PhaseSpanCreateFn>) {
    let trace_fields = TraceSpanFields::from(trace_ctx);
    let rule_fields = trace_fields.clone();
    let phase_fields = trace_fields;

    let rule_span_create_fn =
        Arc::new(move |rule_name: &str| make_rule_span(rule_name, &rule_fields))
            as Arc<RuleSpanCreateFn>;
    let phase_span_create_fn =
        Arc::new(move |phase_name: &str| make_phase_span(phase_name, &phase_fields))
            as Arc<PhaseSpanCreateFn>;
    (rule_span_create_fn, phase_span_create_fn)
}

fn options_from_tracing_config(config: &TracingConfig) -> Option<RuleInstrumentationOptions> {
    if !config.enabled || config.rule_mode == RuleTraceMode::Disabled {
        return None;
    }
    let mut options = match config.rule_mode {
        RuleTraceMode::Disabled => return None,
        RuleTraceMode::PhaseOnly => RuleInstrumentationOptions::phase_only(),
        RuleTraceMode::Full => RuleInstrumentationOptions::full(),
    };
    if config.plan_diff {
        options = options.with_plan_diff();
    }
    Some(options)
}

struct AnalyzerPhaseSentinel {
    phase_span_create_fn: Arc<PhaseSpanCreateFn>,
    plan_diff: bool,
}

impl Debug for AnalyzerPhaseSentinel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnalyzerPhaseSentinel").finish()
    }
}

impl AnalyzerRule for AnalyzerPhaseSentinel {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        PLANNING_CONTEXT.with(|cell| {
            let mut guard = cell.borrow_mut();
            let should_open = guard
                .as_ref()
                .map(|ctx| ctx.phase != PlanningPhase::Analyzer)
                .unwrap_or(true);
            if should_open {
                let span = (self.phase_span_create_fn)(phase_names::ANALYZE_LOGICAL_PLAN);
                let plan_before = if self.plan_diff && !span.is_disabled() {
                    Some(plan.format_for_diff())
                } else {
                    None
                };
                *guard = Some(PlanningContext {
                    phase: PlanningPhase::Analyzer,
                    _entered: span.entered(),
                    plan_before,
                    effective_rules: Vec::new(),
                });
            } else if let Some(ctx) = guard.take() {
                close_phase_span(ctx, &plan);
            }
        });
        Ok(plan)
    }

    fn name(&self) -> &str {
        sentinel_names::ANALYZER
    }
}

struct OptimizerPhaseSentinel {
    phase_span_create_fn: Arc<PhaseSpanCreateFn>,
    plan_diff: bool,
}

impl Debug for OptimizerPhaseSentinel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptimizerPhaseSentinel").finish()
    }
}

impl OptimizerRule for OptimizerPhaseSentinel {
    fn name(&self) -> &str {
        sentinel_names::OPTIMIZER
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    #[allow(deprecated)]
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        PLANNING_CONTEXT.with(|cell| {
            let mut guard = cell.borrow_mut();
            let should_open = guard
                .as_ref()
                .map(|ctx| ctx.phase != PlanningPhase::Optimizer)
                .unwrap_or(true);
            if should_open {
                let span = (self.phase_span_create_fn)(phase_names::OPTIMIZE_LOGICAL_PLAN);
                let plan_before = if self.plan_diff && !span.is_disabled() {
                    Some(plan.format_for_diff())
                } else {
                    None
                };
                *guard = Some(PlanningContext {
                    phase: PlanningPhase::Optimizer,
                    _entered: span.entered(),
                    plan_before,
                    effective_rules: Vec::new(),
                });
            } else if let Some(ctx) = guard.take() {
                close_phase_span(ctx, &plan);
            }
        });
        Ok(Transformed::no(plan))
    }
}

struct PhysicalOptimizerPhaseSentinel {
    phase_span_create_fn: Arc<PhaseSpanCreateFn>,
    plan_diff: bool,
}

impl Debug for PhysicalOptimizerPhaseSentinel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalOptimizerPhaseSentinel").finish()
    }
}

impl PhysicalOptimizerRule for PhysicalOptimizerPhaseSentinel {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        PLANNING_CONTEXT.with(|cell| {
            let mut guard = cell.borrow_mut();
            let should_open = guard
                .as_ref()
                .map(|ctx| ctx.phase != PlanningPhase::PhysicalOptimizer)
                .unwrap_or(true);
            if should_open {
                let span = (self.phase_span_create_fn)(phase_names::OPTIMIZE_PHYSICAL_PLAN);
                let plan_before = if self.plan_diff && !span.is_disabled() {
                    Some(plan.format_for_diff())
                } else {
                    None
                };
                *guard = Some(PlanningContext {
                    phase: PlanningPhase::PhysicalOptimizer,
                    _entered: span.entered(),
                    plan_before,
                    effective_rules: Vec::new(),
                });
            } else if let Some(ctx) = guard.take() {
                close_phase_span(ctx, &plan);
            }
        });
        Ok(plan)
    }

    fn name(&self) -> &str {
        sentinel_names::PHYSICAL_OPTIMIZER
    }

    fn schema_check(&self) -> bool {
        true
    }
}

struct SingleSpanTreeTraverser<'a> {
    apply_order: ApplyOrder,
    rule: &'a dyn OptimizerRule,
    config: &'a dyn OptimizerConfig,
}

impl<'a> SingleSpanTreeTraverser<'a> {
    fn new(
        apply_order: ApplyOrder,
        rule: &'a dyn OptimizerRule,
        config: &'a dyn OptimizerConfig,
    ) -> Self {
        Self {
            apply_order,
            rule,
            config,
        }
    }
}

impl TreeNodeRewriter for SingleSpanTreeTraverser<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::TopDown {
            self.rule.rewrite(node, self.config)
        } else {
            Ok(Transformed::no(node))
        }
    }

    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::BottomUp {
            self.rule.rewrite(node, self.config)
        } else {
            Ok(Transformed::no(node))
        }
    }
}

struct InstrumentedAnalyzerRule {
    inner: Arc<dyn AnalyzerRule + Send + Sync>,
    options: RuleInstrumentationOptions,
    span_create_fn: Arc<RuleSpanCreateFn>,
}

impl Debug for InstrumentedAnalyzerRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedAnalyzerRule")
            .field("inner", &self.inner)
            .field("options", &self.options)
            .finish()
    }
}

impl InstrumentedAnalyzerRule {
    fn new(
        inner: Arc<dyn AnalyzerRule + Send + Sync>,
        options: RuleInstrumentationOptions,
        span_create_fn: Arc<RuleSpanCreateFn>,
    ) -> Self {
        Self {
            inner,
            options,
            span_create_fn,
        }
    }
}

impl AnalyzerRule for InstrumentedAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let span = (self.span_create_fn)(self.name());
        let _enter = span.enter();
        let before_plan = if !span.is_disabled() {
            Some(plan.clone())
        } else {
            None
        };
        let result = self.inner.analyze(plan, config);
        if !span.is_disabled() {
            match &result {
                Ok(after_plan) => {
                    if let Some(before_plan) = before_plan {
                        if &before_plan != after_plan {
                            let before_str = before_plan.format_for_diff();
                            let after_str = after_plan.format_for_diff();
                            detect_and_record_modification(
                                &before_str,
                                &after_str,
                                &span,
                                self.options.plan_diff,
                                self.name(),
                            );
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "AnalyzerRule failed");
                }
            }
        }
        result
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

struct InstrumentedOptimizerRule {
    inner: Arc<dyn OptimizerRule + Send + Sync>,
    options: RuleInstrumentationOptions,
    span_create_fn: Arc<RuleSpanCreateFn>,
}

impl Debug for InstrumentedOptimizerRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedOptimizerRule")
            .field("inner", &self.inner)
            .field("options", &self.options)
            .finish()
    }
}

impl InstrumentedOptimizerRule {
    fn new(
        inner: Arc<dyn OptimizerRule + Send + Sync>,
        options: RuleInstrumentationOptions,
        span_create_fn: Arc<RuleSpanCreateFn>,
    ) -> Self {
        Self {
            inner,
            options,
            span_create_fn,
        }
    }

    fn apply_inner(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        match self.inner.apply_order() {
            Some(apply_order) => plan.rewrite_with_subqueries(&mut SingleSpanTreeTraverser::new(
                apply_order,
                self.inner.as_ref(),
                config,
            )),
            None => self.inner.rewrite(plan, config),
        }
    }
}

impl OptimizerRule for InstrumentedOptimizerRule {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    #[allow(deprecated)]
    fn supports_rewrite(&self) -> bool {
        self.inner.supports_rewrite()
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let span = (self.span_create_fn)(self.name());
        let _enter = span.enter();
        let before_plan = if !span.is_disabled() {
            Some(plan.clone())
        } else {
            None
        };

        let result = self.apply_inner(plan, config);
        if !span.is_disabled() {
            match &result {
                Ok(transformed) => {
                    if transformed.transformed {
                        if let Some(before_plan) = before_plan {
                            let before_str = before_plan.format_for_diff();
                            let after_str = transformed.data.format_for_diff();
                            detect_and_record_modification(
                                &before_str,
                                &after_str,
                                &span,
                                self.options.plan_diff,
                                self.name(),
                            );
                        }
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "OptimizerRule failed");
                }
            }
        }
        result
    }
}

struct InstrumentedPhysicalOptimizerRule {
    inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    options: RuleInstrumentationOptions,
    span_create_fn: Arc<RuleSpanCreateFn>,
}

impl Debug for InstrumentedPhysicalOptimizerRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedPhysicalOptimizerRule")
            .field("inner", &self.inner)
            .field("options", &self.options)
            .finish()
    }
}

impl InstrumentedPhysicalOptimizerRule {
    fn new(
        inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
        options: RuleInstrumentationOptions,
        span_create_fn: Arc<RuleSpanCreateFn>,
    ) -> Self {
        Self {
            inner,
            options,
            span_create_fn,
        }
    }
}

impl PhysicalOptimizerRule for InstrumentedPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let span = (self.span_create_fn)(self.name());
        let _enter = span.enter();
        let before_plan = if !span.is_disabled() {
            Some(Arc::clone(&plan))
        } else {
            None
        };
        let result = self.inner.optimize(plan, config);
        if let Some(before_plan) = before_plan {
            match &result {
                Ok(after_plan) => {
                    if !Arc::ptr_eq(&before_plan, after_plan) {
                        let before_str = before_plan.format_for_diff();
                        let after_str = after_plan.format_for_diff();
                        detect_and_record_modification(
                            &before_str,
                            &after_str,
                            &span,
                            self.options.plan_diff,
                            self.name(),
                        );
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "PhysicalOptimizerRule failed");
                }
            }
        }
        result
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema_check(&self) -> bool {
        self.inner.schema_check()
    }
}

macro_rules! instrument_phase_rules {
    (
        $fn_name:ident,
        $rule_trait:ty,
        $level_field:ident,
        $sentinel_ty:ty,
        $wrapper_ty:ty
    ) => {
        fn $fn_name(
            rules: Vec<Arc<$rule_trait>>,
            options: &RuleInstrumentationOptions,
            span_create_fn: &Arc<RuleSpanCreateFn>,
            phase_span_create_fn: &Arc<PhaseSpanCreateFn>,
        ) -> Vec<Arc<$rule_trait>> {
            let level = options.$level_field;
            if !level.phase_span_enabled() || rules.is_empty() {
                return rules;
            }

            let mut result = Vec::with_capacity(rules.len() + 2);
            result.push(Arc::new(<$sentinel_ty> {
                phase_span_create_fn: Arc::clone(phase_span_create_fn),
                plan_diff: options.plan_diff,
            }) as Arc<$rule_trait>);
            for rule in rules {
                if level.rule_spans_enabled() {
                    result.push(Arc::new(<$wrapper_ty>::new(
                        rule,
                        options.clone(),
                        Arc::clone(span_create_fn),
                    )) as Arc<$rule_trait>);
                } else {
                    result.push(rule);
                }
            }
            result.push(Arc::new(<$sentinel_ty> {
                phase_span_create_fn: Arc::clone(phase_span_create_fn),
                plan_diff: options.plan_diff,
            }) as Arc<$rule_trait>);
            result
        }
    };
}

instrument_phase_rules!(
    instrument_analyzer_rules,
    dyn AnalyzerRule + Send + Sync,
    analyzer,
    AnalyzerPhaseSentinel,
    InstrumentedAnalyzerRule
);

instrument_phase_rules!(
    instrument_optimizer_rules,
    dyn OptimizerRule + Send + Sync,
    optimizer,
    OptimizerPhaseSentinel,
    InstrumentedOptimizerRule
);

instrument_phase_rules!(
    instrument_physical_optimizer_rules,
    dyn PhysicalOptimizerRule + Send + Sync,
    physical_optimizer,
    PhysicalOptimizerPhaseSentinel,
    InstrumentedPhysicalOptimizerRule
);

pub fn instrument_session_state(
    state: SessionState,
    config: &TracingConfig,
    trace_ctx: &TraceRuleContext,
) -> SessionState {
    let Some(options) = options_from_tracing_config(config) else {
        return state;
    };

    let (rule_span_create_fn, phase_span_create_fn) = make_span_creators(trace_ctx);

    let analyzers = instrument_analyzer_rules(
        state.analyzer().rules.clone(),
        &options,
        &rule_span_create_fn,
        &phase_span_create_fn,
    );
    let optimizers = instrument_optimizer_rules(
        Vec::from(state.optimizers()),
        &options,
        &rule_span_create_fn,
        &phase_span_create_fn,
    );
    let physical_optimizers = instrument_physical_optimizer_rules(
        Vec::from(state.physical_optimizers()),
        &options,
        &rule_span_create_fn,
        &phase_span_create_fn,
    );

    SessionStateBuilder::from(state)
        .with_analyzer_rules(analyzers)
        .with_optimizer_rules(optimizers)
        .with_physical_optimizer_rules(physical_optimizers)
        .build()
}
