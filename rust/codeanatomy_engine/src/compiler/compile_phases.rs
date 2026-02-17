//! Decomposed compile-contract phase helpers.

use std::collections::BTreeMap;

use datafusion::prelude::{DataFrame, SessionContext};
use datafusion_common::{DataFusionError, Result};

use crate::compiler::compile_contract::CompilePlanArtifact;
use crate::compiler::cost_model::{
    derive_task_costs, schedule_tasks_with_quality, CostModelConfig, StatsQuality,
};
use crate::compiler::optimizer_pipeline::{
    run_optimizer_compile_only, OptimizerPipelineConfig, RuleFailurePolicy,
};
use crate::compiler::plan_bundle::{self, ProviderIdentity};
use crate::compiler::pushdown_probe_extract::{
    extract_input_filter_predicates, verify_pushdown_contracts,
};
use crate::compiler::scheduling::{TaskGraph, TaskSchedule};
use crate::contracts::pushdown_mode::PushdownEnforcementMode;
use crate::executor::warnings::{RunWarning, WarningCode, WarningStage};
use crate::providers::pushdown_contract::{PushdownContractReport, PushdownProbe};
use crate::providers::registration::probe_provider_pushdown;
use crate::rules::registry::CpgRuleSet;
use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::outputs::OutputTarget;

pub(crate) struct TaskSchedulePhaseResult {
    pub dependency_map: BTreeMap<String, Vec<String>>,
    pub task_schedule: Option<TaskSchedule>,
    pub stats_quality: Option<StatsQuality>,
    pub task_costs: BTreeMap<String, f64>,
    pub bottom_level_costs: BTreeMap<String, f64>,
    pub slack_by_task: BTreeMap<String, f64>,
    pub warnings: Vec<RunWarning>,
}

pub(crate) fn build_task_schedule_phase(spec: &SemanticExecutionSpec) -> TaskSchedulePhaseResult {
    let view_deps: Vec<(String, Vec<String>)> = spec
        .view_definitions
        .iter()
        .map(|view| (view.name.clone(), view.view_dependencies.clone()))
        .collect();
    let scan_deps: Vec<(String, Vec<String>)> = spec
        .input_relations
        .iter()
        .map(|input| (input.logical_name.clone(), Vec::new()))
        .collect();
    let output_deps: Vec<(String, Vec<String>)> = spec
        .output_targets
        .iter()
        .map(|target| (target.table_name.clone(), vec![target.source_view.clone()]))
        .collect();

    let mut dependency_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut task_schedule: Option<TaskSchedule> = None;
    let mut stats_quality: Option<StatsQuality> = None;
    let mut task_costs: BTreeMap<String, f64> = BTreeMap::new();
    let mut bottom_level_costs: BTreeMap<String, f64> = BTreeMap::new();
    let mut slack_by_task: BTreeMap<String, f64> = BTreeMap::new();
    let mut warnings = Vec::new();

    if let Ok(task_graph) = TaskGraph::from_inferred_deps(&view_deps, &scan_deps, &output_deps) {
        dependency_map = task_graph
            .dependencies
            .iter()
            .map(|(task, deps)| (task.clone(), deps.iter().cloned().collect()))
            .collect();
        let cost_outcome = derive_task_costs(&task_graph, None, &CostModelConfig::default());
        warnings.extend(cost_outcome.warnings.clone());
        let schedule = schedule_tasks_with_quality(
            &task_graph,
            &cost_outcome.costs,
            cost_outcome.stats_quality,
        );
        stats_quality = Some(cost_outcome.stats_quality);
        task_costs = cost_outcome.costs;
        bottom_level_costs = schedule.bottom_level_costs.clone();
        slack_by_task = schedule.slack_by_task.clone();
        task_schedule = Some(schedule);
    }

    TaskSchedulePhaseResult {
        dependency_map,
        task_schedule,
        stats_quality,
        task_costs,
        bottom_level_costs,
        slack_by_task,
        warnings,
    }
}

pub(crate) struct PushdownProbePhaseResult {
    pub pushdown_probe_map: BTreeMap<String, PushdownProbe>,
    pub warnings: Vec<RunWarning>,
}

pub(crate) async fn pushdown_probe_phase(
    ctx: &SessionContext,
    spec: &SemanticExecutionSpec,
) -> PushdownProbePhaseResult {
    let mut pushdown_probe_map: BTreeMap<String, PushdownProbe> = BTreeMap::new();
    let (pushdown_predicates, mut warnings) = extract_input_filter_predicates(ctx, spec).await;

    for (table_name, filters) in pushdown_predicates {
        if filters.is_empty() {
            continue;
        }
        match probe_provider_pushdown(ctx, &table_name, &filters).await {
            Ok(probe) => {
                pushdown_probe_map.insert(table_name, probe);
            }
            Err(err) => warnings.push(
                RunWarning::new(
                    WarningCode::CompliancePushdownProbeFailed,
                    WarningStage::Compliance,
                    format!("Pushdown probe failed: {err}"),
                )
                .with_context("table_name", table_name),
            ),
        }
    }

    PushdownProbePhaseResult {
        pushdown_probe_map,
        warnings,
    }
}

fn enforce_pushdown_contracts(
    table_name: &str,
    mode: PushdownEnforcementMode,
    report: Option<&PushdownContractReport>,
    warnings: &mut Vec<RunWarning>,
) -> Result<()> {
    let Some(report) = report else {
        return Ok(());
    };
    for violation in &report.violations {
        match mode {
            PushdownEnforcementMode::Strict => {
                return Err(DataFusionError::Plan(format!(
                    "Pushdown contract violation on table '{}': {}",
                    violation.table_name, violation.predicate_text
                )))
            }
            PushdownEnforcementMode::Warn => warnings.push(
                RunWarning::new(
                    WarningCode::PushdownContractViolation,
                    WarningStage::Compilation,
                    format!(
                        "Pushdown contract violation on '{}': {}",
                        violation.table_name, violation.predicate_text
                    ),
                )
                .with_context("table_name", table_name.to_string())
                .with_context("predicate", violation.predicate_text.clone()),
            ),
            PushdownEnforcementMode::Disabled => {}
        }
    }
    Ok(())
}

pub(crate) struct ArtifactPhaseResult {
    pub plan_artifacts: Vec<CompilePlanArtifact>,
    pub warnings: Vec<RunWarning>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn compile_artifacts_phase(
    ctx: &SessionContext,
    spec: &SemanticExecutionSpec,
    ruleset: &CpgRuleSet,
    output_plans: &[(OutputTarget, DataFrame)],
    pushdown_probe_map: &BTreeMap<String, PushdownProbe>,
    stats_quality: Option<StatsQuality>,
    provider_identities: &[ProviderIdentity],
    planning_surface_hash: [u8; 32],
) -> ArtifactPhaseResult {
    let mut plan_artifacts = Vec::new();
    let mut warnings = Vec::new();
    let pushdown_mode = spec.runtime.pushdown_enforcement_mode;

    for (target, df) in output_plans {
        match plan_bundle::capture_plan_bundle_runtime(ctx, df).await {
            Ok(runtime) => {
                let state = ctx.state();
                let state_config = state.config();
                let config_options = state_config.options();
                let optimizer_config = OptimizerPipelineConfig {
                    max_passes: config_options.optimizer.max_passes,
                    failure_policy: if config_options.optimizer.skip_failed_rules {
                        RuleFailurePolicy::SkipFailed
                    } else {
                        RuleFailurePolicy::FailFast
                    },
                    capture_pass_traces: true,
                    capture_plan_diffs: true,
                };
                let mut optimizer_traces = Vec::new();
                match run_optimizer_compile_only(ctx, runtime.p0_logical.clone(), &optimizer_config)
                    .await
                {
                    Ok(report) => {
                        optimizer_traces = report.pass_traces;
                        warnings.extend(report.warnings);
                    }
                    Err(err) => warnings.push(RunWarning::new(
                        WarningCode::ComplianceOptimizerLabFailed,
                        WarningStage::Compilation,
                        format!("Optimizer trace capture failed: {err}"),
                    )),
                }

                let pushdown_report = if pushdown_probe_map.is_empty() {
                    None
                } else {
                    Some(verify_pushdown_contracts(
                        &runtime.p1_optimized,
                        pushdown_probe_map,
                        pushdown_mode,
                    ))
                };
                if let Err(err) = enforce_pushdown_contracts(
                    target.table_name.as_str(),
                    pushdown_mode,
                    pushdown_report.as_ref(),
                    &mut warnings,
                ) {
                    warnings.push(RunWarning::new(
                        WarningCode::PushdownContractViolation,
                        WarningStage::Compilation,
                        format!("Pushdown enforcement failed: {err}"),
                    ));
                    continue;
                }

                match plan_bundle::build_plan_bundle_artifact_with_warnings(
                    plan_bundle::PlanBundleArtifactBuildRequest {
                        ctx,
                        runtime: &runtime,
                        rulepack_fingerprint: ruleset.fingerprint,
                        provider_identities: provider_identities.to_vec(),
                        optimizer_traces,
                        pushdown_report,
                        deterministic_inputs: spec.are_inputs_deterministic(),
                        no_volatile_udfs: true,
                        deterministic_optimizer: true,
                        stats_quality: stats_quality.map(|value| format!("{value:?}")),
                        capture_substrait: spec.runtime.capture_substrait,
                        capture_sql: false,
                        capture_delta_codec: spec.runtime.capture_delta_codec,
                        planning_surface_hash,
                    },
                )
                .await
                {
                    Ok((artifact, capture_warnings)) => {
                        warnings.extend(capture_warnings);
                        plan_artifacts.push(CompilePlanArtifact {
                            table_name: target.table_name.clone(),
                            artifact,
                        });
                    }
                    Err(err) => warnings.push(RunWarning::new(
                        WarningCode::PlanBundleArtifactBuildFailed,
                        WarningStage::PlanBundle,
                        format!("Compile artifact construction failed: {err}"),
                    )),
                }
            }
            Err(err) => warnings.push(RunWarning::new(
                WarningCode::PlanBundleRuntimeCaptureFailed,
                WarningStage::PlanBundle,
                format!("Compile runtime capture failed: {err}"),
            )),
        }
    }

    ArtifactPhaseResult {
        plan_artifacts,
        warnings,
    }
}
