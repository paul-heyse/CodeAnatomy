//! Compile-only contract surface for deterministic planning metadata.
//!
//! This module provides a compile boundary that returns plan artifacts,
//! scheduling/cost metadata, and warnings without materializing outputs.

use std::collections::BTreeMap;

use datafusion_common::{DataFusionError, Result};
use serde::{Deserialize, Serialize};

use crate::compiler::cost_model::{
    derive_task_costs, schedule_tasks_with_quality, CostModelConfig, StatsQuality,
};
use crate::compiler::optimizer_pipeline::{
    run_optimizer_compile_only, OptimizerPipelineConfig, RuleFailurePolicy,
};
use crate::compiler::plan_bundle::{self, PlanBundleArtifact, ProviderIdentity};
use crate::compiler::plan_compiler::SemanticPlanCompiler;
use crate::compiler::pushdown_probe_extract::{
    extract_input_filter_predicates, verify_pushdown_contracts,
};
use crate::compiler::scheduling::{TaskGraph, TaskSchedule};
use crate::executor::orchestration::prepare_execution_context;
use crate::executor::warnings::{RunWarning, WarningCode, WarningStage};
use crate::providers::pushdown_contract::{
    PushdownContractReport, PushdownEnforcementMode as ContractEnforcementMode, PushdownProbe,
};
use crate::providers::registration::probe_provider_pushdown;
use crate::rules::registry::CpgRuleSet;
use crate::session::factory::SessionFactory;
use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::runtime::{PushdownEnforcementMode, TracingConfig};

/// Compile contract request.
pub struct CompileRequest<'a> {
    pub session_factory: &'a SessionFactory,
    pub spec: &'a SemanticExecutionSpec,
    pub ruleset: &'a CpgRuleSet,
    pub tracing_config: Option<&'a TracingConfig>,
}

/// Per-output compile artifact entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompilePlanArtifact {
    pub table_name: String,
    pub artifact: PlanBundleArtifact,
}

/// Compile-only response payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompileResponse {
    pub spec_hash: [u8; 32],
    pub rulepack_fingerprint: [u8; 32],
    pub envelope_hash: [u8; 32],
    pub planning_surface_hash: [u8; 32],
    pub provider_identity_hash: [u8; 32],
    pub provider_identities: Vec<ProviderIdentity>,
    pub task_schedule: Option<TaskSchedule>,
    pub stats_quality: Option<StatsQuality>,
    pub task_costs: BTreeMap<String, f64>,
    pub bottom_level_costs: BTreeMap<String, f64>,
    pub slack_by_task: BTreeMap<String, f64>,
    pub dependency_map: BTreeMap<String, Vec<String>>,
    pub plan_artifacts: Vec<CompilePlanArtifact>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<RunWarning>,
}

/// Compile a request to deterministic planning artifacts without materialization.
pub async fn compile_request(request: CompileRequest<'_>) -> Result<CompileResponse> {
    let CompileRequest {
        session_factory,
        spec,
        ruleset,
        tracing_config,
    } = request;

    let prepared =
        prepare_execution_context(session_factory, spec, ruleset, tracing_config).await?;
    let mut warnings = prepared.preflight_warnings;

    let compiler = SemanticPlanCompiler::new(&prepared.ctx, spec);
    let compilation = compiler.compile_with_warnings().await?;
    warnings.extend(compilation.warnings);
    let output_plans = compilation.outputs;

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

    let mut pushdown_probe_map: BTreeMap<String, PushdownProbe> = BTreeMap::new();
    let (pushdown_predicates, pushdown_warnings) =
        extract_input_filter_predicates(&prepared.ctx, spec).await;
    warnings.extend(pushdown_warnings);
    for (table_name, filters) in pushdown_predicates {
        if filters.is_empty() {
            continue;
        }
        match probe_provider_pushdown(&prepared.ctx, &table_name, &filters).await {
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

    let mut plan_artifacts = Vec::new();
    let pushdown_mode = map_pushdown_mode(spec.runtime.pushdown_enforcement_mode);
    for (target, df) in &output_plans {
        match plan_bundle::capture_plan_bundle_runtime(&prepared.ctx, df).await {
            Ok(runtime) => {
                let state = prepared.ctx.state();
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
                match run_optimizer_compile_only(
                    &prepared.ctx,
                    runtime.p0_logical.clone(),
                    &optimizer_config,
                )
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
                        &pushdown_probe_map,
                        pushdown_mode.clone(),
                    ))
                };
                enforce_pushdown_contracts(
                    target.table_name.as_str(),
                    pushdown_mode.clone(),
                    pushdown_report.as_ref(),
                    &mut warnings,
                )?;

                match plan_bundle::build_plan_bundle_artifact_with_warnings(
                    plan_bundle::PlanBundleArtifactBuildRequest {
                        ctx: &prepared.ctx,
                        runtime: &runtime,
                        rulepack_fingerprint: ruleset.fingerprint,
                        provider_identities: prepared.provider_identities.clone(),
                        optimizer_traces,
                        pushdown_report,
                        deterministic_inputs: spec
                            .input_relations
                            .iter()
                            .all(|relation| relation.version_pin.is_some()),
                        no_volatile_udfs: true,
                        deterministic_optimizer: true,
                        stats_quality: stats_quality.map(|value| format!("{value:?}")),
                        capture_substrait: spec.runtime.capture_substrait,
                        capture_sql: false,
                        capture_delta_codec: spec.runtime.capture_delta_codec,
                        planning_surface_hash: prepared.envelope.planning_surface_hash,
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

    Ok(CompileResponse {
        spec_hash: spec.spec_hash,
        rulepack_fingerprint: ruleset.fingerprint,
        envelope_hash: prepared.envelope.envelope_hash,
        planning_surface_hash: prepared.envelope.planning_surface_hash,
        provider_identity_hash: prepared.envelope.provider_identity_hash,
        provider_identities: prepared.provider_identities,
        task_schedule,
        stats_quality,
        task_costs,
        bottom_level_costs,
        slack_by_task,
        dependency_map,
        plan_artifacts,
        warnings,
    })
}

/// Build compile response directly from runtime plans and labels.
pub fn compile_response_to_json(response: &CompileResponse) -> Result<String> {
    serde_json::to_string_pretty(response)
        .map_err(|err| DataFusionError::Plan(format!("Failed to serialize CompileResponse: {err}")))
}

fn map_pushdown_mode(mode: PushdownEnforcementMode) -> ContractEnforcementMode {
    match mode {
        PushdownEnforcementMode::Warn => ContractEnforcementMode::Warn,
        PushdownEnforcementMode::Strict => ContractEnforcementMode::Strict,
        PushdownEnforcementMode::Disabled => ContractEnforcementMode::Disabled,
    }
}

fn enforce_pushdown_contracts(
    table_name: &str,
    mode: ContractEnforcementMode,
    report: Option<&PushdownContractReport>,
    warnings: &mut Vec<RunWarning>,
) -> Result<()> {
    let Some(report) = report else {
        return Ok(());
    };
    for violation in &report.violations {
        match mode {
            ContractEnforcementMode::Strict => {
                return Err(DataFusionError::Plan(format!(
                    "Pushdown contract violation on table '{}': {}",
                    violation.table_name, violation.predicate_text
                )))
            }
            ContractEnforcementMode::Warn => warnings.push(
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
            ContractEnforcementMode::Disabled => {}
        }
    }
    Ok(())
}
