//! Unified pipeline orchestration for the compile-materialize-metrics lifecycle.
//!
//! Extracts the shared execution sequence from `runner::run_full_pipeline` and
//! `python::materializer::CpgMaterializer::execute` so both paths delegate to a
//! single orchestrator. This prevents behavior drift in plan bundle capture,
//! materialization, metrics collection, maintenance, and warning propagation.
//!
//! ## Boundary contract
//!
//! The pipeline does NOT handle:
//! - Session construction (differs between native and Python paths)
//! - Input registration (differs between native and Python paths)
//! - Envelope capture (differs between native and Python paths)
//! - Compliance capture (EXPLAIN VERBOSE, rulepack snapshot) -- Python-path-specific
//! - Tuner logic -- Python-path-specific
//! - Tracing initialization -- stays in callers (wraps entire execution)
//!
//! The pipeline DOES handle:
//! - Plan compilation via `SemanticPlanCompiler`
//! - Plan bundle capture (when `compliance_capture` is true) with warning propagation
//! - Execution and materialization to Delta tables
//! - Physical metrics collection from executed plan trees
//! - Post-materialization maintenance (when configured)
//! - RunResult assembly with all artifacts and warnings

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion_common::{DataFusionError, Result};

use crate::compiler::cost_model::{
    derive_task_costs, schedule_tasks_with_quality, CostModelConfig,
};
use crate::compiler::optimizer_pipeline::{
    run_optimizer_compile_only, OptimizerPipelineConfig, RuleFailurePolicy,
};
use crate::compiler::plan_bundle::{self, PlanBundleArtifact};
use crate::compiler::plan_compiler::SemanticPlanCompiler;
use crate::compiler::pushdown_probe_extract::{
    extract_input_filter_predicates, verify_pushdown_contracts,
};
use crate::compiler::scheduling::TaskGraph;
use crate::executor::delta_writer::LineageContext;
use crate::executor::maintenance;
use crate::executor::metrics_collector::{self, summarize_collected_metrics, CollectedMetrics};
use crate::executor::result::RunResult;
use crate::executor::runner::execute_and_materialize_with_plans;
use crate::executor::warnings::{warning_counts_by_code, RunWarning, WarningCode, WarningStage};
use crate::providers::pushdown_contract::{
    PushdownEnforcementMode as ContractEnforcementMode, PushdownProbe,
};
use crate::providers::registration::probe_provider_pushdown;
use crate::session::envelope::SessionEnvelope;
use crate::spec::execution_spec::SemanticExecutionSpec;
use crate::spec::runtime::PushdownEnforcementMode;

/// Outcome of the unified pipeline execution.
///
/// Contains the fully assembled `RunResult` plus the collected metrics
/// in their raw form for downstream consumers that need to inspect them
/// independently (e.g., the adaptive tuner in the Python path).
pub struct PipelineOutcome {
    /// Complete execution result with materialization outcomes, metrics,
    /// plan bundles, maintenance reports, and warnings.
    pub run_result: RunResult,
    /// Raw collected metrics from physical plan trees, exposed separately
    /// so callers (e.g., tuner) can inspect without re-parsing the RunResult.
    pub collected_metrics: CollectedMetrics,
}

/// Execute the shared compile-materialize-metrics pipeline.
///
/// Orchestrates the core execution lifecycle that is common to both the
/// native (`runner::run_full_pipeline`) and Python (`CpgMaterializer::execute`)
/// paths:
///
/// 1. Compile the spec into output DataFrames via `SemanticPlanCompiler`
/// 2. Capture plan bundles when `compliance_capture` is enabled (best-effort,
///    failures recorded as warnings)
/// 3. Execute and materialize to Delta tables, retaining physical plan
///    references for post-execution metrics
/// 4. Collect real physical metrics from executed plan trees
/// 5. Run post-materialization Delta maintenance when configured (best-effort,
///    failures recorded as warnings)
/// 6. Assemble and return the complete `RunResult`
///
/// # Parameters
///
/// * `ctx` -- Active `SessionContext` with all inputs registered and rules applied.
/// * `spec` -- The immutable execution spec driving compilation.
/// * `envelope_hash` -- BLAKE3 hash of the session envelope (captured after input
///   registration per Scope 11 ordering contract).
/// * `rulepack_fingerprint` -- BLAKE3 fingerprint of the compiled rulepack.
/// * `provider_identities` -- Identity hashes for each registered provider.
/// * `planning_surface_hash` -- BLAKE3 hash of planning-surface manifest.
/// * `preflight_warnings` -- Non-fatal warnings from shared pre-pipeline orchestration.
///
/// # Errors
///
/// Returns `datafusion_common::Result` errors from plan compilation or
/// materialization. Best-effort operations (plan bundle capture, maintenance)
/// do not propagate errors; they are captured as warnings in the result.
pub async fn execute_pipeline(
    ctx: &SessionContext,
    spec: &SemanticExecutionSpec,
    envelope_hash: [u8; 32],
    rulepack_fingerprint: [u8; 32],
    provider_identities: Vec<plan_bundle::ProviderIdentity>,
    planning_surface_hash: [u8; 32],
    preflight_warnings: Vec<RunWarning>,
) -> Result<PipelineOutcome> {
    let mut warnings = preflight_warnings;
    let run_started_at = chrono::Utc::now().to_rfc3339();

    let mut builder = RunResult::builder()
        .with_spec_hash(spec.spec_hash)
        .with_envelope_hash(envelope_hash)
        .with_rulepack_fingerprint(rulepack_fingerprint)
        .started_now();
    let mut stats_quality_label: Option<String> = None;

    // 1. Compile spec into output DataFrames
    let compiler = SemanticPlanCompiler::new(ctx, spec);
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
    if let Ok(task_graph) = TaskGraph::from_inferred_deps(&view_deps, &scan_deps, &output_deps) {
        let cost_outcome = derive_task_costs(&task_graph, None, &CostModelConfig::default());
        warnings.extend(cost_outcome.warnings.clone());
        let schedule = schedule_tasks_with_quality(
            &task_graph,
            &cost_outcome.costs,
            cost_outcome.stats_quality,
        );
        stats_quality_label = Some(format!("{:?}", cost_outcome.stats_quality));
        builder = builder
            .with_task_schedule(Some(schedule))
            .with_stats_quality(Some(cost_outcome.stats_quality));
    }

    let mut pushdown_probe_map: BTreeMap<String, PushdownProbe> = BTreeMap::new();
    let (pushdown_predicates, pushdown_warnings) = extract_input_filter_predicates(ctx, spec).await;
    warnings.extend(pushdown_warnings);
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

    // 2. Capture/evaluate plan bundles before execution.
    //
    // Pushdown contract enforcement is always evaluated regardless of
    // compliance-capture settings. Plan-bundle artifact capture remains gated
    // by `compliance_capture`.
    let mut plan_bundles: Vec<PlanBundleArtifact> = Vec::new();
    for (target, df) in &output_plans {
        match plan_bundle::capture_plan_bundle_runtime(ctx, df).await {
            Ok(runtime) => {
                let pushdown_mode = map_pushdown_mode(spec.runtime.pushdown_enforcement_mode);
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
                    pushdown_mode,
                    pushdown_report.as_ref(),
                    &mut warnings,
                )?;

                if !spec.runtime.compliance_capture {
                    continue;
                }

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
                let optimizer_report =
                    run_optimizer_compile_only(ctx, runtime.p0_logical.clone(), &optimizer_config)
                        .await;
                let mut optimizer_traces = Vec::new();
                match optimizer_report {
                    Ok(report) => {
                        optimizer_traces = report.pass_traces;
                        warnings.extend(report.warnings);
                    }
                    Err(err) => {
                        warnings.push(RunWarning::new(
                            WarningCode::ComplianceOptimizerLabFailed,
                            WarningStage::Compilation,
                            format!("Optimizer trace capture failed: {err}"),
                        ));
                    }
                }

                match plan_bundle::build_plan_bundle_artifact_with_warnings(
                    plan_bundle::PlanBundleArtifactBuildRequest {
                        ctx,
                        runtime: &runtime,
                        rulepack_fingerprint,
                        provider_identities: provider_identities.clone(),
                        optimizer_traces,
                        pushdown_report,
                        deterministic_inputs: spec
                            .input_relations
                            .iter()
                            .all(|relation| relation.version_pin.is_some()),
                        no_volatile_udfs: true,
                        deterministic_optimizer: true,
                        stats_quality: stats_quality_label.clone(),
                        capture_substrait: spec.runtime.capture_substrait,
                        capture_sql: false, // SQL text capture not enabled by default
                        capture_delta_codec: spec.runtime.capture_delta_codec,
                        planning_surface_hash,
                    },
                )
                .await
                {
                    Ok((artifact, capture_warnings)) => {
                        plan_bundles.push(artifact);
                        warnings.extend(capture_warnings);
                    }
                    Err(e) => warnings.push(RunWarning::new(
                        WarningCode::PlanBundleArtifactBuildFailed,
                        WarningStage::PlanBundle,
                        format!("Plan bundle artifact construction failed: {e}"),
                    )),
                }
            }
            Err(e) => warnings.push(RunWarning::new(
                WarningCode::PlanBundleRuntimeCaptureFailed,
                WarningStage::PlanBundle,
                format!("Plan bundle runtime capture failed: {e}"),
            )),
        }
    }
    if !plan_bundles.is_empty() {
        builder = builder.with_plan_bundles(plan_bundles);
    }

    // 3. Execute and materialize, retaining physical plan references for metrics
    let provider_identity_hash_input: Vec<(String, [u8; 32])> = provider_identities
        .iter()
        .map(|identity| (identity.table_name.clone(), identity.identity_hash))
        .collect();
    let provider_identity_hash =
        SessionEnvelope::hash_provider_identities(&provider_identity_hash_input);
    let lineage = LineageContext {
        spec_hash: spec.spec_hash,
        envelope_hash,
        planning_surface_hash,
        provider_identity_hash,
        rulepack_fingerprint,
        rulepack_profile: format!("{:?}", spec.rulepack_profile),
        runtime_profile_name: spec
            .runtime_profile
            .as_ref()
            .map(|profile| profile.profile_name.clone()),
        run_started_at_rfc3339: run_started_at,
        runtime_lineage_tags: spec.runtime.lineage_tags.clone(),
    };

    let (results, physical_plans) =
        execute_and_materialize_with_plans(ctx, output_plans, &lineage).await?;

    // 4. Collect real physical metrics from executed plan trees.
    //
    // Aggregate metrics across all physical plans by summing per-plan metrics.
    let collected = aggregate_physical_metrics(&physical_plans);
    if !physical_plans.is_empty() {
        let summary = summarize_collected_metrics(&collected);
        builder = builder
            .with_collected_metrics(Some(collected.clone()))
            .with_trace_metrics_summary(Some(summary));
    }

    // 5. Post-materialization Delta maintenance (when configured).
    //
    // Failures are captured as warnings, not propagated as errors.
    if let Some(schedule) = &spec.maintenance {
        let output_locations: Vec<(String, String)> = results
            .iter()
            .map(|r| {
                let location = spec
                    .output_targets
                    .iter()
                    .find(|t| t.table_name == r.table_name)
                    .and_then(|t| t.delta_location.clone())
                    .unwrap_or_else(|| r.table_name.clone());
                (r.table_name.clone(), location)
            })
            .collect();
        match maintenance::execute_maintenance(ctx, schedule, &output_locations).await {
            Ok(maintenance_reports) => {
                builder = builder.with_maintenance_reports(maintenance_reports);
            }
            Err(e) => warnings.push(RunWarning::new(
                WarningCode::MaintenanceFailed,
                WarningStage::Maintenance,
                format!("Post-materialization maintenance failed: {e}"),
            )),
        }
    }

    // 6. Assemble the final RunResult
    for result in results {
        builder = builder.add_output(result);
    }

    let mut run_result = builder.with_warnings(warnings).build();
    if let Some(summary) = run_result.trace_metrics_summary.as_mut() {
        summary.warning_count_total = run_result.warnings.len() as u64;
        summary.warning_counts_by_code = warning_counts_by_code(&run_result.warnings);
    }

    Ok(PipelineOutcome {
        run_result,
        collected_metrics: collected,
    })
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
    report: Option<&crate::providers::pushdown_contract::PushdownContractReport>,
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
                )));
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

/// Aggregate physical metrics across all executed plan trees.
///
/// Sums per-plan metrics (output rows, spill counts, elapsed compute, etc.)
/// and computes a weighted-average scan selectivity across plans that have
/// scan data. If no scans were found, defaults to 1.0 (passthrough).
fn aggregate_physical_metrics(physical_plans: &[Arc<dyn ExecutionPlan>]) -> CollectedMetrics {
    let mut aggregated = CollectedMetrics::default();
    let mut total_scan_selectivity_sum = 0.0f64;
    let mut selectivity_samples = 0u64;

    for plan in physical_plans {
        let plan_metrics = metrics_collector::collect_plan_metrics(plan.as_ref());
        aggregated.output_rows += plan_metrics.output_rows;
        aggregated.spill_count += plan_metrics.spill_count;
        aggregated.spilled_bytes += plan_metrics.spilled_bytes;
        aggregated.elapsed_compute_nanos += plan_metrics.elapsed_compute_nanos;
        aggregated.peak_memory_bytes += plan_metrics.peak_memory_bytes;
        aggregated.partition_count += plan_metrics.partition_count;
        aggregated
            .operator_metrics
            .extend(plan_metrics.operator_metrics);
        total_scan_selectivity_sum += plan_metrics.scan_selectivity;
        selectivity_samples += 1;
    }

    // Scan selectivity: weighted average across plans with scan data.
    // If no scans were found, default to 1.0 (passthrough).
    aggregated.scan_selectivity = if selectivity_samples > 0 {
        total_scan_selectivity_sum / selectivity_samples as f64
    } else {
        1.0
    };

    aggregated
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_physical_metrics_empty() {
        let plans: Vec<Arc<dyn ExecutionPlan>> = vec![];
        let metrics = aggregate_physical_metrics(&plans);
        assert_eq!(metrics.output_rows, 0);
        assert_eq!(metrics.spill_count, 0);
        assert!((metrics.scan_selectivity - 1.0).abs() < f64::EPSILON);
    }
}
