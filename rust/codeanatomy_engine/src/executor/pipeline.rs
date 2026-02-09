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

use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use datafusion_common::Result;

use crate::compiler::plan_bundle::{self, PlanBundleArtifact};
use crate::compiler::plan_compiler::{deprecated_template_warning, SemanticPlanCompiler};
use crate::executor::maintenance;
use crate::executor::metrics_collector::{self, CollectedMetrics, summarize_collected_metrics};
use crate::executor::result::RunResult;
use crate::executor::runner::execute_and_materialize_with_plans;
use crate::spec::execution_spec::SemanticExecutionSpec;

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
    preflight_warnings: Vec<String>,
) -> Result<PipelineOutcome> {
    let mut warnings = preflight_warnings;
    if let Some(template_warning) = deprecated_template_warning(spec) {
        warnings.push(template_warning);
    }

    let mut builder = RunResult::builder()
        .with_spec_hash(spec.spec_hash)
        .with_envelope_hash(envelope_hash)
        .with_rulepack_fingerprint(rulepack_fingerprint)
        .started_now();

    // 1. Compile spec into output DataFrames
    let compiler = SemanticPlanCompiler::new(ctx, spec);
    let output_plans = compiler.compile().await?;

    // 2. Capture plan bundles before execution (when compliance_capture is true).
    //
    // Plan bundle capture must happen BEFORE the output_plans Vec is consumed by
    // execute_and_materialize_with_plans, because the Vec is moved into that function.
    // Failures are captured as warnings instead of being fatal.
    let mut plan_bundles: Vec<PlanBundleArtifact> = Vec::new();
    if spec.runtime.compliance_capture {
        for (_target, df) in &output_plans {
            match plan_bundle::capture_plan_bundle_runtime(ctx, df).await {
                Ok(runtime) => {
                    match plan_bundle::build_plan_bundle_artifact_with_warnings(
                        ctx,
                        &runtime,
                        rulepack_fingerprint,
                        provider_identities.clone(),
                        spec.runtime.capture_substrait,
                        false, // SQL text capture not enabled by default
                        spec.runtime.capture_delta_codec,
                        planning_surface_hash,
                    )
                    .await
                    {
                        Ok((artifact, capture_warnings)) => {
                            plan_bundles.push(artifact);
                            warnings.extend(capture_warnings);
                        }
                        Err(e) => warnings.push(format!(
                            "Plan bundle artifact construction failed: {e}"
                        )),
                    }
                }
                Err(e) => warnings.push(format!(
                    "Plan bundle runtime capture failed: {e}"
                )),
            }
        }
    }
    if !plan_bundles.is_empty() {
        builder = builder.with_plan_bundles(plan_bundles);
    }

    // 3. Execute and materialize, retaining physical plan references for metrics
    let (results, physical_plans) = execute_and_materialize_with_plans(
        ctx,
        output_plans,
        &spec.spec_hash,
        &envelope_hash,
    )
    .await?;

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
            Err(e) => warnings.push(format!(
                "Post-materialization maintenance failed: {e}"
            )),
        }
    }

    // 6. Assemble the final RunResult
    for result in results {
        builder = builder.add_output(result);
    }

    let run_result = builder.with_warnings(warnings).build();

    Ok(PipelineOutcome {
        run_result,
        collected_metrics: collected,
    })
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
        aggregated.operator_metrics.extend(plan_metrics.operator_metrics);
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
