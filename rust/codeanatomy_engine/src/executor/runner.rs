//! Execute compiled output plans and materialize to Delta tables.
//!
//! Stream-first: uses physical plan for diagnostics, write_table for materialization.
//!
//! Pipeline stages (in `run_full_pipeline`):
//! 1. Compile spec into output DataFrames
//! 2. Capture plan bundles (when compliance_capture is enabled)
//! 3. Execute and materialize to Delta tables
//! 4. Collect physical metrics from executed plans
//! 5. Run post-materialization maintenance (when configured)
//! 6. Assemble RunResult with all artifacts

use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::*;
use datafusion_common::Result;
use std::sync::Arc;

use crate::compiler::plan_bundle::{self, PlanBundleArtifact};
use crate::executor::delta_writer::{ensure_output_table, extract_row_count, validate_output_schema};
use crate::executor::maintenance;
use crate::executor::metrics_collector::{self, CollectedMetrics, summarize_collected_metrics};
use crate::executor::result::{MaterializationResult, RunResult};
#[cfg(feature = "tracing")]
use crate::executor::tracing as engine_tracing;
use crate::spec::outputs::{MaterializationMode, OutputTarget};

/// Execute compiled output plans and materialize results to Delta tables.
///
/// Stream-first: uses physical plan for diagnostics, write_table for materialization.
///
/// P0 correction #7: write_table().await? returns Vec<RecordBatch> directly.
/// Do NOT chain .collect().await? after write_table.
pub async fn execute_and_materialize(
    ctx: &SessionContext,
    output_plans: Vec<(OutputTarget, DataFrame)>,
    spec_hash: &[u8; 32],
    envelope_hash: &[u8; 32],
) -> Result<Vec<MaterializationResult>> {
    let mut results = Vec::new();

    for (target, df) in output_plans {
        // Get partition count from physical plan for diagnostics
        // P0 correction #6: use create_physical_plan(), NOT execution_plan()
        let plan = df.clone().create_physical_plan().await?;
        let partition_count = plan.output_partitioning().partition_count();

        // Map MaterializationMode to InsertOp
        let insert_op = match target.materialization_mode {
            MaterializationMode::Append => InsertOp::Append,
            MaterializationMode::Overwrite => InsertOp::Overwrite,
        };
        let expected_schema = Arc::new(df.schema().as_arrow().clone());
        let pre_registered_target = ctx.table(&target.table_name).await.ok();
        if pre_registered_target.is_none() || target.delta_location.is_some() {
            if pre_registered_target.is_some() && target.delta_location.is_some() {
                let _ = ctx.deregister_table(&target.table_name)?;
            }
            let delta_location = target
                .delta_location
                .as_deref()
                .unwrap_or(target.table_name.as_str());
            ensure_output_table(
                ctx,
                &target.table_name,
                delta_location,
                &expected_schema,
            )
            .await?;
        }
        let existing_df = match pre_registered_target {
            Some(df) => Some(df),
            None => ctx.table(&target.table_name).await.ok(),
        };
        if let Some(existing_df) = existing_df {
            validate_output_schema(existing_df.schema().as_arrow(), expected_schema.as_ref())?;
        }

        // P0 correction #7: write_table returns Vec<RecordBatch> directly
        // This is the terminal operation — no .collect() afterward
        let write_options = DataFrameWriteOptions::new().with_insert_operation(insert_op);
        let write_result = df.write_table(&target.table_name, write_options).await?;

        let rows_written = extract_row_count(&write_result);

        let _commit_props = crate::executor::delta_writer::build_commit_properties(&target, spec_hash, envelope_hash);

        results.push(MaterializationResult {
            table_name: target.table_name.clone(),
            rows_written,
            partition_count: partition_count as u32,
            delta_version: None,
            files_added: None,
            bytes_written: None,
        });
    }

    Ok(results)
}

/// Execute compiled output plans, materialize, and return physical plan references.
///
/// Identical to [`execute_and_materialize`] but also collects the physical
/// execution plans created during materialization. These plan references
/// contain post-execution metrics (output rows, spill counts, elapsed compute)
/// that the caller can extract via [`metrics_collector::collect_plan_metrics`].
///
/// The physical plans are returned alongside the materialization results so
/// that `run_full_pipeline` can aggregate real metrics into the `RunResult`.
pub async fn execute_and_materialize_with_plans(
    ctx: &SessionContext,
    output_plans: Vec<(OutputTarget, DataFrame)>,
    spec_hash: &[u8; 32],
    envelope_hash: &[u8; 32],
) -> Result<(Vec<MaterializationResult>, Vec<Arc<dyn ExecutionPlan>>)> {
    let mut results = Vec::new();
    let mut physical_plans: Vec<Arc<dyn ExecutionPlan>> = Vec::new();

    for (target, df) in output_plans {
        // Create physical plan for diagnostics and metrics capture
        let plan = df.clone().create_physical_plan().await?;
        let partition_count = plan.output_partitioning().partition_count();

        // Retain the physical plan reference for post-execution metrics
        physical_plans.push(Arc::clone(&plan));

        // Map MaterializationMode to InsertOp
        let insert_op = match target.materialization_mode {
            MaterializationMode::Append => InsertOp::Append,
            MaterializationMode::Overwrite => InsertOp::Overwrite,
        };
        let expected_schema = Arc::new(df.schema().as_arrow().clone());
        let pre_registered_target = ctx.table(&target.table_name).await.ok();
        if pre_registered_target.is_none() || target.delta_location.is_some() {
            if pre_registered_target.is_some() && target.delta_location.is_some() {
                let _ = ctx.deregister_table(&target.table_name)?;
            }
            let delta_location = target
                .delta_location
                .as_deref()
                .unwrap_or(target.table_name.as_str());
            ensure_output_table(
                ctx,
                &target.table_name,
                delta_location,
                &expected_schema,
            )
            .await?;
        }
        let existing_df = match pre_registered_target {
            Some(df) => Some(df),
            None => ctx.table(&target.table_name).await.ok(),
        };
        if let Some(existing_df) = existing_df {
            validate_output_schema(existing_df.schema().as_arrow(), expected_schema.as_ref())?;
        }

        let write_options = DataFrameWriteOptions::new().with_insert_operation(insert_op);
        let write_result = df.write_table(&target.table_name, write_options).await?;

        let rows_written = extract_row_count(&write_result);

        let _commit_props = crate::executor::delta_writer::build_commit_properties(&target, spec_hash, envelope_hash);

        results.push(MaterializationResult {
            table_name: target.table_name.clone(),
            rows_written,
            partition_count: partition_count as u32,
            delta_version: None,
            files_added: None,
            bytes_written: None,
        });
    }

    Ok((results, physical_plans))
}

/// Full pipeline: compile + execute + assemble result.
///
/// Orchestrates the complete execution lifecycle:
/// 1. Compile the spec into output DataFrames
/// 2. Optionally capture plan bundles (WS-P1, when `compliance_capture` is true)
/// 3. Execute and materialize to Delta tables, retaining physical plan references
/// 4. Collect real physical metrics from executed plans (WS-P7)
/// 5. Optionally run post-materialization Delta maintenance (WS-P11)
/// 6. Optionally wrap execution in OpenTelemetry tracing spans (WS-P13)
/// 7. Assemble the complete RunResult with all artifacts
pub async fn run_full_pipeline(
    ctx: &SessionContext,
    spec: &crate::spec::execution_spec::SemanticExecutionSpec,
    envelope_hash: [u8; 32],
    rulepack_fingerprint: [u8; 32],
) -> Result<RunResult> {
    // WS-P13: Create tracing span when feature is enabled
    #[cfg(feature = "tracing")]
    let tracing_config = spec.runtime.effective_tracing();
    #[cfg(feature = "tracing")]
    engine_tracing::init_otel_tracing(&tracing_config)?;
    #[cfg(feature = "tracing")]
    let execution_span = {
        let profile_name = format!("{:?}", spec.rulepack_profile);
        let span_info = engine_tracing::ExecutionSpanInfo::new(
            &spec.spec_hash,
            &envelope_hash,
            &rulepack_fingerprint,
            &profile_name,
        );
        engine_tracing::execution_span(&span_info, &tracing_config)
    };
    #[cfg(feature = "tracing")]
    let _execution_span_guard = execution_span.enter();

    let compiler = crate::compiler::plan_compiler::SemanticPlanCompiler::new(ctx, spec);

    let mut builder = RunResult::builder()
        .with_spec_hash(spec.spec_hash)
        .with_envelope_hash(envelope_hash)
        .with_rulepack_fingerprint(rulepack_fingerprint)
        .started_now();

    // 1. Compile spec into output DataFrames
    let output_plans = compiler.compile().await?;

    // 2. WS-P1: Capture plan bundles before execution (when compliance_capture is true)
    //
    // Plan bundle capture must happen BEFORE the output_plans Vec is consumed by
    // execute_and_materialize_with_plans, because the Vec is moved into that function.
    // We iterate the DataFrames to capture runtime plan handles, then build artifacts.
    let mut plan_bundles: Vec<PlanBundleArtifact> = Vec::new();
    if spec.runtime.compliance_capture {
        for (_target, df) in &output_plans {
            let runtime = plan_bundle::capture_plan_bundle_runtime(ctx, df).await?;
            let artifact = plan_bundle::build_plan_bundle_artifact(
                ctx,
                &runtime,
                rulepack_fingerprint,
                vec![], // Provider identities populated by session factory
                spec.runtime.capture_substrait,
                false, // SQL text capture not enabled by default
            )
            .await?;
            plan_bundles.push(artifact);
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

    // 4. WS-P7: Collect real physical metrics from executed plan trees
    //
    // Aggregate metrics across all physical plans by summing per-plan metrics.
    // This replaces the synthetic placeholder values that were previously used.
    if !physical_plans.is_empty() {
        let mut aggregated = CollectedMetrics::default();
        let mut total_scan_selectivity_sum = 0.0f64;
        let mut selectivity_samples = 0u64;
        for plan in &physical_plans {
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
        // Scan selectivity: compute weighted average across plans that have scan data.
        // If no scans were found, default to 1.0 (passthrough).
        aggregated.scan_selectivity = if selectivity_samples > 0 {
            total_scan_selectivity_sum / selectivity_samples as f64
        } else {
            1.0
        };
        let summary = summarize_collected_metrics(&aggregated);
        builder = builder
            .with_collected_metrics(Some(aggregated))
            .with_trace_metrics_summary(Some(summary));
    }

    // 5. WS-P11: Post-materialization Delta maintenance
    //
    // If the spec includes a maintenance schedule, build output locations from
    // materialization results and execute maintenance operations.
    if let Some(schedule) = &spec.maintenance {
        let output_locations: Vec<(String, String)> = results
            .iter()
            .map(|r| {
                // Use the delta_location from the original output target if available,
                // otherwise fall back to the table name.
                let location = spec
                    .output_targets
                    .iter()
                    .find(|t| t.table_name == r.table_name)
                    .and_then(|t| t.delta_location.clone())
                    .unwrap_or_else(|| r.table_name.clone());
                (r.table_name.clone(), location)
            })
            .collect();
        let maintenance_reports =
            maintenance::execute_maintenance(ctx, schedule, &output_locations).await?;
        builder = builder.with_maintenance_reports(maintenance_reports);
    }

    // 6. Add materialization results to the builder
    for result in results {
        builder = builder.add_output(result);
    }

    let run_result = builder.build();
    #[cfg(feature = "tracing")]
    if tracing_config.enabled {
        engine_tracing::flush_otel_tracing()?;
    }
    Ok(run_result)
}
