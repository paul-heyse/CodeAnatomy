//! Execute compiled output plans and materialize to Delta tables.
//!
//! Stream-first: uses physical plan for diagnostics, write_table for materialization.
//!
//! `run_full_pipeline` delegates the core compile-materialize-metrics sequence
//! to [`crate::executor::pipeline::execute_pipeline`] and wraps it with
//! tracing span lifecycle management. The low-level `execute_and_materialize`
//! and `execute_and_materialize_with_plans` functions remain here as the
//! materialization primitives used by the pipeline.

use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::*;
use datafusion_common::{DataFusionError, Result};
use deltalake::protocol::SaveMode;
use std::sync::Arc;

use crate::executor::delta_writer::{
    build_delta_commit_options, ensure_output_table, extract_row_count, read_write_outcome,
    validate_output_schema, LineageContext,
};
#[cfg(feature = "tracing")]
use crate::executor::warnings::warning_counts_by_code;
use crate::executor::pipeline;
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
    lineage: &LineageContext,
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
        let use_native_delta_writer = target.delta_location.is_some() || pre_registered_target.is_none();
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

        let delta_location = target
            .delta_location
            .as_deref()
            .unwrap_or(target.table_name.as_str());
        let (rows_written, outcome) = if use_native_delta_writer {
            let batches = df.collect().await?;
            let rows_written: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
            let commit_options = build_delta_commit_options(&target, lineage);
            datafusion_ext::delta_mutations::delta_write_batches_request(
                datafusion_ext::delta_mutations::DeltaWriteBatchesRequest {
                    session_ctx: ctx,
                    table_uri: delta_location,
                    storage_options: None,
                    version: None,
                    timestamp: None,
                    batches,
                    save_mode: match target.materialization_mode {
                        MaterializationMode::Append => SaveMode::Append,
                        MaterializationMode::Overwrite => SaveMode::Overwrite,
                    },
                    schema_mode_label: None,
                    partition_columns: if target.partition_by.is_empty() {
                        None
                    } else {
                        Some(target.partition_by.clone())
                    },
                    target_file_size: None,
                    gate: Some(datafusion_ext::DeltaFeatureGate::default()),
                    commit_options: Some(commit_options),
                    extra_constraints: None,
                },
            )
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
            (rows_written, read_write_outcome(delta_location).await)
        } else {
            let mut write_options = DataFrameWriteOptions::new().with_insert_operation(insert_op);
            if !target.partition_by.is_empty() {
                write_options = write_options.with_partition_by(target.partition_by.clone());
            }
            let write_result = df.write_table(&target.table_name, write_options).await?;
            (extract_row_count(&write_result), read_write_outcome(delta_location).await)
        };

        results.push(MaterializationResult {
            table_name: target.table_name.clone(),
            rows_written,
            partition_count: partition_count as u32,
            delta_version: outcome.delta_version,
            files_added: outcome.files_added,
            bytes_written: outcome.bytes_written,
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
    lineage: &LineageContext,
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
        let use_native_delta_writer = target.delta_location.is_some() || pre_registered_target.is_none();
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

        let delta_location = target
            .delta_location
            .as_deref()
            .unwrap_or(target.table_name.as_str());
        let (rows_written, outcome) = if use_native_delta_writer {
            let batches = df.collect().await?;
            let rows_written: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();
            let commit_options = build_delta_commit_options(&target, lineage);
            datafusion_ext::delta_mutations::delta_write_batches_request(
                datafusion_ext::delta_mutations::DeltaWriteBatchesRequest {
                    session_ctx: ctx,
                    table_uri: delta_location,
                    storage_options: None,
                    version: None,
                    timestamp: None,
                    batches,
                    save_mode: match target.materialization_mode {
                        MaterializationMode::Append => SaveMode::Append,
                        MaterializationMode::Overwrite => SaveMode::Overwrite,
                    },
                    schema_mode_label: None,
                    partition_columns: if target.partition_by.is_empty() {
                        None
                    } else {
                        Some(target.partition_by.clone())
                    },
                    target_file_size: None,
                    gate: Some(datafusion_ext::DeltaFeatureGate::default()),
                    commit_options: Some(commit_options),
                    extra_constraints: None,
                },
            )
            .await
            .map_err(|err| DataFusionError::External(Box::new(err)))?;
            (rows_written, read_write_outcome(delta_location).await)
        } else {
            let mut write_options = DataFrameWriteOptions::new().with_insert_operation(insert_op);
            if !target.partition_by.is_empty() {
                write_options = write_options.with_partition_by(target.partition_by.clone());
            }
            let write_result = df.write_table(&target.table_name, write_options).await?;
            (extract_row_count(&write_result), read_write_outcome(delta_location).await)
        };

        results.push(MaterializationResult {
            table_name: target.table_name.clone(),
            rows_written,
            partition_count: partition_count as u32,
            delta_version: outcome.delta_version,
            files_added: outcome.files_added,
            bytes_written: outcome.bytes_written,
        });
    }

    Ok((results, physical_plans))
}

/// Full pipeline: compile + execute + assemble result.
///
/// Orchestrates the complete execution lifecycle by delegating the core
/// compile-materialize-metrics sequence to [`pipeline::execute_pipeline`]
/// and wrapping it with tracing span lifecycle management:
///
/// 1. (caller) Set up tracing spans when the `tracing` feature is enabled
/// 2. (pipeline) Compile, capture bundles, materialize, collect metrics, maintain
/// 3. (caller) Flush tracing after pipeline completes
///
/// # Ordering contract (Scope 11)
///
/// The caller is responsible for capturing the session envelope AFTER
/// input registration so that `information_schema.tables` reflects all
/// registered providers. The `envelope_hash` parameter is the hash of
/// the already-captured envelope. Provider identities are threaded
/// through from the registration phase.
pub async fn run_full_pipeline(
    spec: &crate::spec::execution_spec::SemanticExecutionSpec,
    rulepack_fingerprint: [u8; 32],
    prepared: crate::executor::orchestration::PreparedExecutionContext,
) -> Result<RunResult> {
    let envelope_hash = prepared.envelope.envelope_hash;

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

    // Delegate to the unified pipeline orchestrator.
    let outcome = pipeline::execute_pipeline(
        &prepared.ctx,
        spec,
        envelope_hash,
        rulepack_fingerprint,
        prepared.provider_identities,
        prepared.envelope.planning_surface_hash,
        prepared.preflight_warnings,
    )
    .await?;

    #[cfg(feature = "tracing")]
    if tracing_config.enabled {
        let warning_counts = warning_counts_by_code(&outcome.run_result.warnings);
        engine_tracing::record_warning_summary(
            &execution_span,
            outcome.run_result.warnings.len() as u64,
            &warning_counts,
        );
    }

    #[cfg(feature = "tracing")]
    if tracing_config.enabled {
        engine_tracing::flush_otel_tracing()?;
    }
    Ok(outcome.run_result)
}
