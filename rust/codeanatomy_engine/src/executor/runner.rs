//! Execute compiled output plans and materialize to Delta tables.
//!
//! Stream-first: uses physical plan for diagnostics, write_table for materialization.

use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::*;
use datafusion_common::Result;
use std::sync::Arc;

use crate::executor::delta_writer::{ensure_output_table, extract_row_count, validate_output_schema};
use crate::executor::result::{MaterializationResult, RunResult};
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

        results.push(MaterializationResult {
            table_name: target.table_name.clone(),
            rows_written,
            partition_count: partition_count as u32,
        });
    }

    Ok(results)
}

/// Full pipeline: compile + execute + assemble result.
pub async fn run_full_pipeline(
    ctx: &SessionContext,
    spec: &crate::spec::execution_spec::SemanticExecutionSpec,
    envelope_hash: [u8; 32],
    rulepack_fingerprint: [u8; 32],
) -> Result<RunResult> {
    let compiler = crate::compiler::plan_compiler::SemanticPlanCompiler::new(ctx, spec);

    let mut builder = RunResult::builder()
        .with_spec_hash(spec.spec_hash)
        .with_envelope_hash(envelope_hash)
        .with_rulepack_fingerprint(rulepack_fingerprint)
        .started_now();

    let output_plans = compiler.compile().await?;
    let results = execute_and_materialize(ctx, output_plans).await?;

    for result in results {
        builder = builder.add_output(result);
    }

    Ok(builder.build())
}
