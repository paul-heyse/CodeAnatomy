//! Rust-owned CPG output builders.
//!
//! The hard-cutover contract routes canonical CPG output families through
//! this module via `ViewTransform::CpgEmit`.

use datafusion::prelude::*;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::registry::FunctionRegistry;

use crate::compiler::union_builder;
use crate::spec::relations::CpgOutputKind;

fn require_sources(kind: CpgOutputKind, sources: &[String]) -> Result<()> {
    if sources.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "CpgEmit {:?} requires at least one source",
            kind
        )));
    }
    Ok(())
}

async fn table_or_union(ctx: &SessionContext, sources: &[String]) -> Result<DataFrame> {
    if sources.len() == 1 {
        return ctx.table(&sources[0]).await;
    }
    union_builder::build_union(ctx, sources, &None, false).await
}

fn has_column(df: &DataFrame, column: &str) -> bool {
    df.schema().field_with_name(None, column).is_ok()
}

async fn build_nodes_output(ctx: &SessionContext, sources: &[String]) -> Result<DataFrame> {
    let nodes_df = table_or_union(ctx, sources).await?;
    if has_column(&nodes_df, "entity_id")
        && has_column(&nodes_df, "path")
        && has_column(&nodes_df, "bstart")
        && has_column(&nodes_df, "bend")
    {
        return Ok(nodes_df);
    }

    // Internal-compat fallback: synthesize a minimal node shape from repo files
    // when source unions do not yet expose flattened CPG node columns.
    let mut repo_df = ctx.table("repo_files_v1").await?;
    let stable_id_udf = ctx.udf("stable_id")?;
    repo_df = repo_df.with_column(
        "entity_id",
        stable_id_udf.call(vec![lit("node"), col("path")]),
    )?;
    repo_df = repo_df.with_column("bstart", lit(0_i64))?;
    repo_df = repo_df.with_column("bend", col("size_bytes"))?;
    repo_df.select(vec![
        col("entity_id"),
        col("path"),
        col("bstart"),
        col("bend"),
    ])
}

/// Build a canonical CPG output DataFrame from source views.
pub async fn build_cpg_emit(
    ctx: &SessionContext,
    kind: CpgOutputKind,
    sources: &[String],
) -> Result<DataFrame> {
    require_sources(kind, sources)?;
    match kind {
        CpgOutputKind::Nodes => build_nodes_output(ctx, sources).await,
        CpgOutputKind::Edges => table_or_union(ctx, sources).await,
        CpgOutputKind::Props => table_or_union(ctx, sources).await,
        CpgOutputKind::PropsMap => table_or_union(ctx, sources).await,
        CpgOutputKind::EdgesBySrc => table_or_union(ctx, sources).await,
        CpgOutputKind::EdgesByDst => table_or_union(ctx, sources).await,
    }
}
