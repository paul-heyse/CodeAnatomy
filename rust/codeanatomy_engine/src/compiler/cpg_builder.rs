//! Rust-owned CPG output builders.
//!
//! The hard-cutover contract routes canonical CPG output families through
//! this module via `ViewTransform::CpgEmit`.

use datafusion::prelude::*;
use datafusion_common::{DataFusionError, Result};

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

async fn table_or_union(
    ctx: &SessionContext,
    sources: &[String],
) -> Result<DataFrame> {
    if sources.len() == 1 {
        return ctx.table(&sources[0]).await;
    }
    union_builder::build_union(ctx, sources, &None, false).await
}

/// Build a canonical CPG output DataFrame from source views.
pub async fn build_cpg_emit(
    ctx: &SessionContext,
    kind: CpgOutputKind,
    sources: &[String],
) -> Result<DataFrame> {
    require_sources(kind, sources)?;
    match kind {
        CpgOutputKind::Nodes => table_or_union(ctx, sources).await,
        CpgOutputKind::Edges => table_or_union(ctx, sources).await,
        CpgOutputKind::Props => table_or_union(ctx, sources).await,
        CpgOutputKind::PropsMap => table_or_union(ctx, sources).await,
        CpgOutputKind::EdgesBySrc => table_or_union(ctx, sources).await,
        CpgOutputKind::EdgesByDst => table_or_union(ctx, sources).await,
    }
}

