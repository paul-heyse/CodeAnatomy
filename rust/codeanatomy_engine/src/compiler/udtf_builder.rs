//! UDTF view builders for Delta table function planning primitives.
//!
//! Constructs DataFrames for IncrementalCdf, Metadata, and FileManifest
//! transforms using the registered Delta UDTFs (`read_delta_cdf`,
//! `delta_snapshot`, `delta_add_actions`).
//!
//! These builders express incremental semantics and metadata queries as
//! optimizer-visible plan nodes, allowing predicate pushdown and plan
//! composition with downstream joins/unions.

use datafusion::prelude::*;
use datafusion_common::{ParamValues, Result, ScalarValue};

/// Build an IncrementalCdf view using the `read_delta_cdf` UDTF.
///
/// The CDF result is a standard relation with a `change_type` column
/// (insert, update_preimage, update_postimage, delete) that can be
/// composed with downstream joins/unions/filters in one DAG.
///
/// Uses positional SQL placeholders (`$1`..`$5`) bound via `ParamValues::List`
/// to avoid SQL injection. Requires the `read_delta_cdf` UDTF to accept
/// optional version/timestamp range arguments (uri, start_version,
/// end_version, start_timestamp, end_timestamp).
pub async fn build_incremental_cdf(
    ctx: &SessionContext,
    source: &str,
    starting_version: Option<i64>,
    ending_version: Option<i64>,
    starting_timestamp: Option<&str>,
    ending_timestamp: Option<&str>,
) -> Result<DataFrame> {
    let template = ctx
        .sql("SELECT * FROM read_delta_cdf($1, $2, $3, $4, $5)")
        .await?;
    let bound = template.with_param_values(ParamValues::List(vec![
        ScalarValue::Utf8(Some(source.to_string())).into(),
        starting_version
            .map_or(ScalarValue::Int64(None), |v| ScalarValue::Int64(Some(v)))
            .into(),
        ending_version
            .map_or(ScalarValue::Int64(None), |v| ScalarValue::Int64(Some(v)))
            .into(),
        starting_timestamp
            .map(|v| ScalarValue::Utf8(Some(v.to_string())))
            .unwrap_or(ScalarValue::Utf8(None))
            .into(),
        ending_timestamp
            .map(|v| ScalarValue::Utf8(Some(v.to_string())))
            .unwrap_or(ScalarValue::Utf8(None))
            .into(),
    ]))?;
    Ok(bound)
}

/// Build a Metadata view using the `delta_snapshot` UDTF.
///
/// Produces a single-row relation with table metadata: version, schema_json,
/// protocol, properties, partition_columns. Useful for schema-aware
/// conditional compilation.
pub async fn build_metadata(ctx: &SessionContext, source: &str) -> Result<DataFrame> {
    let sql = format!("SELECT * FROM delta_snapshot('{source}')");
    ctx.sql(&sql).await
}

/// Build a FileManifest view using the `delta_add_actions` UDTF.
///
/// Produces one row per file in the Delta table, with path, size, stats,
/// and partition values. Useful for file-level cost estimation and
/// selective file scanning.
pub async fn build_file_manifest(ctx: &SessionContext, source: &str) -> Result<DataFrame> {
    let sql = format!("SELECT * FROM delta_add_actions('{source}')");
    ctx.sql(&sql).await
}
