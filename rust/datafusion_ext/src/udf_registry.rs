use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion::execution::context::SessionContext;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::{AggregateUDF, WindowUDF};
use datafusion_functions_table::generate_series::RangeFunc;

#[cfg(feature = "async-udf")]
use crate::udf_async;
use crate::macros::{scalar_udfs, table_udfs};
pub use crate::macros::{ScalarUdfSpec, TableUdfSpec};
use crate::{udaf_builtin, udf_custom, udtf_builtin, udtf_external, udwf_builtin};

fn range_table_udtf(_ctx: &SessionContext) -> Result<Arc<dyn TableFunctionImpl>> {
    Ok(Arc::new(RangeFunc {}))
}

pub fn scalar_udf_specs() -> Vec<ScalarUdfSpec> {
    scalar_udfs![
        "arrow_metadata" => udf_custom::arrow_metadata_udf;
        "semantic_tag" => udf_custom::semantic_tag_udf;
        "cpg_score" => udf_custom::cpg_score_udf;
        "stable_hash64" => udf_custom::stable_hash64_udf, aliases: ["hash64"];
        "stable_hash128" => udf_custom::stable_hash128_udf, aliases: ["hash128"];
        "prefixed_hash64" => udf_custom::prefixed_hash64_udf, aliases: ["prefixed_hash"];
        "stable_id" => udf_custom::stable_id_udf;
        "stable_id_parts" => udf_custom::stable_id_parts_udf, aliases: ["stable_id_multi"];
        "prefixed_hash_parts64" => udf_custom::prefixed_hash_parts64_udf, aliases: ["prefixed_hash_parts"];
        "stable_hash_any" => udf_custom::stable_hash_any_udf, aliases: ["stable_hash"];
        "span_make" => udf_custom::span_make_udf, aliases: ["span"];
        "span_len" => udf_custom::span_len_udf;
        "interval_align_score" => udf_custom::interval_align_score_udf;
        "span_overlaps" => udf_custom::span_overlaps_udf;
        "span_contains" => udf_custom::span_contains_udf;
        "span_id" => udf_custom::span_id_udf;
        "utf8_normalize" => udf_custom::utf8_normalize_udf, aliases: ["normalize_utf8"];
        "utf8_null_if_blank" => udf_custom::utf8_null_if_blank_udf, aliases: ["null_if_blank"];
        "qname_normalize" => udf_custom::qname_normalize_udf, aliases: ["qualname_normalize"];
        "map_get_default" => udf_custom::map_get_default_udf;
        "map_normalize" => udf_custom::map_normalize_udf;
        "list_compact" => udf_custom::list_compact_udf, aliases: ["array_compact"];
        "list_unique_sorted" => udf_custom::list_unique_sorted_udf, aliases: ["array_unique_sorted"];
        "struct_pick" => udf_custom::struct_pick_udf, aliases: ["struct_select"];
        "cdf_change_rank" => udf_custom::cdf_change_rank_udf;
        "cdf_is_upsert" => udf_custom::cdf_is_upsert_udf;
        "cdf_is_delete" => udf_custom::cdf_is_delete_udf;
        "col_to_byte" => udf_custom::col_to_byte_udf;
    ]
}

pub fn table_udf_specs() -> Vec<TableUdfSpec> {
    table_udfs![
        "range_table" => range_table_udtf;
    ]
}

pub fn scalar_udf_specs_with_async(enable_async: bool) -> Result<Vec<ScalarUdfSpec>> {
    let mut specs = scalar_udf_specs();
    if !enable_async {
        return Ok(specs);
    }
    #[cfg(feature = "async-udf")]
    {
        specs.push(ScalarUdfSpec {
            name: udf_async::ASYNC_ECHO_NAME,
            builder: udf_async::async_echo_udf,
            aliases: &[],
        });
        return Ok(specs);
    }
    #[cfg(not(feature = "async-udf"))]
    {
        return Err(DataFusionError::Plan(
            "Async UDFs require the async-udf feature".into(),
        ));
    }
}

pub fn builtin_udafs() -> Vec<AggregateUDF> {
    udaf_builtin::builtin_udafs()
}

pub fn builtin_udwfs() -> Vec<WindowUDF> {
    udwf_builtin::builtin_udwfs()
}

pub fn register_all(ctx: &SessionContext) -> Result<()> {
    register_all_with_policy(ctx, false, None, None)
}

pub fn register_all_with_policy(
    ctx: &SessionContext,
    enable_async: bool,
    async_udf_timeout_ms: Option<u64>,
    async_udf_batch_size: Option<usize>,
) -> Result<()> {
    #[cfg(not(feature = "async-udf"))]
    let _ = (async_udf_timeout_ms, async_udf_batch_size);
    let state = ctx.state();
    let config_options = state.config_options();
    for spec in scalar_udf_specs() {
        let mut udf = (spec.builder)();
        let updated = udf.inner().with_updated_config(config_options);
        if let Some(updated) = updated {
            udf = updated;
        }
        if !spec.aliases.is_empty() {
            udf = udf.with_aliases(spec.aliases.iter().copied());
        }
        ctx.register_udf(udf);
    }
    for spec in table_udf_specs() {
        let table_fn = (spec.builder)(ctx)?;
        ctx.register_udtf(spec.name, Arc::clone(&table_fn));
        for alias in spec.aliases {
            ctx.register_udtf(alias, Arc::clone(&table_fn));
        }
    }
    for udaf in udaf_builtin::builtin_udafs() {
        ctx.register_udaf(udaf);
    }
    for udwf in udwf_builtin::builtin_udwfs() {
        ctx.register_udwf(udwf);
    }
    udtf_builtin::register_builtin_udtfs(ctx)?;
    udtf_external::register_external_udtfs(ctx)?;
    if enable_async {
        #[cfg(feature = "async-udf")]
        {
            udf_async::set_async_udf_policy(async_udf_batch_size, async_udf_timeout_ms)?;
            udf_async::register_async_udfs(ctx)?;
        }
        #[cfg(not(feature = "async-udf"))]
        {
            return Err(datafusion_common::DataFusionError::Plan(
                "Async UDFs require the async-udf feature".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{scalar_udf_specs, table_udf_specs};

    #[test]
    fn registry_contains_expected_entries() {
        let names: HashSet<&'static str> = scalar_udf_specs()
            .iter()
            .map(|spec| spec.name)
            .chain(table_udf_specs().iter().map(|spec| spec.name))
            .collect();
        for name in [
            "arrow_metadata",
            "stable_hash64",
            "stable_hash128",
            "prefixed_hash64",
            "stable_id",
            "col_to_byte",
            "range_table",
        ] {
            assert!(names.contains(name), "missing {name}");
        }
    }

    #[test]
    fn registry_has_table_functions() {
        assert!(!table_udf_specs().is_empty(), "expected at least one table UDF");
    }
}
