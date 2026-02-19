use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion_common::Result;
use datafusion_expr::{AggregateUDF, WindowUDF};

#[cfg(feature = "async-udf")]
use crate::async_udf_config::CodeAnatomyAsyncUdfConfig;
pub use crate::macros::{ScalarUdfSpec, TableUdfSpec};
#[cfg(feature = "async-udf")]
use crate::udf_async;
use crate::{scalar_udfs, table_udfs};
use crate::{udaf_builtin, udf, udtf_builtin, udtf_sources, udwf_builtin};

pub fn scalar_udf_specs() -> Vec<ScalarUdfSpec> {
    scalar_udfs![
        "arrow_metadata" => udf::arrow_metadata_udf;
        "semantic_tag" => udf::semantic_tag_udf;
        "cpg_score" => udf::cpg_score_udf;
        "stable_hash64" => udf::stable_hash64_udf, aliases: ["hash64"];
        "stable_hash128" => udf::stable_hash128_udf, aliases: ["hash128"];
        "prefixed_hash64" => udf::prefixed_hash64_udf, aliases: ["prefixed_hash"];
        "stable_id" => udf::stable_id_udf;
        "stable_id_parts" => udf::stable_id_parts_udf, aliases: ["stable_id_multi"];
        "prefixed_hash_parts64" => udf::prefixed_hash_parts64_udf, aliases: ["prefixed_hash_parts"];
        "stable_hash_any" => udf::stable_hash_any_udf, aliases: ["stable_hash"];
        "span_make" => udf::span_make_udf, aliases: ["span"];
        "span_len" => udf::span_len_udf;
        "interval_align_score" => udf::interval_align_score_udf;
        "span_overlaps" => udf::span_overlaps_udf;
        "span_contains" => udf::span_contains_udf;
        "span_id" => udf::span_id_udf;
        "utf8_normalize" => udf::utf8_normalize_udf, aliases: ["normalize_utf8"];
        "utf8_null_if_blank" => udf::utf8_null_if_blank_udf, aliases: ["null_if_blank"];
        "qname_normalize" => udf::qname_normalize_udf, aliases: ["qualname_normalize"];
        "map_get_default" => udf::map_get_default_udf;
        "map_normalize" => udf::map_normalize_udf;
        "list_compact" => udf::list_compact_udf, aliases: ["array_compact"];
        "list_unique_sorted" => udf::list_unique_sorted_udf, aliases: ["array_unique_sorted"];
        "struct_pick" => udf::struct_pick_udf, aliases: ["struct_select"];
        "cdf_change_rank" => udf::cdf_change_rank_udf;
        "cdf_is_upsert" => udf::cdf_is_upsert_udf;
        "cdf_is_delete" => udf::cdf_is_delete_udf;
        "col_to_byte" => udf::col_to_byte_udf;
        "canonicalize_byte_span" => udf::canonicalize_byte_span_udf;
    ]
}

pub fn table_udf_specs() -> Vec<TableUdfSpec> {
    table_udfs![]
}

pub fn scalar_udf_specs_with_async(enable_async: bool) -> Result<Vec<ScalarUdfSpec>> {
    if !enable_async {
        return Ok(scalar_udf_specs());
    }
    #[cfg(feature = "async-udf")]
    {
        let mut specs = scalar_udf_specs();
        specs.push(ScalarUdfSpec {
            name: udf_async::ASYNC_ECHO_NAME,
            builder: udf_async::async_echo_udf,
            aliases: &[],
        });
        Ok(specs)
    }
    #[cfg(not(feature = "async-udf"))]
    {
        Err(datafusion_common::DataFusionError::Plan(
            "Async UDFs require the async-udf feature".into(),
        ))
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
    udtf_sources::register_external_udtfs(ctx)?;
    if enable_async {
        #[cfg(feature = "async-udf")]
        {
            {
                let state_ref = ctx.state_ref();
                let mut state = state_ref.write();
                let config = state.config_mut();
                config.set_extension(Arc::new(CodeAnatomyAsyncUdfConfig {
                    ideal_batch_size: async_udf_batch_size,
                    timeout_ms: async_udf_timeout_ms,
                }));
            }
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
            "canonicalize_byte_span",
        ] {
            assert!(names.contains(name), "missing {name}");
        }
    }

    #[test]
    fn registry_has_table_functions() {
        assert!(
            table_udf_specs().is_empty(),
            "expected no custom table UDFs"
        );
    }
}
