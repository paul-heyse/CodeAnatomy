use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion::execution::context::SessionContext;
use datafusion_common::Result;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_functions_table::generate_series::RangeFunc;

#[cfg(feature = "async-udf")]
use crate::udf_async;
use crate::{udaf_builtin, udf_custom, udtf_builtin, udtf_external, udwf_builtin};

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum UdfKind {
    Scalar,
    Aggregate,
    Window,
    Table,
}

#[allow(dead_code)]
pub enum UdfHandle {
    Scalar(ScalarUDF),
    Aggregate(AggregateUDF),
    Window(WindowUDF),
    Table(Arc<dyn TableFunctionImpl>),
}

pub struct UdfSpec {
    pub name: &'static str,
    pub kind: UdfKind,
    pub builder: fn() -> UdfHandle,
    pub aliases: &'static [&'static str],
}

fn range_table_udtf() -> UdfHandle {
    UdfHandle::Table(Arc::new(RangeFunc {}))
}

pub fn all_udfs() -> Vec<UdfSpec> {
    vec![
        UdfSpec {
            name: "arrow_metadata",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::arrow_metadata_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "semantic_tag",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::semantic_tag_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "cpg_score",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::cpg_score_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "stable_hash64",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::stable_hash64_udf()),
            aliases: &["hash64"],
        },
        UdfSpec {
            name: "stable_hash128",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::stable_hash128_udf()),
            aliases: &["hash128"],
        },
        UdfSpec {
            name: "prefixed_hash64",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::prefixed_hash64_udf()),
            aliases: &["prefixed_hash"],
        },
        UdfSpec {
            name: "stable_id",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::stable_id_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "stable_id_parts",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::stable_id_parts_udf()),
            aliases: &["stable_id_multi"],
        },
        UdfSpec {
            name: "prefixed_hash_parts64",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::prefixed_hash_parts64_udf()),
            aliases: &["prefixed_hash_parts"],
        },
        UdfSpec {
            name: "stable_hash_any",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::stable_hash_any_udf()),
            aliases: &["stable_hash"],
        },
        UdfSpec {
            name: "span_make",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::span_make_udf()),
            aliases: &["span"],
        },
        UdfSpec {
            name: "span_len",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::span_len_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "interval_align_score",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::interval_align_score_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "span_overlaps",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::span_overlaps_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "span_contains",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::span_contains_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "span_id",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::span_id_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "utf8_normalize",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::utf8_normalize_udf()),
            aliases: &["normalize_utf8"],
        },
        UdfSpec {
            name: "utf8_null_if_blank",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::utf8_null_if_blank_udf()),
            aliases: &["null_if_blank"],
        },
        UdfSpec {
            name: "qname_normalize",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::qname_normalize_udf()),
            aliases: &["qualname_normalize"],
        },
        UdfSpec {
            name: "map_get_default",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::map_get_default_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "map_normalize",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::map_normalize_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "list_compact",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::list_compact_udf()),
            aliases: &["array_compact"],
        },
        UdfSpec {
            name: "list_unique_sorted",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::list_unique_sorted_udf()),
            aliases: &["array_unique_sorted"],
        },
        UdfSpec {
            name: "struct_pick",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::struct_pick_udf()),
            aliases: &["struct_select"],
        },
        UdfSpec {
            name: "cdf_change_rank",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::cdf_change_rank_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "cdf_is_upsert",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::cdf_is_upsert_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "cdf_is_delete",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::cdf_is_delete_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "col_to_byte",
            kind: UdfKind::Scalar,
            builder: || UdfHandle::Scalar(udf_custom::col_to_byte_udf()),
            aliases: &[],
        },
        UdfSpec {
            name: "range_table",
            kind: UdfKind::Table,
            builder: range_table_udtf,
            aliases: &[],
        },
    ]
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
    let state = ctx.state();
    let config_options = state.config_options();
    for spec in all_udfs() {
        match (spec.kind, (spec.builder)()) {
            (UdfKind::Scalar, UdfHandle::Scalar(udf)) => {
                let mut udf = udf
                    .inner()
                    .with_updated_config(config_options)
                    .unwrap_or(udf);
                if !spec.aliases.is_empty() {
                    udf = udf.with_aliases(spec.aliases.iter().copied());
                }
                ctx.register_udf(udf);
            }
            (UdfKind::Aggregate, UdfHandle::Aggregate(udaf)) => {
                let udaf = if spec.aliases.is_empty() {
                    udaf
                } else {
                    udaf.with_aliases(spec.aliases.iter().copied())
                };
                ctx.register_udaf(udaf);
            }
            (UdfKind::Window, UdfHandle::Window(udwf)) => {
                let udwf = if spec.aliases.is_empty() {
                    udwf
                } else {
                    udwf.with_aliases(spec.aliases.iter().copied())
                };
                ctx.register_udwf(udwf);
            }
            (UdfKind::Table, UdfHandle::Table(udtf)) => {
                ctx.register_udtf(spec.name, Arc::clone(&udtf));
                for alias in spec.aliases {
                    ctx.register_udtf(alias, Arc::clone(&udtf));
                }
            }
            _ => {
                panic!("UDF spec kind mismatch for {}", spec.name);
            }
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

    use super::{all_udfs, UdfKind};

    #[test]
    fn registry_contains_expected_entries() {
        let names: HashSet<&'static str> = all_udfs().iter().map(|spec| spec.name).collect();
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
        let has_table = all_udfs()
            .iter()
            .any(|spec| matches!(spec.kind, UdfKind::Table));
        assert!(has_table, "expected at least one table UDF");
    }
}
