use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_functions_table::generate_series::RangeFunc;
use datafusion::catalog::TableFunctionImpl;
use datafusion_common::Result;

use crate::{udaf_builtin, udtf_builtin, udtf_external, udwf_builtin, udf_custom};

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

pub fn register_all(ctx: &SessionContext) -> Result<()> {
    for spec in all_udfs() {
        match (spec.kind, (spec.builder)()) {
            (UdfKind::Scalar, UdfHandle::Scalar(udf)) => {
                let udf = if spec.aliases.is_empty() {
                    udf
                } else {
                    udf.with_aliases(spec.aliases.iter().copied())
                };
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{UdfKind, all_udfs};

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
