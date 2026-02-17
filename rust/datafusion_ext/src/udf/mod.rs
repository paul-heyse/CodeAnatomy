pub mod cdf;
pub mod collection;
pub mod common;
pub mod hash;
pub mod metadata;
pub mod position;
pub mod primitives;
pub mod span;
pub mod string;
pub mod struct_ops;

pub use cdf::{cdf_change_rank_udf, cdf_is_delete_udf, cdf_is_upsert_udf};
pub use collection::{
    list_compact_udf, list_unique_sorted_udf, map_get_default_udf, map_normalize_udf,
};
pub use hash::{
    prefixed_hash64_udf, prefixed_hash_parts64_udf, stable_hash128_udf, stable_hash64_udf,
    stable_hash_any_udf, stable_id_parts_udf, stable_id_udf,
};
pub use metadata::{arrow_metadata_udf, cpg_score_udf};
pub use position::{col_to_byte_udf, position_encoding_udf};
pub use primitives::install_function_factory_native;
pub use span::{
    interval_align_score_udf, span_contains_udf, span_id_udf, span_len_udf, span_make_udf,
    span_overlaps_udf,
};
pub use string::{
    qname_normalize_udf, semantic_tag_udf, utf8_normalize_udf, utf8_null_if_blank_udf,
};
pub use struct_ops::struct_pick_udf;

use std::collections::BTreeMap;

use datafusion_expr::ScalarUDFImpl;

use crate::function_types::FunctionKind;
use crate::registry::metadata::FunctionMetadata;
use crate::udf_config::{CodeAnatomyUdfConfig, UdfConfigValue};

pub fn config_defaults_for(udf: &dyn ScalarUDFImpl) -> Option<BTreeMap<String, UdfConfigValue>> {
    if let Some(udf) = udf.as_any().downcast_ref::<string::Utf8NormalizeUdf>() {
        return Some(udf.policy.utf8_normalize_defaults());
    }
    if let Some(udf) = udf.as_any().downcast_ref::<collection::MapNormalizeUdf>() {
        return Some(udf.policy.map_normalize_defaults());
    }
    if let Some(udf) = udf.as_any().downcast_ref::<span::SpanMakeUdf>() {
        return Some(udf.policy.span_make_defaults());
    }
    let defaults = CodeAnatomyUdfConfig::default();
    match udf.name() {
        "utf8_normalize" => Some(defaults.utf8_normalize_defaults()),
        "map_normalize" => Some(defaults.map_normalize_defaults()),
        "span_make" => Some(defaults.span_make_defaults()),
        _ => None,
    }
}

pub(crate) fn function_metadata(name: &str) -> Option<FunctionMetadata> {
    let rewrite_tags: &'static [&'static str] = match name {
        "stable_hash64" | "stable_hash128" | "prefixed_hash64" | "stable_id"
        | "stable_hash_any" | "sha256" => &["hash"],
        "stable_id_parts" | "prefixed_hash_parts64" => &["id", "hash"],
        "col_to_byte" => &["position_encoding"],
        "span_make" => &["span", "position_encoding"],
        "span_len" | "span_overlaps" | "span_contains" => &["span"],
        "span_id" => &["span", "id", "hash"],
        "interval_align_score" => &["interval", "alignment", "score"],
        "utf8_normalize" | "utf8_null_if_blank" => &["string_norm", "string"],
        "qname_normalize" => &["string_norm", "symbol", "string"],
        "map_get_default" | "map_normalize" => &["nested", "map"],
        "list_compact" | "list_unique_sorted" => &["nested", "list"],
        "struct_pick" => &["nested", "struct"],
        "cdf_change_rank" | "cdf_is_upsert" | "cdf_is_delete" => &["incremental", "cdf", "delta"],
        "dedupe_best_by_score" => &["dedupe", "window"],
        "row_number_window" | "lag_window" | "lead_window" => &["window"],
        "cpg_score" => &[],
        _ => return None,
    };

    let has_simplify = matches!(
        name,
        "stable_hash64"
            | "stable_hash128"
            | "prefixed_hash64"
            | "stable_id"
            | "stable_id_parts"
            | "prefixed_hash_parts64"
            | "stable_hash_any"
            | "utf8_normalize"
            | "span_make"
            | "span_len"
            | "interval_align_score"
            | "span_overlaps"
            | "span_contains"
            | "span_id"
            | "map_get_default"
            | "map_normalize"
            | "list_compact"
            | "list_unique_sorted"
            | "cpg_score"
    );

    let kind = if matches!(
        name,
        "dedupe_best_by_score" | "row_number_window" | "lag_window" | "lead_window"
    ) {
        FunctionKind::Table
    } else {
        FunctionKind::Scalar
    };

    Some(FunctionMetadata {
        name: None,
        kind,
        rewrite_tags,
        has_simplify,
        has_coerce_types: false,
        has_short_circuits: false,
        has_groups_accumulator: false,
        has_retract_batch: false,
        has_reverse_expr: false,
        has_sort_options: false,
    })
}
