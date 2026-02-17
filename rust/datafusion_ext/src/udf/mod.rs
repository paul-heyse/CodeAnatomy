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
pub use string::{qname_normalize_udf, semantic_tag_udf, utf8_normalize_udf, utf8_null_if_blank_udf};
pub use struct_ops::struct_pick_udf;

use std::collections::BTreeMap;

use datafusion_expr::ScalarUDFImpl;

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
