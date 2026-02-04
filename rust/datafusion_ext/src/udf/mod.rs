pub mod cdf;
pub mod collection;
pub mod common;
pub mod function_factory;
pub mod hash;
pub mod metadata;
pub mod position;
pub mod span;
pub mod string;
pub mod struct_ops;

pub use cdf::*;
pub use collection::*;
pub use function_factory::install_function_factory_native;
pub use hash::*;
pub use metadata::*;
pub use position::*;
pub use span::*;
pub use string::*;
pub use struct_ops::*;

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
