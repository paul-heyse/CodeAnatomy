use std::collections::BTreeMap;

use datafusion::config::ConfigOptions;
use datafusion_common::config::ConfigExtension;
use serde::Serialize;

const PREFIX: &str = "codeanatomy_udf";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CodeAnatomyUdfConfig {
    pub utf8_normalize_form: String,
    pub utf8_normalize_casefold: bool,
    pub utf8_normalize_collapse_ws: bool,
    pub span_default_line_base: i32,
    pub span_default_col_unit: String,
    pub span_default_end_exclusive: bool,
    pub map_normalize_key_case: String,
    pub map_normalize_sort_keys: bool,
}

impl Default for CodeAnatomyUdfConfig {
    fn default() -> Self {
        Self {
            utf8_normalize_form: "NFKC".to_string(),
            utf8_normalize_casefold: true,
            utf8_normalize_collapse_ws: true,
            span_default_line_base: 0,
            span_default_col_unit: "byte".to_string(),
            span_default_end_exclusive: true,
            map_normalize_key_case: "lower".to_string(),
            map_normalize_sort_keys: true,
        }
    }
}

impl CodeAnatomyUdfConfig {
    pub fn from_config(config: &ConfigOptions) -> Self {
        config
            .extensions
            .get::<CodeAnatomyUdfConfig>()
            .cloned()
            .unwrap_or_default()
    }

    pub fn utf8_normalize_defaults(&self) -> BTreeMap<String, UdfConfigValue> {
        let mut values = BTreeMap::new();
        values.insert(
            "default_form".to_string(),
            UdfConfigValue::String(self.utf8_normalize_form.clone()),
        );
        values.insert(
            "default_casefold".to_string(),
            UdfConfigValue::Bool(self.utf8_normalize_casefold),
        );
        values.insert(
            "default_collapse_ws".to_string(),
            UdfConfigValue::Bool(self.utf8_normalize_collapse_ws),
        );
        values
    }

    pub fn span_make_defaults(&self) -> BTreeMap<String, UdfConfigValue> {
        let mut values = BTreeMap::new();
        values.insert(
            "default_line_base".to_string(),
            UdfConfigValue::Int(self.span_default_line_base as i64),
        );
        values.insert(
            "default_col_unit".to_string(),
            UdfConfigValue::String(self.span_default_col_unit.clone()),
        );
        values.insert(
            "default_end_exclusive".to_string(),
            UdfConfigValue::Bool(self.span_default_end_exclusive),
        );
        values
    }

    pub fn map_normalize_defaults(&self) -> BTreeMap<String, UdfConfigValue> {
        let mut values = BTreeMap::new();
        values.insert(
            "default_key_case".to_string(),
            UdfConfigValue::String(self.map_normalize_key_case.clone()),
        );
        values.insert(
            "default_sort_keys".to_string(),
            UdfConfigValue::Bool(self.map_normalize_sort_keys),
        );
        values
    }
}

crate::impl_extension_options!(
    CodeAnatomyUdfConfig,
    prefix = PREFIX,
    unknown_key = "Unknown CodeAnatomy UDF config key: {key}",
    fields = [
        (
            utf8_normalize_form,
            String,
            "Default Unicode normalization form for utf8_normalize."
        ),
        (
            utf8_normalize_casefold,
            bool,
            "Default casefold flag for utf8_normalize."
        ),
        (
            utf8_normalize_collapse_ws,
            bool,
            "Default collapse_whitespace flag for utf8_normalize."
        ),
        (
            span_default_line_base,
            i32,
            "Default line base for span_make when not specified."
        ),
        (
            span_default_col_unit,
            String,
            "Default column unit for span_make when not specified."
        ),
        (
            span_default_end_exclusive,
            bool,
            "Default end_exclusive flag for span_make when not specified."
        ),
        (
            map_normalize_key_case,
            String,
            "Default key case for map_normalize."
        ),
        (
            map_normalize_sort_keys,
            bool,
            "Default sort_keys flag for map_normalize."
        ),
    ]
);

impl ConfigExtension for CodeAnatomyUdfConfig {
    const PREFIX: &'static str = PREFIX;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum UdfConfigValue {
    Bool(bool),
    Int(i64),
    String(String),
}
