use std::any::Any;
use std::collections::BTreeMap;

use datafusion::config::ConfigOptions;
use datafusion_common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion_common::{DataFusionError, Result};

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

impl ExtensionOptions for CodeAnatomyUdfConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        match key {
            "utf8_normalize_form" => {
                self.utf8_normalize_form = value.trim().to_string();
            }
            "utf8_normalize_casefold" => {
                self.utf8_normalize_casefold = parse_bool(value, key)?;
            }
            "utf8_normalize_collapse_ws" => {
                self.utf8_normalize_collapse_ws = parse_bool(value, key)?;
            }
            "span_default_line_base" => {
                self.span_default_line_base = parse_i32(value, key)?;
            }
            "span_default_col_unit" => {
                self.span_default_col_unit = value.trim().to_string();
            }
            "span_default_end_exclusive" => {
                self.span_default_end_exclusive = parse_bool(value, key)?;
            }
            "map_normalize_key_case" => {
                self.map_normalize_key_case = value.trim().to_string();
            }
            "map_normalize_sort_keys" => {
                self.map_normalize_sort_keys = parse_bool(value, key)?;
            }
            _ => {
                return Err(DataFusionError::Plan(format!(
                    "Unknown CodeAnatomy UDF config key: {key}"
                )))
            }
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![
            ConfigEntry {
                key: format!("{PREFIX}.utf8_normalize_form"),
                value: Some(self.utf8_normalize_form.clone()),
                description: "Default Unicode normalization form for utf8_normalize.",
            },
            ConfigEntry {
                key: format!("{PREFIX}.utf8_normalize_casefold"),
                value: Some(self.utf8_normalize_casefold.to_string()),
                description: "Default casefold flag for utf8_normalize.",
            },
            ConfigEntry {
                key: format!("{PREFIX}.utf8_normalize_collapse_ws"),
                value: Some(self.utf8_normalize_collapse_ws.to_string()),
                description: "Default collapse_whitespace flag for utf8_normalize.",
            },
            ConfigEntry {
                key: format!("{PREFIX}.span_default_line_base"),
                value: Some(self.span_default_line_base.to_string()),
                description: "Default line base for span_make when not specified.",
            },
            ConfigEntry {
                key: format!("{PREFIX}.span_default_col_unit"),
                value: Some(self.span_default_col_unit.clone()),
                description: "Default column unit for span_make when not specified.",
            },
            ConfigEntry {
                key: format!("{PREFIX}.span_default_end_exclusive"),
                value: Some(self.span_default_end_exclusive.to_string()),
                description: "Default end_exclusive flag for span_make when not specified.",
            },
            ConfigEntry {
                key: format!("{PREFIX}.map_normalize_key_case"),
                value: Some(self.map_normalize_key_case.clone()),
                description: "Default key case for map_normalize.",
            },
            ConfigEntry {
                key: format!("{PREFIX}.map_normalize_sort_keys"),
                value: Some(self.map_normalize_sort_keys.to_string()),
                description: "Default sort_keys flag for map_normalize.",
            },
        ]
    }
}

impl ConfigExtension for CodeAnatomyUdfConfig {
    const PREFIX: &'static str = PREFIX;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UdfConfigValue {
    Bool(bool),
    Int(i64),
    String(String),
}

fn parse_bool(value: &str, key: &str) -> Result<bool> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "true" | "t" | "1" | "yes" | "y" => Ok(true),
        "false" | "f" | "0" | "no" | "n" => Ok(false),
        _ => Err(DataFusionError::Plan(format!(
            "Invalid boolean for {key}: {value}"
        ))),
    }
}

fn parse_i32(value: &str, key: &str) -> Result<i32> {
    value.trim().parse::<i32>().map_err(|err| {
        DataFusionError::Plan(format!("Invalid integer for {key}: {value} ({err})"))
    })
}
