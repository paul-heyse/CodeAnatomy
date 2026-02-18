use std::collections::HashMap;

use abi_stable::std_types::{ROption, RString};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use serde::de::{self, DeserializeOwned, Deserializer};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct DeltaProviderOptions {
    pub(crate) table_uri: String,
    pub(crate) storage_options: Option<HashMap<String, String>>,
    pub(crate) version: Option<i64>,
    pub(crate) timestamp: Option<String>,
    pub(crate) scan_config: DeltaScanConfigPayload,
    pub(crate) min_reader_version: Option<i32>,
    pub(crate) min_writer_version: Option<i32>,
    pub(crate) required_reader_features: Option<Vec<String>>,
    pub(crate) required_writer_features: Option<Vec<String>>,
    pub(crate) files: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeltaScanConfigPayload {
    pub(crate) scan_config_version: u32,
    pub(crate) file_column_name: Option<String>,
    pub(crate) enable_parquet_pushdown: bool,
    pub(crate) schema_force_view_types: bool,
    pub(crate) wrap_partition_values: bool,
    #[serde(default, deserialize_with = "deserialize_schema_ipc")]
    pub(crate) schema_ipc: Option<Vec<u8>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DeltaCdfProviderOptions {
    pub(crate) table_uri: String,
    pub(crate) storage_options: Option<HashMap<String, String>>,
    pub(crate) version: Option<i64>,
    pub(crate) timestamp: Option<String>,
    pub(crate) starting_version: Option<i64>,
    pub(crate) ending_version: Option<i64>,
    pub(crate) starting_timestamp: Option<String>,
    pub(crate) ending_timestamp: Option<String>,
    pub(crate) allow_out_of_range: Option<bool>,
    pub(crate) min_reader_version: Option<i32>,
    pub(crate) min_writer_version: Option<i32>,
    pub(crate) required_reader_features: Option<Vec<String>>,
    pub(crate) required_writer_features: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct PluginUdfConfig {
    pub(crate) utf8_normalize_form: Option<String>,
    pub(crate) utf8_normalize_casefold: Option<bool>,
    pub(crate) utf8_normalize_collapse_ws: Option<bool>,
    pub(crate) span_default_line_base: Option<i32>,
    pub(crate) span_default_col_unit: Option<String>,
    pub(crate) span_default_end_exclusive: Option<bool>,
    pub(crate) map_normalize_key_case: Option<String>,
    pub(crate) map_normalize_sort_keys: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(crate) struct PluginUdfOptions {
    pub(crate) enable_async: Option<bool>,
    pub(crate) async_udf_timeout_ms: Option<u64>,
    pub(crate) async_udf_batch_size: Option<usize>,
    pub(crate) udf_config: Option<PluginUdfConfig>,
}

pub(crate) fn parse_udf_options(options: ROption<RString>) -> Result<PluginUdfOptions, String> {
    match options {
        ROption::RSome(value) => serde_json::from_str(value.as_str())
            .map_err(|err| format!("Invalid UDF options JSON: {err}")),
        ROption::RNone => Ok(PluginUdfOptions::default()),
    }
}

pub(crate) fn parse_options<T: DeserializeOwned>(options: ROption<RString>) -> Result<T, String> {
    let options = match options {
        ROption::RSome(value) => value,
        ROption::RNone => return Err("Missing options JSON".to_string()),
    };
    serde_json::from_str(options.as_str()).map_err(|err| format!("Invalid options JSON: {err}"))
}

fn deserialize_schema_ipc<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(text) => {
            STANDARD.decode(text).map(Some).map_err(de::Error::custom)
        }
        serde_json::Value::Array(items) => {
            let mut bytes = Vec::with_capacity(items.len());
            for item in items {
                let Some(num) = item.as_u64() else {
                    return Err(de::Error::custom(
                        "schema_ipc array values must be unsigned integers",
                    ));
                };
                if num > u8::MAX as u64 {
                    return Err(de::Error::custom(
                        "schema_ipc array values must fit in a byte",
                    ));
                }
                bytes.push(num as u8);
            }
            Ok(Some(bytes))
        }
        _ => Err(de::Error::custom(
            "schema_ipc must be a base64 string or array of bytes",
        )),
    }
}

pub(crate) fn resolve_udf_policy(
    options: &PluginUdfOptions,
) -> Result<(bool, Option<u64>, Option<usize>), String> {
    let enable_async = options.enable_async.unwrap_or(false);
    if enable_async {
        let timeout_ms = options.async_udf_timeout_ms.ok_or_else(|| {
            "async_udf_timeout_ms must be set when async UDFs are enabled.".to_string()
        })?;
        if timeout_ms == 0 {
            return Err("async_udf_timeout_ms must be a positive integer.".to_string());
        }
        let batch_size = options.async_udf_batch_size.ok_or_else(|| {
            "async_udf_batch_size must be set when async UDFs are enabled.".to_string()
        })?;
        if batch_size == 0 {
            return Err("async_udf_batch_size must be a positive integer.".to_string());
        }
        return Ok((true, Some(timeout_ms), Some(batch_size)));
    }
    if options.async_udf_timeout_ms.is_some() || options.async_udf_batch_size.is_some() {
        return Err(
            "Async UDF settings require enable_async=true in plugin UDF options.".to_string(),
        );
    }
    Ok((false, None, None))
}
