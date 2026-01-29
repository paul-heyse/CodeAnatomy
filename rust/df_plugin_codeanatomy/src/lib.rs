use std::collections::HashMap;
use std::mem::size_of;
use std::sync::{Arc, OnceLock};

use abi_stable::export_root_module;
use abi_stable::std_types::{ROption, RResult, RString, RStr, RVec};
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_ffi::udaf::FFI_AggregateUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use datafusion_ffi::udtf::FFI_TableFunction;
use datafusion_ffi::udwf::FFI_WindowUDF;
use deltalake::delta_datafusion::{DeltaCdfTableProvider, DeltaScanConfig, DeltaTableProvider};
use deltalake::errors::DeltaTableError;
use df_plugin_api::{
    caps, DfPluginExportsV1, DfPluginManifestV1, DfPluginMod, DfPluginMod_Ref, DfResult,
    DfTableFunctionV1, DfUdfBundleV1, DF_PLUGIN_ABI_MAJOR, DF_PLUGIN_ABI_MINOR,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tokio::runtime::Runtime;

use datafusion_ext::delta_control_plane::{
    add_actions_for_paths, delta_cdf_provider, load_delta_table, DeltaCdfScanOptions,
};
use datafusion_ext::delta_protocol::{gate_from_parts, protocol_gate};
use datafusion_ext::udf_registry::{self, UdfHandle, UdfKind};
#[cfg(feature = "async-udf")]
use datafusion_ext::udf_async;

static ASYNC_RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[derive(Debug, Deserialize)]
struct DeltaProviderOptions {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_force_view_types: Option<bool>,
    wrap_partition_values: Option<bool>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    files: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct DeltaCdfProviderOptions {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    starting_version: Option<i64>,
    ending_version: Option<i64>,
    starting_timestamp: Option<String>,
    ending_timestamp: Option<String>,
    allow_out_of_range: Option<bool>,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct PluginUdfOptions {
    enable_async: Option<bool>,
    async_udf_timeout_ms: Option<u64>,
    async_udf_batch_size: Option<usize>,
}

fn async_runtime() -> &'static Runtime {
    ASYNC_RUNTIME.get_or_init(|| Runtime::new().expect("plugin async runtime"))
}

fn parse_major(version: &str) -> Result<u16, String> {
    let Some((major, _)) = version.split_once('.') else {
        return Err(format!("Invalid version string {version:?}"));
    };
    major
        .parse::<u16>()
        .map_err(|err| format!("Invalid version string {version:?}: {err}"))
}

fn manifest() -> DfPluginManifestV1 {
    DfPluginManifestV1 {
        struct_size: size_of::<DfPluginManifestV1>() as u32,
        plugin_abi_major: DF_PLUGIN_ABI_MAJOR,
        plugin_abi_minor: DF_PLUGIN_ABI_MINOR,
        df_ffi_major: datafusion_ffi::version(),
        datafusion_major: parse_major(datafusion::DATAFUSION_VERSION).unwrap_or(0),
        arrow_major: parse_major(arrow::ARROW_VERSION).unwrap_or(0),
        plugin_name: RString::from("codeanatomy"),
        plugin_version: RString::from(env!("CARGO_PKG_VERSION")),
        build_id: RString::from(env!("CARGO_PKG_VERSION")),
        capabilities: caps::TABLE_PROVIDER
            | caps::SCALAR_UDF
            | caps::AGG_UDF
            | caps::WINDOW_UDF
            | caps::TABLE_FUNCTION,
        features: RVec::new(),
    }
}

fn parse_udf_options(options: ROption<RString>) -> Result<PluginUdfOptions, String> {
    match options {
        ROption::RSome(value) => serde_json::from_str(value.as_str())
            .map_err(|err| format!("Invalid UDF options JSON: {err}")),
        ROption::RNone => Ok(PluginUdfOptions::default()),
    }
}

fn resolve_udf_policy(options: &PluginUdfOptions) -> Result<(bool, Option<u64>, Option<usize>), String> {
    let enable_async = options.enable_async.unwrap_or(false);
    if enable_async {
        let timeout_ms = options
            .async_udf_timeout_ms
            .ok_or_else(|| "async_udf_timeout_ms must be set when async UDFs are enabled.".to_string())?;
        if timeout_ms == 0 {
            return Err("async_udf_timeout_ms must be a positive integer.".to_string());
        }
        let batch_size = options
            .async_udf_batch_size
            .ok_or_else(|| "async_udf_batch_size must be set when async UDFs are enabled.".to_string())?;
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

fn build_udf_bundle_from_specs(specs: Vec<udf_registry::UdfSpec>) -> DfUdfBundleV1 {
    let mut scalar = Vec::new();
    let mut aggregate = Vec::new();
    let mut window = Vec::new();

    for spec in specs {
        match (spec.kind, (spec.builder)()) {
            (UdfKind::Scalar, UdfHandle::Scalar(udf)) => {
                let udf = if spec.aliases.is_empty() {
                    udf
                } else {
                    udf.with_aliases(spec.aliases.iter().copied())
                };
                scalar.push(FFI_ScalarUDF::from(Arc::new(udf)));
            }
            (UdfKind::Aggregate, UdfHandle::Aggregate(udaf)) => {
                let udaf = if spec.aliases.is_empty() {
                    udaf
                } else {
                    udaf.with_aliases(spec.aliases.iter().copied())
                };
                aggregate.push(FFI_AggregateUDF::from(Arc::new(udaf)));
            }
            (UdfKind::Window, UdfHandle::Window(udwf)) => {
                let udwf = if spec.aliases.is_empty() {
                    udwf
                } else {
                    udwf.with_aliases(spec.aliases.iter().copied())
                };
                window.push(FFI_WindowUDF::from(Arc::new(udwf)));
            }
            _ => {}
        }
    }

    for udaf in udf_registry::builtin_udafs() {
        aggregate.push(FFI_AggregateUDF::from(Arc::new(udaf)));
    }
    for udwf in udf_registry::builtin_udwfs() {
        window.push(FFI_WindowUDF::from(Arc::new(udwf)));
    }

    DfUdfBundleV1 {
        scalar: RVec::from(scalar),
        aggregate: RVec::from(aggregate),
        window: RVec::from(window),
    }
}

fn build_udf_bundle_with_options(options: PluginUdfOptions) -> Result<DfUdfBundleV1, String> {
    let (enable_async, timeout_ms, batch_size) = resolve_udf_policy(&options)?;
    if enable_async {
        #[cfg(feature = "async-udf")]
        {
            udf_async::set_async_udf_policy(batch_size, timeout_ms)
                .map_err(|err| format!("Failed to set async UDF policy: {err}"))?;
        }
        #[cfg(not(feature = "async-udf"))]
        {
            return Err("Async UDFs require the async-udf feature.".to_string());
        }
    }
    let specs = udf_registry::all_udfs_with_async(enable_async)
        .map_err(|err| format!("Failed to build UDF bundle: {err}"))?;
    Ok(build_udf_bundle_from_specs(specs))
}

fn build_udf_bundle() -> DfUdfBundleV1 {
    build_udf_bundle_with_options(PluginUdfOptions::default())
        .unwrap_or_else(|err| panic!("Failed to build UDF bundle: {err}"))
}

fn build_table_functions() -> Vec<DfTableFunctionV1> {
    let mut functions = Vec::new();
    for spec in udf_registry::all_udfs() {
        if spec.kind != UdfKind::Table {
            continue;
        }
        if let UdfHandle::Table(table_fn) = (spec.builder)() {
            let ffi_fn = FFI_TableFunction::from(Arc::clone(&table_fn));
            functions.push(DfTableFunctionV1 {
                name: RString::from(spec.name),
                function: ffi_fn.clone(),
            });
            for alias in spec.aliases {
                functions.push(DfTableFunctionV1 {
                    name: RString::from(*alias),
                    function: ffi_fn.clone(),
                });
            }
        }
    }
    functions
}

fn exports() -> DfPluginExportsV1 {
    let table_provider_names = RVec::from(vec![
        RString::from("delta"),
        RString::from("delta_cdf"),
    ]);
    DfPluginExportsV1 {
        table_provider_names,
        udf_bundle: build_udf_bundle(),
        table_functions: RVec::from(build_table_functions()),
    }
}

fn parse_options<T: DeserializeOwned>(options: ROption<RString>) -> Result<T, String> {
    let options = match options {
        ROption::RSome(value) => value,
        ROption::RNone => return Err("Missing options JSON".to_string()),
    };
    serde_json::from_str(options.as_str())
        .map_err(|err| format!("Invalid options JSON: {err}"))
}

fn delta_scan_config_from_options(options: &DeltaProviderOptions) -> DeltaScanConfig {
    let mut config = DeltaScanConfig::new();
    if let Some(name) = &options.file_column_name {
        config.file_column_name = Some(name.clone());
    }
    if let Some(pushdown) = options.enable_parquet_pushdown {
        config.enable_parquet_pushdown = pushdown;
    }
    if let Some(force_view) = options.schema_force_view_types {
        config.schema_force_view_types = force_view;
    }
    if let Some(wrap) = options.wrap_partition_values {
        config.wrap_partition_values = wrap;
    }
    config
}

fn build_delta_provider(options: DeltaProviderOptions) -> Result<FFI_TableProvider, String> {
    let runtime = async_runtime();
    let gate = gate_from_parts(
        options.min_reader_version,
        options.min_writer_version,
        options.required_reader_features,
        options.required_writer_features,
    );
    let result: Result<DeltaTableProvider, DeltaTableError> = runtime.block_on(async {
        let table = load_delta_table(
            &options.table_uri,
            options.storage_options.clone(),
            options.version,
            options.timestamp.clone(),
        )
        .await?;
        let snapshot = datafusion_ext::delta_protocol::delta_snapshot_info(
            &options.table_uri,
            &table,
        )
        .await?;
        protocol_gate(&snapshot, &gate)?;
        let eager_snapshot = table.snapshot()?.snapshot().clone();
        let log_store = table.log_store();
        let scan_config = delta_scan_config_from_options(&options);
        let mut provider = DeltaTableProvider::try_new(eager_snapshot, log_store, scan_config)?;
        if let Some(files) = options.files.as_ref() {
            if !files.is_empty() {
                let add_actions = add_actions_for_paths(&table, files)?;
                provider = provider.with_files(add_actions);
            }
        }
        Ok(provider)
    });
    let provider = result.map_err(|err| format!("Delta provider failed: {err}"))?;
    Ok(FFI_TableProvider::new(Arc::new(provider), true, None))
}

fn build_delta_cdf_provider(options: DeltaCdfProviderOptions) -> Result<FFI_TableProvider, String> {
    let runtime = async_runtime();
    let gate = gate_from_parts(
        options.min_reader_version,
        options.min_writer_version,
        options.required_reader_features,
        options.required_writer_features,
    );
    let cdf_options = DeltaCdfScanOptions {
        starting_version: options.starting_version,
        ending_version: options.ending_version,
        starting_timestamp: options.starting_timestamp,
        ending_timestamp: options.ending_timestamp,
        allow_out_of_range: options.allow_out_of_range.unwrap_or(false),
    };
    let result: Result<DeltaCdfTableProvider, DeltaTableError> = runtime.block_on(async {
        let (provider, _) = delta_cdf_provider(
            &options.table_uri,
            options.storage_options.clone(),
            options.version,
            options.timestamp.clone(),
            cdf_options,
            Some(gate),
        )
        .await?;
        Ok(provider)
    });
    let provider = result.map_err(|err| format!("Delta CDF provider failed: {err}"))?;
    Ok(FFI_TableProvider::new(Arc::new(provider), true, None))
}

extern "C" fn create_table_provider(
    name: RStr<'_>,
    options_json: ROption<RString>,
) -> DfResult<FFI_TableProvider> {
    let result = match name.to_string().as_str() {
        "delta" => parse_options::<DeltaProviderOptions>(options_json)
            .and_then(build_delta_provider),
        "delta_cdf" => parse_options::<DeltaCdfProviderOptions>(options_json)
            .and_then(build_delta_cdf_provider),
        other => Err(format!("Unknown table provider {other}")),
    };
    match result {
        Ok(value) => RResult::ROk(value),
        Err(err) => RResult::RErr(RString::from(err)),
    }
}

extern "C" fn plugin_manifest() -> DfPluginManifestV1 {
    manifest()
}

extern "C" fn plugin_exports() -> DfPluginExportsV1 {
    exports()
}

extern "C" fn plugin_udf_bundle(options_json: ROption<RString>) -> DfResult<DfUdfBundleV1> {
    let result = parse_udf_options(options_json).and_then(build_udf_bundle_with_options);
    match result {
        Ok(value) => RResult::ROk(value),
        Err(err) => RResult::RErr(RString::from(err)),
    }
}

#[export_root_module]
pub fn get_library() -> DfPluginMod_Ref {
    DfPluginMod {
        manifest: plugin_manifest,
        exports: plugin_exports,
        udf_bundle_with_options: plugin_udf_bundle,
        create_table_provider,
    }
    .leak_into_prefix()
}
