use std::collections::HashMap;
use std::io::Cursor;
use std::mem::size_of;
use std::sync::Arc;

use abi_stable::export_root_module;
use abi_stable::prefix_type::PrefixTypeTrait;
use abi_stable::std_types::{ROption, RResult, RString, RStr, RVec};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use datafusion::arrow;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::config::ConfigOptions;
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_ffi::udaf::FFI_AggregateUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use datafusion_ffi::udtf::FFI_TableFunction;
use datafusion_ffi::udwf::FFI_WindowUDF;
use deltalake::delta_datafusion::{
    DeltaCdfTableProvider, DeltaScanConfig, DeltaScanConfigBuilder, DeltaTableProvider,
};
use deltalake::errors::DeltaTableError;
use deltalake::kernel::EagerSnapshot;
use df_plugin_api::{
    caps, DfPluginExportsV1, DfPluginManifestV1, DfPluginMod, DfPluginMod_Ref, DfResult,
    DfTableFunctionV1, DfUdfBundleV1, DF_PLUGIN_ABI_MAJOR, DF_PLUGIN_ABI_MINOR,
};
use serde::de::{self, DeserializeOwned, Deserializer};
use serde::Deserialize;
use tokio::runtime::Runtime;

use datafusion_ext::delta_control_plane::{
    add_actions_for_paths, delta_cdf_provider, load_delta_table, DeltaCdfScanOptions,
};
use datafusion_ext::delta_protocol::{gate_from_parts, protocol_gate};
use datafusion_ext::udf_config::CodeAnatomyUdfConfig;
use datafusion_ext::udf_registry;
#[cfg(feature = "async-udf")]
use datafusion_ext::udf_async;
use datafusion::execution::context::SessionContext;

const DELTA_SCAN_CONFIG_VERSION: u32 = 1;


#[derive(Debug, Deserialize)]
struct DeltaProviderOptions {
    table_uri: String,
    storage_options: Option<HashMap<String, String>>,
    version: Option<i64>,
    timestamp: Option<String>,
    scan_config: DeltaScanConfigPayload,
    min_reader_version: Option<i32>,
    min_writer_version: Option<i32>,
    required_reader_features: Option<Vec<String>>,
    required_writer_features: Option<Vec<String>>,
    files: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct DeltaScanConfigPayload {
    scan_config_version: u32,
    file_column_name: Option<String>,
    enable_parquet_pushdown: bool,
    schema_force_view_types: bool,
    wrap_partition_values: bool,
    #[serde(default, deserialize_with = "deserialize_schema_ipc")]
    schema_ipc: Option<Vec<u8>>,
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
struct PluginUdfConfig {
    utf8_normalize_form: Option<String>,
    utf8_normalize_casefold: Option<bool>,
    utf8_normalize_collapse_ws: Option<bool>,
    span_default_line_base: Option<i32>,
    span_default_col_unit: Option<String>,
    span_default_end_exclusive: Option<bool>,
    map_normalize_key_case: Option<String>,
    map_normalize_sort_keys: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct PluginUdfOptions {
    enable_async: Option<bool>,
    async_udf_timeout_ms: Option<u64>,
    async_udf_batch_size: Option<usize>,
    udf_config: Option<PluginUdfConfig>,
}

fn async_runtime() -> &'static Runtime {
    datafusion_ext::async_runtime::shared_runtime()
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
        serde_json::Value::String(text) => STANDARD
            .decode(text)
            .map(Some)
            .map_err(de::Error::custom),
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

fn config_options_from_udf_options(options: &PluginUdfOptions) -> ConfigOptions {
    let mut config = ConfigOptions::default();
    let mut policy = CodeAnatomyUdfConfig::default();
    if let Some(overrides) = options.udf_config.as_ref() {
        if let Some(value) = overrides.utf8_normalize_form.as_ref() {
            policy.utf8_normalize_form = value.clone();
        }
        if let Some(value) = overrides.utf8_normalize_casefold {
            policy.utf8_normalize_casefold = value;
        }
        if let Some(value) = overrides.utf8_normalize_collapse_ws {
            policy.utf8_normalize_collapse_ws = value;
        }
        if let Some(value) = overrides.span_default_line_base {
            policy.span_default_line_base = value;
        }
        if let Some(value) = overrides.span_default_col_unit.as_ref() {
            policy.span_default_col_unit = value.clone();
        }
        if let Some(value) = overrides.span_default_end_exclusive {
            policy.span_default_end_exclusive = value;
        }
        if let Some(value) = overrides.map_normalize_key_case.as_ref() {
            policy.map_normalize_key_case = value.clone();
        }
        if let Some(value) = overrides.map_normalize_sort_keys {
            policy.map_normalize_sort_keys = value;
        }
    }
    config.extensions.insert(policy);
    config
}

fn build_udf_bundle_from_specs(
    specs: Vec<udf_registry::ScalarUdfSpec>,
    config: &ConfigOptions,
) -> DfUdfBundleV1 {
    let mut scalar = Vec::new();
    for spec in specs {
        let mut udf = (spec.builder)();
        if let Some(updated) = udf.inner().with_updated_config(config) {
            udf = updated;
        }
        if !spec.aliases.is_empty() {
            udf = udf.with_aliases(spec.aliases.iter().copied());
        }
        scalar.push(FFI_ScalarUDF::from(Arc::new(udf)));
    }
    let aggregate = udf_registry::builtin_udafs()
        .into_iter()
        .map(|udaf| FFI_AggregateUDF::from(Arc::new(udaf)))
        .collect::<Vec<_>>();
    let window = udf_registry::builtin_udwfs()
        .into_iter()
        .map(|udwf| FFI_WindowUDF::from(Arc::new(udwf)))
        .collect::<Vec<_>>();
    DfUdfBundleV1 {
        scalar: RVec::from(scalar),
        aggregate: RVec::from(aggregate),
        window: RVec::from(window),
    }
}

fn build_udf_bundle_with_options(options: PluginUdfOptions) -> Result<DfUdfBundleV1, String> {
    let (enable_async, timeout_ms, batch_size) = resolve_udf_policy(&options)?;
    #[cfg(not(feature = "async-udf"))]
    let _ = (timeout_ms, batch_size);
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
    let specs = udf_registry::scalar_udf_specs_with_async(enable_async)
        .map_err(|err| format!("Failed to build UDF bundle: {err}"))?;
    let config = config_options_from_udf_options(&options);
    Ok(build_udf_bundle_from_specs(specs, &config))
}

fn build_udf_bundle() -> DfUdfBundleV1 {
    match build_udf_bundle_with_options(PluginUdfOptions::default()) {
        Ok(bundle) => bundle,
        Err(err) => {
            eprintln!("Failed to build UDF bundle: {err}");
            DfUdfBundleV1 {
                scalar: RVec::new(),
                aggregate: RVec::new(),
                window: RVec::new(),
            }
        }
    }
}

fn build_table_functions() -> Vec<DfTableFunctionV1> {
    let mut functions = Vec::new();
    let ctx = SessionContext::new();
    for spec in udf_registry::table_udf_specs() {
        let table_fn = match (spec.builder)(&ctx) {
            Ok(table_fn) => table_fn,
            Err(err) => {
                eprintln!("Failed to build table UDF {}: {err}", spec.name);
                continue;
            }
        };
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

fn schema_from_ipc(payload: &[u8]) -> Result<SchemaRef, DeltaTableError> {
    let reader = StreamReader::try_new(Cursor::new(payload), None)
        .map_err(DeltaTableError::from)?;
    Ok(Arc::clone(&reader.schema()))
}

fn apply_file_column_builder(
    scan_config: DeltaScanConfig,
    snapshot: &EagerSnapshot,
) -> Result<DeltaScanConfig, DeltaTableError> {
    let Some(file_column_name) = scan_config.file_column_name.clone() else {
        return Ok(scan_config);
    };
    let mut builder = DeltaScanConfigBuilder::new().with_file_column_name(&file_column_name);
    builder = builder.wrap_partition_values(scan_config.wrap_partition_values);
    builder = builder.with_parquet_pushdown(scan_config.enable_parquet_pushdown);
    if let Some(schema) = scan_config.schema.clone() {
        builder = builder.with_schema(schema);
    }
    let built = builder.build(snapshot)?;
    Ok(DeltaScanConfig {
        file_column_name: built.file_column_name,
        wrap_partition_values: built.wrap_partition_values,
        enable_parquet_pushdown: built.enable_parquet_pushdown,
        schema: built.schema,
        schema_force_view_types: scan_config.schema_force_view_types,
    })
}

fn delta_scan_config_from_options(
    options: &DeltaProviderOptions,
    snapshot: &EagerSnapshot,
) -> Result<DeltaScanConfig, DeltaTableError> {
    let mut config = DeltaScanConfig::new();
    let payload = &options.scan_config;
    if payload.scan_config_version != DELTA_SCAN_CONFIG_VERSION {
        return Err(DeltaTableError::Generic(format!(
            "Unsupported scan_config_version {} (expected {})",
            payload.scan_config_version, DELTA_SCAN_CONFIG_VERSION
        )));
    }
    config.file_column_name = payload.file_column_name.clone();
    config.enable_parquet_pushdown = payload.enable_parquet_pushdown;
    config.schema_force_view_types = payload.schema_force_view_types;
    config.wrap_partition_values = payload.wrap_partition_values;
    if let Some(schema_ipc) = payload.schema_ipc.as_ref() {
        config.schema = Some(schema_from_ipc(schema_ipc)?);
    }
    apply_file_column_builder(config, snapshot)
}

fn build_delta_provider(options: DeltaProviderOptions) -> Result<FFI_TableProvider, String> {
    let runtime = async_runtime();
    let gate = gate_from_parts(
        options.min_reader_version,
        options.min_writer_version,
        options.required_reader_features.clone(),
        options.required_writer_features.clone(),
    );
    let result: Result<DeltaTableProvider, DeltaTableError> = runtime.block_on(async {
        let table = load_delta_table(
            &options.table_uri,
            options.storage_options.clone(),
            options.version,
            options.timestamp.clone(),
            None,
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
        let scan_config = delta_scan_config_from_options(&options, &eager_snapshot)?;
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

#[cfg(test)]
mod tests {
    use super::build_table_functions;
    use super::build_udf_bundle_with_options;
    use super::delta_scan_config_from_options;
    use super::{DeltaProviderOptions, DeltaScanConfigPayload, DELTA_SCAN_CONFIG_VERSION};
    use super::PluginUdfOptions;
    use std::sync::Arc;

    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DataFusionError, Result};
    use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
    use datafusion_ffi::udaf::ForeignAggregateUDF;
    use datafusion_ffi::udf::ForeignScalarUDF;
    use datafusion_ffi::udtf::ForeignTableFunction;
    use datafusion_ffi::udwf::ForeignWindowUDF;
    use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType};
    use deltalake::DeltaOps;
    use tokio::runtime::Runtime;

    use datafusion_ext::delta_control_plane::{scan_config_from_session, DeltaScanOverrides};
    use datafusion_ext::{registry_snapshot, udf_registry};

    fn schema_to_ipc(schema: &ArrowSchema) -> Vec<u8> {
        let mut buffer = Vec::new();
        let batch = RecordBatch::new_empty(Arc::new(schema.clone()));
        let mut writer = StreamWriter::try_new(&mut buffer, schema)
            .expect("create ipc writer");
        writer.write(&batch).expect("write schema batch");
        writer.finish().expect("finish ipc writer");
        buffer
    }

    #[test]
    fn plugin_delta_scan_config_matches_control_plane() {
        let runtime = Runtime::new().expect("tokio runtime");
        let table = runtime
            .block_on(async {
                let ops = DeltaOps::new_in_memory();
                ops.create()
                    .with_column(
                        "id",
                        DeltaDataType::Primitive(PrimitiveType::Long),
                        true,
                        None,
                    )
                    .await
            })
            .expect("create delta table");
        let snapshot = table.snapshot().expect("snapshot").snapshot().clone();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int64,
            true,
        )]));
        let schema_ipc = schema_to_ipc(arrow_schema.as_ref());
        let overrides = DeltaScanOverrides {
            file_column_name: Some("source_file".to_string()),
            enable_parquet_pushdown: Some(false),
            schema_force_view_types: Some(false),
            wrap_partition_values: Some(false),
            schema: Some(Arc::clone(&arrow_schema)),
        };
        let ctx = SessionContext::new();
        let state = ctx.state();
        let control_plane = scan_config_from_session(&state, Some(&snapshot), overrides)
            .expect("control plane scan config");

        let scan_config = DeltaScanConfigPayload {
            scan_config_version: DELTA_SCAN_CONFIG_VERSION,
            file_column_name: Some("source_file".to_string()),
            enable_parquet_pushdown: false,
            schema_force_view_types: false,
            wrap_partition_values: false,
            schema_ipc: Some(schema_ipc),
        };
        let options = DeltaProviderOptions {
            table_uri: "memory:///".to_string(),
            storage_options: None,
            version: None,
            timestamp: None,
            scan_config,
            min_reader_version: None,
            min_writer_version: None,
            required_reader_features: None,
            required_writer_features: None,
            files: None,
        };
        let plugin = delta_scan_config_from_options(&options, &snapshot)
            .expect("plugin scan config");

        assert_eq!(plugin.file_column_name, control_plane.file_column_name);
        assert_eq!(plugin.enable_parquet_pushdown, control_plane.enable_parquet_pushdown);
        assert_eq!(plugin.wrap_partition_values, control_plane.wrap_partition_values);
        assert_eq!(plugin.schema_force_view_types, control_plane.schema_force_view_types);
        assert_eq!(
            plugin.schema.as_ref().map(|schema| schema.as_ref()),
            control_plane.schema.as_ref().map(|schema| schema.as_ref())
        );
    }

    fn register_plugin_bundle(ctx: &SessionContext) -> Result<()> {
        let bundle = build_udf_bundle_with_options(PluginUdfOptions::default())
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;
        for udf in bundle.scalar.iter() {
            let foreign = ForeignScalarUDF::try_from(udf)?;
            ctx.register_udf(ScalarUDF::new_from_shared_impl(Arc::new(foreign)));
        }
        for udaf in bundle.aggregate.iter() {
            let foreign = ForeignAggregateUDF::try_from(udaf)?;
            ctx.register_udaf(AggregateUDF::new_from_shared_impl(Arc::new(foreign)));
        }
        for udwf in bundle.window.iter() {
            let foreign = ForeignWindowUDF::try_from(udwf)?;
            ctx.register_udwf(WindowUDF::new_from_shared_impl(Arc::new(foreign)));
        }
        for table_fn in build_table_functions() {
            let name = table_fn.name.to_string();
            let foreign = ForeignTableFunction::from(table_fn.function.clone());
            ctx.register_udtf(name.as_str(), Arc::new(foreign));
        }
        Ok(())
    }

    #[test]
    fn plugin_snapshot_matches_native() -> Result<()> {
        let native_ctx = SessionContext::new();
        udf_registry::register_all(&native_ctx)?;
        let native = registry_snapshot::registry_snapshot(&native_ctx.state());

        let plugin_ctx = SessionContext::new();
        register_plugin_bundle(&plugin_ctx)?;
        let plugin = registry_snapshot::registry_snapshot(&plugin_ctx.state());

        assert_eq!(native.scalar, plugin.scalar);
        assert_eq!(native.aggregate, plugin.aggregate);
        assert_eq!(native.window, plugin.window);
        assert_eq!(native.table, plugin.table);
        assert_eq!(native.aliases, plugin.aliases);
        assert_eq!(native.parameter_names, plugin.parameter_names);
        assert_eq!(native.volatility, plugin.volatility);
        assert_eq!(native.rewrite_tags, plugin.rewrite_tags);
        assert_eq!(native.simplify, plugin.simplify);
        assert_eq!(native.coerce_types, plugin.coerce_types);
        assert_eq!(native.short_circuits, plugin.short_circuits);
        assert_eq!(native.signature_inputs, plugin.signature_inputs);
        assert_eq!(native.return_types, plugin.return_types);
        assert_eq!(native.config_defaults, plugin.config_defaults);
        assert_eq!(native.custom_udfs, plugin.custom_udfs);
        Ok(())
    }
}
