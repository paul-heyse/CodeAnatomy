use abi_stable::std_types::{ROption, RResult, RStr, RString};
use datafusion_ffi::table_provider::FFI_TableProvider;
use deltalake::delta_datafusion::{
    DeltaCdfTableProvider, DeltaScanConfig, DeltaScanConfigBuilder, DeltaTableProvider,
};
use deltalake::errors::DeltaTableError;
use deltalake::kernel::EagerSnapshot;
use df_plugin_api::DfResult;
use df_plugin_common::{schema_from_ipc, DELTA_SCAN_CONFIG_VERSION};

use datafusion_ext::delta_control_plane::{
    add_actions_for_paths, delta_cdf_provider, load_delta_table, DeltaCdfProviderRequest,
    DeltaCdfScanOptions,
};
use datafusion_ext::delta_protocol::{gate_from_parts, protocol_gate, TableVersion};

use crate::options::{parse_options, DeltaCdfProviderOptions, DeltaProviderOptions};
use crate::task_context::global_task_ctx_provider;

fn async_runtime() -> Result<&'static tokio::runtime::Runtime, String> {
    datafusion_ext::async_runtime::shared_runtime()
        .map_err(|err| format!("Failed to acquire shared runtime: {err}"))
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

pub(crate) fn delta_scan_config_from_options(
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
        config.schema = Some(schema_from_ipc(schema_ipc).map_err(DeltaTableError::from)?);
    }
    apply_file_column_builder(config, snapshot)
}

fn build_delta_provider(options: DeltaProviderOptions) -> Result<FFI_TableProvider, String> {
    let runtime = async_runtime()?;
    let gate = gate_from_parts(
        options.min_reader_version,
        options.min_writer_version,
        options.required_reader_features.clone(),
        options.required_writer_features.clone(),
    );
    let result: Result<DeltaTableProvider, DeltaTableError> = runtime.block_on(async {
        let table_version = TableVersion::from_options(options.version, options.timestamp.clone())?;
        let table = load_delta_table(
            &options.table_uri,
            options.storage_options.clone(),
            table_version,
            None,
        )
        .await?;
        let snapshot =
            datafusion_ext::delta_protocol::delta_snapshot_info(&options.table_uri, &table).await?;
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
    let task_ctx_provider = global_task_ctx_provider();
    Ok(FFI_TableProvider::new(
        std::sync::Arc::new(provider),
        true,
        None,
        &task_ctx_provider,
        None,
    ))
}

fn build_delta_cdf_provider(options: DeltaCdfProviderOptions) -> Result<FFI_TableProvider, String> {
    let runtime = async_runtime()?;
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
        let table_version = TableVersion::from_options(options.version, options.timestamp.clone())?;
        let (provider, _) = delta_cdf_provider(DeltaCdfProviderRequest {
            table_uri: &options.table_uri,
            storage_options: options.storage_options.clone(),
            table_version,
            options: cdf_options,
            gate: Some(gate),
        })
        .await?;
        Ok(provider)
    });
    let provider = result.map_err(|err| format!("Delta CDF provider failed: {err}"))?;
    let task_ctx_provider = global_task_ctx_provider();
    Ok(FFI_TableProvider::new(
        std::sync::Arc::new(provider),
        true,
        None,
        &task_ctx_provider,
        None,
    ))
}

pub(crate) extern "C" fn create_table_provider(
    name: RStr<'_>,
    options_json: ROption<RString>,
) -> DfResult<FFI_TableProvider> {
    let result = match name.to_string().as_str() {
        "delta" => {
            parse_options::<DeltaProviderOptions>(options_json).and_then(build_delta_provider)
        }
        "delta_cdf" => parse_options::<DeltaCdfProviderOptions>(options_json)
            .and_then(build_delta_cdf_provider),
        other => Err(format!("Unknown table provider {other}")),
    };
    match result {
        Ok(value) => RResult::ROk(value),
        Err(err) => RResult::RErr(RString::from(err)),
    }
}
