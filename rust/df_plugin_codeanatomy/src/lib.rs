mod options;
mod providers;
mod task_context;
mod udf_bundle;

use std::mem::size_of;

use abi_stable::export_root_module;
use abi_stable::prefix_type::PrefixTypeTrait;
use abi_stable::std_types::{ROption, RResult, RString, RVec};
use datafusion::arrow;
use df_plugin_api::{
    caps, DfPluginExportsV1, DfPluginManifestV1, DfPluginMod, DfPluginMod_Ref, DfResult,
    DfUdfBundleV1, DF_PLUGIN_ABI_MAJOR, DF_PLUGIN_ABI_MINOR,
};
use df_plugin_common::parse_major;

use crate::options::parse_udf_options;
use crate::providers::create_table_provider;
use crate::udf_bundle::{build_udf_bundle_with_options, exports};

extern "C" fn relation_planner_names() -> RVec<RString> {
    RVec::from(vec![RString::from("codeanatomy_relation")])
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
            | caps::TABLE_FUNCTION
            | caps::RELATION_PLANNER,
        features: RVec::from(vec![RString::from(
            "relation_planner:codeanatomy_relation",
        )]),
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
        relation_planner_names,
        create_table_provider,
    }
    .leak_into_prefix()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{
        DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
    };
    use datafusion::arrow::ipc::writer::StreamWriter;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::TableFunctionImpl;
    use datafusion::error::{DataFusionError, Result};
    use datafusion::logical_expr::{
        AggregateUDF, AggregateUDFImpl, ScalarUDF, ScalarUDFImpl, WindowUDF, WindowUDFImpl,
    };
    use datafusion::prelude::SessionContext;
    use deltalake::kernel::{DataType as DeltaDataType, PrimitiveType};
    use deltalake::DeltaTable;
    use df_plugin_common::DELTA_SCAN_CONFIG_VERSION;
    use tokio::runtime::Runtime;

    use datafusion_ext::delta_control_plane::{scan_config_from_session, DeltaScanOverrides};
    use datafusion_ext::{registry_snapshot, udf_registry, udtf_builtin};

    use crate::options::{DeltaProviderOptions, DeltaScanConfigPayload, PluginUdfOptions};
    use crate::providers::delta_scan_config_from_options;
    use crate::udf_bundle::{build_table_functions, build_udf_bundle_with_options};

    fn schema_to_ipc(schema: &ArrowSchema) -> Vec<u8> {
        let mut buffer = Vec::new();
        let batch = RecordBatch::new_empty(Arc::new(schema.clone()));
        let mut writer = StreamWriter::try_new(&mut buffer, schema).expect("create ipc writer");
        writer.write(&batch).expect("write schema batch");
        writer.finish().expect("finish ipc writer");
        buffer
    }

    #[test]
    fn plugin_delta_scan_config_matches_control_plane() {
        let runtime = Runtime::new().expect("tokio runtime");
        let table = runtime
            .block_on(async {
                DeltaTable::new_in_memory()
                    .create()
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
        let plugin =
            delta_scan_config_from_options(&options, &snapshot).expect("plugin scan config");

        assert_eq!(plugin.file_column_name, control_plane.file_column_name);
        assert_eq!(
            plugin.enable_parquet_pushdown,
            control_plane.enable_parquet_pushdown
        );
        assert_eq!(
            plugin.wrap_partition_values,
            control_plane.wrap_partition_values
        );
        assert_eq!(
            plugin.schema_force_view_types,
            control_plane.schema_force_view_types
        );
        assert_eq!(
            plugin.schema.as_ref().map(|schema| schema.as_ref()),
            control_plane.schema.as_ref().map(|schema| schema.as_ref())
        );
    }

    fn register_plugin_bundle(ctx: &SessionContext) -> Result<()> {
        let bundle = build_udf_bundle_with_options(PluginUdfOptions::default())
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;
        for udf in bundle.scalar.iter() {
            let foreign_impl: Arc<dyn ScalarUDFImpl> = udf.into();
            ctx.register_udf(ScalarUDF::new_from_shared_impl(foreign_impl));
        }
        for udaf in bundle.aggregate.iter() {
            let foreign_impl: Arc<dyn AggregateUDFImpl> = udaf.into();
            ctx.register_udaf(AggregateUDF::new_from_shared_impl(foreign_impl));
        }
        for udwf in bundle.window.iter() {
            let foreign_impl: Arc<dyn WindowUDFImpl> = udwf.into();
            ctx.register_udwf(WindowUDF::new_from_shared_impl(foreign_impl));
        }
        for table_fn in build_table_functions() {
            let name = table_fn.name.to_string();
            let foreign_fn: Arc<dyn TableFunctionImpl> = table_fn.function.clone().into();
            ctx.register_udtf(name.as_str(), foreign_fn);
        }
        udtf_builtin::register_builtin_udtfs(ctx)?;
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

        assert_eq!(native.scalar(), plugin.scalar());
        assert_eq!(native.aggregate(), plugin.aggregate());
        assert_eq!(native.window(), plugin.window());
        assert_eq!(native.table(), plugin.table());
        assert_eq!(native.aliases(), plugin.aliases());
        assert_eq!(native.volatility(), plugin.volatility());
        assert_eq!(native.rewrite_tags(), plugin.rewrite_tags());
        assert_eq!(native.simplify(), plugin.simplify());
        assert_eq!(native.short_circuits(), plugin.short_circuits());

        let native_coerce_keys: Vec<String> = native.coerce_types().keys().cloned().collect();
        let plugin_coerce_keys: Vec<String> = plugin.coerce_types().keys().cloned().collect();
        assert_eq!(native_coerce_keys, plugin_coerce_keys);

        let native_parameter_keys: Vec<String> = native.parameter_names().keys().cloned().collect();
        let plugin_parameter_keys: Vec<String> = plugin.parameter_names().keys().cloned().collect();
        assert_eq!(native_parameter_keys, plugin_parameter_keys);

        let native_signature_keys: Vec<String> =
            native.signature_inputs().keys().cloned().collect();
        let plugin_signature_keys: Vec<String> =
            plugin.signature_inputs().keys().cloned().collect();
        assert_eq!(native_signature_keys, plugin_signature_keys);

        let native_return_keys: Vec<String> = native.return_types().keys().cloned().collect();
        let plugin_return_keys: Vec<String> = plugin.return_types().keys().cloned().collect();
        assert_eq!(native_return_keys, plugin_return_keys);

        for (name, defaults) in plugin.config_defaults() {
            assert_eq!(native.config_defaults().get(name), Some(defaults));
        }
        assert_eq!(native.custom_udfs(), plugin.custom_udfs());
        Ok(())
    }
}
