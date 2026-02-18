use std::sync::Arc;

use datafusion::config::ConfigOptions;
use datafusion::execution::context::SessionContext;
use datafusion_ffi::udaf::FFI_AggregateUDF;
use datafusion_ffi::udf::FFI_ScalarUDF;
use datafusion_ffi::udtf::FFI_TableFunction;
use datafusion_ffi::udwf::FFI_WindowUDF;
use df_plugin_api::{DfPluginExportsV1, DfTableFunctionV1, DfUdfBundleV1};

#[cfg(feature = "async-udf")]
use datafusion_ext::async_udf_config::CodeAnatomyAsyncUdfConfig;
use datafusion_ext::udf_config::CodeAnatomyUdfConfig;
use datafusion_ext::udf_registry;
use datafusion_ext::udtf_sources;

use abi_stable::std_types::{RString, RVec};

use crate::options::{resolve_udf_policy, PluginUdfOptions};
use crate::task_context::global_task_ctx_provider;

fn config_options_from_udf_options(
    options: &PluginUdfOptions,
    timeout_ms: Option<u64>,
    batch_size: Option<usize>,
) -> ConfigOptions {
    #[cfg(not(feature = "async-udf"))]
    let _ = (timeout_ms, batch_size);
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
    #[cfg(feature = "async-udf")]
    if timeout_ms.is_some() || batch_size.is_some() {
        config.extensions.insert(CodeAnatomyAsyncUdfConfig {
            ideal_batch_size: batch_size,
            timeout_ms,
        });
    }
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

pub(crate) fn build_udf_bundle_with_options(options: PluginUdfOptions) -> Result<DfUdfBundleV1, String> {
    let (enable_async, timeout_ms, batch_size) = resolve_udf_policy(&options)?;
    #[cfg(not(feature = "async-udf"))]
    if enable_async {
        return Err("Async UDFs require the async-udf feature.".to_string());
    }
    #[cfg(not(feature = "async-udf"))]
    let _ = (timeout_ms, batch_size);
    let specs = udf_registry::scalar_udf_specs_with_async(enable_async)
        .map_err(|err| format!("Failed to build UDF bundle: {err}"))?;
    let config = config_options_from_udf_options(&options, timeout_ms, batch_size);
    Ok(build_udf_bundle_from_specs(specs, &config))
}

fn build_udf_bundle() -> DfUdfBundleV1 {
    match build_udf_bundle_with_options(PluginUdfOptions::default()) {
        Ok(bundle) => bundle,
        Err(err) => {
            tracing::error!(error = %err, "Failed to build UDF bundle");
            DfUdfBundleV1 {
                scalar: RVec::new(),
                aggregate: RVec::new(),
                window: RVec::new(),
            }
        }
    }
}

pub(crate) fn build_table_functions() -> Vec<DfTableFunctionV1> {
    let mut functions = Vec::new();
    let ctx = SessionContext::new();
    let specs = udf_registry::table_udf_specs()
        .into_iter()
        .chain(udtf_sources::external_udtf_specs());
    for spec in specs {
        let table_fn = match (spec.builder)(&ctx) {
            Ok(table_fn) => table_fn,
            Err(err) => {
                tracing::error!(table_udf = spec.name, error = %err, "Failed to build table UDF");
                continue;
            }
        };
        let task_ctx_provider = global_task_ctx_provider();
        let ffi_fn = FFI_TableFunction::new(
            Arc::clone(&table_fn),
            None,
            &task_ctx_provider,
            None,
        );
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

pub(crate) fn exports() -> DfPluginExportsV1 {
    let table_provider_names = RVec::from(vec![RString::from("delta"), RString::from("delta_cdf")]);
    DfPluginExportsV1 {
        table_provider_names,
        udf_bundle: build_udf_bundle(),
        table_functions: RVec::from(build_table_functions()),
    }
}
