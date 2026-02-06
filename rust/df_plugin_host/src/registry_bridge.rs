use std::collections::HashSet;
use std::sync::Arc;

use abi_stable::std_types::{ROption, RResult, RStr, RString};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_ffi::table_provider::{FFI_TableProvider, ForeignTableProvider};
use datafusion_ffi::udaf::ForeignAggregateUDF;
use datafusion_ffi::udf::ForeignScalarUDF;
use datafusion_ffi::udtf::ForeignTableFunction;
use datafusion_ffi::udwf::ForeignWindowUDF;
use deltalake::delta_datafusion::{DeltaLogicalCodec, DeltaPhysicalCodec};

use df_plugin_api::{caps, DfResult};

use crate::loader::PluginHandle;

impl PluginHandle {
    fn install_delta_plan_codecs(ctx: &SessionContext) {
        let state_ref = ctx.state_ref();
        let mut state = state_ref.write();
        let config = state.config_mut();
        config.set_extension(Arc::new(DeltaLogicalCodec {}));
        config.set_extension(Arc::new(DeltaPhysicalCodec {}));
    }

    fn require_capability(&self, mask: u64, label: &str) -> Result<()> {
        let manifest = self.manifest();
        if (manifest.capabilities & mask) != 0 {
            return Ok(());
        }
        Err(DataFusionError::Plan(format!(
            "Plugin {name} missing capability {label}",
            name = manifest.plugin_name
        )))
    }

    pub fn register_udfs(&self, ctx: &SessionContext, options_json: Option<&str>) -> Result<()> {
        let options = match options_json {
            Some(value) => ROption::RSome(RString::from(value)),
            None => ROption::RNone,
        };
        let udf_bundle = (self.module().udf_bundle_with_options())(options);
        let udf_bundle = df_result_to_result(udf_bundle)?;
        if !udf_bundle.scalar.is_empty() {
            self.require_capability(caps::SCALAR_UDF, "scalar_udf")?;
        }
        if !udf_bundle.aggregate.is_empty() {
            self.require_capability(caps::AGG_UDF, "aggregate_udf")?;
        }
        if !udf_bundle.window.is_empty() {
            self.require_capability(caps::WINDOW_UDF, "window_udf")?;
        }
        for udf in udf_bundle.scalar.iter() {
            let foreign = ForeignScalarUDF::try_from(udf)?;
            ctx.register_udf(ScalarUDF::new_from_shared_impl(Arc::new(foreign)));
        }
        for udaf in udf_bundle.aggregate.iter() {
            let foreign = ForeignAggregateUDF::try_from(udaf)?;
            ctx.register_udaf(AggregateUDF::new_from_shared_impl(Arc::new(foreign)));
        }
        for udwf in udf_bundle.window.iter() {
            let foreign = ForeignWindowUDF::try_from(udwf)?;
            ctx.register_udwf(WindowUDF::new_from_shared_impl(Arc::new(foreign)));
        }
        Ok(())
    }

    pub fn register_table_functions(&self, ctx: &SessionContext) -> Result<()> {
        let exports = (self.module().exports())();
        if !exports.table_functions.is_empty() {
            self.require_capability(caps::TABLE_FUNCTION, "table_function")?;
        }
        for table_fn in exports.table_functions.iter() {
            let name = table_fn.name.to_string();
            let foreign = ForeignTableFunction::from(table_fn.function.clone());
            ctx.register_udtf(name.as_str(), Arc::new(foreign));
        }
        Ok(())
    }

    pub fn register_table_providers(
        &self,
        ctx: &SessionContext,
        table_names: Option<&[String]>,
        options_json: Option<&std::collections::HashMap<String, String>>,
    ) -> Result<()> {
        Self::install_delta_plan_codecs(ctx);
        let exports = (self.module().exports())();
        if !exports.table_provider_names.is_empty() {
            self.require_capability(caps::TABLE_PROVIDER, "table_provider")?;
        }
        let requested = table_names.map(|names| names.iter().cloned().collect::<HashSet<_>>());
        let mut found = HashSet::new();
        for name in exports.table_provider_names.iter() {
            let name_str = name.to_string();
            if let Some(requested) = &requested {
                if !requested.contains(&name_str) {
                    continue;
                }
                found.insert(name_str.clone());
            }
            let options = options_json.and_then(|map| map.get(&name_str));
            let options = match options {
                Some(value) => ROption::RSome(RString::from(value.as_str())),
                None => ROption::RNone,
            };
            let create_table_provider = self.module().create_table_provider();
            let provider = create_table_provider(RStr::from_str(name_str.as_str()), options);
            let provider = df_result_to_result(provider)?;
            let foreign = ForeignTableProvider::from(&provider);
            ctx.register_table(name_str.as_str(), Arc::new(foreign))?;
        }
        if let Some(requested) = requested {
            let missing: Vec<String> = requested.difference(&found).cloned().collect();
            if !missing.is_empty() {
                return Err(DataFusionError::Plan(format!(
                    "Plugin did not export requested table providers: {}",
                    missing.join(", ")
                )));
            }
        }
        Ok(())
    }

    pub fn create_table_provider(
        &self,
        name: &str,
        options_json: Option<&str>,
    ) -> Result<FFI_TableProvider> {
        let exports = (self.module().exports())();
        self.require_capability(caps::TABLE_PROVIDER, "table_provider")?;
        let available = exports
            .table_provider_names
            .iter()
            .any(|entry| entry.to_string() == name);
        if !available {
            return Err(DataFusionError::Plan(format!(
                "Plugin did not export requested table provider: {name}"
            )));
        }
        let options = match options_json {
            Some(value) => ROption::RSome(RString::from(value)),
            None => ROption::RNone,
        };
        let create_table_provider = self.module().create_table_provider();
        let provider = create_table_provider(RStr::from_str(name), options);
        df_result_to_result(provider)
    }
}

fn df_result_to_result<T>(result: DfResult<T>) -> Result<T> {
    match result {
        RResult::ROk(value) => Ok(value),
        RResult::RErr(err) => Err(DataFusionError::Plan(err.to_string())),
    }
}
