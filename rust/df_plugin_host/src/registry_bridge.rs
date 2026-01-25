use std::collections::HashSet;
use std::sync::Arc;

use abi_stable::std_types::{ROption, RResult, RString, RStr};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionContext;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_ffi::table_provider::ForeignTableProvider;
use datafusion_ffi::udaf::ForeignAggregateUDF;
use datafusion_ffi::udf::ForeignScalarUDF;
use datafusion_ffi::udtf::ForeignTableFunction;
use datafusion_ffi::udwf::ForeignWindowUDF;

use df_plugin_api::DfResult;

use crate::loader::PluginHandle;

impl PluginHandle {
    pub fn register_udfs(&self, ctx: &SessionContext) -> Result<()> {
        let exports = (self.module().exports())();
        for udf in exports.udf_bundle.scalar.iter() {
            let foreign = ForeignScalarUDF::try_from(udf)?;
            ctx.register_udf(ScalarUDF::new_from_shared_impl(Arc::new(foreign)));
        }
        for udaf in exports.udf_bundle.aggregate.iter() {
            let foreign = ForeignAggregateUDF::try_from(udaf)?;
            ctx.register_udaf(AggregateUDF::new_from_shared_impl(Arc::new(foreign)));
        }
        for udwf in exports.udf_bundle.window.iter() {
            let foreign = ForeignWindowUDF::try_from(udwf)?;
            ctx.register_udwf(WindowUDF::new_from_shared_impl(Arc::new(foreign)));
        }
        Ok(())
    }

    pub fn register_table_functions(&self, ctx: &SessionContext) -> Result<()> {
        let exports = (self.module().exports())();
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
        let exports = (self.module().exports())();
        let requested = table_names.map(|names| {
            names
                .iter()
                .cloned()
                .collect::<HashSet<_>>()
        });
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
}

fn df_result_to_result<T>(result: DfResult<T>) -> Result<T> {
    match result {
        RResult::ROk(value) => Ok(value),
        RResult::RErr(err) => Err(DataFusionError::Plan(err.to_string())),
    }
}
