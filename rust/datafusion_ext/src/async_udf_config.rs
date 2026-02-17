use datafusion::config::ConfigOptions;
use datafusion_common::config::ConfigExtension;

const PREFIX: &str = "codeanatomy_async_udf";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct CodeAnatomyAsyncUdfConfig {
    pub ideal_batch_size: Option<usize>,
    pub timeout_ms: Option<u64>,
}

impl CodeAnatomyAsyncUdfConfig {
    pub fn from_config(config: &ConfigOptions) -> Self {
        config
            .extensions
            .get::<CodeAnatomyAsyncUdfConfig>()
            .cloned()
            .unwrap_or_default()
    }
}

crate::impl_extension_options!(
    CodeAnatomyAsyncUdfConfig,
    prefix = PREFIX,
    unknown_key = "Unknown async UDF config key: {key}",
    fields = [
        (
            ideal_batch_size,
            Option<usize>,
            "Preferred batch size for async UDF execution."
        ),
        (
            timeout_ms,
            Option<u64>,
            "Async UDF timeout in milliseconds."
        ),
    ]
);

impl ConfigExtension for CodeAnatomyAsyncUdfConfig {
    const PREFIX: &'static str = PREFIX;
}

