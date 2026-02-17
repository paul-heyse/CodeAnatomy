use datafusion::config::ConfigOptions;
use datafusion_ext::async_udf_config::CodeAnatomyAsyncUdfConfig;

#[test]
fn async_udf_config_defaults() {
    let config = CodeAnatomyAsyncUdfConfig::default();
    assert_eq!(config.ideal_batch_size, None);
    assert_eq!(config.timeout_ms, None);
}

#[test]
fn async_udf_config_from_config_extensions() {
    let mut options = ConfigOptions::new();
    options.extensions.insert(CodeAnatomyAsyncUdfConfig {
        ideal_batch_size: Some(128),
        timeout_ms: Some(1_000),
    });
    let config = CodeAnatomyAsyncUdfConfig::from_config(&options);
    assert_eq!(config.ideal_batch_size, Some(128));
    assert_eq!(config.timeout_ms, Some(1_000));
}
