//! OpenTelemetry bootstrap and global subscriber initialization.

use std::sync::OnceLock;
use std::time::Duration;

use datafusion_common::{DataFusionError, Result};
use opentelemetry::trace::TracerProvider;
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::{
    BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider,
};
use opentelemetry_sdk::Resource;
use tracing_subscriber::prelude::*;

use crate::spec::runtime::{OtlpProtocol, TracingConfig};

static TRACING_INITIALIZED: OnceLock<()> = OnceLock::new();
static TRACER_PROVIDER: OnceLock<SdkTracerProvider> = OnceLock::new();

pub fn init_otel_tracing(config: &TracingConfig) -> Result<()> {
    if !config.enabled {
        return Ok(());
    }
    if TRACING_INITIALIZED.get().is_some() {
        return Ok(());
    }

    let provider = build_tracer_provider(config)?;
    global::set_tracer_provider(provider.clone());
    let tracer = provider.tracer("codeanatomy_engine");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = tracing_subscriber::registry().with(otel_layer);

    if let Err(error) = tracing::subscriber::set_global_default(subscriber) {
        tracing::debug!(error = %error, "Tracing subscriber already initialized");
    }

    let _ = TRACER_PROVIDER.set(provider);
    let _ = TRACING_INITIALIZED.set(());
    Ok(())
}

pub fn flush_otel_tracing() -> Result<()> {
    let Some(provider) = TRACER_PROVIDER.get() else {
        return Ok(());
    };
    provider.force_flush().map_err(|error| {
        DataFusionError::Execution(format!(
            "Failed to flush OpenTelemetry tracing provider: {error}"
        ))
    })?;
    Ok(())
}

fn build_tracer_provider(config: &TracingConfig) -> Result<SdkTracerProvider> {
    let endpoint = config
        .otlp_endpoint
        .clone()
        .unwrap_or_else(|| "http://127.0.0.1:4318/v1/traces".to_string());
    let protocol = config.otlp_protocol.unwrap_or(OtlpProtocol::HttpProtobuf);

    let exporter = match protocol {
        OtlpProtocol::Grpc => SpanExporter::builder()
            .with_tonic()
            .with_endpoint(endpoint.clone())
            .with_timeout(Duration::from_millis(
                config.export_policy.bsp_export_timeout_ms,
            ))
            .build()
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "Failed to build OTLP gRPC span exporter: {error}"
                ))
            })?,
        OtlpProtocol::HttpJson => SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpJson)
            .with_endpoint(endpoint.clone())
            .with_timeout(Duration::from_millis(
                config.export_policy.bsp_export_timeout_ms,
            ))
            .build()
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "Failed to build OTLP HTTP/JSON span exporter: {error}"
                ))
            })?,
        OtlpProtocol::HttpProtobuf => SpanExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpBinary)
            .with_endpoint(endpoint)
            .with_timeout(Duration::from_millis(
                config.export_policy.bsp_export_timeout_ms,
            ))
            .build()
            .map_err(|error| {
                DataFusionError::Plan(format!(
                    "Failed to build OTLP HTTP/Protobuf span exporter: {error}"
                ))
            })?,
    };

    let batch_config = BatchConfigBuilder::default()
        .with_max_queue_size(config.export_policy.bsp_max_queue_size as usize)
        .with_max_export_batch_size(config.export_policy.bsp_max_export_batch_size as usize)
        .with_scheduled_delay(Duration::from_millis(
            config.export_policy.bsp_schedule_delay_ms,
        ))
        .build();

    let batch_processor = BatchSpanProcessor::builder(exporter)
        .with_batch_config(batch_config)
        .build();

    let mut resource_attributes = Vec::new();
    resource_attributes.push(KeyValue::new(
        "service.name",
        config
            .otel_service_name
            .clone()
            .unwrap_or_else(|| "codeanatomy_engine".to_string()),
    ));
    for (key, value) in &config.otel_resource_attributes {
        resource_attributes.push(KeyValue::new(key.clone(), value.clone()));
    }
    let resource = Resource::builder_empty()
        .with_attributes(resource_attributes)
        .build();

    let sampler = parse_sampler(
        &config.export_policy.traces_sampler,
        config.export_policy.traces_sampler_arg.as_deref(),
    );
    Ok(SdkTracerProvider::builder()
        .with_resource(resource)
        .with_sampler(sampler)
        .with_span_processor(batch_processor)
        .build())
}

fn parse_sampler(name: &str, arg: Option<&str>) -> Sampler {
    let normalized = name.trim().to_ascii_lowercase();
    let ratio = arg
        .and_then(|value| value.parse::<f64>().ok())
        .map(|value| value.clamp(0.0, 1.0))
        .unwrap_or(1.0);
    match normalized.as_str() {
        "always_on" => Sampler::AlwaysOn,
        "always_off" => Sampler::AlwaysOff,
        "traceidratio" => Sampler::TraceIdRatioBased(ratio),
        "parentbased_always_off" => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
        "parentbased_traceidratio" => {
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)))
        }
        _ => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_for(protocol: OtlpProtocol) -> TracingConfig {
        let endpoint = match protocol {
            OtlpProtocol::Grpc => "http://127.0.0.1:4317",
            OtlpProtocol::HttpJson | OtlpProtocol::HttpProtobuf => {
                "http://127.0.0.1:4318/v1/traces"
            }
        };
        TracingConfig {
            enabled: true,
            otlp_endpoint: Some(endpoint.to_string()),
            otlp_protocol: Some(protocol),
            ..TracingConfig::default()
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_build_tracer_provider_supports_all_protocols() {
        let protocols = [
            OtlpProtocol::Grpc,
            OtlpProtocol::HttpProtobuf,
            OtlpProtocol::HttpJson,
        ];
        for protocol in protocols {
            let cfg = config_for(protocol);
            let provider = build_tracer_provider(&cfg)
                .expect("tracer provider should build for supported protocol");
            drop(provider);
        }
    }

    #[test]
    fn test_parse_sampler_traceidratio_is_clamped() {
        let sampler = parse_sampler("traceidratio", Some("2.5"));
        assert!(matches!(sampler, Sampler::TraceIdRatioBased(value) if (value - 1.0).abs() < f64::EPSILON));

        let sampler = parse_sampler("traceidratio", Some("-1.0"));
        assert!(matches!(sampler, Sampler::TraceIdRatioBased(value) if (value - 0.0).abs() < f64::EPSILON));
    }
}
