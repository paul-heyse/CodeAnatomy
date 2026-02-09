//! Shared tracing context structures.

use crate::spec::runtime::TracingConfig;

/// Identity fields for root execution span correlation.
#[derive(Debug, Clone)]
pub struct ExecutionSpanInfo {
    pub spec_hash: String,
    pub envelope_hash: Option<String>,
    pub rulepack_fingerprint: String,
    pub profile_name: String,
}

impl ExecutionSpanInfo {
    /// Build span info when envelope hash is available.
    pub fn new(
        spec_hash: &[u8; 32],
        envelope_hash: &[u8; 32],
        rulepack_fingerprint: &[u8; 32],
        profile_name: &str,
    ) -> Self {
        Self {
            spec_hash: hex::encode(spec_hash),
            envelope_hash: Some(hex::encode(envelope_hash)),
            rulepack_fingerprint: hex::encode(rulepack_fingerprint),
            profile_name: profile_name.to_string(),
        }
    }

    /// Build span info when envelope hash is not yet known.
    pub fn without_envelope(
        spec_hash: &[u8; 32],
        rulepack_fingerprint: &[u8; 32],
        profile_name: &str,
    ) -> Self {
        Self {
            spec_hash: hex::encode(spec_hash),
            envelope_hash: None,
            rulepack_fingerprint: hex::encode(rulepack_fingerprint),
            profile_name: profile_name.to_string(),
        }
    }
}

/// Rule instrumentation correlation fields.
#[derive(Debug, Clone)]
pub struct TraceRuleContext {
    pub spec_hash: String,
    pub rulepack_fingerprint: String,
    pub profile_name: String,
    pub custom_fields_json: String,
}

impl TraceRuleContext {
    pub fn from_hashes(
        spec_hash: &[u8; 32],
        rulepack_fingerprint: &[u8; 32],
        profile_name: &str,
        config: &TracingConfig,
    ) -> Self {
        let custom_fields_json =
            serde_json::to_string(&config.custom_span_fields).unwrap_or_else(|_| "{}".to_string());
        Self {
            spec_hash: hex::encode(spec_hash),
            rulepack_fingerprint: hex::encode(rulepack_fingerprint),
            profile_name: profile_name.to_string(),
            custom_fields_json,
        }
    }
}

#[cfg(feature = "tracing")]
pub fn execution_span(info: &ExecutionSpanInfo, config: &TracingConfig) -> tracing::Span {
    if !config.enabled {
        return tracing::Span::none();
    }

    let rule_mode = format!("{:?}", config.rule_mode);
    let custom_fields_json =
        serde_json::to_string(&config.custom_span_fields).unwrap_or_else(|_| "{}".to_string());
    let span = tracing::info_span!(
        "codeanatomy_engine.execute",
        otel.name = "codeanatomy_engine.execute",
        spec_hash = tracing::field::Empty,
        envelope_hash = tracing::field::Empty,
        rulepack_fingerprint = tracing::field::Empty,
        profile = tracing::field::Empty,
        tracing_rule_mode = tracing::field::Empty,
        tracing_plan_diff = tracing::field::Empty,
        tracing_preview_limit = tracing::field::Empty,
        trace_custom_fields_json = tracing::field::Empty,
        warning_count_total = tracing::field::Empty,
        warning_counts_by_code_json = tracing::field::Empty,
    );

    span.record("spec_hash", info.spec_hash.as_str());
    if let Some(envelope_hash) = info.envelope_hash.as_ref() {
        span.record("envelope_hash", envelope_hash.as_str());
    }
    span.record(
        "rulepack_fingerprint",
        info.rulepack_fingerprint.as_str(),
    );
    span.record("profile", info.profile_name.as_str());
    span.record("tracing_rule_mode", rule_mode.as_str());
    span.record("tracing_plan_diff", config.plan_diff);
    span.record("tracing_preview_limit", config.preview_limit);
    span.record("trace_custom_fields_json", custom_fields_json.as_str());
    span
}

#[cfg(feature = "tracing")]
pub fn record_envelope_hash(span: &tracing::Span, envelope_hash: &[u8; 32]) {
    let encoded = hex::encode(envelope_hash);
    span.record("envelope_hash", encoded.as_str());
}

#[cfg(feature = "tracing")]
pub fn record_warning_summary(
    span: &tracing::Span,
    warning_count_total: u64,
    warning_counts_by_code: &std::collections::BTreeMap<String, u64>,
) {
    span.record("warning_count_total", warning_count_total);
    let serialized = serde_json::to_string(warning_counts_by_code).unwrap_or_else(|_| "{}".into());
    span.record("warning_counts_by_code_json", serialized.as_str());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_span_info_hex_encoding() {
        let spec_hash = [0xABu8; 32];
        let envelope_hash = [0xCDu8; 32];
        let rulepack_fp = [0x01u8; 32];

        let info = ExecutionSpanInfo::new(&spec_hash, &envelope_hash, &rulepack_fp, "default");

        assert_eq!(info.spec_hash, "ab".repeat(32));
        assert_eq!(info.envelope_hash, Some("cd".repeat(32)));
        assert_eq!(info.rulepack_fingerprint, "01".repeat(32));
        assert_eq!(info.profile_name, "default");
    }

    #[test]
    fn test_trace_rule_context_serializes_custom_fields() {
        let mut config = TracingConfig::default();
        config
            .custom_span_fields
            .insert("policy".to_string(), "strict".to_string());

        let context = TraceRuleContext::from_hashes(&[1u8; 32], &[2u8; 32], "small", &config);
        assert!(context.custom_fields_json.contains("policy"));
        assert!(context.custom_fields_json.contains("strict"));
    }
}
