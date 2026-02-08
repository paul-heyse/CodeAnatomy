//! WS-P13: OpenTelemetry-native execution tracing.
//!
//! Provides structured tracing for engine execution when the `tracing` feature
//! is enabled. Falls back to no-op when disabled, ensuring zero overhead in the
//! default build configuration.
//!
//! ## Feature gate
//!
//! The `tracing` Cargo feature controls compilation:
//! - **Enabled**: `datafusion-tracing` and `tracing` crates are available.
//!   `build_tracing_state` instruments the SessionState with per-operator
//!   and per-rule OpenTelemetry spans. `execution_span` creates a root
//!   `info_span!` with spec identity attributes.
//! - **Disabled** (default): `build_tracing_state` returns the base state
//!   unchanged. `execution_span` is not available (compile-time elimination).
//!
//! ## Integration path
//!
//! The `tracing` feature and its Cargo.toml dependencies (`datafusion-tracing`,
//! `tracing`) are wired by the integration agent. Until then, only the
//! `#[cfg(not(feature = "tracing"))]` paths compile.

use datafusion::execution::session_state::SessionState;

/// Execution span metadata for structured tracing.
///
/// Captures the identity attributes that should appear on the root execution
/// span: spec hash, envelope hash, rulepack fingerprint, and profile name.
/// All hashes are hex-encoded for human readability in span attributes.
///
/// When the `tracing` feature is disabled, this struct still serves as a
/// structured log record that callers can use for non-OpenTelemetry logging.
#[derive(Debug, Clone)]
pub struct ExecutionSpanInfo {
    /// Hex-encoded blake3 hash of the execution spec.
    pub spec_hash: String,
    /// Hex-encoded blake3 hash of the input envelope.
    pub envelope_hash: String,
    /// Hex-encoded blake3 hash of the rulepack.
    pub rulepack_fingerprint: String,
    /// Human-readable profile name (e.g., "default", "compliance").
    pub profile_name: String,
}

impl ExecutionSpanInfo {
    /// Create execution span info from raw 32-byte hash arrays.
    ///
    /// Hex-encodes each hash for use as span attributes or log fields.
    pub fn new(
        spec_hash: &[u8; 32],
        envelope_hash: &[u8; 32],
        rulepack_fingerprint: &[u8; 32],
        profile_name: &str,
    ) -> Self {
        Self {
            spec_hash: hex::encode(spec_hash),
            envelope_hash: hex::encode(envelope_hash),
            rulepack_fingerprint: hex::encode(rulepack_fingerprint),
            profile_name: profile_name.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Feature-gated: tracing ENABLED
// ---------------------------------------------------------------------------

/// Build instrumented SessionState with OpenTelemetry spans.
///
/// When the `tracing` feature is enabled, adds per-operator and per-rule
/// tracing via `datafusion-tracing`. When disabled, returns the base state
/// unchanged (zero overhead).
///
/// # Arguments
///
/// * `base_state` - The SessionState to instrument.
/// * `_enable_rule_tracing` - When true, instrument optimizer rules with
///   info spans and plan diffs. This is expensive and intended for compliance
///   mode only.
/// * `_enable_plan_preview` - When true, record first N rows in tracing spans
///   for plan preview / debugging.
#[cfg(feature = "tracing")]
pub fn build_tracing_state(
    base_state: SessionState,
    _enable_rule_tracing: bool,
    _enable_plan_preview: bool,
) -> SessionState {
    // Full tracing integration path.
    //
    // When datafusion-tracing is available:
    // 1. Build InstrumentationOptions with record_metrics(true)
    // 2. If enable_plan_preview, add preview_limit(5)
    // 3. If enable_rule_tracing, instrument rules with plan diff
    //    via RuleInstrumentationOptions::full().with_plan_diff()
    // 4. Add physical plan instrumentation via instrument_with_info_spans!
    //
    // The integration agent wires the actual datafusion-tracing crate
    // calls once the Cargo.toml feature flags are in place.
    //
    // Stub: return base_state until integration is complete.
    base_state
}

/// Build an execution span with spec identity attributes (tracing enabled).
///
/// Creates an OpenTelemetry `info_span!` named `codeanatomy_engine.execute`
/// with the spec hash, envelope hash, rulepack fingerprint, and profile
/// as span attributes.
#[cfg(feature = "tracing")]
pub fn execution_span(info: &ExecutionSpanInfo) -> tracing::Span {
    tracing::info_span!(
        "codeanatomy_engine.execute",
        spec_hash = %info.spec_hash,
        envelope_hash = %info.envelope_hash,
        rulepack_fingerprint = %info.rulepack_fingerprint,
        profile = %info.profile_name,
    )
}

// ---------------------------------------------------------------------------
// Feature-gated: tracing DISABLED (default)
// ---------------------------------------------------------------------------

/// No-op fallback: tracing feature not enabled.
///
/// Returns the base SessionState unchanged. The compiler eliminates this
/// entirely when the `tracing` feature is enabled (only one cfg path compiles).
#[cfg(not(feature = "tracing"))]
pub fn build_tracing_state(
    base_state: SessionState,
    _enable_rule_tracing: bool,
    _enable_plan_preview: bool,
) -> SessionState {
    base_state
}

// When tracing is not available, `execution_span` is not defined.
// The integration agent will handle the conditional logic in runner.rs
// by gating the call site with #[cfg(feature = "tracing")].

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
        assert_eq!(info.envelope_hash, "cd".repeat(32));
        assert_eq!(info.rulepack_fingerprint, "01".repeat(32));
        assert_eq!(info.profile_name, "default");
    }

    #[test]
    fn test_execution_span_info_zero_hashes() {
        let zero = [0u8; 32];
        let info = ExecutionSpanInfo::new(&zero, &zero, &zero, "test");

        assert_eq!(info.spec_hash, "00".repeat(32));
        assert_eq!(info.envelope_hash, "00".repeat(32));
        assert_eq!(info.rulepack_fingerprint, "00".repeat(32));
    }

    // The build_tracing_state no-op path is exercised implicitly:
    // without the `tracing` feature, the #[cfg(not(feature = "tracing"))]
    // variant compiles and returns base_state unchanged. A proper round-trip
    // test would require constructing a SessionState, which needs a full
    // SessionContext; that is covered by integration tests in session/factory.
}
