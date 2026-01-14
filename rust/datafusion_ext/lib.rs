//! Optional DataFusion extension hooks.
//!
//! This module is a placeholder for Rust-backed extensions such as:
//! - RuntimeEnv cache managers
//! - Streaming table providers
//! - FunctionFactory registration
//! - Tracing/metrics export
//!
//! Each hook is gated behind a feature flag so builds can opt in explicitly.

#[cfg(feature = "cache_manager")]
pub fn install_cache_manager() -> bool {
    // Hook for RuntimeEnvBuilder::with_cache_manager once enabled.
    true
}

#[cfg(not(feature = "cache_manager"))]
pub fn install_cache_manager() -> bool {
    false
}

#[cfg(feature = "streaming")]
pub fn streaming_table_provider() -> bool {
    // Hook for StreamingTable registration once enabled.
    true
}

#[cfg(not(feature = "streaming"))]
pub fn streaming_table_provider() -> bool {
    false
}

#[cfg(feature = "function_factory")]
pub fn install_function_factory() -> bool {
    // Hook for FunctionFactory registration once enabled.
    true
}

#[cfg(not(feature = "function_factory"))]
pub fn install_function_factory() -> bool {
    false
}

#[cfg(feature = "tracing")]
pub fn enable_tracing() -> bool {
    // Hook for tracing export once enabled.
    true
}

#[cfg(not(feature = "tracing"))]
pub fn enable_tracing() -> bool {
    false
}

#[cfg(feature = "metrics")]
pub fn collect_metrics_json() -> Option<String> {
    // Hook for metrics export once enabled.
    None
}

#[cfg(not(feature = "metrics"))]
pub fn collect_metrics_json() -> Option<String> {
    None
}

#[cfg(feature = "tracing")]
pub fn collect_traces_json() -> Option<String> {
    // Hook for tracing export once enabled.
    None
}

#[cfg(not(feature = "tracing"))]
pub fn collect_traces_json() -> Option<String> {
    None
}
