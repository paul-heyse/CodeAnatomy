//! Extraction-specific session construction.
//!
//! When extraction transitions to Rust, this module provides
//! extraction-specific session construction separate from the
//! execution session in `factory.rs`.
//!
//! **Status:** Future stub. Python `session/factory.py` is the
//! current authority for extraction sessions. This file documents
//! the target API.

use std::sync::Arc;

use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use datafusion_common::Result;

/// Configuration for extraction-specific sessions.
pub struct ExtractionConfig {
    /// Number of parallel threads for extraction.
    pub parallelism: usize,
    /// Optional memory limit in bytes. Defaults to 4GB.
    pub memory_limit_bytes: Option<usize>,
    /// Batch size for extraction (default: 8192).
    pub batch_size: usize,
}

impl Default for ExtractionConfig {
    fn default() -> Self {
        Self {
            parallelism: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            memory_limit_bytes: None,
            batch_size: 8192,
        }
    }
}

/// Build an extraction-specific DataFusion session.
///
/// Extraction sessions differ from execution sessions in:
/// - Default batch size (8192 vs execution default)
/// - Memory limits (4GB default vs execution-specific)
/// - UDF set (extraction-specific UDFs only)
/// - Provider registration (Delta providers for extraction inputs)
pub fn build_extraction_session(
    config: &ExtractionConfig,
) -> Result<SessionContext> {
    let session_config = SessionConfig::new()
        .with_target_partitions(config.parallelism)
        .with_batch_size(config.batch_size)
        .set_bool("datafusion.execution.collect_statistics", true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.enable_page_index", true);

    let memory_limit = config
        .memory_limit_bytes
        .unwrap_or(4 * 1024 * 1024 * 1024); // 4GB default

    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(memory_limit)))
        .build()?;

    let builder = SessionStateBuilder::new()
        .with_config(session_config)
        .with_runtime_env(Arc::new(runtime_env));

    // TODO: Register extraction-specific UDFs when extraction transitions.
    // TODO: Register Delta providers for extraction inputs.

    let state = builder.build();
    Ok(SessionContext::new_with_state(state))
}
