//! Extraction-specific session construction.
//!
//! Provides extraction-specific session construction separate from the
//! execution session in `factory.rs`.

use std::sync::Arc;

use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::*;
use datafusion_common::Result;
use serde::{Deserialize, Serialize};

/// Configuration for extraction-specific sessions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractionConfig {
    /// Number of parallel threads for extraction.
    pub parallelism: usize,
    /// Optional memory limit in bytes. Defaults to 4GB.
    pub memory_limit_bytes: Option<usize>,
    /// Batch size for extraction (default: 8192).
    pub batch_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_extraction_session() {
        let config = ExtractionConfig {
            parallelism: 2,
            memory_limit_bytes: Some(256 * 1024 * 1024),
            batch_size: 2048,
        };
        let ctx = build_extraction_session(&config).expect("session");
        let settings = ctx.copied_config().options();
        let partitions = settings.execution.target_partitions;
        let batch_size = settings.execution.batch_size;
        assert_eq!(partitions, 2);
        assert_eq!(batch_size, 2048);
    }
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
pub fn build_extraction_session(config: &ExtractionConfig) -> Result<SessionContext> {
    let session_config = SessionConfig::new()
        .with_target_partitions(config.parallelism)
        .with_batch_size(config.batch_size)
        .set_bool("datafusion.execution.collect_statistics", true)
        .set_bool("datafusion.execution.parquet.pushdown_filters", true)
        .set_bool("datafusion.execution.parquet.enable_page_index", true);

    let memory_limit = config.memory_limit_bytes.unwrap_or(4 * 1024 * 1024 * 1024); // 4GB default

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
